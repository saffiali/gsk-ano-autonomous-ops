"""R4 — rolling, seasonal, per-entity learned baselines.

Requirement R4 asks for "rolling multi-month (3-month) learned baselines that
adapt to per-entity, per-hour and per-day-of-week seasonality", replacing static
thresholds. This module is that replacement.

The estimation problem
----------------------
Split a quarter of history per entity and per metric into 7 x 24 = 168
hour-of-week cells and you have a classic sparse-cell problem. A cell holds
roughly thirteen observations per scrape-interval-per-hour, which is plenty at
3 months — and nothing at all in the first week, or on a replay corpus that is
only a few days long. Two things follow:

**Robustness.** Cells are summarised by median and median absolute deviation,
not mean and standard deviation. The history a live system learns from
*contains* the incidents it must predict, and requirement R5 forbids the
detector from ever reading a label that would let it exclude them. A median
tolerates up to half the cell being corrupt; a mean does not tolerate one
outlier.

**Shrinkage.** Each cell is blended with its parents up a hierarchy —
``(entity, feature, day-of-week, hour)`` -> ``(entity, feature, hour)`` ->
``(entity, feature)`` — with a weight that grows as the cell fills:

    w = n / (n + k)        est = w * cell + (1 - w) * parent

This is the standard empirical-Bayes shrinkage estimator. With no data the
estimate is the entity's overall behaviour; as the cell fills it becomes the
cell's own behaviour; in between it is a defensible compromise. The practical
effect is that seasonality is *learned when it is there and ignored when it is
not*, with no switch for anyone to set wrongly.

Causality
---------
:meth:`SeasonalBaselineStore.score` reads the model as it stands, and
:meth:`SeasonalBaselineStore.observe` then folds the observation in. Callers
that keep that order — :mod:`ano.detect.unresponsiveness` does — can never
score a point against a baseline that already contains it. There is no
lookahead anywhere in this file.

What "rolling" means here
-------------------------
Observations older than :data:`DEFAULT_RETENTION_S` (3 months) are evicted, and
each cell additionally keeps at most :data:`DEFAULT_CELL_CAPACITY` of its most
recent observations so memory stays bounded on a long run. Both are FIFO on
observation time, so the window genuinely rolls rather than freezing after a
warm-up.
"""

from __future__ import annotations

import bisect
import datetime as _dt
import math
from collections import deque
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass

from ano.detect.robust import MAD_TO_SIGMA, quantile, robust_z

__all__ = [
    "DEFAULT_RETENTION_S",
    "DEFAULT_CELL_CAPACITY",
    "DEFAULT_SHRINKAGE_K",
    "DEFAULT_MIN_SAMPLES",
    "SEASONAL_PERIOD_HOURS",
    "BaselineEstimate",
    "SeasonalCell",
    "SeasonalBaselineStore",
    "hour_of_day",
    "day_of_week",
]

#: Rolling window length. Three months, as requirement R4 specifies, expressed
#: in seconds: 91 days.
DEFAULT_RETENTION_S: float = 91 * 24 * 3600.0

#: Most recent observations retained per seasonal cell. At a 30 s scrape
#: interval a cell accrues ~120 observations per occurrence and occurs 13 times
#: in the window, so 512 keeps roughly the most recent third of the quarter per
#: cell while bounding memory at 168 cells x 512 floats per (entity, feature).
DEFAULT_CELL_CAPACITY: int = 512

#: Shrinkage strength. A cell reaches half weight against its parent at this
#: many observations. Twenty is a few scrape intervals' worth, so a cell earns
#: its independence quickly once it is genuinely populated but never dominates
#: on a single sample.
DEFAULT_SHRINKAGE_K: float = 20.0

#: Below this many observations anywhere in the hierarchy the baseline reports
#: itself as not ready and the detector abstains, rather than scoring against
#: an estimate built from three points. This is what makes the warm-up period
#: quiet instead of noisy.
DEFAULT_MIN_SAMPLES: int = 12

#: The seasonal period: one week, in hours. 7 x 24 = 168 cells per series.
SEASONAL_PERIOD_HOURS: int = 168


def hour_of_day(moment: float) -> int:
    """Hour of day in UTC for an epoch-seconds instant.

    Args:
        moment: Epoch seconds.

    Returns:
        ``0..23``.
    """
    return int(moment // 3600) % 24


def day_of_week(moment: float) -> int:
    """Day of week in UTC for an epoch-seconds instant.

    Args:
        moment: Epoch seconds.

    Returns:
        ``0`` for Monday through ``6`` for Sunday, matching
        :meth:`datetime.date.weekday`.
    """
    return int((moment // 86400) + 3) % 7


@dataclass(frozen=True, slots=True)
class BaselineEstimate:
    """What the baseline believes about one feature on one entity, at one time.

    Attributes:
        entity: Entity the estimate is for.
        feature: Feature name.
        center: Learned centre (a shrunken median).
        sigma: Learned scale (a shrunken MAD, rescaled to a sigma-equivalent).
        z: Robust z-score of the observation against ``center``/``sigma``.
        ready: Whether enough history exists to be believed. When ``False`` the
            detector must abstain on this feature.
        sample_count: Observations in the exact seasonal cell.
        support_count: Observations across the whole hierarchy for this series.
        cell_weight: Weight the exact cell received against its parents, in
            ``[0, 1]``. Zero means the estimate is entirely the entity-level
            fallback; one means the cell stood alone.
        hour: Hour of day the estimate was read at.
        weekday: Day of week the estimate was read at.
        ceiling: Learned upper extreme for the series, used as the failure
            level for unbounded features. ``None`` when unavailable.
    """

    entity: str
    feature: str
    center: float
    sigma: float
    z: float
    ready: bool
    sample_count: int
    support_count: int
    cell_weight: float
    hour: int
    weekday: int
    ceiling: float | None = None

    def deviation_ratio(self) -> float:
        """Observed value as a multiple of the centre, reconstructed from ``z``.

        Returns:
            ``value / center``, or ``1.0`` when the centre is zero.
        """
        if self.center == 0.0:
            return 1.0
        return (self.center + self.z * self.sigma) / self.center

    def observed(self) -> float:
        """The observation this estimate scored."""
        return self.center + self.z * self.sigma


class SeasonalCell:
    """One ``(day-of-week, hour)`` cell of one entity-feature series.

    Holds the most recent observations within the rolling window and summarises
    them on demand. Summaries are cached and invalidated on mutation, because
    the detector reads every cell far more often than it writes one.
    """

    __slots__ = (
        "_points",
        "_sorted",
        "_capacity",
        "_median",
        "_mad",
        "_ceiling",
        "_dirty",
    )

    def __init__(self, capacity: int = DEFAULT_CELL_CAPACITY) -> None:
        """Create an empty cell.

        Args:
            capacity: Maximum retained observations.

        Raises:
            ValueError: If ``capacity`` is not positive.
        """
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._points: deque[tuple[float, float]] = deque()
        self._sorted: list[float] = []
        self._capacity = capacity
        self._median = 0.0
        self._mad = 0.0
        self._ceiling: float | None = None
        self._dirty = True

    def __len__(self) -> int:
        return len(self._points)

    def add(self, moment: float, value: float) -> None:
        """Record an observation.

        Args:
            moment: Epoch seconds.
            value: The observed value.
        """
        self._points.append((moment, value))
        if len(self._points) > self._capacity:
            _, old = self._points.popleft()
            idx = bisect.bisect_left(self._sorted, old)
            del self._sorted[idx]
        bisect.insort(self._sorted, value)
        self._dirty = True

    def evict_before(self, cutoff: float) -> int:
        """Drop observations older than ``cutoff``.

        Args:
            cutoff: Epoch seconds; observations at or before it are dropped.

        Returns:
            How many observations were dropped.
        """
        dropped = 0
        while self._points and self._points[0][0] < cutoff:
            _, old = self._points.popleft()
            idx = bisect.bisect_left(self._sorted, old)
            del self._sorted[idx]
            dropped += 1
        if dropped:
            self._dirty = True
        return dropped

    def values(self) -> tuple[float, ...]:
        """Retained observations, oldest first."""
        return tuple(value for _, value in self._points)

    def _refresh(self) -> None:
        """Recompute the cached median, MAD, and ceiling."""
        count = len(self._sorted)
        if count == 0:
            self._median = 0.0
            self._mad = 0.0
            self._ceiling = None
        else:
            middle = count // 2
            self._median = (
                self._sorted[middle]
                if count % 2
                else (self._sorted[middle - 1] + self._sorted[middle]) / 2.0
            )
            deviations = sorted(abs(value - self._median) for value in self._sorted)
            self._mad = (
                deviations[middle]
                if count % 2
                else (deviations[middle - 1] + deviations[middle]) / 2.0
            )
            pos = 0.995 * (count - 1)
            lower = math.floor(pos)
            upper = math.ceil(pos)
            if lower == upper:
                self._ceiling = self._sorted[int(pos)]
            else:
                weight = pos - lower
                self._ceiling = (
                    self._sorted[lower] * (1.0 - weight) + self._sorted[upper] * weight
                )
        self._dirty = False

    @property
    def median(self) -> float:
        """Median of the retained observations; ``0.0`` when empty."""
        if self._dirty:
            self._refresh()
        return self._median

    @property
    def mad(self) -> float:
        """Median absolute deviation of the retained observations."""
        if self._dirty:
            self._refresh()
        return self._mad

    @property
    def ceiling_value(self) -> float | None:
        """Learned ceiling (0.995 quantile) of the retained observations."""
        if self._dirty:
            self._refresh()
        return self._ceiling


class SeasonalBaselineStore:
    """Rolling per-entity, per-hour-of-day, per-day-of-week baselines.

    Key methods are :meth:`observe` (learn) and :meth:`score` (read). Keeping
    them separate is what lets the caller enforce causality; see the module
    docstring.

    Args:
        retention_s: Rolling window length in seconds. Defaults to 3 months.
        cell_capacity: Maximum observations retained per seasonal cell.
        shrinkage_k: Half-weight point for cell-versus-parent shrinkage.
        min_samples: Hierarchy-wide observations required before an estimate
            reports ``ready``.

    Raises:
        ValueError: On a non-positive retention, capacity or shrinkage.
    """

    __slots__ = (
        "_retention_s",
        "_cell_capacity",
        "_shrinkage_k",
        "_min_samples",
        "_cells",
        "_hourly",
        "_global",
        "_latest",
        "_series",
    )

    def __init__(
        self,
        *,
        retention_s: float = DEFAULT_RETENTION_S,
        cell_capacity: int = DEFAULT_CELL_CAPACITY,
        shrinkage_k: float = DEFAULT_SHRINKAGE_K,
        min_samples: int = DEFAULT_MIN_SAMPLES,
    ) -> None:
        if retention_s <= 0.0:
            raise ValueError("retention_s must be positive")
        if cell_capacity <= 0:
            raise ValueError("cell_capacity must be positive")
        if shrinkage_k <= 0.0:
            raise ValueError("shrinkage_k must be positive")
        self._retention_s = retention_s
        self._cell_capacity = cell_capacity
        self._shrinkage_k = shrinkage_k
        self._min_samples = max(1, int(min_samples))
        # (entity, feature, weekday, hour) -> cell
        self._cells: dict[tuple[str, str, int, int], SeasonalCell] = {}
        # (entity, feature, hour) -> cell  : the day-of-week marginal
        self._hourly: dict[tuple[str, str, int], SeasonalCell] = {}
        # (entity, feature) -> cell        : the entity's overall behaviour
        self._global: dict[tuple[str, str], SeasonalCell] = {}
        self._latest: float = -math.inf
        self._series: set[tuple[str, str]] = set()

    # -- learning ---------------------------------------------------------

    def observe(self, entity: str, feature: str, moment: float, value: float) -> None:
        """Fold one observation into the model.

        Non-finite values are ignored: a feature computed from a zero
        denominator must not be allowed to define what normal looks like.

        Args:
            entity: Entity id.
            feature: Feature name.
            moment: Epoch seconds.
            value: Observed value.
        """
        if not math.isfinite(value):
            return
        weekday = day_of_week(moment)
        hour = hour_of_day(moment)
        self._cell(self._cells, (entity, feature, weekday, hour)).add(moment, value)
        self._cell(self._hourly, (entity, feature, hour)).add(moment, value)
        self._cell(self._global, (entity, feature)).add(moment, value)
        self._series.add((entity, feature))
        if moment > self._latest:
            self._latest = moment

    def observe_frame_step(
        self,
        entity: str,
        moment: float,
        values: Mapping[str, float | None],
    ) -> None:
        """Fold a whole time step's features for one entity.

        Args:
            entity: Entity id.
            moment: Epoch seconds.
            values: Feature name -> value, ``None`` meaning not computable.
        """
        for feature, value in values.items():
            if value is not None:
                self.observe(entity, feature, moment, value)

    def _cell(self, table: dict, key: tuple) -> SeasonalCell:
        """Fetch or create a cell in ``table``."""
        found = table.get(key)
        if found is None:
            found = SeasonalCell(self._cell_capacity)
            table[key] = found
        return found

    def evict(self, now: float | None = None) -> int:
        """Drop observations that have rolled out of the window.

        Called opportunistically rather than on every write, because eviction
        walks every cell and the window is three months long.

        Args:
            now: Current time in epoch seconds. Defaults to the latest
                observation seen.

        Returns:
            How many observations were dropped.
        """
        moment = self._latest if now is None else now
        if not math.isfinite(moment):
            return 0
        cutoff = moment - self._retention_s
        dropped = 0
        for table in (self._cells, self._hourly, self._global):
            for cell in table.values():
                dropped += cell.evict_before(cutoff)
        return dropped

    # -- reading ----------------------------------------------------------

    def series(self) -> tuple[tuple[str, str], ...]:
        """Every ``(entity, feature)`` the store has seen, sorted."""
        return tuple(sorted(self._series))

    def support(self, entity: str, feature: str) -> int:
        """Total observations retained for a series."""
        cell = self._global.get((entity, feature))
        return 0 if cell is None else len(cell)

    def estimate(
        self, entity: str, feature: str, moment: float
    ) -> tuple[float, float, int, int, float]:
        """Shrunken centre and scale for a series at a seasonal position.

        Args:
            entity: Entity id.
            feature: Feature name.
            moment: Epoch seconds, which selects the seasonal cell.

        Returns:
            ``(center, sigma, cell_count, support_count, cell_weight)``.
        """
        weekday = day_of_week(moment)
        hour = hour_of_day(moment)

        overall = self._global.get((entity, feature))
        support = 0 if overall is None else len(overall)
        if support == 0:
            return 0.0, 0.0, 0, 0, 0.0

        base_center = overall.median
        base_mad = overall.mad

        hourly = self._hourly.get((entity, feature, hour))
        hour_count = 0 if hourly is None else len(hourly)
        hour_weight = hour_count / (hour_count + self._shrinkage_k)
        hour_center = (
            hour_weight * hourly.median + (1.0 - hour_weight) * base_center
            if hourly is not None
            else base_center
        )
        hour_mad = (
            hour_weight * hourly.mad + (1.0 - hour_weight) * base_mad
            if hourly is not None
            else base_mad
        )

        exact = self._cells.get((entity, feature, weekday, hour))
        cell_count = 0 if exact is None else len(exact)
        cell_weight = cell_count / (cell_count + self._shrinkage_k)
        center = (
            cell_weight * exact.median + (1.0 - cell_weight) * hour_center
            if exact is not None
            else hour_center
        )
        mad = (
            cell_weight * exact.mad + (1.0 - cell_weight) * hour_mad
            if exact is not None
            else hour_mad
        )

        return center, MAD_TO_SIGMA * mad, cell_count, support, cell_weight

    def ceiling(self, entity: str, feature: str, *, q: float = 0.995) -> float | None:
        """The series' learned upper extreme.

        For features with no physical limit this stands in for "as bad as this
        entity has ever been", giving the trajectory extrapolator something
        real to aim at instead of a number chosen by hand. Requirement R4
        removes static thresholds from the detection path, so the failure level
        has to be learned like everything else.

        Args:
            entity: Entity id.
            feature: Feature name.
            q: Quantile taken as the extreme.

        Returns:
            The quantile, or ``None`` when the series has too little history.
        """
        overall = self._global.get((entity, feature))
        if overall is None or len(overall) < self._min_samples:
            return None
        if q == 0.995:
            return overall.ceiling_value
        return quantile(overall._sorted, q)

    def score(
        self,
        entity: str,
        feature: str,
        moment: float,
        value: float,
        *,
        absolute_floor: float = 0.0,
        relative_floor: float = 0.0,
    ) -> BaselineEstimate:
        """Score an observation against the model **as it currently stands**.

        Does not learn. Call :meth:`observe` afterwards to fold the point in;
        that ordering is what keeps scoring causal.

        Args:
            entity: Entity id.
            feature: Feature name.
            moment: Epoch seconds.
            value: The observation.
            absolute_floor: Minimum scale in the feature's own units.
            relative_floor: Minimum scale as a fraction of ``|center|``.

        Returns:
            The estimate, with ``ready`` false when history is insufficient.
        """
        center, sigma, cell_count, support, cell_weight = self.estimate(
            entity, feature, moment
        )
        ready = support >= self._min_samples
        z = 0.0
        if ready:
            z = robust_z(
                value,
                center,
                sigma,
                absolute_floor=absolute_floor,
                relative_floor=relative_floor,
            )
            if not math.isfinite(z):
                z = 0.0
        effective_sigma = max(sigma, absolute_floor, relative_floor * abs(center))
        overall = self._global.get((entity, feature))
        ceiling_val = (
            overall.ceiling_value
            if (overall is not None and support >= self._min_samples)
            else None
        )
        return BaselineEstimate(
            entity=entity,
            feature=feature,
            center=center,
            sigma=effective_sigma,
            z=z,
            ready=ready,
            sample_count=cell_count,
            support_count=support,
            cell_weight=cell_weight,
            hour=hour_of_day(moment),
            weekday=day_of_week(moment),
            ceiling=ceiling_val,
        )

    # -- inspection -------------------------------------------------------

    def hour_profile(self, entity: str, feature: str) -> dict[int, float]:
        """Learned hour-of-day profile: hour -> median.

        The inspectable dump feature #71 requires. Only populated hours appear.

        Args:
            entity: Entity id.
            feature: Feature name.

        Returns:
            Hour -> learned median.
        """
        return {
            hour: cell.median
            for (owner, name, hour), cell in sorted(self._hourly.items())
            if owner == entity and name == feature and len(cell)
        }

    def weekday_profile(self, entity: str, feature: str) -> dict[int, float]:
        """Learned day-of-week profile: weekday -> median over that day's cells.

        The inspectable dump feature #72 requires. Each day's median is taken
        over all of that day's observations, pooled across hours, so a
        weekday/weekend difference shows up directly.

        Args:
            entity: Entity id.
            feature: Feature name.

        Returns:
            Weekday (0=Monday) -> learned median.
        """
        pooled: dict[int, list[float]] = {}
        for (owner, name, weekday, _hour), cell in self._cells.items():
            if owner == entity and name == feature and len(cell):
                pooled.setdefault(weekday, []).extend(cell.values())
        profile: dict[int, float] = {}
        for weekday, values in sorted(pooled.items()):
            ordered = sorted(values)
            middle = len(ordered) // 2
            profile[weekday] = (
                ordered[middle]
                if len(ordered) % 2
                else (ordered[middle - 1] + ordered[middle]) / 2.0
            )
        return profile

    def seasonal_profile(self, entity: str, feature: str) -> dict[tuple[int, int], float]:
        """Full ``(weekday, hour) -> median`` grid for a series."""
        return {
            (weekday, hour): cell.median
            for (owner, name, weekday, hour), cell in sorted(self._cells.items())
            if owner == entity and name == feature and len(cell)
        }

    def describe(self) -> dict[str, object]:
        """A machine-readable summary of the learned model.

        Written beside a run's artefacts so an auditor can confirm the baseline
        is genuinely learned, genuinely rolling and genuinely seasonal, rather
        than a constant wearing a costume.

        Returns:
            Window length, cell counts and per-series support.
        """
        return {
            "retention_s": self._retention_s,
            "retention_days": self._retention_s / 86400.0,
            "shrinkage_k": self._shrinkage_k,
            "min_samples": self._min_samples,
            "seasonal_period_hours": SEASONAL_PERIOD_HOURS,
            "series_count": len(self._series),
            "populated_seasonal_cells": sum(1 for c in self._cells.values() if len(c)),
            "latest_observation": None if not math.isfinite(self._latest) else self._latest,
            "support": {
                f"{entity}|{feature}": self.support(entity, feature)
                for entity, feature in sorted(self._series)
            },
        }

    def warm(
        self,
        entity: str,
        feature: str,
        points: Iterable[tuple[float, float]],
    ) -> int:
        """Bulk-load history for a series.

        Used when a corpus ships a history prefix that should train the model
        without being scored against it.

        Args:
            entity: Entity id.
            feature: Feature name.
            points: ``(epoch_seconds, value)`` pairs.

        Returns:
            How many observations were accepted.
        """
        accepted = 0
        for moment, value in points:
            self.observe(entity, feature, moment, value)
            accepted += 1
        return accepted


def _unused() -> Iterator[Sequence[float]]:  # pragma: no cover - typing anchor
    """Anchor for imports referenced only in annotations."""
    yield ()
