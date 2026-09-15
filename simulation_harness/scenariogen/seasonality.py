"""Per-entity seasonality: hour-of-day, day-of-week and slow trend.

Requirement R4 asks for *"rolling multi-month (3-month) learned baselines that
adapt to per-entity, per-hour and per-day-of-week seasonality"*. A baseline
learner can only demonstrate that if the data actually **has** that structure,
and has it **differently per entity** — otherwise a single global constant
would score just as well and the feature would be untested.

So each entity gets:

* its own **hour-of-day shape** — a UK business-hours curve, perturbed
  per-entity so no two entities share a profile;
* its own **day-of-week shape** — weekday/weekend contrast, again perturbed,
  plus a Monday batch bump on some entities;
* a slow **multi-week trend** (organic growth or decline);
* **autocorrelated** noise (AR(1)), because real telemetry is not white noise
  and a detector tuned against white noise would flatter itself.

Nothing here knows about faults or detectors. It describes what *normal* looks
like; everything abnormal is applied on top of it by ``scenariogen.simulate``.
"""

from __future__ import annotations

import datetime as _dt
import math
from collections.abc import Sequence
from dataclasses import dataclass

from ano.contracts.determinism import rng

__all__ = [
    "BASE_HOUR_SHAPE",
    "BASE_DOW_SHAPE",
    "SeasonalProfile",
    "build_profile",
    "AutoCorrelatedNoise",
]

#: Base hour-of-day demand multiplier for a UK enterprise application, index 0
#: = 00:00 UTC. Overnight trough, a morning ramp, a lunch dip, an afternoon
#: peak and an evening decline, plus a small overnight batch bump at 02:00.
BASE_HOUR_SHAPE: tuple[float, ...] = (
    0.18, 0.14, 0.26, 0.22, 0.16, 0.20,  # 00-05, note the 02:00 batch bump
    0.34, 0.62, 0.88, 1.00, 0.98, 0.92,  # 06-11
    0.74, 0.86, 0.97, 0.99, 0.93, 0.78,  # 12-17
    0.56, 0.42, 0.34, 0.29, 0.24, 0.20,  # 18-23
)

#: Base day-of-week multiplier, index 0 = Monday (``datetime.weekday()``).
BASE_DOW_SHAPE: tuple[float, ...] = (1.06, 1.00, 1.00, 1.02, 0.94, 0.34, 0.27)


@dataclass(frozen=True, slots=True)
class SeasonalProfile:
    """One entity's normal-demand shape.

    Args:
        level: Mean requests per second at shape 1.0.
        hour_shape: 24 multipliers, this entity's own.
        dow_shape: 7 multipliers, this entity's own.
        trend_per_day: Fractional change per day, compounded. Small — this is
            organic growth, not an incident.
        epoch_nanos: Reference instant that ``trend_per_day`` is measured from.
    """

    level: float
    hour_shape: tuple[float, ...]
    dow_shape: tuple[float, ...]
    trend_per_day: float
    epoch_nanos: int

    def shape_at(self, moment: _dt.datetime) -> float:
        """Multiplicative seasonal factor at ``moment`` (no noise, no trend).

        Interpolates between adjacent hours so demand ramps smoothly rather
        than stepping on the hour — a step would be a giveaway artefact and
        would also hand a change-point detector a free win.
        """
        hour = moment.hour
        fraction = (moment.minute * 60 + moment.second) / 3600.0
        current = self.hour_shape[hour]
        following = self.hour_shape[(hour + 1) % 24]
        hourly = current + (following - current) * fraction
        return hourly * self.dow_shape[moment.weekday()]

    def demand_at(self, moment: _dt.datetime, nanos: int) -> float:
        """Offered load (requests/second) with seasonality and trend, no noise."""
        days_elapsed = (nanos - self.epoch_nanos) / 86_400e9
        trend = (1.0 + self.trend_per_day) ** days_elapsed
        return self.level * self.shape_at(moment) * trend


def build_profile(
    seed: int,
    entity_id: str,
    base_level: float,
    epoch_nanos: int,
) -> SeasonalProfile:
    """Derive an entity's own seasonal profile from the run seed.

    The perturbations are modest (±18% on hours, ±10% on days) — enough that
    every entity is distinguishable and a global baseline is demonstrably
    worse than a per-entity one, but not so much that the estate stops looking
    like one organisation's workload.
    """
    generator = rng(seed, "scenariogen", "seasonality", entity_id)

    hour_shape = tuple(
        max(0.05, value * generator.uniform(0.82, 1.18)) for value in BASE_HOUR_SHAPE
    )
    dow_shape = [value * generator.uniform(0.90, 1.10) for value in BASE_DOW_SHAPE]
    # Some entities carry a Monday batch reconciliation; some do not. This is
    # a per-entity structural difference, not just a scale difference, so a
    # baseline that ignores day-of-week genuinely mis-predicts on Mondays.
    if generator.random() < 0.4:
        dow_shape[0] *= 1.35
    # A weekend-heavy entity (overnight batch / site operations) inverts the
    # usual contrast, which punishes any hard-coded "weekends are quiet" rule.
    if generator.random() < 0.25:
        dow_shape[5] *= 2.2
        dow_shape[6] *= 2.0

    return SeasonalProfile(
        level=base_level * generator.uniform(0.75, 1.30),
        hour_shape=hour_shape,
        dow_shape=tuple(dow_shape),
        trend_per_day=generator.uniform(-0.0008, 0.0022),
        epoch_nanos=epoch_nanos,
    )


class AutoCorrelatedNoise:
    """AR(1) multiplicative noise: ``x[t] = phi*x[t-1] + sqrt(1-phi^2)*e[t]``.

    Real metric series are strongly autocorrelated — consecutive scrapes of the
    same host are not independent draws. Using white noise would make anomalies
    trivially separable from the residual, which would inflate every detection
    score for the wrong reason.

    The scaling keeps the stationary standard deviation equal to ``sigma``
    regardless of ``phi``, so changing the correlation does not accidentally
    change the noise amplitude.
    """

    __slots__ = ("_phi", "_sigma", "_innovation_scale", "_state", "_generator")

    def __init__(self, generator, sigma: float, phi: float = 0.72) -> None:
        if not 0.0 <= phi < 1.0:
            raise ValueError("phi must be in [0, 1)")
        self._phi = phi
        self._sigma = sigma
        self._innovation_scale = math.sqrt(1.0 - phi * phi)
        self._generator = generator
        self._state = generator.gauss(0.0, 1.0)

    def next(self) -> float:
        """Return the next standardised deviate (mean 0, sd 1)."""
        self._state = (
            self._phi * self._state
            + self._innovation_scale * self._generator.gauss(0.0, 1.0)
        )
        return self._state

    def factor(self, floor: float = 0.25) -> float:
        """Return a multiplicative factor around 1.0, clamped away from zero."""
        return max(floor, 1.0 + self._sigma * self.next())
