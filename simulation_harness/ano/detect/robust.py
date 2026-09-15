"""Robust statistics, hand-rolled on the Python standard library.

Why robust estimators rather than mean/standard deviation
---------------------------------------------------------
A baseline is learned from live telemetry that *contains* the degradations we
are trying to predict. A mean/σ baseline is poisoned by exactly the events it
must flag: one thread-starvation hang inflates σ enough that the next one
scores as normal. The median and the median absolute deviation have a 50%
breakdown point — half the window can be arbitrarily corrupt before the
estimate moves — so a baseline built from them stays honest without anyone
having to tell it which samples were incidents. That matters here because
requirement R5 forbids the detector from ever seeing a ground-truth label, so
"just exclude the incident periods" is not available to us.

The same argument drives the slope estimator: a Theil-Sen slope (the median of
all pairwise slopes) survives the spikes and counter glitches that ordinary
least squares chases.

No third-party numerics are available (and none are permitted), so everything
here is written against :mod:`math` and :mod:`statistics`.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Sequence

__all__ = [
    "MAD_TO_SIGMA",
    "median",
    "mad",
    "robust_sigma",
    "robust_z",
    "quantile",
    "theil_sen_slope",
    "pearson",
    "rank",
    "spearman",
    "ewma",
    "clamp",
    "logistic",
    "safe_ratio",
]

#: Scale factor making the median absolute deviation a consistent estimator of
#: the standard deviation for normally distributed data: ``1 / Phi^-1(3/4)``.
MAD_TO_SIGMA = 1.4826022185056018


def clamp(value: float, low: float, high: float) -> float:
    """Constrain ``value`` to ``[low, high]``.

    Args:
        value: The value to constrain.
        low: Lower bound.
        high: Upper bound.

    Returns:
        ``value`` clamped into the closed interval.

    Raises:
        ValueError: If ``low > high``.
    """
    if low > high:
        raise ValueError(f"empty interval [{low}, {high}]")
    if value < low:
        return low
    if value > high:
        return high
    return value


def logistic(x: float) -> float:
    """The logistic function, overflow-safe at both extremes.

    ``math.exp`` raises :class:`OverflowError` for arguments beyond roughly
    -745, which a log-odds sum can easily reach. The two-branch form avoids it.

    Args:
        x: Log-odds.

    Returns:
        A probability in ``(0, 1)``.
    """
    if x >= 0.0:
        return 1.0 / (1.0 + math.exp(-x))
    exp_x = math.exp(x)
    return exp_x / (1.0 + exp_x)


def safe_ratio(numerator: float, denominator: float, *, floor: float = 1e-12) -> float:
    """``numerator / denominator`` with the denominator floored away from zero.

    Used pervasively for utilisation and efficiency features, where a zero
    denominator means "no work observed" rather than "infinite ratio".

    Args:
        numerator: Dividend.
        denominator: Divisor.
        floor: Smallest magnitude the divisor is allowed to take.

    Returns:
        The ratio, or ``0.0`` when the numerator is zero.
    """
    if numerator == 0.0:
        return 0.0
    if abs(denominator) < floor:
        denominator = math.copysign(floor, denominator if denominator else 1.0)
    return numerator / denominator


def median(values: Sequence[float]) -> float:
    """Median of ``values``.

    Args:
        values: A non-empty sequence.

    Returns:
        The median.

    Raises:
        ValueError: If ``values`` is empty.
    """
    if not values:
        raise ValueError("median of an empty sequence")
    return statistics.median(values)


def mad(values: Sequence[float], *, center: float | None = None) -> float:
    """Median absolute deviation.

    Args:
        values: A non-empty sequence.
        center: Centre to deviate from. Defaults to the median of ``values``.

    Returns:
        ``median(|x - center|)``. Zero for a constant series.

    Raises:
        ValueError: If ``values`` is empty.
    """
    if not values:
        raise ValueError("mad of an empty sequence")
    mid = median(values) if center is None else center
    return statistics.median([abs(value - mid) for value in values])


def robust_sigma(values: Sequence[float], *, center: float | None = None) -> float:
    """A standard-deviation-equivalent scale from the MAD.

    Args:
        values: A non-empty sequence.
        center: Centre to deviate from. Defaults to the median.

    Returns:
        ``1.4826 * MAD(values)``.
    """
    return MAD_TO_SIGMA * mad(values, center=center)


def robust_z(
    value: float,
    center: float,
    sigma: float,
    *,
    absolute_floor: float = 0.0,
    relative_floor: float = 0.0,
) -> float:
    """A median/MAD z-score with a scale floor.

    A perfectly flat series has ``sigma == 0``, which would make every
    deviation infinitely significant — a real and common failure mode for
    counters that sit at zero until something breaks. The floor is what stops
    a one-unit move on a dead-flat signal from scoring as a 10-sigma event.

    The relative floor is expressed as a fraction of ``|center|``, so a signal
    measured in milliseconds and a signal measured in requests-per-second get
    proportionate treatment without per-signal tuning.

    Args:
        value: The observation.
        center: The learned centre (a median).
        sigma: The learned scale.
        absolute_floor: Minimum scale in the signal's own units.
        relative_floor: Minimum scale as a fraction of ``|center|``.

    Returns:
        The floored robust z-score. ``0.0`` when the effective scale is zero
        and the value equals the centre.
    """
    scale = max(sigma, absolute_floor, relative_floor * abs(center))
    if scale <= 0.0:
        return 0.0 if value == center else math.inf * (1.0 if value > center else -1.0)
    return (value - center) / scale


def quantile(values: Sequence[float], q: float) -> float:
    """Linear-interpolated quantile.

    :func:`statistics.quantiles` cannot return the 0th or 100th percentile and
    needs at least two points; both cases occur here, so this does it directly.

    Args:
        values: A non-empty sequence. Need not be sorted.
        q: Quantile in ``[0, 1]``.

    Returns:
        The interpolated quantile.

    Raises:
        ValueError: If ``values`` is empty or ``q`` lies outside ``[0, 1]``.
    """
    if not values:
        raise ValueError("quantile of an empty sequence")
    if not 0.0 <= q <= 1.0:
        raise ValueError(f"q must be in [0, 1], got {q}")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = q * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[int(position)]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def theil_sen_slope(
    times: Sequence[float],
    values: Sequence[float],
    *,
    max_pairs: int = 4096,
) -> float:
    """Median-of-pairwise-slopes trend estimate, in value-units per time-unit.

    Ordinary least squares is dominated by the largest residual, which on
    telemetry is usually a scrape gap or a counter reset rather than the trend
    we want. Theil-Sen takes the median over all pairs and is unaffected by up
    to ~29% contaminated points.

    Args:
        times: Strictly increasing-ish time coordinates. Duplicate times are
            skipped rather than raising, because duplicate scrape timestamps do
            occur.
        values: Same length as ``times``.
        max_pairs: Safety valve. With more candidate pairs than this the window
            is decimated to an evenly spaced subset first, keeping the cost
            bounded without biasing the slope.

    Returns:
        The slope, or ``0.0`` when fewer than two distinct time points exist.

    Raises:
        ValueError: If the two sequences differ in length.
    """
    if len(times) != len(values):
        raise ValueError("times and values must be the same length")
    count = len(times)
    if count < 2:
        return 0.0

    if count * (count - 1) // 2 > max_pairs:
        # Decimate evenly. Keeping the endpoints preserves the window's span,
        # which is what the slope is actually measured over.
        keep = max(2, int(math.isqrt(2 * max_pairs)))
        step = (count - 1) / (keep - 1)
        indices = sorted({int(round(i * step)) for i in range(keep)} | {0, count - 1})
        times = [times[i] for i in indices]
        values = [values[i] for i in indices]
        count = len(times)

    slopes: list[float] = []
    for i in range(count - 1):
        for j in range(i + 1, count):
            delta_t = times[j] - times[i]
            if delta_t == 0.0:
                continue
            slopes.append((values[j] - values[i]) / delta_t)
    if not slopes:
        return 0.0
    return statistics.median(slopes)


def pearson(xs: Sequence[float], ys: Sequence[float]) -> float:
    """Pearson product-moment correlation.

    Args:
        xs: First series.
        ys: Second series, same length.

    Returns:
        Correlation in ``[-1, 1]``; ``0.0`` if either series is constant or
        shorter than two points.

    Raises:
        ValueError: If the two sequences differ in length.
    """
    if len(xs) != len(ys):
        raise ValueError("series must be the same length")
    count = len(xs)
    if count < 2:
        return 0.0
    mean_x = statistics.fmean(xs)
    mean_y = statistics.fmean(ys)
    covariance = 0.0
    var_x = 0.0
    var_y = 0.0
    for x, y in zip(xs, ys):
        dx = x - mean_x
        dy = y - mean_y
        covariance += dx * dy
        var_x += dx * dx
        var_y += dy * dy
    if var_x <= 0.0 or var_y <= 0.0:
        return 0.0
    return covariance / math.sqrt(var_x * var_y)


def rank(values: Sequence[float]) -> list[float]:
    """Fractional ranks with ties averaged.

    Args:
        values: Any sequence.

    Returns:
        Ranks, 1-based, tied values sharing their mean rank.
    """
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    position = 0
    while position < len(order):
        end = position
        while end + 1 < len(order) and values[order[end + 1]] == values[order[position]]:
            end += 1
        shared = (position + end) / 2.0 + 1.0
        for k in range(position, end + 1):
            ranks[order[k]] = shared
        position = end + 1
    return ranks


def spearman(xs: Sequence[float], ys: Sequence[float]) -> float:
    """Spearman rank correlation.

    Preferred over Pearson for comparing an application's latency series with a
    database's lock-wait series: the relationship is monotone but emphatically
    not linear, and Pearson understates it.

    Args:
        xs: First series.
        ys: Second series, same length.

    Returns:
        Rank correlation in ``[-1, 1]``.
    """
    if len(xs) != len(ys) or len(xs) < 2:
        return 0.0
    return pearson(rank(xs), rank(ys))


def ewma(values: Sequence[float], alpha: float) -> float:
    """Exponentially weighted moving average, most recent value weighted most.

    Args:
        values: Series in chronological order.
        alpha: Smoothing factor in ``(0, 1]``. Larger reacts faster.

    Returns:
        The smoothed value, or ``0.0`` for an empty series.

    Raises:
        ValueError: If ``alpha`` is outside ``(0, 1]``.
    """
    if not 0.0 < alpha <= 1.0:
        raise ValueError(f"alpha must be in (0, 1], got {alpha}")
    if not values:
        return 0.0
    accumulator = values[0]
    for value in values[1:]:
        accumulator = alpha * value + (1.0 - alpha) * accumulator
    return accumulator
