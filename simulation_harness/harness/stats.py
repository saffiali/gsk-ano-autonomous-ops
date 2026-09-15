"""Order statistics, hand-rolled because the project is standard-library only.

``statistics.quantiles`` exists but refuses inputs of length 1 and its
``method="inclusive"`` cut points are not directly addressable for a single
percentile, so the harness computes percentiles itself. The method is declared
in the results file (``parameters["percentile_method"]``) because "the 10th
percentile" is not a single well-defined quantity and an auditor must not have
to guess which of the nine common definitions was used.

Method: **linear interpolation between closest ranks on the inclusive
(N-1) basis** — the R type-7 definition, the default in NumPy, R and
``statistics.quantiles(method="inclusive")``.
"""

from __future__ import annotations

from collections.abc import Sequence

__all__ = ["PERCENTILE_METHOD", "percentile", "median", "p10", "mean"]

#: Declared in the results file so the percentile definition is unambiguous.
PERCENTILE_METHOD = (
    "linear interpolation between closest ranks on the inclusive (N-1) basis "
    "(R type-7; equivalent to numpy.percentile default and to "
    "statistics.quantiles(method='inclusive'))"
)


def percentile(values: Sequence[float], fraction: float) -> float | None:
    """Return the ``fraction`` percentile of ``values``.

    Args:
        values: Sample values. Not required to be sorted.
        fraction: Percentile position in ``[0.0, 1.0]``; ``0.5`` is the median.

    Returns:
        The interpolated percentile, or ``None`` when ``values`` is empty —
        an empty sample has no percentile, and returning ``0.0`` would let an
        empty denominator masquerade as a measured zero.

    Raises:
        ValueError: if ``fraction`` is outside ``[0.0, 1.0]``.
    """
    if not 0.0 <= fraction <= 1.0:
        raise ValueError(f"fraction must be in [0, 1], got {fraction!r}")
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = fraction * (len(ordered) - 1)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    weight = position - lower_index
    lower = float(ordered[lower_index])
    upper = float(ordered[upper_index])
    return lower + (upper - lower) * weight


def median(values: Sequence[float]) -> float | None:
    """The 50th percentile of ``values``, or ``None`` if empty."""
    return percentile(values, 0.5)


def p10(values: Sequence[float]) -> float | None:
    """The 10th percentile of ``values``, or ``None`` if empty."""
    return percentile(values, 0.10)


def mean(values: Sequence[float]) -> float | None:
    """The arithmetic mean of ``values``, or ``None`` if empty."""
    if not values:
        return None
    return sum(float(value) for value in values) / len(values)
