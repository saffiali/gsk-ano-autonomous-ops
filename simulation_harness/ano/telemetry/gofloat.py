"""Reproduce Go's ``strconv.FormatFloat(f, 'g', -1, 64)`` output.

Why this matters
----------------
The Prometheus reference writer formats every sample value with
``strconv.AppendFloat(f, 'g', -1, 64)``
(``PROMETHEUS_SPEC.md`` §1.6, from ``expfmt/text_create.go`` L473–497). A real
``/metrics`` scrape therefore contains ``1.70884952e+06``, **not**
``1708849.52`` — that exact line is in the genuine node_exporter capture cited
by the spec.

Python's ``repr`` and ``%g`` both disagree with Go here:

* ``repr(1708849.52)`` → ``'1708849.52'``   (Go: ``1.70884952e+06``)
* ``"%g" % 1708849.52`` → ``'1.70885e+06'`` (Go keeps all shortest digits)
* ``repr(2e21)``       → ``'2e+21'``         (agrees, by luck)
* ``"%g" % 200.0``     → ``'200'``           (agrees)

So the rule has to be implemented rather than delegated. Getting it right is
what makes synthetic exposition text indistinguishable from a genuine scrape;
getting it wrong is immediately visible to anyone who has read one.

The algorithm, from Go's ``strconv/ftoa.go``
--------------------------------------------
For verb ``'g'`` with precision ``-1`` ("shortest representation that
round-trips"):

1. Compute the shortest decimal digit string ``d`` and decimal point position
   ``dp`` such that reading them back yields the same ``float64``.
2. Let ``exp = dp - 1`` (the base-10 exponent of the leading digit).
3. Because the precision was "shortest", the exponent threshold ``eprec`` is
   fixed at **6**.
4. Use ``%e`` formatting if ``exp < -4 or exp >= eprec``; otherwise ``%f``.
5. ``%e`` writes ``d[0]`` then ``.`` then the rest (the ``.`` is omitted when
   there is only one digit), then ``e``, a mandatory sign, and **at least two**
   exponent digits.
6. ``%f`` writes the digits with the point placed at ``dp``, with no trailing
   ``.0`` — so ``200.0`` renders ``200``.

Step 1 is exactly what CPython's ``repr`` of a float computes (both use a
shortest-round-trip algorithm), so the digits are obtained from ``repr`` via
``decimal.Decimal`` and then re-laid-out according to Go's rules.
"""

from __future__ import annotations

import math
from decimal import Decimal

__all__ = ["format_go_float", "GO_G_EXPONENT_THRESHOLD"]

#: Go fixes ``eprec = 6`` when the precision is "shortest" (``ftoa.go``).
GO_G_EXPONENT_THRESHOLD = 6


def format_go_float(value: float) -> str:
    """Format ``value`` the way Go's ``%g`` with shortest precision would.

    Args:
        value: Any ``float`` (or ``int``, which is widened).

    Returns:
        The canonical Prometheus exposition spelling: ``NaN``, ``+Inf``,
        ``-Inf``, a bare integer (``200``), a fixed-point decimal
        (``708.64``), or scientific notation (``1.70884952e+06``).
    """
    number = float(value)

    # Exact spellings, case-sensitive (PROMETHEUS_SPEC.md §1.6). Note the
    # mandatory sign on infinities: bare ``Inf`` is never emitted.
    if math.isnan(number):
        return "NaN"
    if math.isinf(number):
        return "+Inf" if number > 0 else "-Inf"

    # Go's fast paths. ``-0.0`` is deliberately normalised to ``0``: a negative
    # zero in a counter is meaningless noise and would make byte-comparison of
    # two equal series fail.
    if number == 0.0:
        return "0"
    if number == 1.0:
        return "1"
    if number == -1.0:
        return "-1"

    negative = number < 0.0
    # ``repr`` gives the shortest digit string that round-trips, which is the
    # same quantity Go's shortest-mode conversion produces.
    decimal_value = Decimal(repr(abs(number)))
    _sign, digit_tuple, decimal_exponent = decimal_value.as_tuple()

    digits = list(digit_tuple)
    # Strip trailing zeros: ``Decimal('200.0')`` reports digits (2,0,0,0) with
    # exponent -1, but the shortest digit string is "2" with dp = 3.
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        decimal_exponent += 1
    digit_string = "".join(str(d) for d in digits)
    num_digits = len(digit_string)
    decimal_point = num_digits + decimal_exponent  # Go's digs.dp
    exponent = decimal_point - 1  # Go's exp

    if exponent < -4 or exponent >= GO_G_EXPONENT_THRESHOLD:
        body = _format_scientific(digit_string, exponent)
    else:
        body = _format_fixed(digit_string, decimal_point)
    return "-" + body if negative else body


def _format_scientific(digits: str, exponent: int) -> str:
    """Go's ``fmtE``: ``d.ddde±XX`` with at least two exponent digits."""
    mantissa = digits[0] if len(digits) == 1 else f"{digits[0]}.{digits[1:]}"
    sign = "+" if exponent >= 0 else "-"
    return f"{mantissa}e{sign}{abs(exponent):02d}"


def _format_fixed(digits: str, decimal_point: int) -> str:
    """Go's ``fmtF``: place the point at ``decimal_point``, no trailing ``.0``."""
    if decimal_point <= 0:
        # 0.00ddd — leading zeros between the point and the first digit.
        return "0." + "0" * (-decimal_point) + digits
    if decimal_point >= len(digits):
        # Integral value: pad with zeros, emit no fractional part at all.
        return digits + "0" * (decimal_point - len(digits))
    return digits[:decimal_point] + "." + digits[decimal_point:]
