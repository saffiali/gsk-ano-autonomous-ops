"""Scenario time: the scrape grid the whole generator runs on.

Two design decisions are worth stating up front, because both were forced by
requirements rather than convenience.

**The evaluation window is fixed wall-clock, the history varies.**
Requirement R4 wants rolling *three-month* learned baselines, so the corpus has
to contain months of history. But a full three months at 60-second resolution
is ~130k scrapes per target and blows the capacity budget in ``DECISIONS.md``
D7. So history is generated at a coarser interval — exactly as a real
long-retention store downsamples — and only the evaluation window is at full
resolution.

The window is pinned to the *same 48 hours of wall-clock time* in every
profile, and history extends **backwards** from it. That matters: if the window
floated with the history length, the scenario would land on a different
day-of-week at each profile and the seasonal context of every event would
change. Pinning it means ``quick`` and ``full`` differ only in sampling
resolution and baseline depth, never in what happened.

**No wall clock is read anywhere.** Every instant in the corpus derives from
:data:`WINDOW_START`. Reading ``datetime.now()`` would destroy byte-identical
determinism, which is the one property this milestone exists to provide.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator
from dataclasses import dataclass

from ano.telemetry.timefmt import datetime_to_nanos

__all__ = [
    "WINDOW_START",
    "WINDOW_HOURS",
    "Profile",
    "PROFILES",
    "Tick",
    "Timeline",
    "build_timeline",
]

#: Start of the evaluation window: Tuesday 2026-04-07 00:00:00 UTC.
#:
#: A Tuesday so the window spans two ordinary business days and the events land
#: against a weekday seasonal profile. Fixed, not derived from the clock.
WINDOW_START = _dt.datetime(2026, 4, 7, 0, 0, 0, tzinfo=_dt.UTC)

#: Length of the evaluation window. Identical in every profile.
WINDOW_HOURS = 48

#: Phase names. ``history`` is unlabelled training data; ``window`` is the
#: labelled span the harness scores over.
PHASE_HISTORY = "history"
PHASE_WINDOW = "window"


@dataclass(frozen=True, slots=True)
class Profile:
    """A generation size/resolution preset.

    Attributes:
        name: Profile name, as passed to ``--profile``.
        history_days: Days of unlabelled history before the window.
        history_interval_s: Scrape interval during history.
        window_interval_s: Scrape interval during the evaluation window.
    """

    name: str
    history_days: int
    history_interval_s: int
    window_interval_s: int

    @property
    def history_ticks(self) -> int:
        return self.history_days * 86_400 // self.history_interval_s

    @property
    def window_ticks(self) -> int:
        return WINDOW_HOURS * 3600 // self.window_interval_s

    @property
    def total_ticks(self) -> int:
        return self.history_ticks + self.window_ticks


#: ``full`` is the deliverable: 90 days of history satisfies R4's three-month
#: baseline, and 60-second window resolution is a realistic GMP scrape interval.
#: ``demo`` halves the cost for interactive use. ``quick`` exists so the unit
#: tests can generate a whole corpus in-process without a capacity problem; it
#: keeps the same 48-hour window and the same events, only shallower history and
#: coarser sampling.
PROFILES: dict[str, Profile] = {
    "full": Profile("full", history_days=90, history_interval_s=1800, window_interval_s=60),
    "demo": Profile("demo", history_days=30, history_interval_s=1800, window_interval_s=120),
    "quick": Profile("quick", history_days=5, history_interval_s=3600, window_interval_s=600),
}


@dataclass(frozen=True, slots=True)
class Tick:
    """One scrape instant, for every target simultaneously.

    Attributes:
        index: Position in the timeline, from 0.
        moment: The instant, timezone-aware UTC.
        nanos: The same instant as integer nanoseconds since the epoch.
        interval_s: Seconds since the previous tick. Counter deltas are
            multiplied by this, so a coarse history tick accumulates
            proportionally more than a fine window tick — which is what makes
            the two resolutions splice together without a discontinuity in any
            ``_total`` series.
        phase: :data:`PHASE_HISTORY` or :data:`PHASE_WINDOW`.
    """

    index: int
    moment: _dt.datetime
    nanos: int
    interval_s: int
    phase: str


@dataclass(frozen=True, slots=True)
class Timeline:
    """The full ordered scrape grid."""

    profile: Profile
    ticks: tuple[Tick, ...]
    history_start: _dt.datetime
    window_start: _dt.datetime
    window_end: _dt.datetime

    @property
    def epoch_nanos(self) -> int:
        """Nanoseconds at the first tick. Seasonal trend is measured from here."""
        return self.ticks[0].nanos

    @property
    def history_start_nanos(self) -> int:
        return datetime_to_nanos(self.history_start)

    @property
    def window_start_nanos(self) -> int:
        return datetime_to_nanos(self.window_start)

    @property
    def window_end_nanos(self) -> int:
        return datetime_to_nanos(self.window_end)

    def history_ticks(self) -> Iterator[Tick]:
        """Every tick before the evaluation window."""
        return (tick for tick in self.ticks if tick.phase == PHASE_HISTORY)

    def window_ticks(self) -> Iterator[Tick]:
        """Every tick inside the evaluation window."""
        return (tick for tick in self.ticks if tick.phase == PHASE_WINDOW)


def build_timeline(profile: Profile) -> Timeline:
    """Build the scrape grid for ``profile``.

    The history grid is half-open ``[history_start, WINDOW_START)`` and the
    window grid is half-open ``[WINDOW_START, window_end)``, so no instant is
    scraped twice at the splice.
    """
    history_start = WINDOW_START - _dt.timedelta(days=profile.history_days)
    window_end = WINDOW_START + _dt.timedelta(hours=WINDOW_HOURS)

    ticks: list[Tick] = []
    moment = history_start
    index = 0
    step = _dt.timedelta(seconds=profile.history_interval_s)
    while moment < WINDOW_START:
        ticks.append(
            Tick(index, moment, datetime_to_nanos(moment),
                 profile.history_interval_s, PHASE_HISTORY)
        )
        index += 1
        moment += step

    step = _dt.timedelta(seconds=profile.window_interval_s)
    moment = WINDOW_START
    while moment < window_end:
        ticks.append(
            Tick(index, moment, datetime_to_nanos(moment),
                 profile.window_interval_s, PHASE_WINDOW)
        )
        index += 1
        moment += step

    return Timeline(
        profile=profile,
        ticks=tuple(ticks),
        history_start=history_start,
        window_start=WINDOW_START,
        window_end=window_end,
    )
