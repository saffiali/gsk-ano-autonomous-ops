"""Public API entrypoint for scenario generation.

Contract C1 / harness adapter interface:
    scenariogen.api:generate(seed: int, out_dir: str) -> None
"""

from __future__ import annotations

import os
from scenariogen.generate import generate as _generate

__all__ = ["generate"]


def generate(seed: int, out_dir: str, profile: str | None = None) -> None:
    """Generate one complete scenario corpus.

    Args:
        seed: Scenario seed.
        out_dir: Directory to write into.
        profile: Optional profile override (default is from env or 'full').
    """
    profile_name = profile or os.environ.get("GSK_ANO_SCENARIO_PROFILE", "full")
    _generate(seed, out_dir, profile_name=profile_name)
