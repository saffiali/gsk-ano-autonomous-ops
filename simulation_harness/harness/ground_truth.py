"""Reading contract-C1 ground truth from disk. Harness-only, structurally.

Requirement R5:

    "The scenario generator must not be able to see the detector's internals,
    and the detectors must not read the ground-truth labels at inference time."

The C1 *schema* is shared vocabulary and rightly lives in
``ano.contracts.labels``. The act of **opening a label file** is not shared: it
is the single privilege that separates the evaluator from the system under
evaluation. Keeping it here means a detector that wanted the answers would have
to ``import harness``, which is a visible, greppable violation — whereas a call
to a reader sitting in the shared schema package would look entirely ordinary
and no import-graph rule would catch it.

Writing is deliberately *not* moved: ``scenariogen`` writes the label file, and
producing ground truth is not the leak R5 guards against.
"""

from __future__ import annotations

import os

from ano.contracts.labels import LabelFile, parse_label_file

__all__ = ["read_label_file"]


def read_label_file(path: str | os.PathLike[str]) -> LabelFile:
    """Read and validate a contract-C1 label file from disk.

    Args:
        path: The label file, normally ``<seed_dir>/labels.json``.

    Returns:
        The validated :class:`~ano.contracts.labels.LabelFile`.

    Raises:
        ValidationError: If the file is not a valid C1 document.
        OSError: If it cannot be read.
    """
    with open(path, "r", encoding="utf-8") as handle:
        return parse_label_file(handle.read())
