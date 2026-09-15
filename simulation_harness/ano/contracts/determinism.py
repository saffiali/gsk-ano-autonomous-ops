"""Determinism utilities — seeded RNG factory and stable hashing.

Why this module exists
----------------------
Two acceptance criteria depend on it:

* "Re-running the harness with the same seed produces identical scores."
* "The harness reports results over at least 5 distinct seeds."

CPython makes both easy to break by accident:

* The builtin ``hash()`` is **salted per process** for ``str`` and ``bytes``
  (PEP 456). Two runs of the same code with the same seed produce different
  values, so anything derived from ``hash("some log line")`` — a feature
  bucket, a shard index, a random-projection sign — silently differs between
  runs. **Using builtin ``hash()`` on str/bytes anywhere in this project is
  forbidden.** Use :func:`stable_hash_int`, :func:`stable_bucket` or
  :func:`stable_sign` instead; they are backed by ``hashlib.blake2b`` and are
  stable across processes, machines and Python builds.
* The module-level ``random`` functions share one global generator. Any code
  that calls ``random.random()`` couples its output to every other caller's
  call order. **Always take a generator from :func:`rng` and pass it around.**

Domain separation
-----------------
Every helper takes a ``namespace``: a tuple of strings naming what the value is
for (``("embedding", "sign")``, ``("scenariogen", "entity-pick")``, ...). The
namespace is folded into the digest, so two different consumers deriving values
from the same seed do not accidentally produce correlated streams. Namespaces
are part of your module's behaviour: changing one changes your outputs, so pick
them once and leave them alone.

Example:
    >>> from ano.contracts.determinism import rng, stable_bucket
    >>> generator = rng(1234, "scenariogen", "onset-jitter")
    >>> generator.random() == rng(1234, "scenariogen", "onset-jitter").random()
    True
    >>> stable_bucket("tomcat-07", 64, "embedding") == stable_bucket(
    ...     "tomcat-07", 64, "embedding")
    True
"""

from __future__ import annotations

import hashlib
import random
from collections.abc import Iterable

__all__ = [
    "HASH_PERSON",
    "DIGEST_SIZE",
    "rng",
    "derive_seed",
    "stable_digest",
    "stable_hash_hex",
    "stable_hash_int",
    "stable_unit_float",
    "stable_bucket",
    "stable_sign",
    "stable_id",
    "hash_stream",
]

#: blake2b personalisation string. It scopes every digest this project produces
#: to this project, so a digest can never collide with one computed elsewhere
#: with the same key. Must be <= 16 bytes. Changing it changes every derived
#: value in the system, so it is frozen.
HASH_PERSON = b"gsk-ano-v1"

#: Default digest length in bytes (128 bits) — ample for bucketing and ids.
DIGEST_SIZE = 16

_ENCODING = "utf-8"
#: Separator used when folding a namespace into a digest. 0x1f (unit separator)
#: cannot appear in UTF-8 text produced by encoding a normal string, so
#: ("a", "bc") and ("ab", "c") can never collide.
_SEP = b"\x1f"


def _as_bytes(value: bytes | bytearray | memoryview | str) -> bytes:
    """Normalise a hashable input to bytes without using builtin ``hash()``."""
    if isinstance(value, str):
        return value.encode(_ENCODING)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError(
        "stable hashing accepts str or bytes-like input only, got "
        f"{type(value).__name__}; convert your object to a canonical string "
        "first (see ano.contracts.common.dumps_canonical)"
    )


def _namespaced(data: bytes, namespace: Iterable[str]) -> bytes:
    parts = [data]
    for item in namespace:
        if not isinstance(item, str):
            raise TypeError(
                f"namespace components must be str, got {type(item).__name__}"
            )
        parts.append(item.encode(_ENCODING))
    return _SEP.join(parts)


def stable_digest(
    value: bytes | bytearray | memoryview | str,
    *namespace: str,
    digest_size: int = DIGEST_SIZE,
) -> bytes:
    """Return a process-stable blake2b digest of ``value`` under ``namespace``.

    Args:
        value: The data to hash. ``str`` is encoded as UTF-8.
        *namespace: Domain-separation components.
        digest_size: Digest length in bytes, 1..64.

    Returns:
        ``digest_size`` raw bytes. Identical for identical inputs in any
        process, on any machine, regardless of ``PYTHONHASHSEED``.
    """
    if not 1 <= digest_size <= 64:
        raise ValueError(f"digest_size must be in 1..64, got {digest_size}")
    return hashlib.blake2b(
        _namespaced(_as_bytes(value), namespace),
        digest_size=digest_size,
        person=HASH_PERSON,
    ).digest()


def stable_hash_hex(
    value: bytes | bytearray | memoryview | str,
    *namespace: str,
    digest_size: int = DIGEST_SIZE,
) -> str:
    """Hex form of :func:`stable_digest`. Use for ids that appear in output."""
    return stable_digest(value, *namespace, digest_size=digest_size).hex()


def stable_hash_int(
    value: bytes | bytearray | memoryview | str,
    *namespace: str,
    bits: int = 64,
) -> int:
    """Non-negative integer hash of ``value``, stable across processes.

    This is the drop-in replacement for builtin ``hash()``.

    Args:
        value: Data to hash.
        *namespace: Domain-separation components.
        bits: Width of the result, 8..512 and a multiple of 8.
    """
    if bits < 8 or bits > 512 or bits % 8:
        raise ValueError(f"bits must be a multiple of 8 in 8..512, got {bits}")
    return int.from_bytes(
        stable_digest(value, *namespace, digest_size=bits // 8), "big"
    )


def stable_unit_float(
    value: bytes | bytearray | memoryview | str, *namespace: str
) -> float:
    """Deterministic float in ``[0.0, 1.0)`` derived from ``value``.

    Uses 53 bits — exactly the mantissa width of a double — so the result is
    uniform over representable values and never rounds to 1.0.
    """
    return (stable_hash_int(value, *namespace, bits=64) >> 11) / float(1 << 53)


def stable_bucket(
    value: bytes | bytearray | memoryview | str, buckets: int, *namespace: str
) -> int:
    """Map ``value`` deterministically into ``range(buckets)``.

    This is the "hashing trick" primitive used for log-message feature vectors.

    Args:
        value: Token, n-gram or other key.
        buckets: Number of buckets; must be >= 1.
        *namespace: Domain-separation components.
    """
    if buckets < 1:
        raise ValueError(f"buckets must be >= 1, got {buckets}")
    # 256 bits of entropy makes the modulo bias negligible for any practical
    # bucket count (bias < 2**-200 for buckets < 2**56).
    return stable_hash_int(value, *namespace, bits=256) % buckets


def stable_sign(
    value: bytes | bytearray | memoryview | str, *namespace: str
) -> int:
    """Deterministic ``+1`` / ``-1`` derived from ``value``.

    The sign half of a signed random projection: pairing
    ``stable_bucket(token, dim)`` with ``stable_sign(token)`` gives an unbiased
    sketch without storing a projection matrix.
    """
    return 1 if stable_hash_int(value, *namespace, "sign", bits=64) & 1 else -1


def stable_id(
    value: bytes | bytearray | memoryview | str,
    *namespace: str,
    prefix: str = "",
    length: int = 16,
) -> str:
    """Deterministic, human-pasteable identifier derived from ``value``.

    Args:
        value: Canonical content the id should be a function of.
        *namespace: Domain-separation components.
        prefix: Optional prefix, joined with ``-`` (e.g. ``"pred"`` ->
            ``"pred-3f0a..."``).
        length: Number of hex characters after the prefix (2..32).

    Returns:
        A stable id. Two records with identical content get the same id, which
        is what makes an artifact byte-reproducible between runs.
    """
    if not 2 <= length <= 32:
        raise ValueError(f"length must be in 2..32, got {length}")
    digest = stable_hash_hex(value, *namespace, digest_size=16)[:length]
    return f"{prefix}-{digest}" if prefix else digest


def derive_seed(seed: int, *namespace: str, bits: int = 63) -> int:
    """Derive a sub-seed from a master ``seed`` and a ``namespace``.

    Lets each component own an independent random stream that is still a pure
    function of the single seed the operator supplied.

    Args:
        seed: Master seed (any integer).
        *namespace: Domain-separation components.
        bits: Width of the derived seed. Default 63 keeps it positive and
            comfortably inside a signed 64-bit field.
    """
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise TypeError(f"seed must be an int, got {type(seed).__name__}")
    return stable_hash_int(str(seed), *namespace, "seed", bits=64) >> (64 - bits)


def rng(seed: int, *namespace: str) -> random.Random:
    """Return a fresh, independent :class:`random.Random` for ``namespace``.

    **This is the only sanctioned way to obtain randomness in this project.**
    The module-level ``random.*`` functions share one global generator whose
    output depends on every other caller in the process; that is incompatible
    with "same seed => identical scores".

    Args:
        seed: Master seed for the run.
        *namespace: Names the consumer, e.g. ``("scenariogen", "db_lock")``.

    Returns:
        A ``random.Random`` seeded from ``seed`` and ``namespace``. Calling it
        twice with the same arguments returns two generators that produce the
        same sequence.
    """
    return random.Random(derive_seed(seed, *namespace))


def hash_stream(
    values: Iterable[bytes | bytearray | memoryview | str],
    *namespace: str,
    digest_size: int = DIGEST_SIZE,
) -> str:
    """Hex digest over an ordered sequence of values.

    Order matters and is part of the digest — use it to fingerprint a whole
    artifact (for example, every line of a results file) in one value.
    """
    hasher = hashlib.blake2b(digest_size=digest_size, person=HASH_PERSON)
    for item in namespace:
        hasher.update(item.encode(_ENCODING))
        hasher.update(_SEP)
    for value in values:
        payload = _as_bytes(value)
        # Length-prefix each element so that ["ab","c"] and ["a","bc"] differ.
        hasher.update(len(payload).to_bytes(8, "big"))
        hasher.update(payload)
    return hasher.hexdigest()
