"""Canonical boundaries for deterministic crypto up/down market slugs."""

from __future__ import annotations

CRYPTO_5M_WINDOW_S = 300


def crypto_5m_bounds_from_slug(slug: str) -> tuple[int, int]:
    """Return ``(round_start, round_end)`` for an official 5-minute slug.

    Polymarket encodes the window *start* in ``asset-updown-5m-<epoch>``.
    Keeping this conversion centralized prevents research replays from
    accidentally treating the slug timestamp as the close boundary.
    """

    normalized = str(slug).strip().lower()
    marker = "-updown-5m-"
    if marker not in normalized:
        raise ValueError(f"not a crypto 5m up/down slug: {slug!r}")
    raw_start = normalized.rsplit(marker, 1)[1]
    try:
        round_start = int(raw_start)
    except ValueError as exc:
        raise ValueError(f"invalid crypto 5m slug epoch: {slug!r}") from exc
    if round_start <= 0 or round_start % CRYPTO_5M_WINDOW_S:
        raise ValueError(f"unaligned crypto 5m slug epoch: {slug!r}")
    return round_start, round_start + CRYPTO_5M_WINDOW_S
