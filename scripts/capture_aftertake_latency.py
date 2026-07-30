#!/usr/bin/env python3
"""Capture public CLOB arrival and residual-ask survival without placing orders.

This experiment is deliberately read-only: it uses Gamma and the public CLOB
WebSocket, never creates an authenticated client, never calls an account or
order endpoint, and has no Telegram dependency.  It measures the timing that
can be observed from the client side; it cannot by itself prove a venue's
internal queue ordering or undocumented matching rules.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aftertake.config import DEFAULT_ASSETS, Settings
from aftertake.live_sizing import compute_live_entry_size
from aftertake.market_stream import MarketBookStream
from aftertake.pm_client import BalanceAllowance, MarketMetadata, PolymarketPublicClient
from aftertake.post_close import (
    PairedBook,
    PostCloseConfig,
    PostCloseDecision,
    PostCloseWinnerClassifier,
    classifier_family_config,
)
from aftertake.runner import current_crypto_5m_slug

ROOT = Path(__file__).resolve().parents[1]
PRE_CLOSE_CAPTURE_S = 12.0
POST_CLOSE_CAPTURE_S = 2.5
PROFILE_DEFINITIONS = (
    ("v67_legacy_100_100_3", 0.100, 0.100, 3, "v67"),
    ("v67_current_50_100_3", 0.050, 0.100, 3, "v67"),
    ("v67_prior_50_050_3", 0.050, 0.050, 3, "v67"),
    ("v7_event_vacuum3", 0.050, 0.0, 2, "v7"),
    ("v8_clob_refill_guard_250ms", 0.050, 0.0, 2, "v8"),
)


@dataclass(frozen=True)
class Profile:
    name: str
    start_s: float
    spacing_s: float
    confirmations: int
    classifier: str


def _profile_config(profile: Profile) -> PostCloseConfig:
    base = classifier_family_config(profile.classifier)
    return replace(
        base,
        post_close_start_s=profile.start_s,
        confirmation_spacing_s=profile.spacing_s,
        confirmations=profile.confirmations,
    )


@dataclass(frozen=True)
class Candidate:
    profile: str
    observed_at: float
    side: str
    limit_price: float
    qty: float
    source_timestamp: Optional[float]
    confirmation_timestamps: Tuple[float, ...]
    confirmation_source_timestamps: Tuple[Optional[float], ...]


@dataclass
class ArrivalObservation:
    latency_ms: int
    observed_at: float
    book_age_at_target_ms: float
    ask: Optional[float]
    ask_size: float
    fully_marketable: bool


@dataclass
class PassiveAssetProbe:
    asset: str
    slug: str
    round_start: int
    round_end: int
    settings: Settings
    latencies_ms: Tuple[int, ...]
    profiles: Tuple[Profile, ...] = field(default_factory=tuple)
    callback_rows: List[Dict[str, Any]] = field(default_factory=list)
    candidates: Dict[str, Candidate] = field(default_factory=dict)
    arrivals: Dict[str, Dict[int, ArrivalObservation]] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._lock = threading.Lock()
        self._classifiers = {
            profile.name: PostCloseWinnerClassifier(_profile_config(profile))
            for profile in self.profiles
        }
        self._last_callback_at: Optional[float] = None
        self._last_book: Optional[PairedBook] = None
        self._metadata = MarketMetadata(
            condition_id="passive-latency-probe",
            tick_size="0.01",
            min_order_size=0.0,
            neg_risk=False,
            fee_rate=0.0,
            tokens={},
            raw={"mode": "public_read_only"},
        )
        self._collateral = BalanceAllowance(
            balance=self.settings.dry_run_simulated_balance,
            allowance=self.settings.dry_run_simulated_balance,
            raw={"simulated": True},
        )

    def on_book(self, book: PairedBook) -> None:
        """Receive a paired public-book snapshot on the WebSocket thread.

        The callback only performs local arithmetic and appends to memory;
        disk output happens after the bounded capture window finishes.
        """

        with self._lock:
            if self._last_callback_at is not None and book.observed_at <= self._last_callback_at:
                return
            previous = self._last_callback_at
            previous_book = self._last_book
            self._last_callback_at = book.observed_at
            self._last_book = book
            if self.round_end - PRE_CLOSE_CAPTURE_S <= book.observed_at <= self.round_end + POST_CLOSE_CAPTURE_S:
                self.callback_rows.append(
                    {
                        "observed_at": book.observed_at,
                        "source_timestamp": book.source_timestamp,
                        "source_to_receive_ms": _source_lag_ms(book),
                        "interarrival_ms": (
                            (book.observed_at - previous) * 1000.0 if previous is not None else None
                        ),
                        "yes_bid": book.yes.best_bid,
                        "yes_ask": book.yes.best_ask,
                        "no_bid": book.no.best_bid,
                        "no_ask": book.no.best_ask,
                    }
                )
            for classifier in self._classifiers.values():
                classifier.record(book)
            if not (self.round_end <= book.observed_at <= self.round_end + POST_CLOSE_CAPTURE_S):
                return
            for profile in self.profiles:
                self._capture_candidate(profile, book)
            self._observe_arrivals(previous_book, book)

    def _capture_candidate(self, profile: Profile, book: PairedBook) -> None:
        if profile.name in self.candidates:
            return
        classifier = self._classifiers[profile.name]
        initial = classifier.evaluate(
            round_end_ts=float(self.round_end), now_ts=book.observed_at, qty=self.settings.qty
        )
        candidate = self._candidate_from_decision(profile, initial, book)
        if candidate is None:
            return
        sized = classifier.evaluate(
            round_end_ts=float(self.round_end),
            now_ts=book.observed_at,
            qty=candidate.qty,
            min_near_touch_qty_multiplier=1.0,
        )
        if sized.action == "enter":
            self.candidates[profile.name] = candidate
            self.arrivals[profile.name] = {}

    def _candidate_from_decision(
        self, profile: Profile, decision: PostCloseDecision, book: PairedBook
    ) -> Optional[Candidate]:
        if decision.action != "enter" or decision.entry_ask is None or decision.entry_ask_size is None:
            return None
        sizing = compute_live_entry_size(
            price=decision.entry_ask,
            available_size=decision.entry_ask_size,
            collateral=self._collateral,
            metadata=self._metadata,
            max_account_fraction=self.settings.live_max_account_risk_fraction,
            quantity_step=self.settings.live_quantity_floor_step,
        )
        if not sizing.accepted:
            return None
        confirmations = tuple(float(value) for value in decision.audit.get("confirmation_timestamps") or ())
        source_by_observed = {
            row["observed_at"]: row["source_timestamp"] for row in self.callback_rows
        }
        confirmation_sources = tuple(source_by_observed.get(value) for value in confirmations)
        return Candidate(
            profile=profile.name,
            observed_at=book.observed_at,
            side=decision.side,
            limit_price=float(decision.entry_ask),
            qty=float(sizing.qty),
            source_timestamp=book.source_timestamp,
            confirmation_timestamps=confirmations,
            confirmation_source_timestamps=confirmation_sources,
        )

    def _observe_arrivals(self, previous_book: Optional[PairedBook], book: PairedBook) -> None:
        for profile, candidate in self.candidates.items():
            profile_arrivals = self.arrivals[profile]
            for latency_ms in self.latencies_ms:
                if latency_ms in profile_arrivals:
                    continue
                target = candidate.observed_at + latency_ms / 1000.0
                if book.observed_at < target:
                    continue
                # An order reaching the venue at ``target`` sees the latest
                # local state already known at that time, not a later callback
                # that happened to tell us the book changed afterwards.
                arrival_book = book if book.observed_at <= target else previous_book
                if arrival_book is None or arrival_book.observed_at < candidate.observed_at:
                    continue
                side = arrival_book.yes if candidate.side == "YES" else arrival_book.no
                ask = side.best_ask
                marketable = bool(
                    ask is not None
                    and ask <= candidate.limit_price + 1e-12
                    and side.ask_size + 1e-12 >= candidate.qty
                )
                profile_arrivals[latency_ms] = ArrivalObservation(
                    latency_ms=latency_ms,
                    observed_at=arrival_book.observed_at,
                    book_age_at_target_ms=round((target - arrival_book.observed_at) * 1000.0, 3),
                    ask=ask,
                    ask_size=side.ask_size,
                    fully_marketable=marketable,
                )

    def report(self) -> Dict[str, Any]:
        with self._lock:
            callbacks = list(self.callback_rows)
            candidates = dict(self.candidates)
            arrivals = {name: dict(values) for name, values in self.arrivals.items()}
        post = [row for row in callbacks if row["observed_at"] >= self.round_end]
        return {
            "kind": "aftertake_passive_latency_probe",
            "asset": self.asset,
            "slug": self.slug,
            "round_start": self.round_start,
            "round_end": self.round_end,
            "capture": {
                "mode": "public_websocket_only_no_orders_no_telegram",
                "callbacks_total": len(callbacks),
                "post_close_callbacks": len(post),
                "first_post_close_callback_ms": _first_offset_ms(post, self.round_end),
                "source_to_receive_ms": _distribution(row["source_to_receive_ms"] for row in callbacks),
                "interarrival_ms": _distribution(row["interarrival_ms"] for row in post),
            },
            "profiles": {
                profile.name: self._profile_report(profile, candidates, arrivals)
                for profile in self.profiles
            },
            "errors": list(self.errors),
        }

    def _profile_report(
        self,
        profile: Profile,
        candidates: Dict[str, Candidate],
        arrivals: Dict[str, Dict[int, ArrivalObservation]],
    ) -> Dict[str, Any]:
        candidate = candidates.get(profile.name)
        if candidate is None:
            return {**asdict(profile), "candidate": None}
        observed = arrivals.get(profile.name, {})
        return {
            **asdict(profile),
            "candidate": {
                "offset_ms": round((candidate.observed_at - self.round_end) * 1000.0, 3),
                "side": candidate.side,
                "limit_price": candidate.limit_price,
                "qty": candidate.qty,
                "source_to_receive_ms": _source_lag_ms_values(
                    candidate.observed_at, candidate.source_timestamp
                ),
                "confirmation_offsets_ms": [
                    round((value - self.round_end) * 1000.0, 3)
                    for value in candidate.confirmation_timestamps
                ],
            },
            "simulated_arrivals": {
                str(latency_ms): asdict(observed[latency_ms]) if latency_ms in observed else None
                for latency_ms in self.latencies_ms
            },
        }


def _source_lag_ms(book: PairedBook) -> Optional[float]:
    return _source_lag_ms_values(book.observed_at, book.source_timestamp)


def _source_lag_ms_values(observed_at: float, source_timestamp: Optional[float]) -> Optional[float]:
    if source_timestamp is None or not math.isfinite(float(source_timestamp)):
        return None
    return round((float(observed_at) - float(source_timestamp)) * 1000.0, 3)


def _distribution(values: Iterable[Optional[float]]) -> Dict[str, Optional[float]]:
    cleaned = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
    if not cleaned:
        return {"count": 0, "min": None, "p50": None, "p95": None, "max": None}
    return {
        "count": len(cleaned),
        "min": round(cleaned[0], 3),
        "p50": round(statistics.median(cleaned), 3),
        "p95": round(_percentile(cleaned, 0.95), 3),
        "max": round(cleaned[-1], 3),
    }


def _percentile(values: List[float], quantile: float) -> float:
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def _first_offset_ms(rows: List[Dict[str, Any]], round_end: int) -> Optional[float]:
    if not rows:
        return None
    return round((float(rows[0]["observed_at"]) - float(round_end)) * 1000.0, 3)


def _profiles() -> Tuple[Profile, ...]:
    return tuple(Profile(*definition) for definition in PROFILE_DEFINITIONS)


def _parse_assets(raw: str) -> Tuple[str, ...]:
    assets = tuple(asset.strip().upper() for asset in raw.split(",") if asset.strip())
    invalid = sorted(set(assets) - set(DEFAULT_ASSETS))
    if invalid:
        raise ValueError("unsupported assets: %s" % ",".join(invalid))
    return assets or DEFAULT_ASSETS


def _write_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(payload, sort_keys=True) + "\n")
        output.flush()


def _capture_asset(
    *, asset: str, round_start: int, settings: Settings, latencies_ms: Tuple[int, ...], out_dir: Path
) -> Dict[str, Any]:
    slug, expected_start, round_end = current_crypto_5m_slug(asset, round_start)
    if expected_start != round_start:
        raise RuntimeError("round boundary did not map to expected slug")
    public = PolymarketPublicClient()
    market = public.market_by_slug(slug)
    probe = PassiveAssetProbe(
        asset=asset,
        slug=slug,
        round_start=round_start,
        round_end=round_end,
        settings=settings,
        latencies_ms=latencies_ms,
        profiles=_profiles(),
    )
    stream = MarketBookStream(
        yes_token_id=market.token_for_side("YES"),
        no_token_id=market.token_for_side("NO"),
        on_book=probe.on_book,
    )
    stream.start()
    try:
        deadline = round_end + POST_CLOSE_CAPTURE_S
        while time.time() < deadline:
            time.sleep(min(0.05, max(0.0, deadline - time.time())))
        if stream.last_error:
            probe.errors.append(stream.last_error)
    finally:
        stream.close()
    payload = probe.report()
    _write_jsonl(out_dir / f"passive_latency_{slug}.jsonl", payload)
    return payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--assets", default=",".join(DEFAULT_ASSETS))
    parser.add_argument("--latencies-ms", default="100,300,700,1000")
    parser.add_argument("--out-dir", type=Path, default=ROOT / "out" / "aftertake_passive_latency")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.rounds <= 0:
        raise SystemExit("--rounds must be > 0")
    assets = _parse_assets(args.assets)
    try:
        latencies_ms = tuple(sorted({int(value) for value in args.latencies_ms.split(",") if value.strip()}))
    except ValueError as exc:
        raise SystemExit("--latencies-ms must be comma-separated integers") from exc
    if not latencies_ms or latencies_ms[0] < 0:
        raise SystemExit("--latencies-ms must contain non-negative values")
    settings = Settings.from_env()
    if not settings.dry_run:
        settings = Settings(
            dry_run=True,
            assets=settings.assets,
            asset=settings.asset,
            qty=settings.qty,
            live_max_account_risk_fraction=settings.live_max_account_risk_fraction,
            live_quantity_floor_step=settings.live_quantity_floor_step,
            dry_run_simulated_balance=settings.dry_run_simulated_balance,
        )
        settings.validate()
    next_round = int(time.time()) - int(time.time()) % 300 + 300
    for _ in range(args.rounds):
        capture_start = next_round + 300 - int(PRE_CLOSE_CAPTURE_S)
        while time.time() < capture_start:
            time.sleep(min(0.25, capture_start - time.time()))
        threads: List[threading.Thread] = []
        payloads: List[Dict[str, Any]] = []
        payload_lock = threading.Lock()
        current_round = next_round

        def run_asset(
            asset: str,
            *,
            round_start: int = current_round,
            collected: List[Dict[str, Any]] = payloads,
            collected_lock: threading.Lock = payload_lock,
        ) -> None:
            try:
                payload = _capture_asset(
                    asset=asset,
                    round_start=round_start,
                    settings=settings,
                    latencies_ms=latencies_ms,
                    out_dir=args.out_dir,
                )
            except Exception as exc:
                payload = {
                    "kind": "aftertake_passive_latency_probe",
                    "asset": asset,
                    "round_start": round_start,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                _write_jsonl(args.out_dir / f"passive_latency_errors_{round_start}.jsonl", payload)
            with collected_lock:
                collected.append(payload)

        for asset in assets:
            thread = threading.Thread(target=run_asset, args=(asset,), daemon=False, name=f"probe-{asset}")
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        print(json.dumps({"kind": "round_complete", "round_start": current_round, "assets": len(payloads)}), flush=True)
        next_round += 300
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
