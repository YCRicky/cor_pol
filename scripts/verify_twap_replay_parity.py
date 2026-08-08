#!/usr/bin/env python3
"""Verify the committed tail candidate gate against the recorded feature cache.

This is intentionally a read-only check.  It selects candidates without
looking at ``pm_winner`` and only reads that final PM/UMA label when counting
the reported wins and losses.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from aftertake.twap_tail import SUPPORTED_ASSETS, TailRuleConfig, replay_feature_decision  # noqa: E402


def _split_by_asset(rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_asset: dict[str, list[dict[str, Any]]] = {asset: [] for asset in SUPPORTED_ASSETS}
    for row in rows:
        asset = str(row.get("asset", "")).upper()
        if asset in by_asset:
            by_asset[asset].append(row)
    train: list[dict[str, Any]] = []
    test: list[dict[str, Any]] = []
    for asset in SUPPORTED_ASSETS:
        ordered = sorted(by_asset[asset], key=lambda row: (int(row["start_ts_ms"]), int(row["round"])))
        if len(ordered) < 2:
            raise ValueError(f"{asset} has fewer than two feature rows")
        cut = max(1, min(len(ordered) - 1, int(len(ordered) * 0.60)))
        train.extend(ordered[:cut])
        test.extend(ordered[cut:])
    return train, test


def _score(rows: Iterable[dict[str, Any]], config: TailRuleConfig) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    for row in rows:
        accepted, reason = replay_feature_decision(row, config=config)
        if accepted:
            selected.append(row)
        else:
            reasons[reason] += 1
    wins = sum(str(row["pm_side"]).upper() == str(row["pm_winner"]).upper() for row in selected)
    return {
        "input_rows": sum(reasons.values()) + len(selected),
        "eligible": len(selected),
        "wins": wins,
        "losses": len(selected) - wins,
        "skip_counts": dict(sorted(reasons.items())),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-cache", type=Path, required=True)
    parser.add_argument("--expected-train", type=int, default=250)
    parser.add_argument("--expected-test", type=int, default=193)
    args = parser.parse_args()

    payload = json.loads(args.feature_cache.read_text(encoding="utf-8"))
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError("feature cache has no records list")
    config = TailRuleConfig()
    config.validate()
    train_rows, test_rows = _split_by_asset(records)
    train = _score(train_rows, config)
    test = _score(test_rows, config)
    combined = {
        "eligible": train["eligible"] + test["eligible"],
        "wins": train["wins"] + test["wins"],
        "losses": train["losses"] + test["losses"],
    }
    result = {
        "strategy_version": config.strategy_version,
        "feature_cutoff_ms_before_end": int(config.decision_lead_s * 1000),
        "leader_rule": "bid >= 0.90",
        "source": "Binance Spot kline open + aggTrade path; PM/UMA label only",
        "train": train,
        "test": test,
        "combined": combined,
    }
    print(json.dumps(result, sort_keys=True))
    if (
        train["eligible"] != args.expected_train
        or train["losses"] != 0
        or test["eligible"] != args.expected_test
        or test["losses"] != 0
        or combined != {"eligible": 443, "wins": 443, "losses": 0}
    ):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
