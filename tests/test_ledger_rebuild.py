import json
from pathlib import Path

from misprice_pm.ledger import rebuild_ledger


def append(path: Path, row: dict):
    with path.open("a") as f:
        f.write(json.dumps(row) + "\n")


def test_rebuild_ledger_counts_pm_settles_only(tmp_path):
    log = tmp_path / "run.jsonl"
    append(log, {"kind": "trade_open", "slug": "a", "side": "NO", "entry_price": 0.44, "qty": 5})
    append(log, {"kind": "settle", "slug": "a", "settlement_source": "binance", "pnl": 99, "win": True})
    append(log, {"kind": "settle", "slug": "a", "settlement_source": "pm", "pnl": -2.38624, "win": False})
    append(log, {"kind": "settle", "slug": "b", "settlement_source": "jina", "pnl": 7, "win": True})

    ledger = rebuild_ledger([log])

    assert ledger.trades == 1
    assert ledger.wins == 0
    assert round(ledger.total_pnl, 6) == -2.38624
    assert ledger.rejected_settlements == 2


def test_rebuild_ledger_tracks_pending_trade_opens(tmp_path):
    log = tmp_path / "run.jsonl"
    append(log, {"kind": "trade_open", "slug": "a", "side": "YES", "entry_price": 0.6, "qty": 5, "trade_id": "t1"})

    ledger = rebuild_ledger([log])

    assert len(ledger.pending_trade_ids) == 1
    assert ledger.pending_trade_ids == {"t1"}
