import importlib.util
import sys
from pathlib import Path

from aftertake.config import Settings
from aftertake.post_close import PairedBook, SideBook


def _probe_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "capture_aftertake_latency.py"
    spec = importlib.util.spec_from_file_location("capture_aftertake_latency", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _preclose_book(ts):
    return PairedBook(
        ts,
        SideBook(0.50, 100, 100, 0.52, 100, 100),
        SideBook(0.48, 100, 100, 0.50, 100, 100),
        ts - 0.01,
    )


def _postclose_book(ts):
    return PairedBook(
        ts,
        SideBook(0.35, 2, 2, 0.37, 100, 2),
        SideBook(0.58, 100, 100, 0.85, 100, 100),
        ts - 0.01,
    )


def _postclose_ask_removed_book(ts):
    return PairedBook(
        ts,
        SideBook(0.35, 2, 2, 0.37, 100, 2),
        SideBook(0.58, 100, 100, 0.95, 100, 100),
        ts - 0.01,
    )


def test_passive_probe_records_candidate_and_simulated_arrival_without_orders():
    module = _probe_module()
    settings = Settings(dry_run=True, assets=("BTC",), asset="BTC")
    settings.validate()
    probe = module.PassiveAssetProbe(
        asset="BTC",
        slug="btc-updown-5m-1000",
        round_start=700,
        round_end=1_000,
        settings=settings,
        latencies_ms=(100, 300),
        profiles=module._profiles(),
    )

    for book in (
        _preclose_book(990.0),
        _preclose_book(995.0),
        _preclose_book(999.9),
        _postclose_book(1_000.055),
        _postclose_book(1_000.170),
        _postclose_book(1_000.290),
        _postclose_ask_removed_book(1_000.700),
    ):
        probe.on_book(book)

    report = probe.report()
    profile = report["profiles"]["production_50_100_3"]

    assert report["capture"]["callbacks_total"] == 7
    assert profile["candidate"]["side"] == "NO"
    assert profile["candidate"]["offset_ms"] == 290.0
    assert profile["simulated_arrivals"]["300"]["fully_marketable"] is True
    assert profile["simulated_arrivals"]["300"]["observed_at"] == 1_000.29
    assert profile["simulated_arrivals"]["300"]["ask"] == 0.85
