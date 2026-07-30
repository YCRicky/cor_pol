from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
replay = pytest.importorskip("scripts.replay_aftertake_profiles")


def test_market_record_treats_slug_epoch_as_round_start(monkeypatch, tmp_path):
    frame = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "local_timestamp": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "event_type": ["book"],
            "winning_outcome": ["up"],
        }
    )
    observed_round_ends = []

    monkeypatch.setattr(replay, "_download", lambda *_args: Path("unused.parquet"))
    monkeypatch.setattr(replay.pd, "read_parquet", lambda _path: frame)

    def capture_slice(source, round_end, latency_s):
        observed_round_ends.append((source, round_end, latency_s))
        return source

    monkeypatch.setattr(replay, "_replay_slice", capture_slice)
    monkeypatch.setattr(replay, "_reconstructed_books", lambda _frame: iter(()))
    monkeypatch.setattr(replay, "_profiles", lambda: ())

    replay._market_record(
        "btc-updown-5m-900",
        key="",
        cache=tmp_path,
        latency_s=0.970,
        sizing_cfg=replay.ReplaySizing(5.0, 100.0, 0.5, 1.0),
        fingerprint="test",
    )

    assert observed_round_ends == [(frame, 1200, 0.970)]
