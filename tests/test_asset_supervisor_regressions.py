import threading
import time

import aftertake.runner as runner_module
from aftertake.config import Settings
from aftertake.execution import OrderExecutor
from aftertake.post_close import PostCloseDecision
from aftertake.runner import _run_asset_rounds
from aftertake.state import StateStore


def test_asset_supervisor_allows_worker_to_finish_within_reconcile_timeout(
    tmp_path, monkeypatch
):
    """The post-close cleanup grace cannot be shorter than normal reconciliation."""

    store = StateStore(tmp_path / "state.sqlite3")
    completed = threading.Event()
    worker_elapsed = []
    settings = Settings(
        dry_run=True,
        assets=("BTC",),
        reconcile_timeout_s=0.20,
        out_dir=tmp_path / "out",
        state_db=tmp_path / "state.sqlite3",
    )

    def slow_but_healthy_round(**_kwargs):
        started = time.monotonic()
        time.sleep(0.05)
        worker_elapsed.append(time.monotonic() - started)
        completed.set()
        return [PostCloseDecision("hold", "clean_round")]

    monkeypatch.setattr(runner_module, "run_round", slow_but_healthy_round)
    monkeypatch.setattr(runner_module, "ASSET_ROUND_COMPLETION_GRACE_S", 0.005)
    monkeypatch.setattr(runner_module.time, "time", lambda: 1_200.25)

    try:
        results = _run_asset_rounds(
            settings=settings,
            store=store,
            public=object(),
            executor=OrderExecutor(settings, store),
            live_gateway=None,
            round_start=900,
            timeout_s=0.005,
        )
        assert completed.wait(timeout=0.5)
        assert worker_elapsed[0] < settings.reconcile_timeout_s
        assert results["BTC"][-1].reason == "clean_round"
    finally:
        completed.wait(timeout=0.5)
        store.close()
