from misprice_pm.config import Settings


def test_original_strategy_risk_defaults_are_present(monkeypatch):
    for name in (
        "MISPRICE_MAX_DAILY_LOSS",
        "MISPRICE_MAX_OPEN_POSITIONS",
        "MISPRICE_MAX_CONSECUTIVE_LOSSES",
        "MISPRICE_MIN_SECONDS_BETWEEN_ENTRIES",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = Settings.from_env()

    assert settings.max_daily_loss == 25.0
    assert settings.max_open_positions == 1
    assert settings.max_consecutive_losses == 5
    assert settings.min_seconds_between_entries == 60
