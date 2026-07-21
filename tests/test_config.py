import pytest

from misprice_pm.config import Settings


def _clear_env(monkeypatch):
    monkeypatch.setattr("misprice_pm.config.load_dotenv", lambda *args, **kwargs: None)
    for name in (
        "MISPRICE_DRY_RUN",
        "MISPRICE_ASSET",
        "MISPRICE_BINANCE_SYMBOL",
        "MISPRICE_QTY",
        "MISPRICE_MIN_ENTRY_ASK",
        "MISPRICE_MAX_ENTRY_ASK",
        "MISPRICE_MIN_TRANSITION_BP",
        "MISPRICE_MAX_PRE_ABS_BP",
        "MISPRICE_MIN_ABS_BP",
        "MISPRICE_REPRICE_PER_BP",
        "MISPRICE_MIN_LAG_DEPTH",
        "MISPRICE_MIN_ELAPSED_S",
        "MISPRICE_MAX_ELAPSED_S",
        "MISPRICE_BAN_ELAPSED_START_S",
        "MISPRICE_BAN_ELAPSED_END_S",
        "MISPRICE_LOOP_INTERVAL_S",
        "MISPRICE_OUT_DIR",
        "MISPRICE_MAX_DAILY_LOSS",
        "MISPRICE_MAX_OPEN_POSITIONS",
        "MISPRICE_MAX_CONSECUTIVE_LOSSES",
        "MISPRICE_MIN_SECONDS_BETWEEN_ENTRIES",
        "PRIVATE_KEY",
        "CLOB_API_KEY",
        "CLOB_SECRET",
        "CLOB_PASS_PHRASE",
        "CLOB_SIGNATURE_TYPE",
        "CLOB_FUNDER_ADDRESS",
        "TG_BOT_TOKEN",
        "TG_CHAT_ID",
    ):
        monkeypatch.delenv(name, raising=False)


def test_defaults_use_v3_repricing_lag_strategy_controls(monkeypatch):
    _clear_env(monkeypatch)

    settings = Settings.from_env()

    assert settings.dry_run is True
    assert settings.max_entry_ask == 0.65
    assert settings.min_transition_bp == 3.0
    assert settings.max_pre_abs_bp == 2.5
    assert settings.min_abs_bp == 3.5
    assert settings.reprice_per_bp == 0.04
    assert settings.min_lag_depth == 0.035
    assert settings.min_elapsed_s == 20
    assert settings.max_elapsed_s == 220
    assert settings.ban_elapsed_start_s == -1
    assert settings.ban_elapsed_end_s == -1
    assert settings.max_daily_loss == 25.0
    assert settings.max_open_positions == 1
    assert settings.max_consecutive_losses == 5
    assert settings.min_seconds_between_entries == 60
    assert settings.polymarket_signature_type is None


def test_live_requires_canonical_account_identity(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("MISPRICE_DRY_RUN", "false")

    with pytest.raises(ValueError, match="PRIVATE_KEY"):
        Settings.from_env()


def test_cor_pol_proxy_account_environment_is_accepted(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("MISPRICE_DRY_RUN", "false")
    monkeypatch.setenv("PRIVATE_KEY", "0x" + "a" * 64)
    monkeypatch.setenv("CLOB_FUNDER_ADDRESS", "0x" + "b" * 40)
    monkeypatch.setenv("CLOB_SIGNATURE_TYPE", "POLY_PROXY")
    monkeypatch.setenv("CLOB_API_KEY", "old-key")
    monkeypatch.setenv("CLOB_SECRET", "old-secret")
    monkeypatch.setenv("CLOB_PASS_PHRASE", "old-passphrase")

    settings = Settings.from_env()

    assert settings.is_live is True
    assert settings.polymarket_signature_type == 1
    assert settings.has_static_api_creds is True
    assert settings.clob_api_key == "old-key"


def test_live_requires_the_established_cor_pol_l2_credential_set(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("MISPRICE_DRY_RUN", "false")
    monkeypatch.setenv("PRIVATE_KEY", "0x" + "a" * 64)
    monkeypatch.setenv("CLOB_FUNDER_ADDRESS", "0x" + "b" * 40)
    monkeypatch.setenv("CLOB_SIGNATURE_TYPE", "POLY_PROXY")

    with pytest.raises(ValueError, match="CLOB_API_KEY, CLOB_SECRET, CLOB_PASS_PHRASE"):
        Settings.from_env()


def test_partial_l2_credentials_are_rejected_even_in_dry_mode(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("CLOB_API_KEY", "key-only")

    with pytest.raises(ValueError, match="credentials"):
        Settings.from_env()
