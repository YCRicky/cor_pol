import pytest

from aftertake.config import Settings


def _clear_env(monkeypatch):
    monkeypatch.setattr("aftertake.config.load_dotenv", lambda *args, **kwargs: None)
    for name in (
        "AFTERTAKE_DRY_RUN",
        "AFTERTAKE_ASSET",
        "AFTERTAKE_QTY",
        "AFTERTAKE_OUT_DIR",
        "AFTERTAKE_MAX_DAILY_LOSS",
        "AFTERTAKE_MAX_OPEN_POSITIONS",
        "AFTERTAKE_MAX_CONSECUTIVE_LOSSES",
        "AFTERTAKE_MIN_SECONDS_BETWEEN_ENTRIES",
        "AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION",
        "AFTERTAKE_LIVE_QTY_FLOOR_STEP",
        "AFTERTAKE_DRY_RUN_SIM_BALANCE",
        "AFTERTAKE_RESOLVE_OVERRIDES",
        "CLOB_API_URL",
        "CHAIN_ID",
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_PASSPHRASE",
        "FUNDER_ADDRESS",
        "SIGNATURE_TYPE",
        "POLY_BUILDER_CODE",
        "TG_BOT_TOKEN",
        "TG_CHAT_ID",
    ):
        monkeypatch.delenv(name, raising=False)


def test_defaults_are_aftertake_only(monkeypatch):
    _clear_env(monkeypatch)
    settings = Settings.from_env()

    assert settings.dry_run is True
    assert settings.asset == "BTC"
    assert settings.qty == 5
    assert settings.live_max_account_risk_fraction == 0.5
    assert settings.live_quantity_floor_step == 1.0
    assert settings.dry_run_simulated_balance == 100.0
    assert settings.resolve_overrides
    assert settings.state_db.name == "aftertake.sqlite3"


def test_dry_run_sim_balance_can_be_configured(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("AFTERTAKE_DRY_RUN_SIM_BALANCE", "250")

    settings = Settings.from_env()

    assert settings.dry_run_simulated_balance == 250


def test_invalid_resolve_override_is_rejected(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("AFTERTAKE_RESOLVE_OVERRIDES", "gamma-api.polymarket.com=not-an-ip")

    with pytest.raises(ValueError, match="illegal IP"):
        Settings.from_env()


def test_live_requires_canonical_clob_identity(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("AFTERTAKE_DRY_RUN", "false")
    with pytest.raises(ValueError, match="POLYMARKET_PRIVATE_KEY"):
        Settings.from_env()


def test_live_accepts_requested_polymarket_account_environment(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("AFTERTAKE_DRY_RUN", "false")
    monkeypatch.setenv("CLOB_API_URL", "https://clob.polymarket.com")
    monkeypatch.setenv("CHAIN_ID", "137")
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "0x" + "a" * 64)
    monkeypatch.setenv("FUNDER_ADDRESS", "0x" + "b" * 40)
    monkeypatch.setenv("SIGNATURE_TYPE", '"2"')
    monkeypatch.setenv("POLYMARKET_API_KEY", "key")
    monkeypatch.setenv("POLYMARKET_API_SECRET", "secret")
    monkeypatch.setenv("POLYMARKET_PASSPHRASE", "passphrase")

    settings = Settings.from_env()
    assert settings.is_live is True
    assert settings.polymarket_signature_type == 2
    assert settings.has_static_api_creds is True


def test_partial_l2_credentials_are_rejected_in_shadow_mode(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("POLYMARKET_API_KEY", "key-only")
    with pytest.raises(ValueError, match="credentials"):
        Settings.from_env()
