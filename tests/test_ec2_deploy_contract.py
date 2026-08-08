from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cor_pol_ec2_deployment_replaces_the_legacy_main_py_service_contract():
    deploy = (ROOT / "deploy" / "ec2" / "deploy_cor_pol.sh").read_text(encoding="utf-8")
    unit = (ROOT / "deploy" / "systemd" / "cor-pol.service.example").read_text(
        encoding="utf-8"
    )
    runner = (ROOT / "src" / "aftertake" / "runner.py").read_text(encoding="utf-8")

    assert 'REPO_DIR="/opt/cor_pol"' in deploy
    assert 'UNIT_SOURCE="${REPO_DIR}/deploy/systemd/cor-pol.service.example"' in deploy
    assert 'UNIT_TARGET="/etc/systemd/system/cor-pol.service"' in deploy
    assert "systemctl restart cor-pol" in deploy
    assert "AFTERTAKE_DRY_RUN" in deploy
    assert "/opt/aftertake" not in deploy
    assert "main.py" not in deploy

    assert "EnvironmentFile=/opt/cor_pol/.env" in unit
    assert "Environment=PYTHONPATH=/opt/cor_pol/src" in unit
    assert "ExecStartPre" not in unit
    assert "ExecStart=/opt/cor_pol/.venv/bin/python -m aftertake.runner --forever" in unit
    assert "/opt/aftertake" not in unit
    assert "main.py" not in unit

    # systemd starts from StateDirectory, not the checkout. Runtime revision
    # probes must therefore not execute Git and emit fatal journal messages.
    assert "_code_revision" not in runner
    assert "git rev-parse" not in runner
    assert '["git"' not in runner


def test_ec2_deploy_enforces_multi_asset_live_universe_without_touching_secrets():
    deploy = (ROOT / "deploy" / "ec2" / "deploy_cor_pol.sh").read_text(encoding="utf-8")
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")

    assert "AFTERTAKE_STRATEGY=twap_tail_v2" in env_example
    assert "AFTERTAKE_ASSETS=BTC,ETH,SOL,XRP,BNB,DOGE" in env_example
    assert "AFTERTAKE_QTY=5" in env_example
    assert "AFTERTAKE_TAIL_DECISION_LEAD_S=10.25" in env_example
    assert "AFTERTAKE_TAIL_LEADER_BID_THRESHOLD=0.90" in env_example
    assert "AFTERTAKE_TAIL_MAX_DECISION_LATENESS_S=0.25" in env_example
    assert "AFTERTAKE_TAIL_BINANCE_MAX_TRADE_AGE_S=2.0" in env_example
    assert "AFTERTAKE_TAIL_WEAK_CANDLE_ABS_MOVE_BPS=5.0" in env_example
    assert "AFTERTAKE_TAIL_WEAK_PATH_REVERSAL_BPS=2.0" in env_example
    assert "AFTERTAKE_TAIL_LIMIT_PRICE=0.99" in env_example
    assert "AFTERTAKE_ASSET=BTC" not in env_example
    assert "AFTERTAKE_MAX_OPEN_POSITIONS=3" in env_example

    assert "normalize_runtime_env" in deploy
    assert deploy.index("normalize_runtime_env()") < deploy.index("normalize_runtime_env\n")
    assert "ensure_env_kv AFTERTAKE_STRATEGY twap_tail_v2" in deploy
    assert "ensure_env_kv AFTERTAKE_ASSETS BTC,ETH,SOL,XRP,BNB,DOGE" in deploy
    assert "ensure_env_default AFTERTAKE_QTY 5" in deploy
    assert "ensure_env_default AFTERTAKE_ORDER_TYPE GTC" in deploy
    assert "ensure_env_kv AFTERTAKE_TAIL_DECISION_LEAD_S 10.25" in deploy
    assert "ensure_env_kv AFTERTAKE_TAIL_LEADER_BID_THRESHOLD 0.90" in deploy
    assert "ensure_env_kv AFTERTAKE_TAIL_MAX_DECISION_LATENESS_S 0.25" in deploy
    assert "ensure_env_kv AFTERTAKE_TAIL_BINANCE_MAX_TRADE_AGE_S 2.0" in deploy
    assert "ensure_env_kv AFTERTAKE_TAIL_WEAK_CANDLE_ABS_MOVE_BPS 5.0" in deploy
    assert "ensure_env_kv AFTERTAKE_TAIL_WEAK_PATH_REVERSAL_BPS 2.0" in deploy
    assert "ensure_env_kv AFTERTAKE_TAIL_LIMIT_PRICE 0.99" in deploy
    assert "require_twap_tail_contract" in deploy
    assert "AFTERTAKE_QTY must be a positive number" in deploy
    assert "AFTERTAKE_QTY must be 50" not in deploy
    assert "AFTERTAKE_TAIL_DECISION_LEAD_S must be 10.25" in deploy
    assert "AFTERTAKE_TAIL_LEADER_BID_THRESHOLD must be 0.90" in deploy
    assert "AFTERTAKE_TAIL_MAX_DECISION_LATENESS_S must be 0.25" in deploy
    assert "comment_out_legacy_env AFTERTAKE_ASSET" in deploy
    assert "POLYMARKET_PRIVATE_KEY" in deploy  # still only validates, never rewrites secrets
    assert "ensure_env_kv POLYMARKET_PRIVATE_KEY" not in deploy
