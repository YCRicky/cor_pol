from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cor_pol_ec2_deployment_replaces_the_legacy_main_py_service_contract():
    deploy = (ROOT / "deploy" / "ec2" / "deploy_cor_pol.sh").read_text(encoding="utf-8")
    unit = (ROOT / "deploy" / "systemd" / "cor-pol.service.example").read_text(
        encoding="utf-8"
    )

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
