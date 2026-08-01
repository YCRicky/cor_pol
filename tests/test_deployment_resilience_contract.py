from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_systemd_restart_policy_cannot_be_exhausted_by_repeated_runtime_failures():
    """A recoverable crash loop must not leave the live service permanently stopped."""

    unit = (ROOT / "deploy" / "systemd" / "cor-pol.service.example").read_text(
        encoding="utf-8"
    )

    assert "Restart=always" in unit
    rate_limit_disabled = (
        "StartLimitIntervalSec=0" in unit or "StartLimitBurst=0" in unit
    )
    assert rate_limit_disabled, "systemd start-rate limiting must be explicitly disabled"


def test_ec2_deploy_runs_regressions_clock_gate_and_pid_stability_check():
    script = (ROOT / "deploy" / "ec2" / "deploy_cor_pol.sh").read_text(encoding="utf-8")

    assert "NTPSynchronized" in script
    assert "-m ruff check" in script
    assert "-m pytest -q" in script
    assert "-m compileall" in script
    assert "initial_pid=" in script
    assert "current_pid=" in script
    assert "systemctl is-active --quiet cor-pol" in script
