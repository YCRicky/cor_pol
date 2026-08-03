import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_railway_worker_contract_is_singleton_and_non_sleeping():
    config = json.loads((REPO_ROOT / "railway.json").read_text(encoding="utf-8"))
    deploy = config["deploy"]

    assert config["build"] == {"builder": "DOCKERFILE", "dockerfilePath": "Dockerfile"}
    assert deploy["startCommand"] == "python -m aftertake.runner --forever"
    assert deploy["numReplicas"] == 1
    assert deploy["sleepApplication"] is False
    assert deploy["restartPolicyType"] == "ON_FAILURE"
    assert deploy["restartPolicyMaxRetries"] == 3
    assert deploy["requiredMountPath"] == "/data"
    assert deploy["overlapSeconds"] == 0
    assert deploy["drainingSeconds"] == 20
    assert "healthcheckPath" not in deploy
    assert "healthcheckTimeout" not in deploy
    assert "port" not in deploy


def test_dockerfile_is_explicit_and_excludes_env_files():
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    copy_lines = [line.strip() for line in dockerfile.splitlines() if line.strip().startswith("COPY ")]

    assert copy_lines == ["COPY pyproject.toml README.md ./", "COPY src ./src"]
    assert ".env" not in "\n".join(copy_lines)
    assert "pip install --no-cache-dir \".[live]\"" in dockerfile
    assert "PYTHONUNBUFFERED=1" in dockerfile
    assert "AFTERTAKE_OUT_DIR=/data/out" in dockerfile
    assert 'CMD ["python", "-m", "aftertake.runner", "--forever"]' in dockerfile
