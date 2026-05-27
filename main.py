from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_dotenv(ROOT / ".env")

for p in (str(ROOT), str(SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

from lab.correlation_arb_bot import main  # noqa: E402


if __name__ == "__main__":
    os.chdir(ROOT)
    main()
