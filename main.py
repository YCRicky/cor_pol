from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

for p in (str(ROOT), str(SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

from lab.correlation_arb_bot import main  # noqa: E402


if __name__ == "__main__":
    os.chdir(ROOT)
    main()
