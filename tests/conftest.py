from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SHARED_SRC = ROOT / "packages" / "sparepart-shared" / "src"

if str(SHARED_SRC) not in sys.path:
    sys.path.insert(0, str(SHARED_SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
