"""Utility to load the project level .env file exactly once."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - handled by requirements install
    load_dotenv = None  # type: ignore

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
ENV_TEMPLATE = PROJECT_ROOT / ".env.template"


@lru_cache(maxsize=1)
def load_project_dotenv() -> Optional[Path]:
    """Load the repository-wide .env file if present."""
    target = ENV_PATH if ENV_PATH.exists() else ENV_TEMPLATE if ENV_TEMPLATE.exists() else None
    if not target:
        return None
    if load_dotenv is None:
        raise RuntimeError(
            "python-dotenv ist nicht installiert. Bitte `pip install python-dotenv` ausführen."
        )
    load_dotenv(target, override=False)
    return target
