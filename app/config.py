import os
from typing import Optional

from .models_settings import get_setting


def get_cfg(key: str, env_name: Optional[str] = None, default: str = "") -> str:
    """
    DB-first config lookup with optional ENV fallback.

    - Reads from SQLite app_settings first
    - If empty/missing, falls back to ENV (if env_name provided)
    - Returns a string (never None)
    """
    val = (get_setting(key, "") or "").strip()
    if val:
        return val

    if env_name:
        return (os.getenv(env_name, default) or "").strip()

    return (default or "").strip()
