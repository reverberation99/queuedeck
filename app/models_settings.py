import json
from datetime import datetime, timezone
from typing import Any, Dict

from .db import get_db


def _ensure_schema() -> None:
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT '',
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS user_admin_settings (
            user_id INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, key)
        );
        """
    )
    db.commit()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stringify(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list, bool, int, float)):
        return json.dumps(v)
    return str(v)


def get_settings() -> Dict[str, str]:
    _ensure_schema()
    db = get_db()
    rows = db.execute(
        "SELECT key, value FROM app_settings ORDER BY key"
    ).fetchall()
    return {str(r["key"]): str(r["value"] or "") for r in rows}


def get_setting(key: str, default: str = "") -> str:
    if not key:
        return default
    _ensure_schema()
    db = get_db()
    row = db.execute(
        "SELECT value FROM app_settings WHERE key = ?",
        (key,),
    ).fetchone()
    if not row:
        return default
    return str(row["value"] or default)


def set_setting(key: str, value: str) -> None:
    if not key:
        return
    _ensure_schema()
    db = get_db()
    db.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (key, str(value or ""), _now_iso()),
    )
    db.commit()


def update_settings(payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return

    _ensure_schema()
    db = get_db()

    for k, v in payload.items():
        if not k:
            continue
        db.execute(
            """
            INSERT INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (str(k), _stringify(v), _now_iso()),
        )

    db.commit()


def delete_setting(key: str) -> None:
    if not key:
        return
    _ensure_schema()
    db = get_db()
    db.execute("DELETE FROM app_settings WHERE key = ?", (key,))
    db.commit()


def get_user_admin_settings(user_id: int) -> Dict[str, str]:
    if int(user_id or 0) <= 0:
        return {}
    _ensure_schema()
    db = get_db()
    rows = db.execute(
        "SELECT key, value FROM user_admin_settings WHERE user_id = ? ORDER BY key",
        (int(user_id),),
    ).fetchall()
    return {str(r["key"]): str(r["value"] or "") for r in rows}


def set_user_admin_setting(user_id: int, key: str, value: str) -> None:
    if int(user_id or 0) <= 0 or not key:
        return
    _ensure_schema()
    db = get_db()
    db.execute(
        """
        INSERT INTO user_admin_settings (user_id, key, value, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (int(user_id), str(key), str(value or ""), _now_iso()),
    )
    db.commit()


def update_user_admin_settings(user_id: int, payload: Dict[str, Any]) -> None:
    if int(user_id or 0) <= 0 or not isinstance(payload, dict):
        return
    _ensure_schema()
    db = get_db()
    for k, v in payload.items():
        if not k:
            continue
        db.execute(
            """
            INSERT INTO user_admin_settings (user_id, key, value, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (int(user_id), str(k), _stringify(v), _now_iso()),
        )
    db.commit()


def delete_user_admin_setting(user_id: int, key: str) -> None:
    if int(user_id or 0) <= 0 or not key:
        return
    _ensure_schema()
    db = get_db()
    db.execute(
        "DELETE FROM user_admin_settings WHERE user_id = ? AND key = ?",
        (int(user_id), str(key)),
    )
    db.commit()


def get_user_setting_scoped(user_id: int, key: str, default: str = "") -> str:
    """
    Resolution order:
      1. admin-managed per-user settings
      2. legacy user_settings table
      3. default
    """
    if int(user_id or 0) <= 0 or not key:
        return default

    _ensure_schema()
    db = get_db()

    row = db.execute(
        "SELECT value FROM user_admin_settings WHERE user_id = ? AND key = ?",
        (int(user_id), str(key)),
    ).fetchone()
    if row and row["value"] is not None and str(row["value"]).strip() != "":
        return str(row["value"]).strip()

    row = db.execute(
        "SELECT value FROM user_settings WHERE user_id = ? AND key = ?",
        (int(user_id), str(key)),
    ).fetchone()
    if row and row["value"] is not None and str(row["value"]).strip() != "":
        return str(row["value"]).strip()

    return default


def get_current_user_scoped_setting(key: str, default: str = "") -> str:
    """
    Request-context aware helper:
      1. admin-managed per-user setting
      2. legacy user setting
      3. global app_setting
      4. default
    """
    try:
        from flask import has_request_context
        if has_request_context():
            from .utils.auth import current_user
            me = current_user() or {}
            user_id = int(me.get("user_id") or 0)
            if user_id > 0:
                v = get_user_setting_scoped(user_id, key, default="")
                if str(v).strip() != "":
                    return str(v).strip()
    except Exception:
        pass

    v = get_setting(key, default="")
    if str(v).strip() != "":
        return str(v).strip()

    return default
