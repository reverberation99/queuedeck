import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from flask import Blueprint, jsonify, request, session, render_template

from .db import get_db
from .models_settings import get_user_setting_scoped
from .utils.auth import current_user
from .clients.jellyfin import clear_nextup_cache

bp = Blueprint("actions", __name__)


@bp.post("/api/action/discover_hide")
def discover_hide():
    guard = _require_login()
    if guard:
        return guard

    try:
        user_id = _current_user_id()
        payload = request.get_json(silent=True) or {}

        media_type = str(payload.get("media_type") or "").strip().lower()
        tmdb_id = str(payload.get("tmdb_id") or "").strip()
        title = str(payload.get("title") or "").strip()
        year = str(payload.get("year") or "").strip()

        if not media_type:
            media_type = "tv"

        if tmdb_id:
            item_id = f"{media_type}:{tmdb_id}"
        else:
            key = "".join(ch for ch in title.lower() if ch.isalnum())
            item_id = f"{media_type}:{key}::{year}"

        _upsert_state(user_id, "discover_item", item_id, hidden=True)
        return jsonify(ok=True, kind="discover_item", item_id=item_id)

    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

# -----------------------------
# Auth guard
# -----------------------------
def _is_logged_in() -> bool:
    return bool(session.get("logged_in") or session.get("user") or session.get("username"))

def _require_login():
    if not _is_logged_in():
        return jsonify({"ok": False, "error": "not_logged_in"}), 401
    return None

def _current_user_id() -> int:
    me = current_user() or {}
    return int(me.get("user_id") or 0)

# -----------------------------
# Small in-process cache for enrichment
# -----------------------------
_ENRICH_CACHE: Dict[str, Tuple[int, Dict[str, str]]] = {}
_ENRICH_TTL_SECONDS = 6 * 60 * 60  # 6 hours

def _cache_get(key: str) -> Optional[Dict[str, str]]:
    now = int(time.time())
    hit = _ENRICH_CACHE.get(key)
    if not hit:
        return None
    ts, val = hit
    if (now - ts) > _ENRICH_TTL_SECONDS:
        _ENRICH_CACHE.pop(key, None)
        return None
    return val

def _cache_set(key: str, val: Dict[str, str]) -> None:
    _ENRICH_CACHE[key] = (int(time.time()), val)

# -----------------------------
# State table (hide/snooze) - PER USER
# -----------------------------
def _ensure_state_schema() -> None:
    db = get_db()

    # Does qd_state already exist?
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='qd_state'"
    ).fetchone()

    if not row:
        db.execute(
            """
            CREATE TABLE qd_state (
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                item_id TEXT NOT NULL,
                hidden INTEGER NOT NULL DEFAULT 0,
                snooze_until INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT,
                PRIMARY KEY (user_id, kind, item_id)
            )
            """
        )
        db.commit()
        return

    # Check existing columns
    cols = db.execute("PRAGMA table_info(qd_state)").fetchall()
    col_names = [str(c["name"]) for c in cols]

    # Already on new schema
    if "user_id" in col_names:
        return

    # Old shared schema detected -> migrate to new per-user schema.
    # We intentionally do NOT copy old rows because they were global/shared
    # and cannot be safely assigned to a specific user.
    db.execute("ALTER TABLE qd_state RENAME TO qd_state_old")

    db.execute(
        """
        CREATE TABLE qd_state (
            user_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            item_id TEXT NOT NULL,
            hidden INTEGER NOT NULL DEFAULT 0,
            snooze_until INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY (user_id, kind, item_id)
        )
        """
    )

    db.execute("DROP TABLE qd_state_old")
    db.commit()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _now_ts() -> int:
    return int(time.time())

def _upsert_state(user_id: int, kind: str, item_id: str, hidden: Optional[bool] = None, snooze_until: Optional[int] = None) -> None:
    _ensure_state_schema()
    db = get_db()

    row = db.execute(
        """
        SELECT user_id, kind, item_id, hidden, snooze_until
        FROM qd_state
        WHERE user_id=? AND kind=? AND item_id=?
        """,
        (user_id, kind, item_id),
    ).fetchone()

    cur_hidden = int(row["hidden"]) if row else 0
    cur_until = int(row["snooze_until"]) if row else 0

    if hidden is not None:
        cur_hidden = 1 if hidden else 0
    if snooze_until is not None:
        cur_until = int(snooze_until)

    db.execute(
        """
        INSERT INTO qd_state(user_id, kind, item_id, hidden, snooze_until, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, kind, item_id) DO UPDATE SET
          hidden=excluded.hidden,
          snooze_until=excluded.snooze_until,
          updated_at=excluded.updated_at
        """,
        (user_id, kind, item_id, cur_hidden, cur_until, _now_iso()),
    )
    db.commit()

def _get_state_all(user_id: int) -> Dict[str, Dict[str, Any]]:
    _ensure_state_schema()
    db = get_db()
    rows = db.execute(
        """
        SELECT user_id, kind, item_id, hidden, snooze_until, updated_at
        FROM qd_state
        WHERE user_id=?
        """,
        (user_id,),
    ).fetchall()

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        k = f"{r['kind']}:{r['item_id']}"
        out[k] = {
            "kind": r["kind"],
            "item_id": r["item_id"],
            "hidden": bool(int(r["hidden"] or 0)),
            "snooze_until": int(r["snooze_until"] or 0),
            "updated_at": r["updated_at"] or "",
        }
    return out

def _get_hidden_and_snoozed_rows(user_id: int) -> Tuple[list, list, int]:
    _ensure_state_schema()
    db = get_db()
    now = _now_ts()

    hidden_rows = db.execute(
        """
        SELECT user_id, kind, item_id, hidden, snooze_until, updated_at
        FROM qd_state
        WHERE user_id=? AND hidden=1
        ORDER BY updated_at DESC
        """,
        (user_id,),
    ).fetchall()

    snoozed_rows = db.execute(
        """
        SELECT user_id, kind, item_id, hidden, snooze_until, updated_at
        FROM qd_state
        WHERE user_id=? AND hidden=0 AND snooze_until > ?
        ORDER BY snooze_until DESC
        """,
        (user_id, now),
    ).fetchall()

    return hidden_rows, snoozed_rows, now

# -----------------------------
# Config helpers (PER USER)
# -----------------------------
def _cfg(db_key: str, env_key: str, fallback: str = "") -> str:
    user_id = _current_user_id()
    if user_id <= 0:
        return ""
    try:
        return get_user_setting_scoped(user_id, db_key, default="")
    except Exception:
        return ""

def _sonarr_base_and_key() -> Tuple[str, str]:
    base = _cfg("sonarr_url", "SONARR_URL", "").rstrip("/")
    api_key = _cfg("sonarr_api_key", "SONARR_API_KEY", "").strip()
    return base, api_key

def _radarr_base_and_key() -> Tuple[str, str]:
    base = _cfg("radarr_url", "RADARR_URL", "").rstrip("/")
    api_key = _cfg("radarr_api_key", "RADARR_API_KEY", "").strip()
    return base, api_key

# -----------------------------
# Jellyfin helpers
# -----------------------------
def _jellyfin_base_and_key() -> Tuple[str, str]:
    base = _cfg("jellyfin_url", "JELLYFIN_URL", "").rstrip("/")
    api_key = _cfg("jellyfin_api_key", "JELLYFIN_API_KEY", "").strip()
    return base, api_key

def _jellyfin_headers(api_key: str) -> Dict[str, str]:
    return {"X-Emby-Token": api_key}

def _jellyfin_username() -> str:
    return _cfg("jellyfin_user", "JELLYFIN_USER", "").strip()

def _jellyfin_get_user_id(base: str, api_key: str, username: str) -> Optional[str]:
    cached = _cfg("jellyfin_user_id", "", "").strip()
    cached_user = _cfg("jellyfin_user_id_for", "", "").strip()
    if cached and cached_user.lower() == (username or "").lower():
        return cached

    if not base or not api_key or not username:
        return None

    r = requests.get(f"{base}/Users", headers=_jellyfin_headers(api_key), timeout=12)
    r.raise_for_status()
    users = r.json() or []
    for u in users:
        if str(u.get("Name", "")).lower() == username.lower():
            uid = str(u.get("Id", "")).strip()
            return uid or None
    return None

def _jellyfin_get_item(base: str, api_key: str, item_id: str) -> Dict[str, Any]:
    username = _jellyfin_username()
    uid = None
    if username:
        try:
            uid = _jellyfin_get_user_id(base, api_key, username)
        except Exception:
            uid = None

    if uid:
        try:
            r = requests.get(
                f"{base}/Users/{uid}/Items/{item_id}",
                headers=_jellyfin_headers(api_key),
                timeout=12,
            )
            if r.status_code < 400:
                return r.json() or {}
        except Exception:
            pass

    r = requests.get(f"{base}/Items/{item_id}", headers=_jellyfin_headers(api_key), timeout=12)
    r.raise_for_status()
    return r.json() or {}

def _jellyfin_open_series_url(base: str, api_key: str, item_id: str) -> Optional[str]:
    try:
        item = _jellyfin_get_item(base, api_key, item_id)
        series_id = (item.get("SeriesId") or item.get("ParentId") or "").strip()
        if not series_id:
            return f"{base}/web/index.html#!/details?id={item_id}"
        return f"{base}/web/index.html#!/details?id={series_id}"
    except Exception:
        return None

# -----------------------------
# Enrichment helpers
# -----------------------------
def _fmt_season_episode(s: Optional[int], e: Optional[int]) -> str:
    if s is None or e is None:
        return ""
    return f"S{int(s):02d}E{int(e):02d}"

def _enrich_row(kind: str, item_id: str) -> Dict[str, str]:
    user_id = _current_user_id()
    cache_key = f"{user_id}:{kind}:{item_id}"
    hit = _cache_get(cache_key)
    if hit:
        return hit

    title = ""
    subtitle = ""
    link = ""

    if kind.startswith("jellyfin_") or kind in ("remaining_series", "jellyfin_series"):
        base, api_key = _jellyfin_base_and_key()
        if base and api_key and item_id:
            try:
                it = _jellyfin_get_item(base, api_key, item_id)
                name = (it.get("Name") or "").strip()
                it_type = (it.get("Type") or "").strip()

                if it_type.lower() == "episode":
                    series = (it.get("SeriesName") or "").strip()
                    s = it.get("ParentIndexNumber")
                    e = it.get("IndexNumber")
                    se = _fmt_season_episode(s if isinstance(s, int) else None, e if isinstance(e, int) else None)

                    if series and se and name:
                        title = f"{series} • {se} • {name}"
                    elif series and se:
                        title = f"{series} • {se}"
                    elif series and name:
                        title = f"{series} • {name}"
                    else:
                        title = series or name or f"Jellyfin Episode ({item_id})"

                    series_id = (it.get("SeriesId") or it.get("ParentId") or "").strip()
                    link = f"{base}/web/index.html#!/details?id={series_id or item_id}"

                else:
                    title = name or f"Jellyfin Item ({item_id})"
                    link = f"{base}/web/index.html#!/details?id={item_id}"

                    year = it.get("ProductionYear")
                    if isinstance(year, int):
                        subtitle = str(year)

            except Exception:
                title = f"{kind} • {item_id}"

    elif kind == "sonarr_episode":
        base, api_key = _sonarr_base_and_key()
        if base and api_key and item_id:
            try:
                r = requests.get(
                    f"{base}/api/v3/episode/{item_id}",
                    headers={"X-Api-Key": api_key},
                    timeout=15,
                )
                r.raise_for_status()
                ep = r.json() or {}
                ep_title = (ep.get("title") or "").strip()
                season = ep.get("seasonNumber")
                number = ep.get("episodeNumber")
                se = _fmt_season_episode(
                    season if isinstance(season, int) else None,
                    number if isinstance(number, int) else None
                )

                series_title = ""
                series_id = ep.get("seriesId")
                if series_id is not None:
                    rs = requests.get(
                        f"{base}/api/v3/series/{series_id}",
                        headers={"X-Api-Key": api_key},
                        timeout=15,
                    )
                    rs.raise_for_status()
                    s = rs.json() or {}
                    series_title = (s.get("title") or "").strip()
                    link = f"{base}/series/{series_id}"

                if series_title and se and ep_title:
                    title = f"{series_title} • {se} • {ep_title}"
                elif series_title and se:
                    title = f"{series_title} • {se}"
                elif series_title and ep_title:
                    title = f"{series_title} • {ep_title}"
                else:
                    title = ep_title or f"Sonarr Episode ({item_id})"

                air_utc = ep.get("airDateUtc") or ep.get("airDate")
                if air_utc:
                    subtitle = f"Airs: {air_utc}"

            except Exception:
                title = f"sonarr_episode • {item_id}"

    elif kind == "sonarr_series":
        base, api_key = _sonarr_base_and_key()
        if base and api_key and item_id:
            try:
                r = requests.get(
                    f"{base}/api/v3/series/{item_id}",
                    headers={"X-Api-Key": api_key},
                    timeout=15,
                )
                r.raise_for_status()
                s = r.json() or {}

                series_title = (s.get("title") or "").strip()
                year = s.get("year")
                title = f"{series_title} ({year})" if (series_title and isinstance(year, int)) else (series_title or f"Sonarr Series ({item_id})")
                link = f"{base}/series/{item_id}"

                status = (s.get("status") or "").strip()
                network = (s.get("network") or "").strip()
                subtitle = " • ".join([x for x in (status, network) if x])

            except Exception:
                title = f"sonarr_series • {item_id}"

    elif kind == "radarr_movie":
        # This card may be sourced from:
        # - a numeric Radarr internal movie id
        # - a numeric TMDb id
        # - or a Jellyfin item id (guid-like string)
        if item_id and str(item_id).isdigit():
            base, api_key = _radarr_base_and_key()
            if base and api_key:
                try:
                    m = None

                    # Try Radarr internal movie id first
                    r = requests.get(
                        f"{base}/api/v3/movie/{item_id}",
                        headers={"X-Api-Key": api_key},
                        timeout=15,
                    )
                    if r.ok:
                        maybe = r.json() or {}
                        if isinstance(maybe, dict) and maybe.get("id"):
                            m = maybe

                    # If not found, try matching item_id as TMDb id against the Radarr movie list
                    if not m:
                        r2 = requests.get(
                            f"{base}/api/v3/movie",
                            headers={"X-Api-Key": api_key},
                            timeout=20,
                        )
                        if r2.ok:
                            rows = r2.json() or []
                            try:
                                tmdb_id_int = int(item_id)
                            except Exception:
                                tmdb_id_int = 0
                            if isinstance(rows, list) and tmdb_id_int:
                                m = next((x for x in rows if int(x.get("tmdbId") or 0) == tmdb_id_int), None)

                    if m:
                        t = (m.get("title") or "").strip()
                        y = m.get("year")
                        radarr_id = m.get("id")
                        title = f"{t} ({y})" if (t and isinstance(y, int)) else (t or f"Radarr Movie ({item_id})")
                        if radarr_id:
                            link = f"{base}/movie/{radarr_id}"

                        for key in ("physicalRelease", "digitalRelease", "inCinemas"):
                            v = m.get(key)
                            if v:
                                subtitle = f"Release: {v}"
                                break
                    else:
                        title = f"Radarr Movie ({item_id})"

                except Exception:
                    title = f"Radarr Movie ({item_id})"
        else:
            # Fallback: treat it like a Jellyfin movie item id
            base, api_key = _jellyfin_base_and_key()
            if base and api_key and item_id:
                try:
                    it = _jellyfin_get_item(base, api_key, item_id)
                    name = (it.get("Name") or "").strip()
                    year = it.get("ProductionYear")

                    if name and isinstance(year, int):
                        title = f"{name} ({year})"
                    else:
                        title = name or f"Movie ({item_id})"

                    link = f"{base}/web/index.html#!/details?id={item_id}"

                    if isinstance(year, int):
                        subtitle = str(year)

                except Exception:
                    title = f"Movie ({item_id})"

    elif kind == "discover_item":
        try:
            from .routes_discover import (
                _tmdb_is_configured,
                _tmdb_auth_headers,
                _tmdb_auth_params,
                enrich_tmdb_item_by_id,
            )

            media_type = ""
            raw_id = str(item_id or "").strip()

            if ":" in raw_id:
                media_type, raw_id = raw_id.split(":", 1)
                media_type = media_type.strip().lower()
                raw_id = raw_id.strip()

            if raw_id.isdigit():
                tmdb_id = int(raw_id)
                kind_part = "tv" if media_type == "tv" else "movie"

                if _tmdb_is_configured():
                    enriched = enrich_tmdb_item_by_id(
                        tmdb_id=tmdb_id,
                        media_type=kind_part,
                        headers=_tmdb_auth_headers(),
                        auth_params=_tmdb_auth_params(),
                    )
                    if enriched:
                        t = str(enriched.get("title") or "").strip()
                        y = str(enriched.get("year") or "").strip()
                        title = f"{t} ({y})" if (t and y) else (t or f"Discover Item ({item_id})")
                        link = f"https://www.themoviedb.org/{kind_part}/{tmdb_id}"

                        genres = enriched.get("genres") or []
                        if isinstance(genres, list) and genres:
                            names = []
                            for g in genres[:3]:
                                if isinstance(g, dict):
                                    n = str(g.get("name") or "").strip()
                                    if n:
                                        names.append(n)
                            if names:
                                subtitle = " • ".join(names)

            if not title:
                title = f"Discover Item ({item_id})"

        except Exception:
            title = f"Discover Item ({item_id})"

    if not title:
        title = f"{kind} • {item_id}"

    out = {"title": title, "subtitle": subtitle, "link": link}
    _cache_set(cache_key, out)
    return out

def _rows_to_dicts(rows, enrich: bool) -> list:
    out = []
    for r in rows:
        kind = r["kind"]
        item_id = r["item_id"]
        d = {
            "kind": kind,
            "item_id": item_id,
            "hidden": bool(int(r["hidden"] or 0)),
            "snooze_until": int(r["snooze_until"] or 0),
            "updated_at": r["updated_at"] or "",
        }
        if enrich:
            d.update(_enrich_row(kind, item_id))
        else:
            d.update({"title": "", "subtitle": "", "link": ""})
        out.append(d)
    return out

# -----------------------------
# Routes: Hidden page
# -----------------------------
@bp.get("/hidden")
def hidden_page():
    guard = _require_login()
    if guard:
        return guard
    return render_template("hidden.html", me=(current_user() or {}))

# -----------------------------
# Routes: state
# -----------------------------
@bp.get("/api/state/list")
def state_list():
    guard = _require_login()
    if guard:
        return guard
    user_id = _current_user_id()
    return jsonify({"ok": True, "state": _get_state_all(user_id), "now": _now_ts()})

@bp.get("/api/state/hidden")
def state_hidden():
    guard = _require_login()
    if guard:
        return guard

    user_id = _current_user_id()
    enrich = str(request.args.get("enrich") or "1").strip().lower() not in ("0", "false", "no")
    hidden_rows, snoozed_rows, now = _get_hidden_and_snoozed_rows(user_id)

    return jsonify({
        "ok": True,
        "now": now,
        "hidden": _rows_to_dicts(hidden_rows, enrich),
        "snoozed": _rows_to_dicts(snoozed_rows, enrich),
    })

@bp.post("/api/state/hide")
def state_hide():
    guard = _require_login()
    if guard:
        return guard

    user_id = _current_user_id()
    data = request.get_json(silent=True) or {}
    kind = str(data.get("kind") or "").strip()
    item_id = str(data.get("item_id") or "").strip()
    if not kind or not item_id:
        return jsonify({"ok": False, "error": "missing_kind_or_item_id"}), 400

    _upsert_state(user_id, kind, item_id, hidden=True)
    clear_nextup_cache()
    return jsonify({"ok": True})

@bp.post("/api/state/unhide")
def state_unhide():
    guard = _require_login()
    if guard:
        return guard

    user_id = _current_user_id()
    data = request.get_json(silent=True) or {}
    kind = str(data.get("kind") or "").strip()
    item_id = str(data.get("item_id") or "").strip()
    if not kind or not item_id:
        return jsonify({"ok": False, "error": "missing_kind_or_item_id"}), 400

    _upsert_state(user_id, kind, item_id, hidden=False)
    return jsonify({"ok": True})

@bp.post("/api/state/snooze")
def state_snooze():
    guard = _require_login()
    if guard:
        return guard

    user_id = _current_user_id()
    data = request.get_json(silent=True) or {}
    kind = str(data.get("kind") or "").strip()
    item_id = str(data.get("item_id") or "").strip()
    seconds = int(data.get("seconds") or 0)

    if not kind or not item_id or seconds <= 0:
        return jsonify({"ok": False, "error": "missing_kind_item_id_or_seconds"}), 400

    until = _now_ts() + seconds
    _upsert_state(user_id, kind, item_id, snooze_until=until)
    return jsonify({"ok": True, "snooze_until": until})

@bp.post("/api/state/unsnooze")
def state_unsnooze():
    guard = _require_login()
    if guard:
        return guard

    user_id = _current_user_id()
    data = request.get_json(silent=True) or {}
    kind = str(data.get("kind") or "").strip()
    item_id = str(data.get("item_id") or "").strip()
    if not kind or not item_id:
        return jsonify({"ok": False, "error": "missing_kind_or_item_id"}), 400

    _upsert_state(user_id, kind, item_id, snooze_until=0)
    return jsonify({"ok": True})

@bp.post("/api/state/unhide_all")
def state_unhide_all():
    guard = _require_login()
    if guard:
        return guard

    user_id = _current_user_id()
    _ensure_state_schema()
    db = get_db()
    db.execute(
        "UPDATE qd_state SET hidden=0, updated_at=? WHERE user_id=?",
        (_now_iso(), user_id),
    )
    db.commit()
    return jsonify({"ok": True})

@bp.post("/api/state/clear_snoozes")
def state_clear_snoozes():
    guard = _require_login()
    if guard:
        return guard

    user_id = _current_user_id()
    _ensure_state_schema()
    db = get_db()
    db.execute(
        "UPDATE qd_state SET snooze_until=0, updated_at=? WHERE user_id=?",
        (_now_iso(), user_id),
    )
    db.commit()
    return jsonify({"ok": True})

# -----------------------------
# Routes: Jellyfin actions
# -----------------------------
@bp.post("/api/jellyfin/mark-played")
def jellyfin_mark_played():
    guard = _require_login()
    if guard:
        return guard

    data = request.get_json(silent=True) or {}
    item_id = str(data.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"ok": False, "error": "missing_item_id"}), 400

    base, api_key = _jellyfin_base_and_key()
    username = _jellyfin_username()
    if not base or not api_key or not username:
        return jsonify({"ok": False, "error": "jellyfin_not_configured"}), 400

    uid = _jellyfin_get_user_id(base, api_key, username)
    if not uid:
        return jsonify({"ok": False, "error": "jellyfin_user_id_not_found"}), 400

    r = requests.post(
        f"{base}/Users/{uid}/PlayedItems/{item_id}",
        headers=_jellyfin_headers(api_key),
        timeout=12,
    )
    if r.status_code >= 400:
        return jsonify({"ok": False, "error": "jellyfin_error", "status": r.status_code, "body": r.text[:300]}), 502

    clear_nextup_cache()
    return jsonify({"ok": True})

@bp.post("/api/jellyfin/mark-unplayed")
def jellyfin_mark_unplayed():
    guard = _require_login()
    if guard:
        return guard

    data = request.get_json(silent=True) or {}
    item_id = str(data.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"ok": False, "error": "missing_item_id"}), 400

    base, api_key = _jellyfin_base_and_key()
    username = _jellyfin_username()
    if not base or not api_key or not username:
        return jsonify({"ok": False, "error": "jellyfin_not_configured"}), 400

    uid = _jellyfin_get_user_id(base, api_key, username)
    if not uid:
        return jsonify({"ok": False, "error": "jellyfin_user_id_not_found"}), 400

    r = requests.delete(
        f"{base}/Users/{uid}/PlayedItems/{item_id}",
        headers=_jellyfin_headers(api_key),
        timeout=12,
    )
    if r.status_code >= 400:
        return jsonify({"ok": False, "error": "jellyfin_error", "status": r.status_code, "body": r.text[:300]}), 502

    clear_nextup_cache()
    return jsonify({"ok": True})

@bp.get("/api/jellyfin/open-series")
def jellyfin_open_series():
    guard = _require_login()
    if guard:
        return guard

    item_id = str(request.args.get("item_id") or "").strip()
    if not item_id:
        return jsonify({"ok": False, "error": "missing_item_id"}), 400

    base, api_key = _jellyfin_base_and_key()
    if not base or not api_key:
        return jsonify({"ok": False, "error": "jellyfin_not_configured"}), 400

    url = _jellyfin_open_series_url(base, api_key, item_id)
    if not url:
        return jsonify({"ok": False, "error": "series_url_not_found"}), 404

    return jsonify({"ok": True, "url": url})


@bp.post("/api/action/stats_hide")
def stats_hide():
    guard = _require_login()
    if guard:
        return guard

    try:
        user_id = _current_user_id()
        payload = request.get_json(silent=True) or {}

        section = str(payload.get("section") or "").strip().lower()
        item_key = str(payload.get("item_key") or "").strip()
        label = str(payload.get("label") or "").strip()

        allowed = {
            "top_shows": "stats_top_show",
            "top_anime": "stats_top_anime",
            "top_genres": "stats_top_genre",
            "queue_health": "stats_queue_health",
            "current_activity": "stats_current_activity",
        }

        kind = allowed.get(section)
        if not kind:
            return jsonify(ok=False, error="invalid_section"), 400

        if not item_key:
            return jsonify(ok=False, error="missing_item_key"), 400

        _upsert_state(user_id, kind, item_key, hidden=True)
        return jsonify(ok=True, kind=kind, item_id=item_key, label=label)

    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@bp.post("/api/action/stats_unhide")
def stats_unhide():
    guard = _require_login()
    if guard:
        return guard

    try:
        user_id = _current_user_id()
        payload = request.get_json(silent=True) or {}

        kind = str(payload.get("kind") or "").strip()
        item_id = str(payload.get("item_id") or "").strip()

        if not kind or not item_id:
            return jsonify(ok=False, error="missing_kind_or_item_id"), 400

        allowed = {
            "stats_top_show",
            "stats_top_anime",
            "stats_top_genre",
            "stats_queue_health",
            "stats_current_activity",
        }
        if kind not in allowed:
            return jsonify(ok=False, error="invalid_kind"), 400

        _upsert_state(user_id, kind, item_id, hidden=False, snooze_until=0)
        return jsonify(ok=True, kind=kind, item_id=item_id)

    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@bp.get("/api/hidden/stats")
def hidden_stats_list():
    guard = _require_login()
    if guard:
        return guard

    try:
        user_id = _current_user_id()
        hidden_rows, _, _ = _get_hidden_and_snoozed_rows(user_id)

        wanted = {
            "stats_top_show",
            "stats_top_anime",
            "stats_top_genre",
            "stats_queue_health",
            "stats_current_activity",
        }

        out = []
        for r in hidden_rows:
            kind = str(r["kind"] or "")
            if kind not in wanted:
                continue

            item_id = str(r["item_id"] or "")
            updated_at = str(r["updated_at"] or "")

            label = item_id
            if "::" in item_id:
                parts = item_id.split("::", 1)
                if len(parts) == 2 and parts[1].strip():
                    label = parts[1].strip()

            out.append({
                "kind": kind,
                "item_id": item_id,
                "label": label,
                "updated_at": updated_at,
            })

        return jsonify(ok=True, items=out)

    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500
