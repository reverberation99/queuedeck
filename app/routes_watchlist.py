import os
import requests
from flask import Blueprint, render_template, jsonify, request
from datetime import datetime, timedelta, timezone

from .utils.auth import login_required, current_user
from .clients.sonarr import get_calendar, get_series_slug_map
from .clients.radarr import get_upcoming_missing, enrich_movies_with_tmdb_release_dates
from .clients.jellyfin_for_you import _fetch_recently_played, _get_user_id
from .models_settings import get_user_setting_scoped
from .db import get_db

bp = Blueprint("watchlist", __name__)


def _current_user_id():
    me = current_user() or {}
    return int(me.get("user_id") or 0)


def _user_setting(key: str) -> str:
    uid = _current_user_id()
    if uid <= 0:
        return ""
    try:
        return get_user_setting_scoped(uid, key, default="")
    except Exception:
        return ""



def _shared_setting(key: str) -> str:
    db = get_db()

    # admin-managed per-user connection settings
    try:
        row = db.execute(
            """
            SELECT uas.value
            FROM user_admin_settings uas
            JOIN users u ON u.id = uas.user_id
            WHERE uas.key = ?
              AND TRIM(COALESCE(uas.value, '')) <> ''
              AND COALESCE(u.is_admin, 0) = 1
            ORDER BY uas.updated_at DESC
            LIMIT 1
            """,
            (key,),
        ).fetchone()
        if row and str(row[0] or "").strip():
            return str(row[0]).strip()
    except Exception:
        pass

    # legacy/global settings table
    for table_name in ("settings", "app_settings"):
        try:
            row = db.execute(
                f"SELECT value FROM {table_name} WHERE key = ? LIMIT 1",
                (key,),
            ).fetchone()
            if row and str(row[0] or "").strip():
                return str(row[0]).strip()
        except Exception:
            pass

    return ""



def _has_user_connection(url_key: str, api_key_key: str) -> bool:
    return bool(_user_setting(url_key).strip() and _user_setting(api_key_key).strip())


def _cfg(key: str, env_key: str = "", fallback: str = "") -> str:
    v = _user_setting(key).strip()
    if v:
        return v

    v = _shared_setting(key).strip()
    if v:
        return v

    if env_key:
        return os.getenv(env_key, fallback).strip()
    return fallback


def _norm_title(s: str) -> str:
    s = str(s or "").strip().lower()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append(" ")
    return " ".join("".join(out).split())


def _jf_headers(api_key: str) -> dict[str, str]:
    return {
        "X-Emby-Token": api_key,
        "Accept": "application/json",
    }


def _safe_sonarr_calendar(start_date: str, end_date: str) -> list[dict]:
    base = _cfg("sonarr_url", "SONARR_URL", "").rstrip("/")
    api_key = _cfg("sonarr_api_key", "SONARR_API_KEY", "").strip()

    if not base or not api_key:
        return []

    try:
        r = requests.get(
            f"{base}/api/v3/calendar",
            headers={"X-Api-Key": api_key},
            params={
                "start": start_date,
                "end": end_date,
                "includeSeries": "true",
                "includeEpisodeFile": "true",
            },
            timeout=25,
        )
        r.raise_for_status()
        data = r.json() or []
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[watchlist-tv] sonarr calendar failed: {e}", flush=True)
        return []


def _seen_range_cutoff(seen_range: str, now: datetime | None = None):
    now = now or datetime.now(timezone.utc)
    sr = str(seen_range or "3m").strip().lower()

    if sr in {"all", "alltime", "year+", "plus"}:
        return None

    days_map = {
        "1m": 31,
        "3m": 92,
        "6m": 183,
        "1y": 366,
    }
    days = days_map.get(sr, 92)
    return now - timedelta(days=days)


def _seen_fetch_limit(seen_range: str) -> int:
    sr = str(seen_range or "3m").strip().lower()
    return {
        "1m": 250,
        "3m": 600,
        "6m": 1200,
        "1y": 2000,
        "all": 3000,
        "alltime": 3000,
        "year+": 3000,
        "plus": 3000,
    }.get(sr, 600)


def _seen_display_cap(seen_range: str) -> int:
    sr = str(seen_range or "3m").strip().lower()
    return {
        "1m": 60,
        "3m": 120,
        "6m": 180,
        "1y": 250,
        "all": 350,
        "alltime": 350,
        "year+": 350,
        "plus": 350,
    }.get(sr, 120)


def _fetch_recent_tv_titles(limit: int = 250) -> set[str]:
    base = _cfg("jellyfin_url", "JELLYFIN_URL", "").rstrip("/")
    api_key = _cfg("jellyfin_api_key", "JELLYFIN_API_KEY", "")
    username = _cfg("jellyfin_user", "JELLYFIN_USER", "")

    if not base or not api_key or not username:
        return set()

    try:
        user_id = _get_user_id(base, api_key, username)
        if not user_id:
            return set()

        params = {
            "Recursive": "true",
            "SortBy": "DatePlayed",
            "SortOrder": "Descending",
            "Limit": str(max(25, min(limit, 500))),
            "Filters": "IsPlayed",
            "IncludeItemTypes": "Episode,Series",
            "Fields": "Path",
            "EnableTotalRecordCount": "false",
            "EnableImages": "false",
        }

        r = requests.get(
            f"{base}/Users/{user_id}/Items",
            headers=_jf_headers(api_key),
            params=params,
            timeout=25,
        )
        r.raise_for_status()
        data = r.json() or {}
        rows = data.get("Items") or []

        out = set()
        for row in rows:
            item_type = str(row.get("Type") or "").strip().lower()
            if item_type == "episode":
                title = str(row.get("SeriesName") or "").strip()
            elif item_type == "series":
                title = str(row.get("Name") or "").strip()
            else:
                continue

            if title:
                out.add(_norm_title(title))

        return out
    except Exception as e:
        print(f"[watchlist-tv] jellyfin recent titles failed: {e}", flush=True)
        return set()



def _recommended_open_url(media_key: str, poster_url: str = "") -> str:
    base = _cfg("jellyfin_url", "JELLYFIN_URL", "").rstrip("/")
    if not base:
        return ""

    media_key = str(media_key or "").strip()
    poster_url = str(poster_url or "").strip()

    item_id = ""

    if media_key.startswith("jellyfin_movie:"):
        item_id = media_key.split(":", 1)[1].strip()
    elif media_key.startswith("jellyfin_series:"):
        item_id = media_key.split(":", 1)[1].strip()

    # fallback: extract Jellyfin item id from poster url like /Items/<id>/Images/Primary
    if (not item_id) and poster_url:
        marker = "/Items/"
        if marker in poster_url:
            try:
                tail = poster_url.split(marker, 1)[1]
                item_id = tail.split("/", 1)[0].strip()
            except Exception:
                item_id = ""

    if not item_id:
        # try TMDB fallback
        return _tmdb_overview(media_key)

    return f"{base}/web/#/details?id={item_id}"




def _jellyfin_series_tmdb_id(series_id: str) -> str:
    series_id = str(series_id or "").strip()
    if not series_id:
        return ""

    base = _cfg("jellyfin_url", "JELLYFIN_URL", "").rstrip("/")
    api_key = _cfg("jellyfin_api_key", "JELLYFIN_API_KEY", "").strip()
    if not base or not api_key:
        return ""

    try:
        r = requests.get(
            f"{base}/Items/{series_id}",
            headers=_jf_headers(api_key),
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}
        prov = data.get("ProviderIds") or {}
        return str(prov.get("Tmdb") or "").strip()
    except Exception:
        return ""
def _recommended_overview(media_key: str) -> str:
    media_key = str(media_key or "").strip()
    item_id = ""

    if media_key.startswith("jellyfin_movie:"):
        item_id = media_key.split(":", 1)[1].strip()
    elif media_key.startswith("jellyfin_series:"):
        item_id = media_key.split(":", 1)[1].strip()

    if not item_id:
        return _tmdb_overview(media_key)

    base = _cfg("jellyfin_url", "JELLYFIN_URL", "").rstrip("/")
    api_key = _cfg("jellyfin_api_key", "JELLYFIN_API_KEY", "").strip()
    username = _cfg("jellyfin_user", "JELLYFIN_USER", "").strip()
    if not base or not api_key:
        return ""

    try:
        user_id = _get_user_id(base, api_key, username) if username else ""
        url = f"{base}/Users/{user_id}/Items/{item_id}" if user_id else f"{base}/Items/{item_id}"

        r = requests.get(
            url,
            headers=_jf_headers(api_key),
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}

        overview = str(data.get("Overview") or "").strip()
        if overview:
            return overview

        prov = data.get("ProviderIds") or {}
        tmdb_id = str(prov.get("Tmdb") or "").strip()
        if tmdb_id:
            return _tmdb_overview(f"tmdb:{tmdb_id}")

        return ""
    except Exception:
        return ""


def _ensure_recommendation_tables():
    db = get_db()

    db.execute("""
    CREATE TABLE IF NOT EXISTS user_direct_recommendations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_user_id INTEGER NOT NULL,
        to_user_id INTEGER NOT NULL,
        media_kind TEXT NOT NULL,
        media_key TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT '',
        title TEXT NOT NULL DEFAULT '',
        poster_url TEXT NOT NULL DEFAULT '',
        note TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        seen INTEGER NOT NULL DEFAULT 0,
        UNIQUE(from_user_id, to_user_id, media_kind, media_key)
    )
    """)

    cols = [r[1] for r in db.execute("PRAGMA table_info(user_direct_recommendations)").fetchall()]
    if "seen" not in cols:
        db.execute("ALTER TABLE user_direct_recommendations ADD COLUMN seen INTEGER NOT NULL DEFAULT 0")

    db.execute("""
    CREATE TABLE IF NOT EXISTS user_recommendation_state (
        user_id INTEGER NOT NULL,
        media_kind TEXT NOT NULL,
        media_key TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (user_id, media_kind, media_key)
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS user_media_feedback (
        user_id INTEGER NOT NULL,
        media_kind TEXT NOT NULL,
        media_key TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT '',
        title TEXT NOT NULL DEFAULT '',
        poster_url TEXT NOT NULL DEFAULT '',
        rating INTEGER,
        recommended INTEGER NOT NULL DEFAULT 0,
        note TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (user_id, media_kind, media_key)
    )
    """)

    db.commit()


def _seerr_watchlist_payload() -> dict:
    try:
        from .routes_seerr import api_seerr_watchlist

        resp = api_seerr_watchlist()
        if isinstance(resp, tuple):
            resp = resp[0]

        if hasattr(resp, "get_json"):
            data = resp.get_json(silent=True) or {}
            return data if isinstance(data, dict) else {}

        return {}
    except Exception as e:
        print(f"[watchlist-movies] seerr fallback failed: {e}", flush=True)
        return {}



def _parse_dt(raw):
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return None


def _episode_tag(season, episode):
    try:
        return f"S{int(season):02d}E{int(episode):02d}"
    except Exception:
        return ""


def _countdown(dt):
    if not dt:
        return ""

    now = datetime.now(timezone.utc)
    delta = dt - now

    if delta.total_seconds() <= 0:
        return ""

    hours = int(delta.total_seconds() // 3600)
    days = delta.days

    if hours <= 6:
        return "Tonight"
    if days == 0:
        return f"In {hours}h"
    if days == 1:
        return "Tomorrow"
    if days < 7:
        return f"In {days} days"
    return f"In {days} days"


def _is_airing_soon(dt):
    if not dt:
        return False
    now = datetime.now(timezone.utc)
    return now <= dt <= (now + timedelta(hours=36))


def _series_poster_url(series: dict, sid: str) -> str:
    images = series.get("images") or []
    if isinstance(images, list):
        for img in images:
            if not isinstance(img, dict):
                continue
            cover_type = str(img.get("coverType") or "").lower()
            remote = str(img.get("remoteUrl") or img.get("url") or "")
            if cover_type in {"poster", "cover"} and remote:
                return remote

    return f"/img/sonarr/series/{sid}.jpg" if sid else ""


def _feedback_user_id() -> int:
    me = current_user() or {}
    return int(me.get("user_id") or 0)


def _normalize_media_kind(raw) -> str:
    kind = str(raw or "").strip().lower()
    return kind if kind in {"movie", "series"} else ""


def _normalize_media_key(raw) -> str:
    return str(raw or "").strip()


def _normalize_rating(raw):
    if raw in (None, "", "null"):
        return None
    try:
        val = int(raw)
    except Exception:
        return None
    if 1 <= val <= 5:
        return val
    return None


def _feedback_row_to_dict(row):
    if not row:
        return {
            "media_kind": "",
            "media_key": "",
            "source": "",
            "title": "",
            "poster_url": "",
            "rating": None,
            "recommended": False,
            "note": "",
            "updated_at": "",
        }

    return {
        "media_kind": str(row["media_kind"] or ""),
        "media_key": str(row["media_key"] or ""),
        "source": str(row["source"] or ""),
        "title": str(row["title"] or ""),
        "poster_url": str(row["poster_url"] or ""),
        "rating": (int(row["rating"]) if row["rating"] is not None else None),
        "recommended": bool(int(row["recommended"] or 0)),
        "note": str(row["note"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


@bp.get("/api/watchlist/feedback")
@login_required
def api_watchlist_feedback_get():
    _ensure_recommendation_tables()
    _ensure_recommendation_tables()
    uid = _feedback_user_id()
    media_kind = _normalize_media_kind(request.args.get("media_kind"))
    media_key = _normalize_media_key(request.args.get("media_key"))

    if uid <= 0:
        return jsonify(ok=False, error="not_logged_in"), 401
    if not media_kind or not media_key:
        return jsonify(ok=False, error="media_kind and media_key are required"), 400

    db = get_db()
    row = db.execute(
        """
        SELECT media_kind, media_key, source, title, poster_url, rating, recommended, note, updated_at
        FROM user_media_feedback
        WHERE user_id = ? AND media_kind = ? AND media_key = ?
        """,
        (uid, media_kind, media_key),
    ).fetchone()

    return jsonify(ok=True, feedback=_feedback_row_to_dict(row))


@bp.post("/api/watchlist/feedback")
@login_required
def api_watchlist_feedback_upsert():
    _ensure_recommendation_tables()
    _ensure_recommendation_tables()
    uid = _feedback_user_id()
    if uid <= 0:
        return jsonify(ok=False, error="not_logged_in"), 401

    payload = request.get_json(silent=True) or {}

    media_kind = _normalize_media_kind(payload.get("media_kind"))
    media_key = _normalize_media_key(payload.get("media_key"))
    source = str(payload.get("source") or "").strip()
    title = str(payload.get("title") or "").strip()
    poster_url = str(payload.get("poster_url") or "").strip()
    rating = _normalize_rating(payload.get("rating"))
    recommended = 1 if bool(payload.get("recommended")) else 0
    note = str(payload.get("note") or "").strip()

    if not media_kind or not media_key:
        return jsonify(ok=False, error="media_kind and media_key are required"), 400

    db = get_db()

    import time
    for _ in range(3):
        try:
            db.execute(
        """
        INSERT INTO user_media_feedback (
            user_id, media_kind, media_key, source, title, poster_url,
            rating, recommended, note, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ON CONFLICT(user_id, media_kind, media_key) DO UPDATE SET
            source = excluded.source,
            title = excluded.title,
            poster_url = excluded.poster_url,
            rating = excluded.rating,
            recommended = excluded.recommended,
            note = excluded.note,
            updated_at = datetime('now')
        """,
        (uid, media_kind, media_key, source, title, poster_url, rating, recommended, note),
    )
            db.commit()
            break
        except Exception as e:
            if "database is locked" in str(e).lower():
                time.sleep(0.1)
                continue
            raise

    row = db.execute(
        """
        SELECT media_kind, media_key, source, title, poster_url, rating, recommended, note, updated_at
        FROM user_media_feedback
        WHERE user_id = ? AND media_kind = ? AND media_key = ?
        """,
        (uid, media_kind, media_key),
    ).fetchone()

    return jsonify(ok=True, feedback=_feedback_row_to_dict(row))


@bp.get("/api/watchlist/recommendations")
@login_required
def api_watchlist_recommendations():
    _ensure_recommendation_tables()
    _ensure_recommendation_tables()
    rows = get_db().execute(
        """
        SELECT
            media_kind,
            media_key,
            source,
            title,
            poster_url,
            COUNT(*) AS recommend_count,
            ROUND(AVG(CASE WHEN rating BETWEEN 1 AND 5 THEN rating END), 2) AS avg_rating,
            MAX(updated_at) AS updated_at
        FROM user_media_feedback
        WHERE recommended = 1
        GROUP BY media_kind, media_key, source, title, poster_url
        ORDER BY recommend_count DESC, avg_rating DESC, updated_at DESC
        LIMIT 100
        """
    ).fetchall()

    items = []
    for row in rows:
        items.append({
            "media_kind": str(row["media_kind"] or ""),
            "media_key": str(row["media_key"] or ""),
            "source": str(row["source"] or ""),
            "title": str(row["title"] or ""),
            "poster_url": str(row["poster_url"] or ""),
            "recommend_count": int(row["recommend_count"] or 0),
            "avg_rating": (float(row["avg_rating"]) if row["avg_rating"] is not None else None),
            "updated_at": str(row["updated_at"] or ""),
        })

    return jsonify(ok=True, items=items)



def _ensure_direct_recommendations_table():
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS user_direct_recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user_id INTEGER NOT NULL,
            to_user_id INTEGER NOT NULL,
            media_kind TEXT NOT NULL,
            media_key TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            poster_url TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(from_user_id, to_user_id, media_kind, media_key),
            FOREIGN KEY (from_user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (to_user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    db.commit()


def _lookup_user_by_username(username: str):
    db = get_db()
    return db.execute(
        "SELECT id, username FROM users WHERE lower(username) = lower(?) LIMIT 1",
        (str(username or "").strip(),)
    ).fetchone()


@bp.post("/api/watchlist/recommend/direct")
@login_required
def api_watchlist_recommend_direct():
    _ensure_recommendation_tables()
    _ensure_recommendation_tables()
    uid = _feedback_user_id()
    if uid <= 0:
        return jsonify(ok=False, error="not_logged_in"), 401

    payload = request.get_json(silent=True) or {}

    media_kind = _normalize_media_kind(payload.get("media_kind"))
    media_key = _normalize_media_key(payload.get("media_key"))
    source = str(payload.get("source") or "").strip()
    title = str(payload.get("title") or "").strip()
    poster_url = str(payload.get("poster_url") or "").strip()
    note = str(payload.get("note") or "").strip()
    to_username = str(payload.get("to_username") or "").strip()

    if not media_kind or not media_key or not to_username:
        return jsonify(ok=False, error="media_kind, media_key, and to_username are required"), 400

    target = _lookup_user_by_username(to_username)
    if not target:
        return jsonify(ok=False, error="target_user_not_found"), 404

    to_user_id = int(target["id"])
    if to_user_id == uid:
        return jsonify(ok=False, error="cannot_recommend_to_self"), 400

    db = get_db()
    db.execute(
        """
        INSERT INTO user_direct_recommendations (
            from_user_id, to_user_id, media_kind, media_key, source, title, poster_url, note, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ON CONFLICT(from_user_id, to_user_id, media_kind, media_key) DO UPDATE SET
            source = excluded.source,
            title = excluded.title,
            poster_url = excluded.poster_url,
            note = excluded.note,
            updated_at = datetime('now')
        """,
        (uid, to_user_id, media_kind, media_key, source, title, poster_url, note),
    )
    db.commit()

    return jsonify(ok=True, to_user={"id": to_user_id, "username": str(target["username"] or "")})


@bp.post("/api/watchlist/recommendation_state")
@login_required
def api_watchlist_recommendation_state():
    _ensure_recommendation_tables()
    _ensure_recommendation_tables()
    uid = _feedback_user_id()
    if uid <= 0:
        return jsonify(ok=False, error="not_logged_in"), 401

    payload = request.get_json(silent=True) or {}

    media_kind = _normalize_media_kind(payload.get("media_kind"))
    media_key = _normalize_media_key(payload.get("media_key"))
    state = str(payload.get("state") or "").strip().lower()

    if not media_kind or not media_key:
        return jsonify(ok=False, error="media_kind and media_key are required"), 400

    if state not in {"active", "dismissed", "watched"}:
        return jsonify(ok=False, error="invalid_state"), 400

    db = get_db()

    if state == "active":
        db.execute(
            """
            DELETE FROM user_recommendation_state
            WHERE user_id = ? AND media_kind = ? AND media_key = ?
            """,
            (uid, media_kind, media_key),
        )
    else:
        db.execute(
            """
            INSERT INTO user_recommendation_state (user_id, media_kind, media_key, state, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(user_id, media_kind, media_key) DO UPDATE SET
                state = excluded.state,
                updated_at = datetime('now')
            """,
            (uid, media_kind, media_key, state),
        )

    db.commit()
    return jsonify(ok=True, state=state)



@bp.get("/api/watchlist/recommended_to_you")
@login_required
def api_watchlist_recommended_to_you():
    _ensure_recommendation_tables()
    _ensure_recommendation_tables()
    uid = _feedback_user_id()
    if uid <= 0:
        return jsonify(ok=False, error="not_logged_in"), 401

    db = get_db()

    state_rows = db.execute(
        """
        SELECT media_kind, media_key, state
        FROM user_recommendation_state
        WHERE user_id = ?
        """
        ,
        (uid,),
    ).fetchall()

    hidden = set()
    for row in state_rows:
        st = str(row["state"] or "").strip().lower()
        if st in {"dismissed", "watched"}:
            hidden.add((str(row["media_kind"] or ""), str(row["media_key"] or "")))

    direct_rows = db.execute(
        """
        SELECT
            r.media_kind,
            r.media_key,
            r.source,
            r.title,
            r.poster_url,
            r.note,
            r.created_at,
            u.username AS from_username
        FROM user_direct_recommendations r
        JOIN users u ON u.id = r.from_user_id
        WHERE r.to_user_id = ?
        ORDER BY r.updated_at DESC, r.created_at DESC
        LIMIT 100
        """,
        (uid,),
    ).fetchall()

    community_rows = db.execute(
        """
        SELECT
            f.media_kind,
            f.media_key,
            f.source,
            f.title,
            f.poster_url,
            COUNT(*) AS recommend_count,
            ROUND(AVG(CASE WHEN f.rating BETWEEN 1 AND 5 THEN f.rating END), 2) AS avg_rating,
            MAX(f.updated_at) AS updated_at
        FROM user_media_feedback f
        WHERE f.recommended = 1
          AND f.user_id <> ?
        GROUP BY f.media_kind, f.media_key, f.source, f.title, f.poster_url
        ORDER BY recommend_count DESC, avg_rating DESC, updated_at DESC
        LIMIT 100
        """,
        (uid,),
    ).fetchall()

    direct_items = []
    for row in direct_rows:
        if (str(row["media_kind"] or ""), str(row["media_key"] or "")) in hidden:
            continue
        direct_items.append({
            "media_kind": str(row["media_kind"] or ""),
            "media_key": str(row["media_key"] or ""),
            "source": str(row["source"] or ""),
            "title": str(row["title"] or ""),
            "poster_url": str(row["poster_url"] or ""),
            "open_url": _recommended_open_url(str(row["media_key"] or ""), str(row["poster_url"] or "")),
            "note": str(row["note"] or ""),
            "from_username": str(row["from_username"] or ""),
            "created_at": str(row["created_at"] or ""),
        })

    community_items = []
    for row in community_rows:
        if (str(row["media_kind"] or ""), str(row["media_key"] or "")) in hidden:
            continue
        community_items.append({
            "media_kind": str(row["media_kind"] or ""),
            "media_key": str(row["media_key"] or ""),
            "source": str(row["source"] or ""),
            "title": str(row["title"] or ""),
            "poster_url": str(row["poster_url"] or ""),
            "open_url": _recommended_open_url(str(row["media_key"] or ""), str(row["poster_url"] or "")),
            "overview": str(row.get("overview") or ""),
            "recommend_count": int(row["recommend_count"] or 0),
            "avg_rating": (float(row["avg_rating"]) if row["avg_rating"] is not None else None),
            "updated_at": str(row["updated_at"] or ""),
        })

    return jsonify(ok=True, direct=direct_items, community=community_items)



@bp.get("/api/watchlist/recommendations_sent")
@login_required
def api_watchlist_recommendations_sent():
    _ensure_recommendation_tables()
    _ensure_recommendation_tables()
    uid = _feedback_user_id()
    if uid <= 0:
        return jsonify(ok=False, error="not_logged_in"), 401

    db = get_db()

    direct_rows = db.execute(
        """
        SELECT r.media_kind, r.media_key, u.username
        FROM user_direct_recommendations r
        JOIN users u ON u.id = r.to_user_id
        WHERE r.from_user_id = ?
        ORDER BY lower(u.username) ASC
        """,
        (uid,),
    ).fetchall()

    global_rows = db.execute(
        """
        SELECT media_kind, media_key
        FROM user_media_feedback
        WHERE user_id = ? AND recommended = 1
        """,
        (uid,),
    ).fetchall()

    items = {}

    for row in direct_rows:
        key = f"{str(row['media_kind'] or '')}::{str(row['media_key'] or '')}"
        cur = items.get(key)
        if not cur:
            cur = {
                "media_kind": str(row["media_kind"] or ""),
                "media_key": str(row["media_key"] or ""),
                "recipients": [],
                "everyone": False,
            }
            items[key] = cur
        username = str(row["username"] or "").strip()
        if username and username not in cur["recipients"]:
            cur["recipients"].append(username)

    for row in global_rows:
        key = f"{str(row['media_kind'] or '')}::{str(row['media_key'] or '')}"
        cur = items.get(key)
        if not cur:
            cur = {
                "media_kind": str(row["media_kind"] or ""),
                "media_key": str(row["media_key"] or ""),
                "recipients": [],
                "everyone": False,
            }
            items[key] = cur
        cur["everyone"] = True

    return jsonify(ok=True, items=list(items.values()))



@bp.get("/api/watchlist/users")
@login_required
def api_watchlist_users():
    uid = _feedback_user_id()
    if uid <= 0:
        return jsonify(ok=False, error="not_logged_in"), 401

    rows = get_db().execute(
        """
        SELECT id, username
        FROM users
        WHERE id <> ?
        ORDER BY lower(username) ASC
        """
        ,
        (uid,),
    ).fetchall()

    users = []
    for row in rows:
        users.append({
            "id": int(row["id"]),
            "username": str(row["username"] or ""),
        })

    return jsonify(ok=True, users=users)



@bp.get("/watchlist")
@login_required
def watchlist_page():
    me = current_user() or {}
    return render_template("watchlist.html", me=me)


@bp.get("/api/watchlist")
@login_required
def api_watchlist():
    try:
        now = datetime.now(timezone.utc)
        seen_range = str(request.args.get("seen_range") or "3m").strip().lower()
        seen_cutoff = _seen_range_cutoff(seen_range, now)
        seen_fetch_limit = _seen_fetch_limit(seen_range)
        seen_display_cap = _seen_display_cap(seen_range)

        sonarr_ok = bool(_cfg("sonarr_url", "SONARR_URL") and _cfg("sonarr_api_key", "SONARR_API_KEY"))
        radarr_ok = bool(_cfg("radarr_url", "RADARR_URL") and _cfg("radarr_api_key", "RADARR_API_KEY"))
        sonarr_user_ok = _has_user_connection("sonarr_url", "sonarr_api_key")
        radarr_user_ok = _has_user_connection("radarr_url", "radarr_api_key")

        tv = []
        movies = []
        calendar_items = []

        # ================= TV =================
        if sonarr_ok:
            cal = _safe_sonarr_calendar(
                now.date().isoformat(),
                (now + timedelta(days=365)).date().isoformat()
            ) or []

            slug_map = get_series_slug_map(force=False) or {}
            base = _cfg("sonarr_url", "SONARR_URL").rstrip("/")
            watched_tv_titles = _fetch_recent_tv_titles(limit=250)

            tv_map = {}

            for ep in cal:
                dt = _parse_dt(ep.get("airDateUtc"))
                if not dt or dt < now:
                    continue

                series = ep.get("series") or {}
                sid = str(ep.get("seriesId") or series.get("id"))
                series_title = str(series.get("title") or "").strip()

                if (not sonarr_user_ok):
                    if watched_tv_titles and _norm_title(series_title) not in watched_tv_titles:
                        continue

                slug = slug_map.get(int(sid)) if sid.isdigit() else ""
                url = f"{base}/series/{slug or sid}"

                row = tv_map.get(sid)
                if not row or dt < _parse_dt(row["air_date"]):
                    row = {
                        "title": series.get("title"),
                        "poster_url": _series_poster_url(series, sid),
                        "air_date": dt.isoformat(),
                        "episode_title": ep.get("title"),
                        "episode_tag": _episode_tag(ep.get("seasonNumber"), ep.get("episodeNumber")),
                        "countdown": _countdown(dt),
                        "is_airing_soon": _is_airing_soon(dt),
                        "sonarr_url": url,
                    }
                    tv_map[sid] = row

                calendar_items.append({
                    "kind": "tv",
                    "source": "sonarr",
                    "title": series.get("title"),
                    "subtitle": f"{row['episode_tag']} • {row['episode_title']}",
                    "date": dt.isoformat(),
                    "poster_url": row["poster_url"],
                    "open_url": url,
                })

            tv = sorted(tv_map.values(), key=lambda x: _parse_dt(x["air_date"]))

        # ================= MOVIES =================
        if radarr_ok and radarr_user_ok:
            try:
                raw = get_upcoming_missing(days=365, limit=500) or []
                raw = enrich_movies_with_tmdb_release_dates(raw, region="US")
                base = _cfg("radarr_url", "RADARR_URL").rstrip("/")

                for m in raw:
                    radarr_digital = _parse_dt(m.get("digitalRelease"))
                    radarr_theater = _parse_dt(m.get("inCinemas"))

                    tmdb_digital = _parse_dt(m.get("tmdb_digital_release"))
                    tmdb_theater = _parse_dt(m.get("tmdb_theatrical_release"))
                    tmdb_physical = _parse_dt(m.get("tmdb_physical_release"))

                    digital = tmdb_digital or radarr_digital
                    theater = tmdb_theater or radarr_theater
                    physical = tmdb_physical

                    # REMOVE if already digitally released
                    if digital and digital <= now:
                        continue

                    # REMOVE useless items (old theater, no digital, no physical)
                    if (theater and theater <= now) and not digital and not physical:
                        continue

                    # determine state (future-focused)
                    if digital and digital > now:
                        state = "Digital Soon"
                    elif theater and theater > now:
                        state = "In Theaters"
                    else:
                        state = "Announced"

                    # choose best upcoming date
                    release_dt = digital or theater or physical

                    movies.append({
                        "title": m.get("title"),
                        "year": m.get("year"),
                        "tmdb_id": m.get("tmdb_id"),
                        "poster_url": f"/img/radarr/tmdb/{m.get('tmdb_id')}.jpg",
                        "status": state,
                        "release_date": release_dt.isoformat() if release_dt else "",
                        "digital_release": digital.isoformat() if digital else "",
                        "in_cinemas": theater.isoformat() if theater else "",
                        "physical_release": physical.isoformat() if physical else "",
                        "radarr_url": f"{base}/movie/{m.get('tmdb_id')}"
                    })

                    if release_dt and release_dt >= now:
                        calendar_items.append({
                            "kind": "movie",
                            "source": "radarr",
                            "title": m.get("title"),
                            "subtitle": state,
                            "date": release_dt.isoformat(),
                            "poster_url": f"/img/radarr/tmdb/{m.get('tmdb_id')}.jpg",
                            "open_url": f"{base}/movie/{m.get('tmdb_id')}"
                        })

                movies.sort(key=lambda x: _parse_dt(x["release_date"]) or datetime.max.replace(tzinfo=timezone.utc))
            except Exception as e:
                print(f"[watchlist-movies] radarr load failed: {e}", flush=True)

        
        # ================= SEEN =================
        seen = {"movies": [], "series": []}

        try:
            jf_base = _user_setting("jellyfin_url").rstrip("/")
            jf_key = _user_setting("jellyfin_api_key")
            jf_user = _user_setting("jellyfin_user")

            if jf_base and jf_key and jf_user:
                user_id = _get_user_id(jf_base, jf_key, jf_user)
                rows = _fetch_recently_played(jf_base, jf_key, user_id, limit=seen_fetch_limit)

                series_map = {}

                for row in rows:
                    typ = str(row.get("Type") or "").strip().lower()
                    played = row.get("DatePlayed")
                    played_dt = _parse_dt(played)

                    if seen_cutoff is not None:
                        # Only filter when Jellyfin actually gives us a valid played date.
                        # Missing DatePlayed should not wipe out the Seen list.
                        if played_dt and played_dt < seen_cutoff:
                            continue

                    if typ == "movie":
                        provider_ids = row.get("ProviderIds") or {}
                        tmdb_id = str(provider_ids.get("Tmdb") or "").strip()
                        media_key = f"tmdb:{tmdb_id}" if tmdb_id else f"jellyfin_movie:{row.get('Id')}"

                        seen["movies"].append({
                            "title": row.get("Name"),
                            "poster_url": f"/img/jellyfin/primary/{row.get('Id')}",
                            "date": played,
                            "media_kind": "movie",
                            "media_key": media_key,
                            "source": "jellyfin",
                        })

                    elif typ == "episode":
                        series_id = str(row.get("SeriesId") or "")
                        series_name = str(row.get("SeriesName") or row.get("Name") or "").strip()
                        if not series_name:
                            continue

                        key = series_id or series_name.lower()
                        cur = series_map.get(key)

                        if not cur:
                            cur = {
                                "title": series_name,
                                "poster_url": f"/img/jellyfin/series/{row.get('SeriesId') or row.get('Id')}",
                                "date": played,
                                "episode_count": 0,
                                "media_kind": "series",
                                "media_key": (
                                    f"tmdb:{_jellyfin_series_tmdb_id(row.get('SeriesId') or row.get('Id'))}"
                                    if _jellyfin_series_tmdb_id(row.get('SeriesId') or row.get('Id'))
                                    else f"jellyfin_series:{row.get('SeriesId') or row.get('Id')}"
                                ),
                                "source": "jellyfin",
                            }
                            series_map[key] = cur

                        cur["episode_count"] += 1

                        prev_dt = _parse_dt(cur.get("date"))
                        new_dt = _parse_dt(played)
                        if new_dt and (not prev_dt or new_dt > prev_dt):
                            cur["date"] = played

                seen["series"] = list(series_map.values())

                seen["movies"].sort(
                    key=lambda x: _parse_dt(x.get("date")) or datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True
                )
                seen["series"].sort(
                    key=lambda x: _parse_dt(x.get("date")) or datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True
                )

                seen["movies"] = seen["movies"][:seen_display_cap]
                seen["series"] = seen["series"][:seen_display_cap]

        except Exception as e:
            print(f"[seen] error: {e}", flush=True)

        if (not radarr_user_ok) and not movies:
            seerr_payload = _seerr_watchlist_payload()
            if seerr_payload.get("ok"):
                seerr_movies = [
                    row for row in (seerr_payload.get("movies") or [])
                    if isinstance(row, dict)
                ]
                if seerr_movies:
                    movies = seerr_movies

                for row in (seerr_payload.get("calendar") or []):
                    if not isinstance(row, dict):
                        continue
                    if str(row.get("kind") or "").strip().lower() != "movie":
                        continue
                    calendar_items.append(row)

        calendar_items.sort(key=lambda x: _parse_dt(x["date"]) or datetime.max.replace(tzinfo=timezone.utc))

        return jsonify(ok=True, tv=tv, movies=movies, calendar=calendar_items, seen=seen)

    except Exception as e:
        return jsonify(ok=False, error=str(e), tv=[], movies=[], calendar=[], seen={"movies":[],"series":[]}), 500


@bp.get("/api/watchlist/community-ratings")
@login_required
def api_watchlist_community_ratings():
    _ensure_recommendation_tables()

    db = get_db()

    rows = db.execute(
        """
        SELECT
            f.media_kind,
            f.media_key,
            MAX(f.source) AS source,
            MAX(f.title) AS title,
            MAX(f.poster_url) AS poster_url,
            ROUND(AVG(CAST(f.rating AS REAL)), 1) AS avg_rating,
            COUNT(*) AS rating_count,
            MAX(f.updated_at) AS updated_at
        FROM user_media_feedback f
        WHERE f.rating IS NOT NULL
        GROUP BY f.media_kind, f.media_key
        ORDER BY datetime(MAX(f.updated_at)) DESC
        LIMIT 100
        """
    ).fetchall()

    items = []
    for row in rows:
        media_kind = str(row["media_kind"] or "")
        media_key = str(row["media_key"] or "")

        rating_rows = db.execute(
            """
            SELECT
                u.username,
                f.rating
            FROM user_media_feedback f
            JOIN users u ON u.id = f.user_id
            WHERE f.media_kind = ?
              AND f.media_key = ?
              AND f.rating IS NOT NULL
            ORDER BY lower(u.username) ASC
            """,
            (media_kind, media_key),
        ).fetchall()

        ratings = []
        for rr in rating_rows:
            try:
                rating_val = int(rr["rating"])
            except Exception:
                continue
            ratings.append({
                "user": str(rr["username"] or ""),
                "rating": rating_val,
            })

        items.append({
            "media_kind": media_kind,
            "media_key": media_key,
            "source": str(row["source"] or ""),
            "title": str(row["title"] or ""),
            "poster_url": str(row["poster_url"] or ""),
            "open_url": _recommended_open_url(media_key, str(row["poster_url"] or "")),
            "overview": _recommended_overview(media_key),
            "avg_rating": float(row["avg_rating"] or 0),
            "rating_count": int(row["rating_count"] or 0),
            "updated_at": str(row["updated_at"] or ""),
            "ratings": ratings,
        })

    return jsonify(ok=True, items=items)



@bp.get("/api/watchlist/notifications")
@login_required
def api_watchlist_notifications():
    _ensure_recommendation_tables()
    _ensure_recommendation_tables()
    uid = _feedback_user_id()
    if uid <= 0:
        return jsonify(ok=False, error="not_logged_in"), 401

    db = get_db()

    count_row = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM user_direct_recommendations r
        WHERE r.to_user_id = ?
          AND COALESCE(r.seen, 0) = 0
        """,
        (uid,),
    ).fetchone()

    rows = db.execute(
        """
        SELECT
            r.title,
            r.note,
            r.created_at,
            u.username AS from_username
        FROM user_direct_recommendations r
        JOIN users u ON u.id = r.from_user_id
        WHERE r.to_user_id = ?
          AND COALESCE(r.seen, 0) = 0
        ORDER BY datetime(r.created_at) DESC
        LIMIT 5
        """,
        (uid,),
    ).fetchall()

    items = [{
        "title": str(r["title"] or ""),
        "note": str(r["note"] or ""),
        "from_username": str(r["from_username"] or ""),
        "created_at": str(r["created_at"] or ""),
    } for r in rows]

    return jsonify(ok=True, count=int(count_row["c"] or 0), items=items)


@bp.post("/api/watchlist/notifications/mark-seen")
@login_required
def api_watchlist_notifications_mark_seen():
    _ensure_recommendation_tables()
    _ensure_recommendation_tables()
    uid = _feedback_user_id()
    if uid <= 0:
        return jsonify(ok=False, error="not_logged_in"), 401

    db = get_db()
    db.execute("""
        UPDATE user_direct_recommendations
        SET seen = 1
        WHERE to_user_id = ?
    """, (uid,))
    db.commit()

    return jsonify(ok=True)


def _tmdb_overview(media_key: str) -> str:
    media_key = str(media_key or "").strip()

    if not media_key.startswith("tmdb:"):
        return ""

    tmdb_id = media_key.split(":", 1)[1].strip()
    if not tmdb_id:
        return ""

    api_key = _cfg("tmdb_api_key", "TMDB_API_KEY", "").strip()
    if not api_key:
        return ""

    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/movie/{tmdb_id}",
            params={"api_key": api_key},
            timeout=20,
        )
        if r.status_code == 404:
            # try tv fallback
            r = requests.get(
                f"https://api.themoviedb.org/3/tv/{tmdb_id}",
                params={"api_key": api_key},
                timeout=20,
            )

        r.raise_for_status()
        data = r.json() or {}
        return str(data.get("overview") or "").strip()
    except Exception:
        return ""
