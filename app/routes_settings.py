import requests
import secrets
from flask import Blueprint, jsonify, request, render_template, redirect
from werkzeug.security import check_password_hash, generate_password_hash

from .db import get_db
from .utils.auth import login_required, current_user
from .models_settings import get_user_admin_settings

try:
    from .routes_discover import _source_cache_clear
except Exception:
    _source_cache_clear = None

bp = Blueprint("settings", __name__)


# ----------------------------
# helpers
# ----------------------------
def _current_user_id() -> int:
    me = current_user() or {}
    return int(me.get("user_id") or 0)


def _user_settings(user_id: int) -> dict[str, str]:
    db = get_db()
    rows = db.execute(
        "SELECT key, value FROM user_settings WHERE user_id = ?",
        (user_id,),
    ).fetchall()
    return {r["key"]: r["value"] for r in rows}


def _save_user_setting(user_id: int, key: str, value: str) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO user_settings(user_id, key, value)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, key)
        DO UPDATE SET value=excluded.value, updated_at=datetime('now')
        """,
        (user_id, key, "" if value is None else str(value)),
    )




def _ensure_app_settings_table() -> None:
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT '',
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )


def _app_settings() -> dict[str, str]:
    db = get_db()
    _ensure_app_settings_table()
    rows = db.execute("SELECT key, value FROM app_settings").fetchall()
    return {str(r["key"]): str(r["value"] or "") for r in rows}


def _save_app_setting(key: str, value: str) -> None:
    db = get_db()
    _ensure_app_settings_table()
    db.execute(
        """
        INSERT INTO app_settings(key, value, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key)
        DO UPDATE SET value=excluded.value, updated_at=datetime('now')
        """,
        (str(key), "" if value is None else str(value)),
    )


def _is_admin_user() -> bool:
    me = current_user() or {}
    return bool(me.get("is_admin"))


def _default_user_settings() -> dict[str, str]:
    """
    Defaults for user-editable preferences only.
    Connection/integration settings are admin-managed per-user now.
    """
    return {
        "limit_continue_watching": "12",
        "limit_nextup_tv": "60",
        "limit_nextup_anime": "60",
        "limit_series_remaining": "30",
        "limit_latest_unwatched_tv": "10",
        "limit_latest_unwatched_anime": "10",
        "limit_radarr_recent": "10",
        "limit_radarr_upcoming": "15",
        "limit_radarr_missing": "15",
        "limit_sonarr_upcoming": "60",
        "limit_sonarr_missing": "60",
        "limit_download_activity": "12",
        "hide_download_activity": "",
        "limit_seerr_requests": "10",
        "limit_discover_results": "24",
        "stats_layout_order": "overview,watch_window,watch_activity,top_shows,top_genres,top_anime,queue_health,current_activity,watch_split",
        "stats_layout_hidden": "",
        "hide_future_nextup_for_hidden_series": "",

        "show_sec_cw": "1",
        "show_sec_nextup_tv": "1",
        "show_sec_sonarr": "1",
        "show_sec_nextup_anime": "1",
        "show_sec_rem": "1",
        "show_sec_latest_tv": "1",
        "show_sec_latest_anime": "1",
        "show_sec_radarr_recent": "1",
        "show_sec_radarr_missing": "1",
        "show_sec_downloads": "1",
        "show_sec_my_requests": "1",
        "homepage_section_order": "",
    }


def _clamp_int(val, default, lo=1, hi=120):
    try:
        iv = int(str(val).strip())
    except Exception:
        return default
    if iv < lo:
        return lo
    if iv > hi:
        return hi
    return iv


def _get_effective_settings(user_id: int, override: dict | None = None) -> dict[str, str]:
    """
    Effective settings resolution:
      1. user preference defaults
      2. admin-managed per-user connection settings
      3. this user's saved preferences
      4. request override
    """
    data = _default_user_settings()

    try:
        admin_data = get_user_admin_settings(user_id)
        if isinstance(admin_data, dict):
            data.update(admin_data)
    except Exception:
        pass

    data.update(_user_settings(user_id))

    if isinstance(override, dict):
        for k, v in override.items():
            data[k] = "" if v is None else str(v)

    # keep aliases in sync
    if not data.get("mytv_view_id") and data.get("jellyfin_view_id"):
        data["mytv_view_id"] = data["jellyfin_view_id"]

    if not data.get("jellyfin_view_id") and data.get("mytv_view_id"):
        data["jellyfin_view_id"] = data["mytv_view_id"]

    return data


def _norm_base(url: str) -> str:
    return (url or "").strip().rstrip("/")


def _ensure_rss_token(user_id: int, rotate: bool = False) -> str:
    current = _user_settings(user_id).get("rss_feed_token", "").strip()
    if current and not rotate:
        return current

    token = secrets.token_urlsafe(24)
    _save_user_setting(user_id, "rss_feed_token", token)

    try:
        db = get_db()
        db.commit()
    except Exception:
        pass

    return token


def _ok_result(name: str, extra: dict | None = None) -> dict:
    out = {"ok": True, "service": name}
    if extra:
        out.update(extra)
    return out


def _fail_result(name: str, err: str) -> dict:
    return {"ok": False, "service": name, "error": (err or "unknown error")[:300]}


# ----------------------------
# connection tests
# ----------------------------
def _test_sonarr(sonarr_url: str, api_key: str) -> dict:
    base = _norm_base(sonarr_url)

    if not base:
        return _fail_result("sonarr", "sonarr_url empty")

    if not api_key:
        return _fail_result("sonarr", "sonarr_api_key empty")

    try:
        r = requests.get(
            f"{base}/api/v3/system/status",
            headers={"X-Api-Key": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        return _ok_result("sonarr", {"version": data.get("version")})
    except Exception as e:
        return _fail_result("sonarr", str(e))


def _test_radarr(radarr_url: str, api_key: str) -> dict:
    base = _norm_base(radarr_url)

    if not base:
        return _fail_result("radarr", "radarr_url empty")

    if not api_key:
        return _fail_result("radarr", "radarr_api_key empty")

    try:
        r = requests.get(
            f"{base}/api/v3/system/status",
            headers={"X-Api-Key": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        return _ok_result("radarr", {"version": data.get("version")})
    except Exception as e:
        return _fail_result("radarr", str(e))


def _test_seerr(seerr_url: str, api_key: str) -> dict:
    base = _norm_base(seerr_url)

    if not base:
        return _fail_result("seerr", "seerr_url empty")

    if not api_key:
        return _fail_result("seerr", "seerr_api_key empty")

    try:
        r = requests.get(
            f"{base}/api/v1/status",
            headers={"X-Api-Key": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json() or {}
        return _ok_result("seerr", {"version": data.get("version")})
    except Exception as e:
        return _fail_result("seerr", str(e))


def _test_jellyfin(jellyfin_url: str, api_key: str, username: str) -> dict:
    base = _norm_base(jellyfin_url)

    if not base:
        return _fail_result("jellyfin", "jellyfin_url empty")

    if not api_key:
        return _fail_result("jellyfin", "jellyfin_api_key empty")

    if not username:
        return _fail_result("jellyfin", "jellyfin_user empty")

    try:
        r = requests.get(
            f"{base}/Users",
            headers={"X-Emby-Token": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()

        users = r.json()
        for u in users:
            if (u.get("Name") or "").lower() == username.lower():
                return _ok_result("jellyfin", {"user": username})

        return _fail_result("jellyfin", f'user "{username}" not found')
    except Exception as e:
        return _fail_result("jellyfin", str(e))


def _jellyfin_get_user_id(base: str, api_key: str, username: str) -> str:
    try:
        r = requests.get(
            f"{base}/Users",
            headers={"X-Emby-Token": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()
        users = r.json() or []
        for u in users:
            if (u.get("Name") or "").lower() == (username or "").lower():
                return str(u.get("Id") or "")
        return ""
    except Exception:
        return ""


def _jellyfin_list_users(jellyfin_url: str, api_key: str) -> dict:
    base = _norm_base(jellyfin_url)

    if not base:
        return {"ok": False, "error": "jellyfin_url empty"}
    if not api_key:
        return {"ok": False, "error": "jellyfin_api_key empty"}

    try:
        r = requests.get(
            f"{base}/Users",
            headers={"X-Emby-Token": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()

        users = r.json() or []
        out = []
        for u in users:
            name = str(u.get("Name") or "").strip()
            uid = str(u.get("Id") or "").strip()
            if not name:
                continue
            out.append({
                "name": name,
                "id": uid,
                "is_disabled": bool((u.get("Policy") or {}).get("IsDisabled", False)),
            })

        out.sort(key=lambda x: x["name"].lower())
        return {"ok": True, "users": out}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def _jellyfin_list_views(jellyfin_url: str, api_key: str, username: str) -> dict:
    base = _norm_base(jellyfin_url)

    if not base:
        return {"ok": False, "error": "jellyfin_url empty"}
    if not api_key:
        return {"ok": False, "error": "jellyfin_api_key empty"}
    if not username:
        return {"ok": False, "error": "jellyfin_user empty"}

    user_id = _jellyfin_get_user_id(base, api_key, username)
    if not user_id:
        return {"ok": False, "error": f'Jellyfin user "{username}" not found'}

    try:
        r = requests.get(
            f"{base}/Users/{user_id}/Views",
            headers={"X-Emby-Token": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()

        data = r.json() or {}
        items = data.get("Items") or []

        out = []
        for it in items:
            name = it.get("Name") or ""
            vid = it.get("Id") or ""
            if name and vid:
                out.append({"name": str(name), "id": str(vid)})

        return {"ok": True, "items": out}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def _admin_bootstrap_needed() -> bool:
    try:
        db = get_db()
        row = db.execute(
            "SELECT COUNT(*) AS c FROM app_settings WHERE TRIM(COALESCE(value, '')) <> ''"
        ).fetchone()
        return int(row["c"] or 0) <= 0
    except Exception:
        return True


# ----------------------------
# routes
# ----------------------------
@bp.get("/settings")
@login_required
def settings_page():
    me = current_user() or {}
    if bool(me.get("is_admin")) and _admin_bootstrap_needed():
        return redirect("/admin/settings")
    return render_template("settings.html", me=me)


@bp.get("/admin/settings")
@login_required
def admin_settings_page():
    me = current_user() or {}
    if not bool(me.get("is_admin")):
        return render_template("settings.html", me=me), 403
    return render_template("admin_settings.html", me=me)


@bp.get("/settings/api")
@login_required
def settings_get():
    user_id = _current_user_id()
    data = _get_effective_settings(user_id)

    data["jellyfin_username"] = data.get("jellyfin_user", "")
    data["jellyfin_view_id"] = data.get("mytv_view_id", "")

    if _is_admin_user():
        appcfg = _app_settings()
        data["tmdb_bearer_token"] = appcfg.get("tmdb_bearer_token", "")
        data["tmdb_api_key"] = appcfg.get("tmdb_api_key", "")
        data["trakt_client_id"] = appcfg.get("trakt_client_id", "")
        data["letterboxd_rss_url"] = appcfg.get("letterboxd_rss_url", "")
        data["letterboxd_rss_urls"] = appcfg.get("letterboxd_rss_urls", "")
        data["discover_weight_tmdb"] = appcfg.get("discover_weight_tmdb", "1.0")
        data["discover_weight_tmdb_popular"] = appcfg.get("discover_weight_tmdb_popular", "0.95")
        data["discover_weight_trakt"] = appcfg.get("discover_weight_trakt", "1.0")
        data["discover_weight_trakt_popular"] = appcfg.get("discover_weight_trakt_popular", "0.90")
        data["discover_weight_letterboxd"] = appcfg.get("discover_weight_letterboxd", "0.58")
        data["discover_weight_tvmaze"] = appcfg.get("discover_weight_tvmaze", "0.80")
        data["discover_weight_anilist"] = appcfg.get("discover_weight_anilist", "0.92")
        data["discover_bonus_2"] = appcfg.get("discover_bonus_2", "0.08")
        data["discover_bonus_3"] = appcfg.get("discover_bonus_3", "0.18")
        data["discover_bonus_4"] = appcfg.get("discover_bonus_4", "0.28")
        data["discover_bonus_5"] = appcfg.get("discover_bonus_5", "0.34")
        data["discover_bonus_6"] = appcfg.get("discover_bonus_6", "0.40")
        data["discover_hot_threshold"] = appcfg.get("discover_hot_threshold", "0.72")
        data["discover_weight_trakt"] = appcfg.get("discover_weight_trakt", "1.0")
        data["discover_bonus_2"] = appcfg.get("discover_bonus_2", "0.08")
        data["discover_bonus_3"] = appcfg.get("discover_bonus_3", "0.18")
        data["discover_bonus_4"] = appcfg.get("discover_bonus_4", "0.28")
        data["discover_hot_threshold"] = appcfg.get("discover_hot_threshold", "0.82")
        data["discover_enrich_scale"] = appcfg.get("discover_enrich_scale", "100")
        data["discover_cache_ttl_minutes"] = appcfg.get("discover_cache_ttl_minutes", "30")
        data["discover_title_overrides"] = appcfg.get("discover_title_overrides", "")

    return jsonify(data)


@bp.post("/settings/api")
@login_required
def settings_post():
    user_id = _current_user_id()
    payload = request.get_json(silent=True) or {}

    if "jellyfin_username" in payload and "jellyfin_user" not in payload:
        payload["jellyfin_user"] = payload.get("jellyfin_username", "")

    if "jellyfin_view_id" in payload and "mytv_view_id" not in payload:
        payload["mytv_view_id"] = payload.get("jellyfin_view_id", "")

    for k, default in {
        "limit_continue_watching": 12,
        "limit_nextup_tv": 60,
        "limit_nextup_anime": 60,
        "limit_series_remaining": 30,
        "limit_latest_unwatched_tv": 10,
        "limit_latest_unwatched_anime": 10,
        "limit_radarr_recent": 10,
        "limit_radarr_upcoming": 15,
        "limit_radarr_missing": 15,
        "limit_sonarr_upcoming": 60,
        "limit_sonarr_missing": 60,
        "limit_download_activity": 12,
        "limit_seerr_requests": 10,
        "limit_discover_results": 24,
    }.items():
        if k in payload:
            payload[k] = str(_clamp_int(payload.get(k), default, lo=1, hi=120))

    if "hide_download_activity" in payload:
        payload["hide_download_activity"] = "1" if str(payload.get("hide_download_activity", "")).strip().lower() in {"1", "true", "yes", "on"} else ""

    cfg = _get_effective_settings(user_id, override=payload)

    keys_to_save = [
        "limit_continue_watching",
        "limit_nextup_tv",
        "limit_nextup_anime",
        "limit_series_remaining",
        "limit_latest_unwatched_tv",
        "limit_latest_unwatched_anime",
        "limit_radarr_recent",
        "limit_radarr_upcoming",
        "limit_radarr_missing",
        "limit_sonarr_upcoming",
        "limit_sonarr_missing",
        "limit_download_activity",
        "hide_download_activity",
        "limit_seerr_requests",
        "limit_discover_results",
        "stats_layout_order",
        "stats_layout_hidden",
        "hide_future_nextup_for_hidden_series",

        "show_sec_cw",
        "show_sec_nextup_tv",
        "show_sec_sonarr",
        "show_sec_nextup_anime",
        "show_sec_rem",
        "show_sec_latest_tv",
        "show_sec_latest_anime",
        "show_sec_radarr_recent",
        "show_sec_radarr_missing",
        "show_sec_downloads",
        "show_sec_my_requests",
        "homepage_section_order",
    ]
    for key in keys_to_save:
        _save_user_setting(user_id, key, cfg.get(key, ""))
    if _is_admin_user():
        _save_app_setting("tmdb_bearer_token", payload.get("tmdb_bearer_token", ""))
        _save_app_setting("tmdb_api_key", payload.get("tmdb_api_key", ""))
        _save_app_setting("trakt_client_id", payload.get("trakt_client_id", ""))
        _save_app_setting("letterboxd_rss_url", payload.get("letterboxd_rss_url", ""))
        _save_app_setting("letterboxd_rss_urls", payload.get("letterboxd_rss_urls", ""))
        _save_app_setting("discover_weight_tmdb", payload.get("discover_weight_tmdb", "1.0"))
        _save_app_setting("discover_weight_tmdb_popular", payload.get("discover_weight_tmdb_popular", "0.95"))
        _save_app_setting("discover_weight_trakt", payload.get("discover_weight_trakt", "1.0"))
        _save_app_setting("discover_weight_trakt_popular", payload.get("discover_weight_trakt_popular", "0.90"))
        _save_app_setting("discover_weight_letterboxd", payload.get("discover_weight_letterboxd", "0.58"))
        _save_app_setting("discover_weight_tvmaze", payload.get("discover_weight_tvmaze", "0.80"))
        _save_app_setting("discover_weight_anilist", payload.get("discover_weight_anilist", "0.92"))
        _save_app_setting("discover_bonus_2", payload.get("discover_bonus_2", "0.08"))
        _save_app_setting("discover_bonus_3", payload.get("discover_bonus_3", "0.18"))
        _save_app_setting("discover_bonus_4", payload.get("discover_bonus_4", "0.28"))
        _save_app_setting("discover_bonus_5", payload.get("discover_bonus_5", "0.34"))
        _save_app_setting("discover_bonus_6", payload.get("discover_bonus_6", "0.40"))
        _save_app_setting("discover_hot_threshold", payload.get("discover_hot_threshold", "0.72"))
        _save_app_setting("discover_weight_trakt", payload.get("discover_weight_trakt", "1.0"))
        _save_app_setting("discover_bonus_2", payload.get("discover_bonus_2", "0.08"))
        _save_app_setting("discover_bonus_3", payload.get("discover_bonus_3", "0.18"))
        _save_app_setting("discover_bonus_4", payload.get("discover_bonus_4", "0.28"))
        _save_app_setting("discover_hot_threshold", payload.get("discover_hot_threshold", "0.82"))
        _save_app_setting("discover_enrich_scale", payload.get("discover_enrich_scale", "100"))
        _save_app_setting("discover_cache_ttl_minutes", payload.get("discover_cache_ttl_minutes", "30"))
        _save_app_setting("discover_title_overrides", payload.get("discover_title_overrides", ""))

    get_db().commit()

    try:
        if _is_admin_user() and _source_cache_clear:
            _source_cache_clear()
    except Exception:
        pass

    return jsonify(ok=True)


@bp.get("/settings/rss-info")
@login_required
def settings_rss_info():
    user_id = _current_user_id()
    token = _ensure_rss_token(user_id)

    effective = _get_effective_settings(user_id)

    has_jellyfin = bool(
        str(effective.get("jellyfin_url") or "").strip() and
        str(effective.get("jellyfin_api_key") or "").strip() and
        str(effective.get("jellyfin_user") or "").strip()
    )

    anime_paths = [p.strip() for p in str(effective.get("anime_paths") or "").split(",") if p.strip()]
    has_anime = bool(has_jellyfin and anime_paths)

    has_sonarr = bool(
        str(effective.get("sonarr_url") or "").strip() and
        str(effective.get("sonarr_api_key") or "").strip()
    )

    has_radarr = bool(
        str(effective.get("radarr_url") or "").strip() and
        str(effective.get("radarr_api_key") or "").strip()
    )

    base = request.host_url.rstrip("/")

    feeds = []

    if has_jellyfin:
        feeds.append({
            "key": "nextup-tv",
            "label": "Next Up — TV",
            "available": True,
            "url": f"{base}/rss/nextup-tv?token={token}",
        })

    if has_anime:
        feeds.append({
            "key": "nextup-anime",
            "label": "Next Up — Anime",
            "available": True,
            "url": f"{base}/rss/nextup-anime?token={token}",
        })

    if has_sonarr:
        feeds.append({
            "key": "airing-tonight-missing",
            "label": "Airing Tonight",
            "available": True,
            "url": f"{base}/rss/airing-tonight-missing?token={token}",
        })

    if has_jellyfin and has_radarr:
        feeds.append({
            "key": "latest-unwatched-movies",
            "label": "Latest Unwatched Movies",
            "available": True,
            "url": f"{base}/rss/latest-unwatched-movies?token={token}",
        })

    return jsonify(
        ok=True,
        token=token,
        feeds=feeds,
    )


@bp.post("/settings/rss-token/regenerate")
@login_required
def settings_rss_regenerate():
    user_id = _current_user_id()
    token = _ensure_rss_token(user_id, rotate=True)
    return jsonify(ok=True, token=token)


@bp.post("/settings/password")
@login_required
def settings_password():
    user_id = _current_user_id()
    payload = request.get_json(silent=True) or {}

    current_password = str(payload.get("current_password") or "")
    new_password = str(payload.get("new_password") or "")
    confirm_password = str(payload.get("confirm_password") or "")

    if not current_password:
        return jsonify(ok=False, error="Current password is required."), 400

    if not new_password:
        return jsonify(ok=False, error="New password is required."), 400

    if len(new_password) < 8:
        return jsonify(ok=False, error="New password must be at least 8 characters."), 400

    if new_password != confirm_password:
        return jsonify(ok=False, error="New password and confirmation do not match."), 400

    if current_password == new_password:
        return jsonify(ok=False, error="New password must be different from your current password."), 400

    db = get_db()
    row = db.execute(
        "SELECT id, password_hash FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()

    if not row:
        return jsonify(ok=False, error="User not found."), 404

    if not check_password_hash(str(row["password_hash"] or ""), current_password):
        return jsonify(ok=False, error="Current password is incorrect."), 400

    db.execute(
        "UPDATE users SET password_hash = ?, updated_at = datetime('now') WHERE id = ?",
        (generate_password_hash(new_password), user_id),
    )
    db.commit()

    return jsonify(ok=True)


@bp.post("/settings/test")
@login_required
def settings_test():
    user_id = _current_user_id()
    payload = request.get_json(silent=True) or {}
    cfg = _get_effective_settings(user_id, override=payload)

    results = {
        "jellyfin": _test_jellyfin(
            cfg.get("jellyfin_url", ""),
            cfg.get("jellyfin_api_key", ""),
            cfg.get("jellyfin_user", ""),
        ),
        "sonarr": _test_sonarr(
            cfg.get("sonarr_url", ""),
            cfg.get("sonarr_api_key", ""),
        ),
        "radarr": _test_radarr(
            cfg.get("radarr_url", ""),
            cfg.get("radarr_api_key", ""),
        ),
        "seerr": _test_seerr(
            cfg.get("seerr_url", ""),
            cfg.get("seerr_api_key", ""),
        ),
    }

    all_ok = all(v.get("ok") is True for v in results.values())
    return jsonify(ok=all_ok, results=results)


@bp.post("/settings/jellyfin/users")
@login_required
def settings_jellyfin_users():
    user_id = _current_user_id()
    payload = request.get_json(silent=True) or {}
    cfg = _get_effective_settings(user_id, override=payload)

    out = _jellyfin_list_users(
        cfg.get("jellyfin_url", ""),
        cfg.get("jellyfin_api_key", ""),
    )

    return jsonify(out), (200 if out.get("ok") else 400)


@bp.get("/settings/jellyfin/views")
@login_required
def settings_jellyfin_views():
    user_id = _current_user_id()
    cfg = _get_effective_settings(user_id)

    out = _jellyfin_list_views(
        cfg.get("jellyfin_url", ""),
        cfg.get("jellyfin_api_key", ""),
        cfg.get("jellyfin_user", ""),
    )

    if out.get("ok"):
        out["views"] = out.get("items", [])

    return jsonify(out), (200 if out.get("ok") else 400)
