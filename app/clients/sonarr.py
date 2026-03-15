import os
from datetime import datetime, timedelta, timezone

import requests

import time

# Simple in-process cache for Sonarr series id -> slug map
_SONARR_SLUG_CACHE: dict[str, object] = {"ts": 0.0, "map": {}}
_SONARR_SLUG_TTL_SEC = 600  # 10 minutes


def get_series_slug_map(force: bool = False) -> dict[int, str]:
    """
    Returns { series_id: slug }. Cached in-process to avoid frequent Sonarr calls.
    """
    now = time.time()
    ts = float(_SONARR_SLUG_CACHE.get("ts") or 0.0)
    if (not force) and (now - ts) < _SONARR_SLUG_TTL_SEC and isinstance(_SONARR_SLUG_CACHE.get("map"), dict):
        return _SONARR_SLUG_CACHE["map"]  # type: ignore

    try:
        data = _sonarr_get("/api/v3/series", params=None)
        out: dict[int, str] = {}
        for row in (data or []):
            sid = row.get("id")
            slug = (row.get("titleSlug") or row.get("slug") or "").strip()
            if isinstance(sid, int) and slug:
                out[sid] = slug
        _SONARR_SLUG_CACHE["ts"] = now
        _SONARR_SLUG_CACHE["map"] = out
        return out
    except Exception:
        # Don't poison the cache; just return whatever we had
        m = _SONARR_SLUG_CACHE.get("map")
        return m if isinstance(m, dict) else {}

def _get_setting_safe(key: str, default: str = "") -> str:
    """
    Admin-managed per-user settings first, then legacy user settings,
    then global app settings, then default.
    """
    try:
        from app.models_settings import get_current_user_scoped_setting
        v = get_current_user_scoped_setting(key, default="")
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    except Exception:
        pass
    return default


def _cfg(db_key: str, env_key: str, fallback: str = "") -> str:
    v = _get_setting_safe(db_key, default="")
    if v:
        return v
    return os.getenv(env_key, fallback).strip()


def _sonarr_headers():
    api_key = _cfg("sonarr_api_key", "SONARR_API_KEY", "")
    if not api_key:
        raise RuntimeError("SONARR_API_KEY is not set (and sonarr_api_key not set in settings)")
    return {"X-Api-Key": api_key}


def _sonarr_base():
    base = _cfg("sonarr_url", "SONARR_URL", "").rstrip("/")
    if not base:
        raise RuntimeError("SONARR_URL is not set (and sonarr_url not set in settings)")
    return base


def _sonarr_get(path: str, params: dict | None = None):
    """
    Internal helper for GET requests to Sonarr.
    """
    base = _sonarr_base()
    url = f"{base}{path}"
    r = requests.get(url, headers=_sonarr_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def get_upcoming(days: int = 7):
    """
    Returns Sonarr calendar items for the next N days (UTC window).
    """
    days = max(1, min(days, 30))  # clamp 1–30

    start = datetime.now(timezone.utc)
    end = start + timedelta(days=days)

    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "includeSeries": "true",
        "includeEpisodeFile": "false",
        "unmonitored": "false",
    }

    return _sonarr_get("/api/v3/calendar", params=params)


def get_calendar(start: str, end: str):
    """
    Returns Sonarr calendar items between two dates.
    start/end format: YYYY-MM-DD
    Used for 'missing' (aired but not downloaded).
    """
    params = {
        "start": start,
        "end": end,
        "includeSeries": "true",
        "includeEpisodeFile": "true",   # needed for hasFile
        "includeImages": "true",
        "unmonitored": "false",
    }

    return _sonarr_get("/api/v3/calendar", params=params)


def get_queue(page_size: int = 200):
    """
    Returns Sonarr queue items (grabbed/downloading/pending).
    Sonarr returns an object with "records".
    """
    page_size = max(1, min(page_size, 2000))
    params = {
        "page": 1,
        "pageSize": page_size,
        "includeSeries": "true",
    }
    data = _sonarr_get("/api/v3/queue", params=params) or {}
    return (data.get("records") or [])

# Cached Sonarr requested-series lookup
_SONARR_REQ_CACHE: dict[str, object] = {"ts": 0.0, "rows": []}
_SONARR_REQ_TTL_SEC = 300  # 5 minutes


def _norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append(" ")
    return " ".join("".join(out).split())


def _get_all_series_cached(force: bool = False):
    now = time.time()
    ts = float(_SONARR_REQ_CACHE.get("ts") or 0.0)
    if (not force) and (now - ts) < _SONARR_REQ_TTL_SEC and isinstance(_SONARR_REQ_CACHE.get("rows"), list):
        return _SONARR_REQ_CACHE["rows"]  # type: ignore

    data = _sonarr_get("/api/v3/series", params=None) or []
    rows = data if isinstance(data, list) else []
    _SONARR_REQ_CACHE["ts"] = now
    _SONARR_REQ_CACHE["rows"] = rows
    return rows


def find_requested_series_batch(items: list[dict]) -> dict[str, dict]:
    """
    items: [{key, title, year, tmdb_id, imdb_id, tvdb_id, media_type}]
    returns: {key: {"in_sonarr": bool, "series_id": str, "title": str}}
    """
    out: dict[str, dict] = {}
    rows = _get_all_series_cached(force=False)

    by_tvdb: dict[str, dict] = {}
    by_imdb: dict[str, dict] = {}
    by_tmdb: dict[str, dict] = {}
    by_title_year: dict[str, dict] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue

        tvdb_id = str(row.get("tvdbId") or "").strip()
        imdb_id = str(row.get("imdbId") or "").strip()
        tmdb_id = str(row.get("tmdbId") or "").strip()
        title = str(row.get("title") or "").strip()
        year = str(row.get("year") or "").strip()

        if tvdb_id:
            by_tvdb[tvdb_id] = row
        if imdb_id:
            by_imdb[imdb_id] = row
        if tmdb_id:
            by_tmdb[tmdb_id] = row
        if title:
            by_title_year[f"{_norm_title(title)}::{year}"] = row

    for raw in items:
        key = str(raw.get("key") or "").strip()
        media_type = str(raw.get("media_type") or "").strip().lower()
        if not key or media_type != "tv":
            continue

        tvdb_id = str(raw.get("tvdb_id") or "").strip()
        imdb_id = str(raw.get("imdb_id") or "").strip()
        tmdb_id = str(raw.get("tmdb_id") or "").strip()
        title = str(raw.get("title") or "").strip()
        year = str(raw.get("year") or "").strip()

        hit = None
        if tvdb_id and tvdb_id in by_tvdb:
            hit = by_tvdb[tvdb_id]
        elif imdb_id and imdb_id in by_imdb:
            hit = by_imdb[imdb_id]
        elif tmdb_id and tmdb_id in by_tmdb:
            hit = by_tmdb[tmdb_id]
        elif title:
            hit = by_title_year.get(f"{_norm_title(title)}::{year}")

        if hit:
            out[key] = {
                "in_sonarr": True,
                "series_id": str(hit.get("id") or ""),
                "title": str(hit.get("title") or title),
            }
        else:
            out[key] = {
                "in_sonarr": False,
                "series_id": "",
                "title": title,
            }

    return out

