import os
from datetime import datetime, timedelta, timezone

import requests


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


def _radarr_headers():
    api_key = _cfg("radarr_api_key", "RADARR_API_KEY", "")
    if not api_key:
        raise RuntimeError("RADARR_API_KEY is not set (and radarr_api_key not set in settings)")
    return {"X-Api-Key": api_key}


def _radarr_base():
    base = _cfg("radarr_url", "RADARR_URL", "").rstrip("/")
    if not base:
        raise RuntimeError("RADARR_URL is not set (and radarr_url not set in settings)")
    return base


def _radarr_get(path: str, params: dict | None = None):
    base = _radarr_base()
    r = requests.get(f"{base}{path}", headers=_radarr_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def get_upcoming_missing(days: int = 90, limit: int = 30):
    """
    Movies that are NOT downloaded (hasFile=false).

    Returned set includes:
    - upcoming missing movies (release date in the future, within cutoff)
    - released missing movies (release date has passed, still no file)

    We use physicalRelease/digitalRelease/inCinemas as a best-effort date.
    """
    days = max(1, min(days, 365))
    limit = max(1, min(limit, 200))

    movies = _radarr_get("/api/v3/movie", params={"includeImages": "false"})
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=days)

    def pick_date(m):
        for key in ("physicalRelease", "digitalRelease", "inCinemas"):
            v = m.get(key)
            if v:
                try:
                    return datetime.fromisoformat(v.replace("Z", "+00:00"))
                except Exception:
                    continue
        return None

    released = []
    upcoming = []

    for m in movies:
        if m.get("hasFile") is True:
            continue

        if m.get("monitored") is False:
            continue

        d = pick_date(m)
        if not d:
            continue

        row = {
            "title": m.get("title"),
            "year": m.get("year"),
            "tmdb_id": m.get("tmdbId"),
            "release_date": d.isoformat(),
            "_released_missing": bool(d <= now),
        }

        if d <= now:
            released.append((d, row))
        elif d <= cutoff:
            upcoming.append((d, row))

    # Released missing first (most recently released first), then upcoming (soonest first)
    released.sort(key=lambda t: t[0], reverse=True)
    upcoming.sort(key=lambda t: t[0])

    merged = [row for _, row in released] + [row for _, row in upcoming]
    return merged


def get_queue(page_size: int = 200):
    """
    Returns Radarr queue items (grabbed/downloading/pending).
    Radarr returns an object with "records".
    """
    page_size = max(1, min(page_size, 2000))
    params = {
        "page": 1,
        "pageSize": page_size,
        "includeMovie": "true",
    }
    data = _radarr_get("/api/v3/queue", params=params) or {}
    return (data.get("records") or [])

import time

_RADARR_REQ_CACHE: dict[str, object] = {"ts": 0.0, "rows": []}
_RADARR_REQ_TTL_SEC = 300  # 5 minutes


def _norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append(" ")
    return " ".join("".join(out).split())


def _get_all_movies_cached(force: bool = False):
    now = time.time()
    ts = float(_RADARR_REQ_CACHE.get("ts") or 0.0)
    if (not force) and (now - ts) < _RADARR_REQ_TTL_SEC and isinstance(_RADARR_REQ_CACHE.get("rows"), list):
        return _RADARR_REQ_CACHE["rows"]  # type: ignore

    data = _radarr_get("/api/v3/movie", params={"includeImages": "false"}) or []
    rows = data if isinstance(data, list) else []
    _RADARR_REQ_CACHE["ts"] = now
    _RADARR_REQ_CACHE["rows"] = rows
    return rows


def find_requested_movies_batch(items: list[dict]) -> dict[str, dict]:
    """
    items: [{key, title, year, tmdb_id, imdb_id, media_type}]
    returns: {key: {"in_radarr": bool, "movie_id": str, "title": str}}
    """
    out: dict[str, dict] = {}
    rows = _get_all_movies_cached(force=False)

    by_tmdb: dict[str, dict] = {}
    by_imdb: dict[str, dict] = {}
    by_title_year: dict[str, dict] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue

        tmdb_id = str(row.get("tmdbId") or "").strip()
        imdb_id = str(row.get("imdbId") or "").strip()
        title = str(row.get("title") or "").strip()
        year = str(row.get("year") or "").strip()

        if tmdb_id:
            by_tmdb[tmdb_id] = row
        if imdb_id:
            by_imdb[imdb_id] = row
        if title:
            by_title_year[f"{_norm_title(title)}::{year}"] = row

    for raw in items:
        key = str(raw.get("key") or "").strip()
        media_type = str(raw.get("media_type") or "").strip().lower()
        if not key or media_type != "movie":
            continue

        tmdb_id = str(raw.get("tmdb_id") or "").strip()
        imdb_id = str(raw.get("imdb_id") or "").strip()
        title = str(raw.get("title") or "").strip()
        year = str(raw.get("year") or "").strip()

        hit = None
        if tmdb_id and tmdb_id in by_tmdb:
            hit = by_tmdb[tmdb_id]
        elif imdb_id and imdb_id in by_imdb:
            hit = by_imdb[imdb_id]
        elif title:
            hit = by_title_year.get(f"{_norm_title(title)}::{year}")

        if hit:
            out[key] = {
                "in_radarr": True,
                "movie_id": str(hit.get("id") or ""),
                "title": str(hit.get("title") or title),
            }
        else:
            out[key] = {
                "in_radarr": False,
                "movie_id": "",
                "title": title,
            }

    return out

