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
            "radarr_id": m.get("id"),
            "status": m.get("status"),
            "has_file": bool(m.get("hasFile")),
            "monitored": bool(m.get("monitored")),
            "release_date": d.isoformat(),
            "digitalRelease": m.get("digitalRelease"),
            "physicalRelease": m.get("physicalRelease"),
            "inCinemas": m.get("inCinemas"),
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



_TMDB_RELEASE_CACHE: dict[str, object] = {"rows": {}, "ts": {}}
_TMDB_RELEASE_TTL_SEC = 43200  # 12 hours


def _tmdb_headers() -> dict:
    try:
        from app.routes_discover import _tmdb_auth_headers
        return _tmdb_auth_headers()
    except Exception:
        return {}


def _tmdb_params(extra: dict | None = None) -> dict:
    try:
        from app.routes_discover import _tmdb_auth_params
        params = dict(_tmdb_auth_params() or {})
    except Exception:
        params = {}
    if extra:
        params.update(extra)
    return params


def _pick_earliest_dt(values):
    vals = [v for v in (values or []) if v]
    if not vals:
        return ""
    vals.sort()
    return vals[0]


def _tmdb_release_dates_for_movie(tmdb_id: str, region: str = "US") -> dict:
    tmdb_id = str(tmdb_id or "").strip()
    region = str(region or "US").strip().upper() or "US"
    if not tmdb_id:
        return {}

    now_ts = time.time()
    rows_cache = _TMDB_RELEASE_CACHE.setdefault("rows", {})
    ts_cache = _TMDB_RELEASE_CACHE.setdefault("ts", {})

    cached = rows_cache.get(tmdb_id)
    cached_ts = float(ts_cache.get(tmdb_id) or 0.0)
    if cached and (now_ts - cached_ts) < _TMDB_RELEASE_TTL_SEC:
        return dict(cached)

    headers = _tmdb_headers()
    params = _tmdb_params()
    if not headers and not params:
        rows_cache[tmdb_id] = {}
        ts_cache[tmdb_id] = now_ts
        return {}

    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/movie/{tmdb_id}/release_dates",
            headers=headers,
            params=params,
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}
    except Exception as e:
        print(f"[watchlist-tmdb] release_dates failed tmdb_id={tmdb_id!r} err={e}", flush=True)
        rows_cache[tmdb_id] = {}
        ts_cache[tmdb_id] = now_ts
        return {}

    results = data.get("results") or []
    chosen = None

    for row in results:
        if str(row.get("iso_3166_1") or "").upper() == region:
            chosen = row
            break

    if chosen is None:
        for row in results:
            if str(row.get("iso_3166_1") or "").upper() == "US":
                chosen = row
                break

    if chosen is None and results:
        chosen = results[0]

    out = {
        "tmdb_premiere_release": "",
        "tmdb_theatrical_release": "",
        "tmdb_digital_release": "",
        "tmdb_physical_release": "",
        "tmdb_tv_release": "",
    }

    release_dates = (chosen or {}).get("release_dates") or []
    grouped = {
        1: [],  # Premiere
        2: [],  # Theatrical (limited)
        3: [],  # Theatrical
        4: [],  # Digital
        5: [],  # Physical
        6: [],  # TV
    }

    for item in release_dates:
        try:
            typ = int(item.get("type") or 0)
        except Exception:
            typ = 0
        dt = str(item.get("release_date") or "").strip()
        if typ in grouped and dt:
            grouped[typ].append(dt)

    theatrical_vals = list(grouped[2]) + list(grouped[3])

    out["tmdb_premiere_release"] = _pick_earliest_dt(grouped[1])
    out["tmdb_theatrical_release"] = _pick_earliest_dt(theatrical_vals)
    out["tmdb_digital_release"] = _pick_earliest_dt(grouped[4])
    out["tmdb_physical_release"] = _pick_earliest_dt(grouped[5])
    out["tmdb_tv_release"] = _pick_earliest_dt(grouped[6])

    rows_cache[tmdb_id] = dict(out)
    ts_cache[tmdb_id] = now_ts
    return out


def enrich_movies_with_tmdb_release_dates(items: list[dict], region: str = "US") -> list[dict]:
    out = []
    for raw in (items or []):
        row = dict(raw or {})
        tmdb_id = str(row.get("tmdb_id") or row.get("tmdbId") or "").strip()
        if tmdb_id:
            extra = _tmdb_release_dates_for_movie(tmdb_id, region=region)
            if extra:
                row.update(extra)
        out.append(row)
    return out


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

