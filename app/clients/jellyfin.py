import os
import ipaddress
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Union

import requests
from flask import has_request_context, request


def _get_setting_safe(key: str, default: str = "") -> str:
    """
    Per-user scoped settings:
      1. admin-managed per-user settings
      2. legacy user settings
      3. default
    """
    try:
        if has_request_context():
            from app.models_settings import get_current_user_scoped_setting
            return get_current_user_scoped_setting(key, default=default)
    except Exception:
        pass
    return default


def _cfg(db_key: str, env_key: str, fallback: str = "") -> str:
    return _get_setting_safe(db_key, default="").strip()


# ----------------------------
# basics (DB-first, ENV fallback)
# ----------------------------

def _base() -> str:
    return _cfg("jellyfin_url", "JELLYFIN_URL", "").rstrip("/")

def _host_looks_internal(host: str) -> bool:
    h = (host or "").strip().lower()
    if not h:
        return True
    h = h.split(",")[0].strip()
    if ":" in h and h.count(":") == 1:
        h = h.split(":", 1)[0]
    if h in {"localhost", "127.0.0.1", "::1"}:
        return True
    if "." not in h:
        return True
    try:
        ip = ipaddress.ip_address(h)
        return bool(ip.is_private or ip.is_loopback or ip.is_link_local)
    except Exception:
        return False

def _play_base() -> str:
    mode = (_get_setting_safe("jellyfin_play_mode", default="auto") or "auto").strip().lower()
    internal = _base()
    external = _cfg("jellyfin_play_base_url", "", "").rstrip("/")

    if mode == "internal":
        return internal
    if mode == "external":
        return external or internal

    if has_request_context():
        host = (request.headers.get("X-Forwarded-Host") or request.host or "").strip()
        if external and not _host_looks_internal(host):
            return external

    return internal


def _api_key() -> str:
    return _cfg("jellyfin_api_key", "JELLYFIN_API_KEY", "").strip()


def _username() -> str:
    return _cfg("jellyfin_user", "JELLYFIN_USER", "").strip()


def _mytv_view_id() -> str:
    return _cfg("mytv_view_id", "MYTV_VIEW_ID", "").strip()


def _anime_paths() -> List[str]:
    raw = _get_setting_safe("anime_paths", default="").strip()
    if not raw:
        raw = os.getenv("ANIME_PATHS", "").strip()
    if not raw:
        return []
    return [p.strip().rstrip("/") for p in raw.split(",") if p.strip()]


def _headers() -> Dict[str, str]:
    key = _api_key()
    return {"X-Emby-Token": key} if key else {}


def _get(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 25) -> Union[Dict[str, Any], List[Any]]:
    r = requests.get(url, headers=_headers(), params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _items_from(data: Union[Dict[str, Any], List[Any]]) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        items = data.get("Items", [])
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
    return []


# ----------------------------
# tiny in-memory cache (like v0.8)
# ----------------------------

_cache: Dict[str, Any] = {}  # key -> (exp_epoch, value)


def _cache_get(key: str) -> Any:
    v = _cache.get(key)
    if not v:
        return None
    exp, val = v
    if time.time() > exp:
        _cache.pop(key, None)
        return None
    return val


def _cache_set(key: str, val: Any, ttl: int = 300) -> None:
    _cache[key] = (time.time() + ttl, val)


# ----------------------------
# user resolve
# ----------------------------

def find_user_id_by_name(username: str) -> str:
    base = _base()
    if not base:
        raise RuntimeError("JELLYFIN_URL not set (and jellyfin_url not set in settings)")
    if not username:
        raise RuntimeError("JELLYFIN_USER not set (and jellyfin_user not set in settings)")

    ck = f"jf_uid:{username.lower()}"
    v = _cache_get(ck)
    if v:
        return v

    data = _get(f"{base}/Users")
    users = _items_from(data)

    for u in users:
        if (u.get("Name") or "").lower() == username.lower():
            uid = u.get("Id")
            if uid:
                _cache_set(ck, uid, ttl=3600)
                return uid

    for u in users:
        if username.lower() in (u.get("Name") or "").lower():
            uid = u.get("Id")
            if uid:
                _cache_set(ck, uid, ttl=3600)
                return uid

    raise RuntimeError(f'Jellyfin user not found: "{username}"')


# ----------------------------
# NextUp helpers (v0.8 behavior)
# ----------------------------

def _nextup_raw(user_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    base = _base()
    data = _get(
        f"{base}/Shows/NextUp",
        params={
            "UserId": user_id,
            "Limit": str(limit),
            "Fields": "PrimaryImageAspectRatio,ImageTags,Path,UserData,DateCreated,PremiereDate",
        },
        timeout=25,
    )
    return _items_from(data)


def _mytv_series_list(user_id: str, view_id: str, limit: int = 500) -> List[Dict[str, Any]]:
    if not view_id:
        return []
    base = _base()
    data = _get(
        f"{base}/Users/{user_id}/Items",
        params={
            "ParentId": view_id,
            "IncludeItemTypes": "Series",
            "Recursive": "true",
            "Limit": str(limit),
            "Fields": "Path",
        },
        timeout=30,
    )
    return _items_from(data)


def _nextup_for_series(user_id: str, series_id: str) -> Optional[Dict[str, Any]]:
    base = _base()
    data = _get(
        f"{base}/Shows/NextUp",
        params={
            "UserId": user_id,
            "SeriesId": series_id,
            "Limit": "1",
            "Fields": "PrimaryImageAspectRatio,ImageTags,Path,UserData,DateCreated,PremiereDate",
        },
        timeout=25,
    )
    items = _items_from(data)
    return items[0] if items else None


def get_next_up(limit: int = 60) -> Dict[str, Any]:
    base = _base()
    if not base:
        raise RuntimeError("JELLYFIN_URL not set (and jellyfin_url not set in settings)")

    user_id = find_user_id_by_name(_username())
    view_id = _mytv_view_id()

    ck = f"jf_nextup_merged:{user_id}:{limit}:{view_id}"
    cached = _cache_get(ck)
    if cached is not None:
        return {"Items": cached}

    merged: List[Dict[str, Any]] = []

    try:
        merged.extend(_nextup_raw(user_id, limit=max(limit, 200)))
    except Exception:
        pass

    if view_id:
        try:
            series_items = _mytv_series_list(user_id, view_id, limit=500)
            for s in series_items:
                sid = s.get("Id")
                if not sid:
                    continue
                ep = _nextup_for_series(user_id, sid)
                if ep:
                    merged.append(ep)
        except Exception:
            pass

    by_series: Dict[str, Dict[str, Any]] = OrderedDict()
    for ep in merged:
        series_id = ep.get("SeriesId") or ""
        series_name = ep.get("SeriesName") or ep.get("Series") or ep.get("Name") or "Series"
        key = series_id or series_name
        if key in by_series:
            continue
        by_series[key] = ep

    out = list(by_series.values())
    _cache_set(ck, out, ttl=15)
    return {"Items": out}

# ----------------------------
# Cache invalidation
# ----------------------------
def clear_nextup_cache():
    keys = [k for k in list(_cache.keys()) if k.startswith("jf_nextup_merged:")]
    for k in keys:
        _cache.pop(k, None)


# ----------------------------
# Recent Unwatched Movies
# ----------------------------

def get_recent_unwatched_movies(limit: int = 10) -> List[Dict[str, Any]]:
    base = _base()
    if not base:
        raise RuntimeError("JELLYFIN_URL not set (and jellyfin_url not set in settings)")

    user_id = find_user_id_by_name(_username())

    data = _get(
        f"{base}/Users/{user_id}/Items",
        params={
            "IncludeItemTypes": "Movie",
            "SortBy": "DateCreated",
            "SortOrder": "Descending",
            "Limit": str(limit),
            "Recursive": "true",
            "Fields": "PrimaryImageAspectRatio,ImageTags,DateCreated,UserData,ProductionYear",
            "Filters": "IsUnplayed",
        },
    )

    items = _items_from(data)

    out: List[Dict[str, Any]] = []
    for it in items:
        item_id = it.get("Id")
        if not item_id:
            continue

        title = it.get("Name") or "Movie"
        year = it.get("ProductionYear")
        date_added = it.get("DateCreated")

        tags = (it.get("ImageTags") or {})
        primary_tag = tags.get("Primary")

        poster_url = f"/img/jellyfin/primary/{item_id}"
        if primary_tag:
            poster_url = f"{poster_url}?tag={primary_tag}"

        out.append(
            {
                "item_id": item_id,
                "title": title,
                "year": year,
                "date_added": date_added,
                "poster_url": poster_url,
                "jellyfin_web_url": f"{_play_base()}/web/index.html#!/details?id={item_id}",
            }
        )

    return out


# ----------------------------
# Series Remaining
# ----------------------------

def _episode_is_real_file(ep: Dict[str, Any]) -> bool:
    loc = (ep.get("LocationType") or "").strip().lower()
    if not loc:
        return True
    if loc in {"virtual", "offline", "unavailable"}:
        return False
    return True


def _is_played(ep: Dict[str, Any]) -> bool:
    ud = ep.get("UserData") or {}
    if ud.get("Played") is True:
        return True
    try:
        if int(ud.get("PlayCount") or 0) > 0:
            return True
    except Exception:
        pass
    return False


def _count_unplayed_episodes_for_series(user_id: str, series_id: str) -> int:
    base = _base()
    data = _get(
        f"{base}/Users/{user_id}/Items",
        params={
            "ParentId": series_id,
            "IncludeItemTypes": "Episode",
            "Recursive": "true",
            "Fields": "UserData,LocationType",
            "Limit": "20000",
        },
        timeout=45,
    )

    items = _items_from(data)

    remaining = 0
    for ep in items:
        if not _episode_is_real_file(ep):
            continue
        if _is_played(ep):
            continue
        remaining += 1

    return remaining


def get_series_remaining_from_nextup(limit_series: int = 30, nextup_limit: int = 200) -> List[Dict[str, Any]]:
    base = _base()
    if not base:
        raise RuntimeError("JELLYFIN_URL not set (and jellyfin_url not set in settings)")

    user_id = find_user_id_by_name(_username())

    nextup = get_next_up(limit=nextup_limit)
    items = _items_from(nextup)

    series_map: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    for it in items:
        sid = it.get("SeriesId") or it.get("ParentId")
        if not sid:
            continue
        if sid in series_map:
            continue

        nid = it.get("Id")

        series_map[sid] = {
            "series_id": sid,
            "series": it.get("SeriesName") or it.get("Series") or it.get("Name") or "Series",
            "next_item_id": nid,
            "next_title": it.get("Name") or "",
            "season": it.get("SeasonName") or "",
            "episode": it.get("IndexNumber"),
            "premiere_date": it.get("PremiereDate"),
            "path": it.get("Path") or "",
            "series_tag": it.get("SeriesPrimaryImageTag"),
            "primary_tag": (it.get("ImageTags") or {}).get("Primary"),
        }

        if len(series_map) >= limit_series:
            break

    out: List[Dict[str, Any]] = []
    for sid, row in series_map.items():
        remaining = _count_unplayed_episodes_for_series(user_id=user_id, series_id=sid)

        series_tag = row.get("series_tag")
        primary_tag = row.get("primary_tag")
        next_item_id = row.get("next_item_id")

        if series_tag:
            primary_image_url = f"/img/jellyfin/series/{sid}?tag={series_tag}"
        else:
            primary_image_url = f"/img/jellyfin/primary/{next_item_id}" if next_item_id else ""
            if primary_tag and next_item_id:
                primary_image_url = f"{primary_image_url}?tag={primary_tag}"

        out.append(
            {
                **row,
                "remaining_episodes": remaining,
                "primary_image_url": primary_image_url,
                "jellyfin_web_url": f"{_play_base()}/web/index.html#!/details?id={next_item_id}",
            }
        )

    out.sort(key=lambda x: int(x.get("remaining_episodes") or 0), reverse=True)
    return out

def _norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        elif ch in (" ", "-", "_", ".", ":"):
            out.append(" ")
    return " ".join("".join(out).split())


def find_in_library_batch(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    items: [{key, title, year, media_type}]
    returns: {key: {"in_library": bool, "item_id": str, "title": str, "year": str}}
    """
    base = _base()
    if not base:
        raise RuntimeError("JELLYFIN_URL not set (and jellyfin_url not set in settings)")

    user_id = find_user_id_by_name(_username())
    out: Dict[str, Dict[str, Any]] = {}

    if not items:
        return out

    def build_search_terms(title: str) -> List[str]:
        terms: List[str] = []
        seen = set()

        def add(val: str):
            v = str(val or "").strip()
            if not v:
                return
            k = v.lower()
            if k in seen:
                return
            seen.add(k)
            terms.append(v)

        add(title)

        lowered = title.strip()
        for prefix in ("The ", "A ", "An "):
            if lowered.startswith(prefix):
                add(lowered[len(prefix):])

        if ":" in title:
            add(title.split(":", 1)[0].strip())

        add(title.replace(" 4 ", " Four "))
        add(title.replace(" 4:", " Four:"))
        add(title.replace(" Four ", " 4 "))
        add(title.replace(" Four:", " 4:"))

        return terms[:6]

    for raw in items:
        key = str(raw.get("key") or "").strip()
        title = str(raw.get("title") or "").strip()
        year = str(raw.get("year") or "").strip()
        media_type = str(raw.get("media_type") or "").strip().lower()
        want_tmdb_id = str(raw.get("tmdb_id") or "").strip()

        if not key or not title or media_type not in ("movie", "tv"):
            continue

        include_types = "Movie" if media_type == "movie" else "Series"
        cache_key = f"jf_findlib:{media_type}:{_norm_title(title)}:{year}:{want_tmdb_id}"
        cached = _cache_get(cache_key)
        if cached is not None:
            out[key] = cached
            continue

        search_terms = build_search_terms(title)
        want_year = year[:4] if year else ""
        want_norms = [_norm_title(t) for t in search_terms if _norm_title(t)]

        candidates: List[Dict[str, Any]] = []
        seen_ids = set()

        for term in search_terms:
            try:
                data = _get(
                    f"{base}/Users/{user_id}/Items",
                    params={
                        "SearchTerm": term,
                        "IncludeItemTypes": include_types,
                        "Recursive": "true",
                        "Limit": "25",
                        "Fields": "ProductionYear,OriginalTitle,SortName,ProviderIds",
                    },
                    timeout=20,
                )
                batch = _items_from(data)
            except Exception:
                batch = []

            for item in batch:
                item_id = str(item.get("Id") or "").strip()
                if item_id and item_id in seen_ids:
                    continue
                if item_id:
                    seen_ids.add(item_id)
                candidates.append(item)

        best = None
        best_score = -1

        # Exact TMDb match wins immediately.
        if want_tmdb_id:
            for item in candidates:
                provider_ids = item.get("ProviderIds") or {}
                cand_tmdb_id = str(provider_ids.get("Tmdb") or "").strip() if isinstance(provider_ids, dict) else ""
                if cand_tmdb_id and cand_tmdb_id == want_tmdb_id:
                    cand_year = str(item.get("ProductionYear") or "")[:4]
                    best = {
                        "in_library": True,
                        "item_id": str(item.get("Id") or ""),
                        "title": str(item.get("Name") or title),
                        "year": cand_year or want_year,
                    }
                    best_score = 9999
                    break

        if best and best.get("in_library"):
            _cache_set(cache_key, best, ttl=600)
            out[key] = best
            continue

        for item in candidates:
            cand_year = str(item.get("ProductionYear") or "")[:4]
            cand_titles = [
                str(item.get("Name") or "").strip(),
                str(item.get("OriginalTitle") or "").strip(),
                str(item.get("SortName") or "").strip(),
            ]
            cand_norms = [_norm_title(x) for x in cand_titles if _norm_title(x)]

            score = 0

            # TV/anime matching must be much stricter than movie matching.
            # Prevent franchise collisions like:
            # - One Piece (anime) vs One Piece (live action)
            # - Fullmetal Alchemist vs Fullmetal Alchemist Brotherhood
            if media_type == "tv":
                def _strip_paren_suffix(v: str) -> str:
                    s = str(v or "").strip()
                    if s.endswith(")") and "(" in s:
                        base = s.rsplit("(", 1)[0].strip()
                        if base:
                            return base
                    return s

                exact_title_match = any(cn == wn for cn in cand_norms for wn in want_norms)

                # Allow safe variants like:
                # - The Office  <-> The Office (US)
                # but do NOT allow broad substring matching.
                paren_variant_match = any(
                    _norm_title(_strip_paren_suffix(cn)) == _norm_title(_strip_paren_suffix(wn))
                    for cn in cand_titles if str(cn or "").strip()
                    for wn in search_terms if str(wn or "").strip()
                )

                if not exact_title_match and not paren_variant_match:
                    continue

                if want_year and cand_year and cand_year != want_year:
                    continue

                score += 120
                if paren_variant_match and not exact_title_match:
                    score += 5
                if want_year and cand_year and cand_year == want_year:
                    score += 20
            else:
                # If TMDb id was supplied and we did not get an exact ProviderIds.Tmdb match,
                # do not allow fuzzy title matching to mark this as in-library.
                if want_tmdb_id:
                    continue

                if any(cn in want_norms for cn in cand_norms):
                    score += 100

                for wn in want_norms:
                    for cn in cand_norms:
                        if not wn or not cn:
                            continue
                        if wn == cn:
                            score += 100
                        elif len(wn) >= 6 and len(cn) >= 6 and (wn in cn or cn in wn):
                            score += 35

                if want_year and cand_year:
                    if cand_year == want_year:
                        score += 20
                    else:
                        score -= 15

            if score > best_score:
                best_score = score
                best = {
                    "in_library": score >= 35,
                    "item_id": str(item.get("Id") or "") if score >= 35 else "",
                    "title": str(item.get("Name") or cand_titles[0] or title),
                    "year": cand_year or want_year,
                }

        if not best or not best.get("in_library"):
            best = {
                "in_library": False,
                "item_id": "",
                "title": title,
                "year": want_year,
            }

        _cache_set(cache_key, best, ttl=600)
        out[key] = best

    return out

    def build_search_terms(title: str) -> List[str]:
        terms: List[str] = []
        seen = set()

        def add(val: str):
            v = str(val or "").strip()
            if not v:
                return
            k = v.lower()
            if k in seen:
                return
            seen.add(k)
            terms.append(v)

        add(title)

        lowered = title.strip()
        for prefix in ("The ", "A ", "An "):
            if lowered.startswith(prefix):
                add(lowered[len(prefix):])

        if ":" in title:
            add(title.split(":", 1)[0].strip())

        add(title.replace(" 4 ", " Four "))
        add(title.replace(" 4:", " Four:"))
        add(title.replace(" Four ", " 4 "))
        add(title.replace(" Four:", " 4:"))

        return terms[:6]

    for raw in items:
        key = str(raw.get("key") or "").strip()
        title = str(raw.get("title") or "").strip()
        year = str(raw.get("year") or "").strip()
        media_type = str(raw.get("media_type") or "").strip().lower()

        if not key or not title or media_type not in ("movie", "tv"):
            continue

        include_types = "Movie" if media_type == "movie" else "Series"
        cache_key = f"jf_findlib:{media_type}:{_norm_title(title)}:{year}"
        cached = _cache_get(cache_key)
        if cached is not None:
            out[key] = cached
            continue

        search_terms = build_search_terms(title)
        want_year = year[:4] if year else ""
        want_norms = [_norm_title(t) for t in search_terms if _norm_title(t)]

        candidates: List[Dict[str, Any]] = []
        seen_ids = set()

        for term in search_terms:
            try:
                data = _get(
                    f"{base}/Users/{user_id}/Items",
                    params={
                        "SearchTerm": term,
                        "IncludeItemTypes": include_types,
                        "Recursive": "true",
                        "Limit": "25",
                        "Fields": "ProductionYear,OriginalTitle,SortName",
                    },
                    timeout=20,
                )
                batch = _items_from(data)
            except Exception:
                batch = []

            for item in batch:
                item_id = str(item.get("Id") or "").strip()
                if item_id and item_id in seen_ids:
                    continue
                if item_id:
                    seen_ids.add(item_id)
                candidates.append(item)

        best = None
        best_score = -1

        for item in candidates:
            cand_year = str(item.get("ProductionYear") or "")[:4]
            cand_titles = [
                str(item.get("Name") or "").strip(),
                str(item.get("OriginalTitle") or "").strip(),
                str(item.get("SortName") or "").strip(),
            ]
            cand_norms = [ _norm_title(x) for x in cand_titles if _norm_title(x) ]

            score = 0

            if any(cn in want_norms for cn in cand_norms):
                score += 100

            for wn in want_norms:
                for cn in cand_norms:
                    if not wn or not cn:
                        continue
                    if wn == cn:
                        score += 100
                    elif len(wn) >= 6 and len(cn) >= 6 and (wn in cn or cn in wn):
                        score += 35

            # For movies, prevent false positives on same-title different-year entries.
            # Example: "Sinners (2025)" must not match "Sinners (1990)".
            if media_type == "movie" and want_year and cand_year and cand_year != want_year:
                continue

            # For movies, do not allow same-title different-year matches.
            if media_type == "movie" and want_year and cand_year and cand_year != want_year:
                continue

            if want_year and cand_year:
                if cand_year == want_year:
                    score += 20
                else:
                    score -= 15

            if score > best_score:
                best_score = score
                best = {
                    "in_library": score >= 35,
                    "item_id": str(item.get("Id") or "") if score >= 35 else "",
                    "title": str(item.get("Name") or cand_titles[0] or title),
                    "year": cand_year or want_year,
                }

        if not best or not best.get("in_library"):
            best = {
                "in_library": False,
                "item_id": "",
                "title": title,
                "year": want_year,
            }

        _cache_set(cache_key, best, ttl=600)
        out[key] = best

    return out

    for raw in items:
        key = str(raw.get("key") or "").strip()
        title = str(raw.get("title") or "").strip()
        year = str(raw.get("year") or "").strip()
        media_type = str(raw.get("media_type") or "").strip().lower()

        if not key or not title or media_type not in ("movie", "tv"):
            continue

        include_types = "Movie" if media_type == "movie" else "Series"
        cache_key = f"jf_findlib:{media_type}:{_norm_title(title)}:{year}"
        cached = _cache_get(cache_key)
        if cached is not None:
            out[key] = cached
            continue

        try:
            data = _get(
                f"{base}/Users/{user_id}/Items",
                params={
                    "SearchTerm": title,
                    "IncludeItemTypes": include_types,
                    "Recursive": "true",
                    "Limit": "15",
                    "Fields": "ProductionYear,OriginalTitle",
                },
                timeout=20,
            )
            candidates = _items_from(data)
        except Exception:
            candidates = []

        want_title = _norm_title(title)
        want_year = year[:4] if year else ""

        best = None

        for item in candidates:
            cand_title = (
                item.get("Name")
                or item.get("OriginalTitle")
                or ""
            )
            cand_year = str(item.get("ProductionYear") or "")[:4]

            if _norm_title(cand_title) != want_title:
                continue

            if want_year and cand_year and cand_year != want_year:
                continue

            best = {
                "in_library": True,
                "item_id": str(item.get("Id") or ""),
                "title": str(item.get("Name") or cand_title or title),
                "year": cand_year or want_year,
            }
            break

        if not best:
            best = {
                "in_library": False,
                "item_id": "",
                "title": title,
                "year": want_year,
            }

        _cache_set(cache_key, best, ttl=600)
        out[key] = best

    return out

