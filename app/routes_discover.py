import time
import requests


def _qd_tmdb_debug(event: str, **kwargs):
    try:
        parts = []
        for k, v in kwargs.items():
            parts.append(f"{k}={repr(v)}")
        print(f"[discover-tmdb] {event} " + " ".join(parts), flush=True)
    except Exception:
        pass

from flask import Blueprint, jsonify, render_template, request

from .db import get_db
from .discover.engine import normalize_and_score_items
from .discover.providers.tmdb import (
    fetch_tmdb_trending,
    fetch_tmdb_popular, enrich_tmdb_item_by_id,
    fetch_tmdb_discover_by_genre,
    fetch_tmdb_discover_by_provider,
    consume_tmdb_enrich_stats
)

from .discover.providers.tvmaze import fetch_tvmaze_airing
from .discover.providers.trakt import (
    fetch_trakt_trending,
    fetch_trakt_popular,
)
from .discover.providers.anilist import (
    fetch_anilist_trending,
    fetch_anilist_popular,
    fetch_anilist_genre,
)
from .discover.providers.jikan import (
    fetch_jikan_anime_hot,
    fetch_jikan_anime_rising,
)

from .utils.auth import login_required, current_user
from .clients.jellyfin import find_in_library_batch
from .clients.sonarr import find_requested_series_batch
from .clients.radarr import find_requested_movies_batch
from app.clients.letterboxd import get_letterboxd_popular, get_letterboxd_popular_feed, get_letterboxd_popular_aggregate, get_letterboxd_feed_sources
from .clients.jellyfin_for_you import build_for_you_profile

bp = Blueprint("discover", __name__)

_DISCOVER_CACHE: dict[tuple[str, str, str, str, str, str, int, str], dict] = {}
_DISCOVER_CACHE_TTL = 300

_DISCOVER_SOURCE_CACHE: dict[tuple[str, str, int, str, str, str], dict] = {}
_DISCOVER_SOURCE_CACHE_TTL = 3600


def _source_cache_clear():
    try:
        _DISCOVER_SOURCE_CACHE.clear()
    except Exception:
        pass


def _source_cache_get(source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_key: str = ""):
    key = (str(source), str(media), int(page), str(genre), str(provider), str(year_key))
    row = _DISCOVER_SOURCE_CACHE.get(key)
    if not row:
        return None

    ts = float(row.get("ts") or 0)
    if (time.time() - ts) > _DISCOVER_SOURCE_CACHE_TTL:
        _DISCOVER_SOURCE_CACHE.pop(key, None)
        return None

    return row.get("payload")


def _source_cache_set(source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_key: str = "", payload: dict | None = None):
    key = (str(source), str(media), int(page), str(genre), str(provider), str(year_key))
    _DISCOVER_SOURCE_CACHE[key] = {
        "ts": time.time(),
        "payload": payload or {},
    }


def _discover_settings_signature() -> str:
    parts = [
        _app_setting("discover_weight_tmdb", "1.0"),
        _app_setting("discover_weight_trakt", "1.0"),
        _app_setting("discover_weight_anilist", "0.92"),
        _app_setting("discover_bonus_2", "0.08"),
        _app_setting("discover_bonus_3", "0.18"),
        _app_setting("discover_bonus_4", "0.28"),
        _app_setting("discover_bonus_5", "0.34"),
        _app_setting("discover_bonus_6", "0.40"),
        _app_setting("discover_bonus_5", "0.34"),
        _app_setting("discover_bonus_6", "0.40"),
        _app_setting("discover_hot_threshold", "0.82"),
        _app_setting("discover_enrich_scale", "100"),
        _app_setting("tmdb_bearer_token", "")[:8],
        _app_setting("tmdb_api_key", "")[:8],
        _app_setting("trakt_client_id", "")[:8],
    ]
    return "|".join(parts)


def _cache_get(source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_from: str = "", year_to: str = ""):
    key = (str(source), str(media), str(genre), str(provider), str(year_from), str(year_to), int(page), _discover_settings_signature())
    row = _DISCOVER_CACHE.get(key)
    if not row:
        return None

    ts = float(row.get("ts") or 0)
    if (time.time() - ts) > _DISCOVER_CACHE_TTL:
        _DISCOVER_CACHE.pop(key, None)
        return None

    return row.get("payload")


def _cache_set(source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_from: str = "", year_to: str = "", payload: dict | None = None):
    key = (str(source), str(media), str(genre), str(provider), str(year_from), str(year_to), int(page), _discover_settings_signature())
    _DISCOVER_CACHE[key] = {
        "ts": time.time(),
        "payload": payload,
    }


def _app_setting(key: str, default: str = "") -> str:
    db = get_db()
    try:
        row = db.execute(
            "SELECT value FROM app_settings WHERE key = ?",
            (str(key),),
        ).fetchone()
    except Exception:
        return default

    if not row:
        return default
    return str(row["value"] or default)


def _tmdb_auth_headers() -> dict:
    bearer = (_app_setting("tmdb_bearer_token") or "").strip()
    if bearer:
        return {
            "Authorization": f"Bearer {bearer}",
            "accept": "application/json",
        }
    return {"accept": "application/json"}


def _tmdb_auth_params() -> dict:
    api_key = (_app_setting("tmdb_api_key") or "").strip()
    return {"api_key": api_key} if api_key else {}


def _tmdb_is_configured() -> bool:
    return bool(
        (_app_setting("tmdb_bearer_token") or "").strip()
        or (_app_setting("tmdb_api_key") or "").strip()
    )


def _trakt_is_configured() -> bool:
    return bool((_app_setting("trakt_client_id") or "").strip())


def _app_float(key: str, default: float) -> float:
    try:
        return float((_app_setting(key, str(default)) or str(default)).strip())
    except Exception:
        return float(default)


def _timed(label: str, fn):
    started = time.time()
    result = fn()
    elapsed = round(time.time() - started, 3)
    print(f"[discover-step] {label} took {elapsed}s", flush=True)
    return result


def _aggregate_enrich_limit(page: int) -> int:
    try:
        page = int(page)
    except Exception:
        page = 1

    if page <= 1:
        base = 80
    elif page == 2:
        base = 24
    elif page == 3:
        base = 8
    else:
        base = 0

    try:
        scale = max(0.0, min(1.0, float(_app_setting("discover_enrich_scale", "100")) / 100.0))
    except Exception:
        scale = 1.0

    return max(0, int(round(base * scale)))


def _prioritize_rich_aggregate_items(items: list[dict]) -> list[dict]:
    def sort_key(x: dict):
        provider_hits = int(x.get("provider_hits") or 0)
        has_poster = 1 if str(x.get("poster_url") or "").strip() else 0
        has_rating = 1 if x.get("vote_average") is not None else 0
        has_overview = 1 if str(x.get("overview") or "").strip() else 0
        composite = float(x.get("composite_score") or 0.0)

        return (
            has_poster,
            has_rating,
            min(provider_hits, 3),
            has_overview,
            composite,
        )

    return sorted(items or [], key=sort_key, reverse=True)


def _get_tmdb_trending_cached(media: str, page: int):
    cached = _source_cache_get("tmdb_trending", media, page)
    if cached is not None:
        return cached

    payload = fetch_tmdb_trending(
        media=media,
        page=page,
        headers=_tmdb_auth_headers(),
        auth_params=_tmdb_auth_params(),
    )
    _source_cache_set("tmdb_trending", media, page, payload=payload)
    return payload


def _get_tmdb_popular_cached(media: str, page: int):
    cached = _source_cache_get("tmdb_popular", media, page)
    if cached is not None:
        return cached

    payload = fetch_tmdb_popular(
        media=media,
        page=page,
        headers=_tmdb_auth_headers(),
        auth_params=_tmdb_auth_params(),
    )
    _source_cache_set("tmdb_popular", media, page, payload=payload)
    return payload


def _get_trakt_trending_cached(media: str, page: int):
    cached = _source_cache_get("trakt_trending", media, page)
    if cached is not None:
        return cached

    payload = fetch_trakt_trending(
        media=media,
        client_id=_app_setting("trakt_client_id"),
        limit=40,
        page=page,
    )
    _source_cache_set("trakt_trending", media, page, payload=payload)
    return payload


def _get_trakt_popular_cached(media: str, page: int):
    cached = _source_cache_get("trakt_popular", media, page)
    if cached is not None:
        return cached

    payload = fetch_trakt_popular(
        media=media,
        client_id=_app_setting("trakt_client_id"),
        limit=40,
        page=page,
    )
    _source_cache_set("trakt_popular", media, page, payload=payload)
    return payload


def _fill_missing_tmdb_artwork(items: list[dict], budget: int = 20) -> list[dict]:
    if not _tmdb_is_configured():
        return items or []

    out = []
    used = 0

    for raw in (items or []):
        item = dict(raw or {})
        tmdb_id = int(item.get("tmdb_id") or 0)
        media_type = str(item.get("media_type") or "").strip().lower()
        has_poster = bool(str(item.get("poster_url") or "").strip())

        if tmdb_id and media_type in ("movie", "tv") and not has_poster and used < max(0, int(budget or 0)):
            try:
                enriched = enrich_tmdb_item_by_id(
                    tmdb_id=tmdb_id,
                    media_type=media_type,
                    headers=_tmdb_auth_headers(),
                    auth_params=_tmdb_auth_params(),
                )
            except Exception:
                enriched = None

            used += 1

            if enriched:
                if not item.get("poster_url") and enriched.get("poster_url"):
                    item["poster_url"] = enriched.get("poster_url")
                if not item.get("backdrop_url") and enriched.get("backdrop_url"):
                    item["backdrop_url"] = enriched.get("backdrop_url")
                if not item.get("overview") and enriched.get("overview"):
                    item["overview"] = enriched.get("overview")
                if not item.get("title") and enriched.get("title"):
                    item["title"] = enriched.get("title")
                if not item.get("year") and enriched.get("year"):
                    item["year"] = enriched.get("year")
                if not item.get("vote_average") and enriched.get("vote_average") is not None:
                    item["vote_average"] = enriched.get("vote_average")

        out.append(item)

    return out


def _get_tmdb_genre_cached(media: str, page: int, genre: str, year_from: str = "", year_to: str = ""):
    year_key = f"{year_from}:{year_to}"
    cached = _source_cache_get("tmdb_genre", media, page, genre=genre, year_key=year_key)
    if cached is not None:
        return cached

    payload = fetch_tmdb_discover_by_genre(
        media=media,
        genre_id=genre,
        page=page,
        headers=_tmdb_auth_headers(),
        auth_params=_tmdb_auth_params(),
        year_from=year_from,
        year_to=year_to,
        pages_deep=3,
    )
    _source_cache_set("tmdb_genre", media, page, genre=genre, year_key=year_key, payload=payload)
    return payload


def _get_tmdb_provider_cached(media: str, page: int, provider: str, year_from: str = "", year_to: str = ""):
    year_key = f"{year_from}:{year_to}"
    cached = _source_cache_get("tmdb_provider", media, page, provider=provider, year_key=year_key)
    if cached is not None:
        return cached

    payload = fetch_tmdb_discover_by_provider(
        media=media,
        provider_id=provider,
        page=page,
        headers=_tmdb_auth_headers(),
        auth_params=_tmdb_auth_params(),
        year_from=year_from,
        year_to=year_to,
        pages_deep=3,
    )
    _source_cache_set("tmdb_provider", media, page, provider=provider, year_key=year_key, payload=payload)
    return payload


def _get_tvmaze_airing_cached(media: str, page: int):
    cached = _source_cache_get("tvmaze_airing", media, page)
    if cached is not None:
        return cached

    payload = fetch_tvmaze_airing(
        media=media,
        page=page,
        days_per_page=3,
    )
    _source_cache_set("tvmaze_airing", media, page, payload=payload)
    return payload


def _get_anilist_trending_cached(page: int):
    cached = _source_cache_get("anilist_trending", "tv", page)
    if cached is not None:
        return cached

    payload = fetch_anilist_trending(page=page, per_page=25)
    _source_cache_set("anilist_trending", "tv", page, payload=payload)
    return payload


def _get_jikan_anime_hot_cached(page: int):
    cached = _source_cache_get("jikan_anime_hot", "tv", page)
    if cached is not None:
        return cached

    payload = fetch_jikan_anime_hot(page=page, per_page=25)
    _source_cache_set("jikan_anime_hot", "tv", page, payload=payload)
    return payload


def _get_jikan_anime_rising_cached(page: int):
    cached = _source_cache_get("jikan_anime_rising", "tv", page)
    if cached is not None:
        return cached

    payload = fetch_jikan_anime_rising(page=page, per_page=25)
    _source_cache_set("jikan_anime_rising", "tv", page, payload=payload)
    return payload


def _get_anilist_popular_cached(page: int):
    cached = _source_cache_get("anilist_popular", "tv", page)
    if cached is not None:
        return cached

    payload = fetch_anilist_popular(page=page, per_page=25)
    _source_cache_set("anilist_popular", "tv", page, payload=payload)
    return payload


def _get_anilist_genre_cached(page: int, genre: str):
    cached = _source_cache_get("anilist_genre", "tv", page, genre=genre)
    if cached is not None:
        return cached

    payload = fetch_anilist_genre(genre=genre, page=page, per_page=25)
    _source_cache_set("anilist_genre", "tv", page, genre=genre, payload=payload)
    return payload


def _normalize_anilist_genre_name(raw: str) -> str:
    val = str(raw or "").strip()
    if not val or val.lower() == "all":
        return "Action"

    tmdb_to_anilist = {
        "28": "Action",
        "12": "Adventure",
        "16": "Animation",
        "35": "Comedy",
        "80": "Crime",
        "99": "Documentary",
        "18": "Drama",
        "10751": "Fantasy",
        "14": "Fantasy",
        "36": "Mystery",
        "27": "Horror",
        "10402": "Music",
        "9648": "Mystery",
        "10749": "Romance",
        "878": "Sci-Fi",
        "10770": "Slice of Life",
        "53": "Thriller",
        "10752": "Action",
        "37": "Western",
    }

    if val in tmdb_to_anilist:
        return tmdb_to_anilist[val]

    valid = {
        "action": "Action",
        "adventure": "Adventure",
        "animation": "Animation",
        "comedy": "Comedy",
        "crime": "Crime",
        "drama": "Drama",
        "ecchi": "Ecchi",
        "fantasy": "Fantasy",
        "hentai": "Hentai",
        "horror": "Horror",
        "mahou shoujo": "Mahou Shoujo",
        "mecha": "Mecha",
        "music": "Music",
        "mystery": "Mystery",
        "psychological": "Psychological",
        "romance": "Romance",
        "sci-fi": "Sci-Fi",
        "slice of life": "Slice of Life",
        "sports": "Sports",
        "supernatural": "Supernatural",
        "thriller": "Thriller",
    }

    return valid.get(val.lower(), "Action")


def _qd_norm_lookup_title(s: str) -> str:
    return "".join(ch.lower() for ch in str(s or "") if ch.isalnum())


def _qd_strip_anime_season_suffix(title: str) -> str:
    s = str(title or "").strip()
    if not s:
        return s

    import re

    out = s

    # Handle season/subtitle style names like:
    # "JUJUTSU KAISEN Season 3: The Culling Game Part 1"
    # "[OSHI NO KO] Season 3"
    # "Show Name - Season 2"
    out = re.sub(r'\s*[:\-–—]\s*season\s+\d+.*$', '', out, flags=re.I).strip(' :-')
    out = re.sub(r'\s+season\s+\d+\s*[:\-–—].*$', '', out, flags=re.I).strip(' :-')

    patterns = [
        r'\s+season\s+\d+\s*part\s*\d+$',
        r'\s+season\s+\d+$',
        r'\s+\d+(st|nd|rd|th)\s+season$',
        r'\s+part\s+\d+$',
        r'\s+cour\s+\d+$',
    ]

    changed = True
    while changed:
        changed = False
        for pat in patterns:
            newer = re.sub(pat, '', out, flags=re.I).strip(' :-')
            if newer != out:
                out = newer
                changed = True

    # If a title still contains "Season N" in the middle, keep only the parent show portion.
    newer = re.sub(r'^(.*?)\s+season\s+\d+.*$', r'\1', out, flags=re.I).strip(' :-')
    if newer:
        out = newer

    return out or s




def _qd_get_title_override(title: str) -> str:
    try:
        raw = str(_app_setting("discover_title_overrides", "") or "")
    except Exception as e:
        _qd_tmdb_debug("override-read-error", title=title, error=str(e))
        return ""

    want = _qd_norm_lookup_title(title)
    _qd_tmdb_debug("override-check", title=title, want=want, raw=raw)

    for line in raw.splitlines():
        if "|" not in line:
            continue

        left, right = [x.strip() for x in line.split("|", 1)]
        if not left or not right:
            continue

        left_norm = _qd_norm_lookup_title(left)
        if left_norm == want:
            _qd_tmdb_debug("override-hit", title=title, left=left, right=right)
            return right

    _qd_tmdb_debug("override-miss", title=title, want=want)
    return ""

def _tmdb_search_id_for_item(title: str, media_type: str, year: str = "") -> str:
    if not _tmdb_is_configured():
        return ""

    import re

    original_title = str(title or "").strip()
    title = original_title

    override = _qd_get_title_override(original_title)
    override_used = bool(override)
    if override:
        title = override

    media_type = str(media_type or "").strip().lower()
    year = str(year or "").strip()[:4]

    # If an admin override is being used, trust the override title and do not
    # constrain the TMDb search by AniList's season/year value.
    if override_used:
        year = ""

    _qd_tmdb_debug(
        "override-state",
        original_title=original_title,
        effective_title=title,
        override=override,
        override_used=override_used,
    )

    if not title or media_type not in ("movie", "tv"):
        return ""

    def _dedupe_queries(values):
        out = []
        seen = set()
        for v in values:
            s = str(v or "").strip()
            if not s:
                continue
            k = s.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(s)
        return out

    def _strip_duplicate_year_suffix(s: str) -> str:
        out = str(s or "").strip()
        out = re.sub(r'\s*\((\d{4})\)\s*\(\1\)\s*$', r' (\1)', out).strip()
        out = re.sub(r'\s*\(\d{4}\)\s*$', '', out).strip()
        return out

    def _strip_final_season_terms(s: str) -> str:
        out = str(s or "").strip()
        out = re.sub(r'\s+final\s+season(\s+part\s+\d+)?$', '', out, flags=re.I).strip(' :-')
        out = re.sub(r'\s+season\s+\d+(\s+part\s+\d+)?$', '', out, flags=re.I).strip(' :-')
        out = re.sub(r'\s+part\s+\d+$', '', out, flags=re.I).strip(' :-')
        return out

    def _split_title_variants(s: str) -> list[str]:
        s = str(s or "").strip()
        if not s:
            return []

        variants = [s]

        stripped = _qd_strip_anime_season_suffix(s)
        variants.append(stripped)

        deduped = _strip_duplicate_year_suffix(s)
        variants.append(deduped)

        final_clean = _strip_final_season_terms(stripped or s)
        variants.append(final_clean)

        if ":" in s:
            left, right = s.split(":", 1)
            variants.append(left.strip())
            variants.append(right.strip())

        return _dedupe_queries(variants)

    def _search_kind(query_title: str, kind: str, year_val: str = "", allow_no_year: bool = False) -> str:
        query_param_sets = []

        params_with_year = dict(_tmdb_auth_params())
        params_with_year["query"] = query_title
        if year_val:
            if kind == "tv":
                params_with_year["first_air_date_year"] = year_val
            else:
                params_with_year["year"] = year_val
        query_param_sets.append(params_with_year)

        if allow_no_year or not year_val:
            params_no_year = dict(_tmdb_auth_params())
            params_no_year["query"] = query_title
            query_param_sets.append(params_no_year)

        want = _qd_norm_lookup_title(query_title)

        for params in query_param_sets:
            try:
                r = requests.get(
                    f"https://api.themoviedb.org/3/search/{kind}",
                    headers=_tmdb_auth_headers(),
                    params=params,
                    timeout=15,
                )
                r.raise_for_status()
                data = r.json() or {}
                results = data.get("results") or []
                _qd_tmdb_debug(
                    "search-results",
                    query=query_title,
                    kind=kind,
                    params=params,
                    result_count=len(results),
                    top_titles=[
                        (
                            str(row.get("name") or row.get("title") or row.get("original_name") or row.get("original_title") or "").strip(),
                            str(row.get("first_air_date") or row.get("release_date") or "")[:4],
                            str(row.get("id") or "").strip(),
                        )
                        for row in results[:5]
                    ],
                )
            except Exception as e:
                _qd_tmdb_debug(
                    "search-error",
                    query=query_title,
                    kind=kind,
                    params=params,
                    error=str(e),
                )
                continue

            for row in results[:8]:
                cand_title = (
                    str(row.get("name") or "").strip()
                    or str(row.get("title") or "").strip()
                    or str(row.get("original_name") or "").strip()
                    or str(row.get("original_title") or "").strip()
                )
                cand_norm = _qd_norm_lookup_title(cand_title)
                cand_date = str(row.get("first_air_date") or row.get("release_date") or "")[:4]
                cand_id = str(row.get("id") or "").strip()

                if not cand_id:
                    continue

                if want and cand_norm == want:
                    if "first_air_date_year" not in params and "year" not in params:
                        _qd_tmdb_debug("match-found", query=query_title, kind=kind, cand_title=cand_title, cand_date=cand_date, cand_id=cand_id, reason="exact-no-year")
                        return cand_id
                    if not year_val or not cand_date or cand_date == year_val:
                        _qd_tmdb_debug("match-found", query=query_title, kind=kind, cand_title=cand_title, cand_date=cand_date, cand_id=cand_id, reason="exact-with-year")
                        return cand_id

            _qd_tmdb_debug("no-match-for-query", query=query_title, kind=kind, year_val=year_val)
        return ""

    kind = "tv" if media_type == "tv" else "movie"
    candidate_queries = _split_title_variants(title)

    _qd_tmdb_debug(
        "resolve-start",
        original_title=original_title,
        effective_title=title,
        override_used=override_used,
        media_type=media_type,
        year=year,
        kind=kind,
        candidate_queries=candidate_queries,
    )

    for query_title in candidate_queries:
        found = _search_kind(
            query_title=query_title,
            kind=kind,
            year_val=year,
            allow_no_year=(media_type == "tv" and (query_title != title or override_used)),
        )
        if found:
            return found

    if media_type == "tv":
        for query_title in candidate_queries:
            found = _search_kind(
                query_title=query_title,
                kind="movie",
                year_val=year,
                allow_no_year=True,
            )
            if found:
                return found

    _qd_tmdb_debug("resolve-miss", original_title=original_title, effective_title=title, media_type=media_type, year=year)
    return ""

def _enrich_anilist_items_with_tmdb(items):
    out = []
    for raw in (items or []):
        item = dict(raw or {})

        tmdb_id = str(item.get("tmdb_id") or "").strip()
        media_type = str(item.get("media_type") or "").strip().lower()

        if not tmdb_id:
            tmdb_id = _tmdb_search_id_for_item(
                title=str(item.get("title") or ""),
                media_type=media_type,
                year=str(item.get("year") or ""),
            )
            if tmdb_id:
                item["tmdb_id"] = tmdb_id

        enriched = None
        if tmdb_id and media_type in ("movie", "tv") and _tmdb_is_configured():
            try:
                enriched = enrich_tmdb_item_by_id(
                    tmdb_id=int(tmdb_id),
                    media_type=media_type,
                    headers=_tmdb_auth_headers(),
                    auth_params=_tmdb_auth_params(),
                )
            except Exception:
                enriched = None

        if enriched:
            merged = dict(enriched)

            # Keep original anime/provider signals alive
            merged_scores = dict(enriched.get("provider_scores") or {})
            merged_scores.update(item.get("provider_scores") or {})
            merged["provider_scores"] = merged_scores

            # Preserve source-hit strength at least as high as original
            merged["provider_hits"] = max(
                int(enriched.get("provider_hits") or 0),
                int(item.get("provider_hits") or 0),
            )

            # Keep original fields when TMDb enrichment doesn't provide them
            for k in (
                "source",
                "title",
                "year",
                "poster_url",
                "external_url",
                "media_type",
                "tmdb_id",
                "imdb_id",
                "tvdb_id",
                "vote_average",
                "vote_count",
                "popularity",
            ):
                if not merged.get(k) and item.get(k):
                    merged[k] = item.get(k)

            if not merged.get("external_url") and merged.get("tmdb_id"):
                merged["external_url"] = f"https://www.themoviedb.org/{'tv' if str(merged.get('media_type') or '') == 'tv' else 'movie'}/{merged.get('tmdb_id')}"

            out.append(merged)
        else:
            if tmdb_id and not item.get("external_url"):
                item["external_url"] = f"https://www.themoviedb.org/{'tv' if media_type == 'tv' else 'movie'}/{tmdb_id}"
            out.append(item)

    return out


@bp.get("/discover")
@login_required
def discover_page():
    return render_template("discover.html", me=(current_user() or {}))


@bp.get("/api/discover/for-you-profile")
@login_required
def api_discover_for_you_profile():
    try:
        profile = build_for_you_profile(limit=200)
        return jsonify(profile)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@bp.post("/api/discover/clear-cache")
@login_required
def api_discover_clear_cache():
    try:
        _DISCOVER_CACHE.clear()
        _DISCOVER_SOURCE_CACHE.clear()
        print("[discover-cache] cleared", flush=True)
        return jsonify(ok=True, cleared=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@bp.post("/api/discover/library-state")
@login_required
def api_discover_library_state():
    try:
        payload = request.get_json(silent=True) or {}
        raw_items = payload.get("items") or []
        if not isinstance(raw_items, list):
            return jsonify(ok=False, error="invalid_items"), 400

        items = []
        for idx, raw in enumerate(raw_items[:100]):
            if not isinstance(raw, dict):
                continue
            items.append({
                "key": str(raw.get("key") or f"row-{idx}"),
                "title": str(raw.get("title") or "").strip(),
                "year": str(raw.get("year") or "").strip(),
                "media_type": str(raw.get("media_type") or "").strip().lower(),
                "tmdb_id": str(raw.get("tmdb_id") or "").strip(),
                "imdb_id": str(raw.get("imdb_id") or "").strip(),
                "tvdb_id": str(raw.get("tvdb_id") or "").strip(),
            })

        library_result = find_in_library_batch(items)

        try:
            sonarr_result = find_requested_series_batch(items)
        except Exception:
            sonarr_result = {}

        try:
            radarr_result = find_requested_movies_batch(items)
        except Exception:
            radarr_result = {}

        merged = {}
        for row in items:
            key = str(row.get("key") or "")
            lib = library_result.get(key) or {}
            son = sonarr_result.get(key) or {}
            rad = radarr_result.get(key) or {}

            in_library = bool(lib.get("in_library"))

            merged[key] = {
                "in_library": in_library,
                "item_id": str(lib.get("item_id") or ""),
                "in_sonarr": (False if in_library else bool(son.get("in_sonarr"))),
                "series_id": ("" if in_library else str(son.get("series_id") or "")),
                "in_radarr": (False if in_library else bool(rad.get("in_radarr"))),
                "movie_id": ("" if in_library else str(rad.get("movie_id") or "")),
            }

            print(
                f"[discover-library-state] key={key} "
                f"title={row.get('title')!r} year={row.get('year')!r} media={row.get('media_type')!r} "
                f"tmdb={row.get('tmdb_id')!r} imdb={row.get('imdb_id')!r} tvdb={row.get('tvdb_id')!r} "
                f"lib={bool(lib.get('in_library'))} lib_item={str(lib.get('item_id') or '')!r} "
                f"sonarr={bool(son.get('in_sonarr'))} sonarr_id={str(son.get('series_id') or '')!r} "
                f"radarr={bool(rad.get('in_radarr'))} radarr_id={str(rad.get('movie_id') or '')!r}",
                flush=True
            )

        return jsonify(ok=True, items=merged)
    except Exception as e:
        return jsonify(ok=False, error=str(e), items={}), 502


@bp.get("/api/discover/letterboxd-sources")
@login_required
def api_discover_letterboxd_sources():
    feeds = get_letterboxd_feed_sources() or []
    return jsonify(
        ok=True,
        items=[
            {
                "key": f"letterboxd_{str(feed.get('key') or '').strip()}",
                "label": str(feed.get("label") or "Feed").strip() or "Feed",
            }
            for feed in feeds
            if str(feed.get("key") or "").strip()
        ],
    )


@bp.get("/api/discover/items")
@login_required
def api_discover_items():
    source = str(request.args.get("source") or "aggregate").strip().lower()
    media = str(request.args.get("media") or "all").strip().lower()
    genre = str(request.args.get("genre") or "all").strip().lower()
    provider = str(request.args.get("provider") or "all").strip().lower()
    min_rating_raw = str(request.args.get("min_rating") or "").strip()
    year_from = str(request.args.get("year_from") or "").strip()
    year_to = str(request.args.get("year_to") or "").strip()

    try:
        page = max(1, min(int(request.args.get("page") or 1), 10))
    except Exception:
        page = 1

    allowed_sources = {"aggregate", "tmdb_trending", "tmdb_popular", "trakt_trending", "trakt_popular", "letterboxd", "letterboxd_aggregate", "tvmaze_airing", "anime_aggregate", "anilist_trending", "anilist_popular", "anilist_genre", "jikan_anime_hot", "jikan_anime_rising"}
    feed_sources = {f"letterboxd_{x.get('key')}" for x in (get_letterboxd_feed_sources() or [])}
    if source not in allowed_sources and source not in feed_sources:
        return jsonify(ok=False, error="unsupported_source"), 400

    if media not in ("all", "movie", "tv", "anime"):
        return jsonify(ok=False, error="unsupported_media"), 400

    min_rating = None
    if min_rating_raw:
        try:
            min_rating = float(min_rating_raw)
        except Exception:
            return jsonify(ok=False, error="invalid_min_rating"), 400

    if genre != "all":
        if source in ("anime_aggregate", "anilist_trending", "anilist_popular", "anilist_genre", "jikan_anime_hot", "jikan_anime_rising"):
            genre = str(genre).strip()
            if not genre:
                return jsonify(ok=False, error="unsupported_genre"), 400
        else:
            try:
                int(genre)
            except Exception:
                return jsonify(ok=False, error="unsupported_genre"), 400

    if source in ("anilist_trending", "anilist_popular"):
        genre = "all"

    if provider != "all":
        try:
            int(provider)
        except Exception:
            return jsonify(ok=False, error="unsupported_provider"), 400

    if year_from:
        try:
            int(year_from)
        except Exception:
            return jsonify(ok=False, error="unsupported_year_from"), 400

    if year_to:
        try:
            int(year_to)
        except Exception:
            return jsonify(ok=False, error="unsupported_year_to"), 400

    if source in ("aggregate", "tmdb_trending") and not _tmdb_is_configured():
        return jsonify(
            ok=False,
            configured=False,
            error="tmdb_not_configured",
            items=[],
        ), 400

    if source in ("trakt_trending", "trakt_popular") and not _trakt_is_configured():
        return jsonify(
            ok=False,
            configured=False,
            error="trakt_not_configured",
            items=[],
        ), 400

    cached = _cache_get(source, media, page, genre, provider, year_from, year_to)
    if cached is not None:
        return jsonify(cached)

    try:
        req_started = time.time()
        items = []
        consume_tmdb_enrich_stats()
        print(f"[discover] source={source} media={media} genre={genre} provider={provider} year_from={year_from} year_to={year_to} page={page}", flush=True)

        if source in ("aggregate", "tmdb_trending"):
            enrich_budget = _aggregate_enrich_limit(page)
            enrich_used = 0

            if provider != "all":
                items = _timed(
                    f"tmdb_provider media={media} page={page} provider={provider}",
                    lambda: _get_tmdb_provider_cached(
                        media=media,
                        page=page,
                        provider=provider,
                        year_from=year_from,
                        year_to=year_to,
                    )
                )
                if genre != "all":
                    items = [
                        i for i in items
                        if str(genre) in [str(x) for x in (i.get("genre_ids") or [])]
                    ]
            elif genre != "all" or year_from or year_to:
                items = _timed(
                    f"tmdb_genre media={media} page={page} genre={genre}",
                    lambda: _get_tmdb_genre_cached(
                        media=media,
                        page=page,
                        genre=genre if genre != "all" else "",
                        year_from=year_from,
                        year_to=year_to,
                    )
                )
            else:
                items = _timed(
                    f"tmdb_trending media={media} page={page}",
                    lambda: _get_tmdb_trending_cached(
                        media=media,
                        page=page,
                    )
                )

        if source == "aggregate" and _trakt_is_configured():
            trakt_items = _timed(
                f"trakt_trending media={media} page={page}",
                lambda: _get_trakt_trending_cached(
                    media=media,
                    page=page,
                )
            )

            by_key = {
                (str(x.get("media_type") or ""), int(x.get("tmdb_id") or 0)): x
                for x in items
                if x.get("tmdb_id")
            }

            enrich_loop_started = time.time()
            for tr in trakt_items:
                key = (str(tr.get("media_type") or ""), int(tr.get("tmdb_id") or 0))
                if key in by_key:
                    existing = by_key[key]
                    merged_scores = dict(existing.get("provider_scores") or {})
                    merged_scores.update(tr.get("provider_scores") or {})
                    existing["provider_scores"] = merged_scores
                    existing["source"] = "TMDb + Trakt"

                    if tr.get("trakt_watchers") is not None:
                        existing["trakt_watchers"] = tr.get("trakt_watchers")
                else:
                    enriched = None
                    if enrich_used < enrich_budget:
                        enriched = enrich_tmdb_item_by_id(
                            tmdb_id=int(tr.get("tmdb_id") or 0),
                            media_type=str(tr.get("media_type") or ""),
                            headers=_tmdb_auth_headers(),
                            auth_params=_tmdb_auth_params(),
                        )
                        enrich_used += 1

                    if enriched:
                        merged = dict(enriched)
                        merged["source"] = "TMDb + Trakt"

                        merged_scores = dict(enriched.get("provider_scores") or {})
                        merged_scores.update(tr.get("provider_scores") or {})
                        merged["provider_scores"] = merged_scores

                        if tr.get("trakt_watchers") is not None:
                            merged["trakt_watchers"] = tr.get("trakt_watchers")
                        items.append(merged)
                    else:
                        items.append(tr)

            enrich_loop_elapsed = round(time.time() - enrich_loop_started, 3)
            print(f"[discover-enrich-loop] source={source} page={page} block=trakt_trending_merge elapsed={enrich_loop_elapsed}s", flush=True)

            letterboxd_items = []
            if media in ("all", "movie"):
                letterboxd_items = _timed(
                    f"letterboxd_aggregate media=movie page={page}",
                    lambda: get_letterboxd_popular_aggregate(page=page)
                )

                enrich_loop_started = time.time()
                for lb in letterboxd_items:
                    key = (str(lb.get("media_type") or ""), int(lb.get("tmdb_id") or 0))
                    if key in by_key:
                        existing = by_key[key]
                        merged_scores = dict(existing.get("provider_scores") or {})
                        merged_scores.update(lb.get("provider_scores") or {})
                        existing["provider_scores"] = merged_scores
                        existing["source"] = "TMDb + Trakt + Letterboxd"
                    else:
                        items.append(lb)
                        if key[1]:
                            by_key[key] = lb

                enrich_loop_elapsed = round(time.time() - enrich_loop_started, 3)
                print(f"[discover-enrich-loop] source={source} page={page} block=letterboxd_merge elapsed={enrich_loop_elapsed}s", flush=True)

            trakt_pop_items = _timed(
                f"trakt_popular media={media} page={page}",
                lambda: _get_trakt_popular_cached(
                    media=media,
                    page=page,
                )
            )

            enrich_loop_started = time.time()
            for tr in trakt_pop_items:
                key = (str(tr.get("media_type") or ""), int(tr.get("tmdb_id") or 0))
                if key in by_key:
                    existing = by_key[key]
                    merged_scores = dict(existing.get("provider_scores") or {})
                    merged_scores.update(tr.get("provider_scores") or {})
                    existing["provider_scores"] = merged_scores
                    existing["source"] = "TMDb + Trakt"
                else:
                    enriched = None
                    if enrich_used < enrich_budget:
                        enriched = enrich_tmdb_item_by_id(
                            tmdb_id=int(tr.get("tmdb_id") or 0),
                            media_type=str(tr.get("media_type") or ""),
                            headers=_tmdb_auth_headers(),
                            auth_params=_tmdb_auth_params(),
                        )
                        enrich_used += 1

                    if enriched:
                        merged = dict(enriched)
                        merged["source"] = "TMDb + Trakt"

                        merged_scores = dict(enriched.get("provider_scores") or {})
                        merged_scores.update(tr.get("provider_scores") or {})
                        merged["provider_scores"] = merged_scores
                        items.append(merged)
                    else:
                        items.append(tr)

            enrich_loop_elapsed = round(time.time() - enrich_loop_started, 3)
            print(f"[discover-enrich-loop] source={source} page={page} block=trakt_popular_merge elapsed={enrich_loop_elapsed}s", flush=True)

            if media == "tv" and int(page) == 1:
                tvmaze_items = _timed(
                    f"tvmaze_airing media=tv page={page}",
                    lambda: _get_tvmaze_airing_cached(
                        media="tv",
                        page=page,
                    )
                )

                def _mk_title_year_key(x):
                    title = str(x.get("title") or "").strip().lower()
                    year = str(x.get("year") or "").strip()[:4]
                    title_norm = "".join(ch if ch.isalnum() else " " for ch in title)
                    title_norm = " ".join(title_norm.split())
                    return f"{title_norm}::{year}"

                by_match = {
                    _mk_title_year_key(x): x
                    for x in items
                    if str(x.get("media_type") or "") == "tv"
                }

                for tvm in tvmaze_items:
                    mkey = str(tvm.get("_match_key") or "")
                    if mkey and mkey in by_match:
                        existing = by_match[mkey]
                        merged_scores = dict(existing.get("provider_scores") or {})
                        merged_scores.update(tvm.get("provider_scores") or {})
                        existing["provider_scores"] = merged_scores
                        existing["source"] = "TMDb + Trakt + TVMaze"
                    else:
                        items.append(tvm)

        elif source == "tmdb_popular":
            items = _timed(
                f"tmdb_popular media={media} page={page}",
                lambda: _get_tmdb_popular_cached(
                    media=media,
                    page=page,
                )
            )

        elif source == "trakt_trending":
            items = _timed(
                f"trakt_trending media={media} page={page}",
                lambda: _get_trakt_trending_cached(
                    media=media,
                    page=page,
                )
            )

            enriched_items = []
            for tr in items:
                enriched = enrich_tmdb_item_by_id(
                    tmdb_id=int(tr.get("tmdb_id") or 0),
                    media_type=str(tr.get("media_type") or ""),
                    headers=_tmdb_auth_headers(),
                    auth_params=_tmdb_auth_params(),
                ) if _tmdb_is_configured() else None

                if enriched:
                    merged = dict(enriched)
                    merged["source"] = "TMDb + Trakt"

                    merged_scores = dict(enriched.get("provider_scores") or {})
                    merged_scores.update(tr.get("provider_scores") or {})
                    merged["provider_scores"] = merged_scores

                    if tr.get("trakt_watchers") is not None:
                        merged["trakt_watchers"] = tr.get("trakt_watchers")
                    enriched_items.append(merged)
                else:
                    enriched_items.append(tr)

            items = enriched_items

        elif source == "trakt_popular":
            items = _timed(
                f"trakt_popular media={media} page={page}",
                lambda: _get_trakt_popular_cached(
                    media=media,
                    page=page,
                )
            )

            enriched_items = []
            for tr in items:
                enriched = enrich_tmdb_item_by_id(
                    tmdb_id=int(tr.get("tmdb_id") or 0),
                    media_type=str(tr.get("media_type") or ""),
                    headers=_tmdb_auth_headers(),
                    auth_params=_tmdb_auth_params(),
                ) if _tmdb_is_configured() else None

                if enriched:
                    merged = dict(enriched)
                    merged["source"] = "TMDb + Trakt"

                    merged_scores = dict(enriched.get("provider_scores") or {})
                    merged_scores.update(tr.get("provider_scores") or {})
                    merged["provider_scores"] = merged_scores
                    enriched_items.append(merged)
                else:
                    enriched_items.append(tr)

            items = enriched_items

        elif source == "letterboxd":
            if media not in ("all", "movie"):
                items = []
            else:
                items = _timed(
                    f"letterboxd media=movie page={page}",
                    lambda: get_letterboxd_popular(page=page)
                )

        elif source == "letterboxd_aggregate":
            if media not in ("all", "movie"):
                items = []
            else:
                items = _timed(
                    f"letterboxd_aggregate media=movie page={page}",
                    lambda: get_letterboxd_popular_aggregate(page=page)
                )

        elif source.startswith("letterboxd_feed_") or source.startswith("letterboxd_feed"):
            if media not in ("all", "movie"):
                items = []
            else:
                feed_key = source.replace("letterboxd_", "", 1)
                items = _timed(
                    f"{source} media=movie page={page}",
                    lambda: get_letterboxd_popular_feed(feed_key=feed_key, page=page)
                )

        elif source == "tvmaze_airing":
            items = _timed(
                f"tvmaze_airing media={media} page={page}",
                lambda: _get_tvmaze_airing_cached(
                    media=media,
                    page=page,
                )
            )

        elif source == "anime_aggregate":
            anilist_items = _timed(
                f"anime_aggregate anilist_trending page={page}",
                lambda: _get_anilist_trending_cached(page=page)
            )
            anilist_items = _timed(
                f"anime_aggregate anilist_trending tmdb_resolve page={page}",
                lambda: _enrich_anilist_items_with_tmdb(anilist_items)
            )

            jikan_hot_items = _timed(
                f"anime_aggregate jikan_anime_hot page={page}",
                lambda: _get_jikan_anime_hot_cached(page=page)
            )
            jikan_hot_items = _timed(
                f"anime_aggregate jikan_anime_hot tmdb_resolve page={page}",
                lambda: _enrich_anilist_items_with_tmdb(jikan_hot_items)
            )

            jikan_rising_items = _timed(
                f"anime_aggregate jikan_anime_rising page={page}",
                lambda: _get_jikan_anime_rising_cached(page=page)
            )
            jikan_rising_items = _timed(
                f"anime_aggregate jikan_anime_rising tmdb_resolve page={page}",
                lambda: _enrich_anilist_items_with_tmdb(jikan_rising_items)
            )

            items = (anilist_items or []) + (jikan_hot_items or []) + (jikan_rising_items or [])

        elif source == "anilist_trending":
            items = _timed(
                f"anilist_trending page={page}",
                lambda: _get_anilist_trending_cached(page=page)
            )
            items = _timed(
                f"anilist_trending tmdb_resolve page={page}",
                lambda: _enrich_anilist_items_with_tmdb(items)
            )

        elif source == "jikan_anime_hot":
            items = _timed(
                f"jikan_anime_hot page={page}",
                lambda: _get_jikan_anime_hot_cached(page=page)
            )
            items = _timed(
                f"jikan_anime_hot tmdb_resolve page={page}",
                lambda: _enrich_anilist_items_with_tmdb(items)
            )

        elif source == "jikan_anime_rising":
            items = _timed(
                f"jikan_anime_rising page={page}",
                lambda: _get_jikan_anime_rising_cached(page=page)
            )
            items = _timed(
                f"jikan_anime_rising tmdb_resolve page={page}",
                lambda: _enrich_anilist_items_with_tmdb(items)
            )

        elif source == "anilist_popular":
            items = _timed(
                f"anilist_popular page={page}",
                lambda: _get_anilist_popular_cached(page=page)
            )
            items = _timed(
                f"anilist_popular tmdb_resolve page={page}",
                lambda: _enrich_anilist_items_with_tmdb(items)
            )

        elif source == "anilist_genre":
            anime_genre = _normalize_anilist_genre_name(genre)
            items = _timed(
                f"anilist_genre genre={anime_genre} page={page}",
                lambda: _get_anilist_genre_cached(page=page, genre=anime_genre)
            )
            items = _timed(
                f"anilist_genre tmdb_resolve genre={anime_genre} page={page}",
                lambda: _enrich_anilist_items_with_tmdb(items)
            )

        enrich_stats = consume_tmdb_enrich_stats()
        try:
            print(
                f"[discover-enrich] source={source} page={page} budget={enrich_budget} used={enrich_used} "
                f"hits={enrich_stats.get('hits',0)} misses={enrich_stats.get('misses',0)} none_hits={enrich_stats.get('none_hits',0)}",
                flush=True
            )
        except Exception:
            print(
                f"[discover-enrich] source={source} page={page} "
                f"hits={enrich_stats.get('hits',0)} misses={enrich_stats.get('misses',0)} none_hits={enrich_stats.get('none_hits',0)}",
                flush=True
            )

        if source in ("aggregate", "trakt_trending", "trakt_popular"):
            items = _timed(
                f"fill_missing_tmdb_artwork source={source} page={page}",
                lambda: _fill_missing_tmdb_artwork(
                    items,
                    budget=120 if source == "aggregate" else 60,
                )
            )

        weights = {
            "tmdb_trending": _app_float("discover_weight_tmdb", 1.0),
            "tmdb_popular": _app_float("discover_weight_tmdb_popular", 0.95),
            "trakt_trending": _app_float("discover_weight_trakt", 1.0),
            "trakt_popular": _app_float("discover_weight_trakt_popular", 0.90),
            "letterboxd": _app_float("discover_weight_letterboxd", 0.58),
            "tvmaze_airing": _app_float("discover_weight_tvmaze", 0.80),
            "anilist_anime": _app_float("discover_weight_anilist", 0.92),
        }

        bonuses = {
            2: _app_float("discover_bonus_2", 0.08),
            3: _app_float("discover_bonus_3", 0.18),
            4: _app_float("discover_bonus_4", 0.28),
            5: _app_float("discover_bonus_5", 0.34),
            6: _app_float("discover_bonus_6", 0.40),
        }

        hot_threshold = _app_float("discover_hot_threshold", 0.82)

        items = _timed(
            f"normalize_and_score_items source={source} page={page}",
            lambda: normalize_and_score_items(
                items,
                weights=weights,
                bonuses=bonuses,
                hot_threshold=hot_threshold,
            )
        )

        try:
            from .routes_actions import _get_hidden_and_snoozed_rows, _current_user_id

            user_id = _current_user_id()
            hidden_rows, _snoozed_rows, _now = _get_hidden_and_snoozed_rows(user_id)

            hidden_discover = {
                str(r["item_id"] or "")
                for r in (hidden_rows or [])
                if str(r["kind"] or "") == "discover_item"
            }

            def _discover_hide_key(it):
                media_type = str(it.get("media_type") or "").strip().lower()
                tmdb_id = str(it.get("tmdb_id") or "").strip()
                title = str(it.get("title") or "").strip().lower()
                year = str(it.get("year") or "").strip()

                if tmdb_id:
                    return f"{media_type}:{tmdb_id}"

                key = "".join(ch for ch in title if ch.isalnum())
                return f"{media_type}:{key}::{year}"

            items = [it for it in (items or []) if _discover_hide_key(it) not in hidden_discover]
        except Exception:
            pass

        if min_rating is not None:
            items = [
                i for i in items
                if float(i.get("vote_average") or 0) >= float(min_rating)
            ]

        if source == "aggregate":
            items = _prioritize_rich_aggregate_items(items)

        payload = {
            "ok": True,
            "configured": True,
            "source": source,
            "media": media,
            "genre": genre,
            "provider": provider,
            "year_from": year_from,
            "year_to": year_to,
            "page": page,
            "items": items,
        }
        elapsed = round(time.time() - req_started, 3)
        print(f"[discover-timing] source={source} page={page} items={len(items)} elapsed={elapsed}s", flush=True)
        _cache_set(source, media, page, genre, provider, year_from, year_to, payload)
        return jsonify(payload)
    except Exception as e:
        try:
            elapsed = round(time.time() - req_started, 3)
            print(f"[discover-timing] source={source} page={page} failed_after={elapsed}s error={e}", flush=True)
        except Exception:
            pass
        return jsonify(
            ok=False,
            configured=True,
            error=str(e),
            items=[],
        ), 502
