import time
import requests

from ..engine import clamp01

TMDB_API_BASE = "https://api.themoviedb.org/3"

_TMDB_ENRICH_CACHE: dict[tuple[str, int], tuple[float, dict | None]] = {}
_TMDB_ENRICH_CACHE_TTL = 3600  # 1 hour

_TMDB_ENRICH_STATS = {
    "hits": 0,
    "misses": 0,
    "none_hits": 0,
}


def consume_tmdb_enrich_stats() -> dict:
    stats = dict(_TMDB_ENRICH_STATS)
    _TMDB_ENRICH_STATS["hits"] = 0
    _TMDB_ENRICH_STATS["misses"] = 0
    _TMDB_ENRICH_STATS["none_hits"] = 0
    return stats


def tmdb_get(path: str, headers: dict, auth_params: dict, params: dict | None = None) -> dict:
    q = {}
    q.update(auth_params or {})
    q.update(params or {})

    r = requests.get(
        f"{TMDB_API_BASE}{path}",
        headers=headers,
        params=q,
        timeout=20,
    )
    r.raise_for_status()
    return r.json() or {}


def tmdb_image_base(headers: dict, auth_params: dict) -> str:
    # Always proxy TMDB artwork through QueueDeck so browsers never hit
    # image.tmdb.org directly.
    return "/img/tmdb"


def tmdb_trend_score(item: dict) -> float:
    popularity = float(item.get("popularity") or 0.0)
    vote_average = float(item.get("vote_average") or 0.0)
    vote_count = float(item.get("vote_count") or 0.0)

    popularity_score = min(popularity / 500.0, 1.0)
    rating_score = min(vote_average / 10.0, 1.0)
    confidence_score = min(vote_count / 2000.0, 1.0)

    score = (
        popularity_score * 0.60 +
        rating_score * 0.25 +
        confidence_score * 0.15
    )
    return round(clamp01(score), 4)


def normalize_tmdb_item(item: dict, image_base: str) -> dict | None:
    media_type = str(item.get("media_type") or "").strip().lower()

    if media_type == "person":
        return None

    if media_type not in ("movie", "tv"):
        if item.get("title") or item.get("release_date"):
            media_type = "movie"
        elif item.get("name") or item.get("first_air_date"):
            media_type = "tv"

    if media_type not in ("movie", "tv"):
        return None

    tmdb_id = item.get("id")
    title = (
        item.get("title")
        or item.get("name")
        or item.get("original_title")
        or item.get("original_name")
        or ""
    ).strip()

    if not tmdb_id or not title:
        return None

    date_val = item.get("release_date") or item.get("first_air_date") or ""
    year = str(date_val)[:4] if str(date_val)[:4] else ""

    poster_path = str(item.get("poster_path") or "").strip()
    poster_url = f"{image_base}{poster_path}" if poster_path else ""

    tmdb_score = tmdb_trend_score(item)

    return {
        "tmdb_id": int(tmdb_id),
        "media_type": media_type,
        "title": title,
        "year": year,
        "overview": str(item.get("overview") or "").strip(),
        "poster_url": poster_url,
        "vote_average": item.get("vote_average"),
        "popularity": item.get("popularity"),
        "source": "TMDb Trending",
        "provider_scores": {
            "tmdb_trending": tmdb_score,
        },
    }


def enrich_tmdb_item_by_id(tmdb_id: int, media_type: str, headers: dict, auth_params: dict) -> dict | None:
    if media_type not in ("movie", "tv"):
        return None

    try:
        tmdb_id = int(tmdb_id)
    except Exception:
        return None

    cache_key = (str(media_type), tmdb_id)
    cached = _TMDB_ENRICH_CACHE.get(cache_key)
    now = time.time()

    if cached:
        ts, payload = cached
        if (now - ts) <= _TMDB_ENRICH_CACHE_TTL:
            if isinstance(payload, dict):
                _TMDB_ENRICH_STATS["hits"] += 1
                return dict(payload)
            _TMDB_ENRICH_STATS["none_hits"] += 1
            return None
        _TMDB_ENRICH_CACHE.pop(cache_key, None)

    _TMDB_ENRICH_STATS["misses"] += 1

    try:
        data = tmdb_get(
            f"/{media_type}/{tmdb_id}",
            headers,
            auth_params,
            {"language": "en-US"},
        )
    except Exception:
        _TMDB_ENRICH_CACHE[cache_key] = (now, None)
        return None

    image_base = tmdb_image_base(headers, auth_params)

    title = (
        data.get("title")
        or data.get("name")
        or data.get("original_title")
        or data.get("original_name")
        or ""
    ).strip()

    if not title:
        _TMDB_ENRICH_CACHE[cache_key] = (now, None)
        return None

    date_val = data.get("release_date") or data.get("first_air_date") or ""
    year = str(date_val)[:4] if str(date_val)[:4] else ""

    poster_path = str(data.get("poster_path") or "").strip()
    poster_url = f"{image_base}{poster_path}" if poster_path else ""

    tmdb_score = tmdb_trend_score(data)

    raw_genres = data.get("genre_ids")
    if not isinstance(raw_genres, list):
        raw_genres = [
            g.get("id") for g in (data.get("genres") or [])
            if isinstance(g, dict) and g.get("id") is not None
        ]

    payload = {
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "title": title,
        "year": year,
        "overview": str(data.get("overview") or "").strip(),
        "poster_url": poster_url,
        "vote_average": data.get("vote_average"),
        "vote_count": data.get("vote_count"),
        "popularity": data.get("popularity"),
        "genre_ids": raw_genres or [],
        "provider_scores": {
            "tmdb_trending": tmdb_score,
        },
    }

    _TMDB_ENRICH_CACHE[cache_key] = (now, payload)
    return dict(payload)


def fetch_tmdb_trending(media: str, page: int, headers: dict, auth_params: dict) -> list[dict]:
    image_base = tmdb_image_base(headers, auth_params)

    if media == "movie":
        data = tmdb_get("/trending/movie/week", headers, auth_params, {"page": page, "language": "en-US"})
    elif media == "tv":
        data = tmdb_get("/trending/tv/week", headers, auth_params, {"page": page, "language": "en-US"})
    else:
        data = tmdb_get("/trending/all/week", headers, auth_params, {"page": page, "language": "en-US"})

    raw = data.get("results") or []
    items = []

    for item in raw:
        norm = normalize_tmdb_item(item, image_base)
        if norm:
            items.append(norm)

    return items

def fetch_tmdb_discover_by_genre(
    media: str,
    genre_id: str,
    page: int,
    headers: dict,
    auth_params: dict,
    year_from: str = "",
    year_to: str = "",
    pages_deep: int = 5,
) -> list[dict]:
    if media not in ("movie", "tv", "all"):
        media = "all"

    media_types = ["movie", "tv"] if media == "all" else [media]
    image_base = tmdb_image_base(headers, auth_params)
    out = []

    try:
        start_page = max(1, int(page))
    except Exception:
        start_page = 1

    try:
        pages_deep = max(1, min(int(pages_deep), 10))
    except Exception:
        pages_deep = 3

    for media_type in media_types:
        for page_num in range(start_page, start_page + pages_deep):
            params = {
                "language": "en-US",
                "page": page_num,
                "sort_by": "popularity.desc",
                "include_adult": "false",
            }

            if genre_id:
                params["with_genres"] = str(genre_id)

            if year_from:
                if media_type == "movie":
                    params["primary_release_date.gte"] = f"{year_from}-01-01"
                else:
                    params["first_air_date.gte"] = f"{year_from}-01-01"

            if year_to:
                if media_type == "movie":
                    params["primary_release_date.lte"] = f"{year_to}-12-31"
                else:
                    params["first_air_date.lte"] = f"{year_to}-12-31"

            data = tmdb_get(
                f"/discover/{media_type}",
                headers,
                auth_params,
                params,
            )

            items = data.get("results") or []
            for item in items:
                title = (
                    item.get("title")
                    or item.get("name")
                    or item.get("original_title")
                    or item.get("original_name")
                    or ""
                ).strip()

                if not title:
                    continue

                date_val = item.get("release_date") or item.get("first_air_date") or ""
                year = str(date_val)[:4] if str(date_val)[:4] else ""

                poster_path = str(item.get("poster_path") or "").strip()
                poster_url = f"{image_base}{poster_path}" if poster_path else ""

                out.append({
                    "tmdb_id": int(item.get("id") or 0),
                    "media_type": media_type,
                    "title": title,
                    "year": year,
                    "overview": str(item.get("overview") or "").strip(),
                    "poster_url": poster_url,
                    "vote_average": item.get("vote_average"),
                    "vote_count": item.get("vote_count"),
                    "popularity": item.get("popularity"),
                    "genre_ids": item.get("genre_ids") or [],
                    "source": "TMDb Discover",
                    "provider_scores": {
                        "tmdb_trending": tmdb_trend_score(item),
                    },
                })

    seen = set()
    deduped = []
    for item in out:
        key = (str(item.get("media_type") or ""), int(item.get("tmdb_id") or 0))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped

def fetch_tmdb_discover_by_provider(
    media: str,
    provider_id: str,
    page: int,
    headers: dict,
    auth_params: dict,
    watch_region: str = "US",
    year_from: str = "",
    year_to: str = "",
    pages_deep: int = 5,
) -> list[dict]:
    if media not in ("movie", "tv", "all"):
        media = "all"

    media_types = ["movie", "tv"] if media == "all" else [media]
    image_base = tmdb_image_base(headers, auth_params)
    out = []

    try:
        start_page = max(1, int(page))
    except Exception:
        start_page = 1

    try:
        pages_deep = max(1, min(int(pages_deep), 10))
    except Exception:
        pages_deep = 3

    for media_type in media_types:
        for page_num in range(start_page, start_page + pages_deep):
            params = {
                "language": "en-US",
                "page": page_num,
                "sort_by": "popularity.desc",
                "include_adult": "false",
                "watch_region": str(watch_region or "US"),
                "with_watch_providers": str(provider_id),
            }

            if year_from:
                if media_type == "movie":
                    params["primary_release_date.gte"] = f"{year_from}-01-01"
                else:
                    params["first_air_date.gte"] = f"{year_from}-01-01"

            if year_to:
                if media_type == "movie":
                    params["primary_release_date.lte"] = f"{year_to}-12-31"
                else:
                    params["first_air_date.lte"] = f"{year_to}-12-31"

            data = tmdb_get(
                f"/discover/{media_type}",
                headers,
                auth_params,
                params,
            )

            items = data.get("results") or []
            for item in items:
                title = (
                    item.get("title")
                    or item.get("name")
                    or item.get("original_title")
                    or item.get("original_name")
                    or ""
                ).strip()

                if not title:
                    continue

                date_val = item.get("release_date") or item.get("first_air_date") or ""
                year = str(date_val)[:4] if str(date_val)[:4] else ""

                poster_path = str(item.get("poster_path") or "").strip()
                poster_url = f"{image_base}{poster_path}" if poster_path else ""

                out.append({
                    "tmdb_id": int(item.get("id") or 0),
                    "media_type": media_type,
                    "title": title,
                    "year": year,
                    "overview": str(item.get("overview") or "").strip(),
                    "poster_url": poster_url,
                    "vote_average": item.get("vote_average"),
                    "vote_count": item.get("vote_count"),
                    "popularity": item.get("popularity"),
                    "genre_ids": item.get("genre_ids") or [],
                    "source": "TMDb Discover",
                    "provider_scores": {
                        "tmdb_trending": tmdb_trend_score(item),
                    },
                })

    seen = set()
    deduped = []
    for item in out:
        key = (str(item.get("media_type") or ""), int(item.get("tmdb_id") or 0))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped



def fetch_tmdb_popular(media: str, page: int, headers: dict, auth_params: dict) -> list[dict]:
    image_base = tmdb_image_base(headers, auth_params)

    if media == "movie":
        data = tmdb_get("/movie/popular", headers, auth_params, {"page": page, "language": "en-US"})
    elif media == "tv":
        data = tmdb_get("/tv/popular", headers, auth_params, {"page": page, "language": "en-US"})
    else:
        movie_data = tmdb_get("/movie/popular", headers, auth_params, {"page": page, "language": "en-US"})
        tv_data = tmdb_get("/tv/popular", headers, auth_params, {"page": page, "language": "en-US"})
        data = {"results": (movie_data.get("results") or []) + (tv_data.get("results") or [])}

    raw = data.get("results") or []
    items = []

    for item in raw:
        norm = normalize_tmdb_item(item, image_base)
        if not norm:
            continue

        pop_score = min(float(item.get("popularity") or 0) / 500.0, 1.0)

        norm["source"] = "TMDb Popular"

        scores = dict(norm.get("provider_scores") or {})
        scores["tmdb_popular"] = round(pop_score, 4)
        norm["provider_scores"] = scores

        items.append(norm)

    return items
