import requests

from ..engine import clamp01

TRAKT_API_BASE = "https://api.trakt.tv"

_TRAKT_SESSION = requests.Session()


def trakt_get(path: str, client_id: str, params: dict | None = None) -> list | dict:
    headers = {
        "trakt-api-version": "2",
        "trakt-api-key": str(client_id or "").strip(),
        "Content-Type": "application/json",
    }

    r = _TRAKT_SESSION.get(
        f"{TRAKT_API_BASE}{path}",
        headers=headers,
        params=params or {},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def trakt_trend_score(item: dict) -> float:
    watchers = float(item.get("watchers") or 0.0)

    inner = item.get("movie") or item.get("show") or {}
    rating = float(inner.get("rating") or 0.0)
    votes = float(inner.get("votes") or 0.0)

    watchers_score = min(watchers / 5000.0, 1.0)
    rating_score = min(rating / 10.0, 1.0)
    confidence_score = min(votes / 50000.0, 1.0)

    score = (
        watchers_score * 0.55 +
        rating_score * 0.25 +
        confidence_score * 0.20
    )
    return round(clamp01(score), 4)


def normalize_trakt_item(item: dict, media: str) -> dict | None:
    inner = item.get("movie") if media == "movie" else item.get("show")
    if not isinstance(inner, dict):
        return None

    ids = inner.get("ids") or {}
    tmdb_id = ids.get("tmdb")
    if not tmdb_id:
        return None

    title = str(inner.get("title") or "").strip()
    if not title:
        return None

    year = str(inner.get("year") or "").strip()

    return {
        "tmdb_id": int(tmdb_id),
        "media_type": media,
        "title": title,
        "year": year,
        "overview": "",
        "poster_url": "",
        "vote_average": inner.get("rating"),
        "popularity": item.get("watchers") or 0,
        "source": "Trakt Trending",
        "provider_scores": {
            "trakt_trending": trakt_trend_score(item),
        },
        "trakt_watchers": item.get("watchers") or 0,
    }


def fetch_trakt_trending(media: str, client_id: str, limit: int = 40, page: int = 1) -> list[dict]:
    from concurrent.futures import ThreadPoolExecutor

    limit = max(1, min(int(limit), 100))
    page = max(1, int(page or 1))

    if media == "all":
        with ThreadPoolExecutor(max_workers=2) as ex:
            fut_movie = ex.submit(fetch_trakt_trending, "movie", client_id, limit, page)
            fut_tv = ex.submit(fetch_trakt_trending, "tv", client_id, limit, page)
            movie_items = fut_movie.result()
            tv_items = fut_tv.result()
        return list(movie_items or []) + list(tv_items or [])

    path = "/movies/trending" if media == "movie" else "/shows/trending"
    data = trakt_get(path, client_id=client_id, params={"limit": limit, "page": page})

    out = []
    for item in (data or []):
        norm = normalize_trakt_item(item, media=media)
        if norm:
            out.append(norm)
    return out

def fetch_trakt_popular(media: str, client_id: str, limit: int = 40, page: int = 1) -> list[dict]:
    import requests

    media = (media or "all").strip().lower()
    limit = max(1, min(int(limit or 40), 100))
    page = max(1, int(page or 1))

    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": (client_id or "").strip(),
    }

    endpoints = []
    if media == "movie":
        endpoints = [("movie", "movies")]
    elif media == "tv":
        endpoints = [("tv", "shows")]
    else:
        endpoints = [("movie", "movies"), ("tv", "shows")]

    out = []

    for media_type, path_part in endpoints:
        r = _TRAKT_SESSION.get(
            f"https://api.trakt.tv/{path_part}/popular",
            headers=headers,
            params={"limit": limit, "page": page},
            timeout=20,
        )
        r.raise_for_status()
        raw = r.json() or []

        total = max(len(raw), 1)

        for idx, item in enumerate(raw, start=1):
            ids = item.get("ids") or {}
            tmdb_id = ids.get("tmdb")
            title = str(item.get("title") or "").strip()
            year = str(item.get("year") or "").strip()

            if not tmdb_id or not title:
                continue

            rank_score = round(max(0.0, 1.0 - ((idx - 1) / total)), 4)

            out.append({
                "media_type": media_type,
                "tmdb_id": int(tmdb_id),
                "imdb_id": ids.get("imdb"),
                "trakt_id": ids.get("trakt"),
                "title": title,
                "year": year,
                "overview": str(item.get("overview") or "").strip(),
                "source": "Trakt Popular",
                "provider_scores": {
                    "trakt_popular": rank_score,
                },
            })

    return out

