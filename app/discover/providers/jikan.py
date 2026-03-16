from __future__ import annotations

import requests


JIKAN_BASE = "https://api.jikan.moe/v4"


def _get(path: str, params: dict | None = None) -> dict:
    r = requests.get(f"{JIKAN_BASE}{path}", params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json() or {}


def _normalize(item: dict, source_key: str) -> dict:
    title = item.get("title") or ""
    title_english = item.get("title_english") or ""
    title_japanese = item.get("title_japanese") or ""
    year = item.get("year")

    poster = ""
    images = item.get("images") or {}
    jpg = images.get("jpg") or {}
    if isinstance(jpg, dict):
        poster = jpg.get("large_image_url") or jpg.get("image_url") or ""

    score = float(item.get("score") or 0)
    members = int(item.get("members") or 0)
    popularity = float(item.get("popularity") or 0)

    synopsis = str(item.get("synopsis") or "").strip()
    rating = str(item.get("rating") or "").strip()
    anime_type = str(item.get("type") or "").strip()
    source = str(item.get("source") or "").strip()

    genres = [str(x.get("name") or "").strip() for x in (item.get("genres") or []) if isinstance(x, dict) and str(x.get("name") or "").strip()]
    themes = [str(x.get("name") or "").strip() for x in (item.get("themes") or []) if isinstance(x, dict) and str(x.get("name") or "").strip()]
    demographics = [str(x.get("name") or "").strip() for x in (item.get("demographics") or []) if isinstance(x, dict) and str(x.get("name") or "").strip()]

    provider_score = 0.0
    if score > 0:
        provider_score = min(max(score / 10.0, 0.0), 1.0)

    return {
        "title": title,
        "title_english": title_english,
        "title_japanese": title_japanese,
        "year": year,
        "poster_url": poster,
        "overview": synopsis,
        "synopsis": synopsis,
        "rating": rating,
        "anime_type": anime_type,
        "anime_source": source,
        "genres": genres,
        "themes": themes,
        "demographics": demographics,
        "vote_average": score,
        "vote_count": members,
        "popularity": popularity,
        "provider_scores": {
            source_key: provider_score,
        },
        "provider_hits": 1,
        "media_type": "tv",
    }


def fetch_jikan_anime_hot(page: int = 1, per_page: int = 25) -> list[dict]:
    data = _get("/top/anime", {"filter": "airing", "page": page, "limit": per_page})
    items = data.get("data") or []
    return [_normalize(x, "jikan_anime_hot") for x in items]


def fetch_jikan_anime_rising(page: int = 1, per_page: int = 25) -> list[dict]:
    data = _get("/seasons/now", {"page": page, "limit": per_page})
    items = data.get("data") or []
    return [_normalize(x, "jikan_anime_rising") for x in items]
