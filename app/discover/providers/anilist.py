import re
import requests


ANILIST_URL = "https://graphql.anilist.co"


def _post(query: str, variables: dict | None = None, timeout: int = 20) -> dict:
    r = requests.post(
        ANILIST_URL,
        json={
            "query": query,
            "variables": variables or {},
        },
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout=timeout,
    )
    r.raise_for_status()
    data = r.json() or {}
    if data.get("errors"):
        raise RuntimeError(str(data["errors"]))
    return data


def _pick_title(title_obj: dict) -> str:
    if not isinstance(title_obj, dict):
        return ""
    return (
        str(title_obj.get("english") or "").strip()
        or str(title_obj.get("romaji") or "").strip()
        or str(title_obj.get("native") or "").strip()
    )


def _clean_description(text: str) -> str:
    s = str(text or "")
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _normalize_media(row: dict, source_label: str, provider_score: float) -> dict:
    title = _pick_title(row.get("title") or {})
    season = str(row.get("season") or "").strip()
    season_year = row.get("seasonYear")
    genres = row.get("genres") or []
    cover = row.get("coverImage") or {}
    start = row.get("startDate") or {}

    year = ""
    if season_year:
        year = str(season_year)
    elif start.get("year"):
        year = str(start.get("year"))

    tmdb_id = ""
    ext = row.get("externalLinks") or []
    if isinstance(ext, list):
        for link in ext:
            if not isinstance(link, dict):
                continue
            site = str(link.get("site") or "").lower()
            url = str(link.get("url") or "")
            if "themoviedb" in site or "tmdb" in site:
                parts = [p for p in url.rstrip("/").split("/") if p]
                if parts and parts[-1].isdigit():
                    tmdb_id = parts[-1]
                    break

    average_score = row.get("averageScore")
    vote_average = round(float(average_score) / 10.0, 1) if average_score is not None else None

    popularity = row.get("popularity")
    popularity = float(popularity) if popularity is not None else 0.0

    return {
        "title": title,
        "year": year,
        "media_type": "tv",
        "tmdb_id": tmdb_id,
        "imdb_id": "",
        "tvdb_id": "",
        "poster_url": str(cover.get("extraLarge") or cover.get("large") or "").strip(),
        "overview": _clean_description(row.get("description") or ""),
        "vote_average": vote_average,
        "vote_count": int(row.get("favourites") or 0),
        "popularity": popularity,
        "genre_ids": genres,
        "genres": genres,
        "source": source_label,
        "provider_scores": {
            "anilist_anime": float(provider_score),
        },
        "anilist_id": int(row.get("id") or 0),
        "anilist_site_url": str(row.get("siteUrl") or "").strip(),
        "season": season,
        "season_year": year,
        "episodes": row.get("episodes"),
        "status": row.get("status"),
    }


def _score_trending(rank: int) -> float:
    return max(0.35, round(1.02 - (min(max(rank, 1), 50) - 1) * 0.01, 4))


def _score_popular(rank: int) -> float:
    return max(0.30, round(0.96 - (min(max(rank, 1), 50) - 1) * 0.009, 4))


def fetch_anilist_trending(page: int = 1, per_page: int = 25) -> list[dict]:
    query = """
    query ($page: Int, $perPage: Int) {
      Page(page: $page, perPage: $perPage) {
        media(type: ANIME, sort: TRENDING_DESC, isAdult: false) {
          id
          siteUrl
          title { romaji english native }
          description(asHtml: false)
          averageScore
          popularity
          favourites
          episodes
          genres
          season
          seasonYear
          status
          startDate { year month day }
          coverImage { large extraLarge }
          externalLinks { site url }
        }
      }
    }
    """
    data = _post(query, {"page": int(page), "perPage": int(per_page)})
    rows = (((data or {}).get("data") or {}).get("Page") or {}).get("media") or []
    out = []
    for idx, row in enumerate(rows, start=1):
        if isinstance(row, dict):
            out.append(_normalize_media(row, "AniList Trending", _score_trending(idx)))
    return out


def fetch_anilist_popular(page: int = 1, per_page: int = 25) -> list[dict]:
    query = """
    query ($page: Int, $perPage: Int) {
      Page(page: $page, perPage: $perPage) {
        media(type: ANIME, sort: POPULARITY_DESC, isAdult: false) {
          id
          siteUrl
          title { romaji english native }
          description(asHtml: false)
          averageScore
          popularity
          favourites
          episodes
          genres
          season
          seasonYear
          status
          startDate { year month day }
          coverImage { large extraLarge }
          externalLinks { site url }
        }
      }
    }
    """
    data = _post(query, {"page": int(page), "perPage": int(per_page)})
    rows = (((data or {}).get("data") or {}).get("Page") or {}).get("media") or []
    out = []
    for idx, row in enumerate(rows, start=1):
        if isinstance(row, dict):
            out.append(_normalize_media(row, "AniList Popular", _score_popular(idx)))
    return out


def fetch_anilist_genre(genre: str, page: int = 1, per_page: int = 25) -> list[dict]:
    query = """
    query ($page: Int, $perPage: Int, $genre: String) {
      Page(page: $page, perPage: $perPage) {
        media(type: ANIME, sort: TRENDING_DESC, genre_in: [$genre], isAdult: false) {
          id
          siteUrl
          title { romaji english native }
          description(asHtml: false)
          averageScore
          popularity
          favourites
          episodes
          genres
          season
          seasonYear
          status
          startDate { year month day }
          coverImage { large extraLarge }
          externalLinks { site url }
        }
      }
    }
    """
    data = _post(query, {"page": int(page), "perPage": int(per_page), "genre": str(genre)})
    rows = (((data or {}).get("data") or {}).get("Page") or {}).get("media") or []
    out = []
    for idx, row in enumerate(rows, start=1):
        if isinstance(row, dict):
            out.append(_normalize_media(row, f"AniList {genre}", _score_trending(idx)))
    return out
