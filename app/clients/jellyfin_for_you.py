from __future__ import annotations

import os
import re
from collections import Counter
from statistics import mean
from typing import Any

import requests

from app.models_settings import get_current_user_scoped_setting


def _cfg(key: str, env_key: str = "", fallback: str = "") -> str:
    try:
        v = get_current_user_scoped_setting(key, default="")
        if v is not None and str(v).strip():
            return str(v).strip()
    except Exception:
        pass
    if env_key:
        return os.getenv(env_key, fallback).strip()
    return fallback


def _jellyfin_base() -> str:
    return _cfg("jellyfin_url", "JELLYFIN_URL", "").rstrip("/")


def _jellyfin_api_key() -> str:
    return _cfg("jellyfin_api_key", "JELLYFIN_API_KEY", "")


def _jellyfin_username() -> str:
    return _cfg("jellyfin_user", "JELLYFIN_USER", "")


def _headers(api_key: str) -> dict[str, str]:
    return {
        "X-Emby-Token": api_key,
        "Accept": "application/json",
    }


def _norm_title(s: str) -> str:
    s = str(s or "").strip().lower()
    s = re.sub(r"\(\d{4}\)", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _watch_key(title: str, year: int | str | None) -> str:
    y = str(year or "").strip()
    return f"{_norm_title(title)}::{y}"


def _get_user_id(base: str, api_key: str, username: str) -> str:
    r = requests.get(f"{base}/Users", headers=_headers(api_key), timeout=20)
    r.raise_for_status()
    rows = r.json() or []
    want = str(username or "").strip().lower()
    for row in rows:
        name = str(row.get("Name") or "").strip().lower()
        if name == want:
            return str(row.get("Id") or "")
    return ""


def _fetch_recently_played(base: str, api_key: str, user_id: str, limit: int = 200) -> list[dict[str, Any]]:
    params = {
        "Recursive": "true",
        "SortBy": "DatePlayed",
        "SortOrder": "Descending",
        "Limit": str(limit),
        "Filters": "IsPlayed",
        "IncludeItemTypes": "Movie,Series,Episode",
        "Fields": "Genres,Studios,ProductionYear,Tags,ProviderIds,Path",
        "EnableTotalRecordCount": "false",
        "EnableImages": "false",
    }
    r = requests.get(
        f"{base}/Users/{user_id}/Items",
        headers=_headers(api_key),
        params=params,
        timeout=25,
    )
    r.raise_for_status()
    data = r.json() or {}
    return data.get("Items") or []


def build_for_you_profile(limit: int = 200) -> dict[str, Any]:
    base = _jellyfin_base()
    api_key = _jellyfin_api_key()
    username = _jellyfin_username()

    if not base or not api_key or not username:
        return {
            "ok": False,
            "reason": "jellyfin_not_configured",
            "genre_weights": {},
            "preferred_media": "all",
            "anime_affinity": 0.0,
            "year_center": None,
            "sample_size": 0,
            "watched_tmdb_ids": [],
            "watched_keys": [],
        }

    user_id = _get_user_id(base, api_key, username)
    if not user_id:
        return {
            "ok": False,
            "reason": "jellyfin_user_not_found",
            "genre_weights": {},
            "preferred_media": "all",
            "anime_affinity": 0.0,
            "year_center": None,
            "sample_size": 0,
            "watched_tmdb_ids": [],
            "watched_keys": [],
        }

    rows = _fetch_recently_played(base, api_key, user_id, limit=limit)

    if not rows:
        return {
            "ok": False,
            "reason": "no_watch_history",
            "genre_weights": {},
            "preferred_media": "all",
            "anime_affinity": 0.0,
            "year_center": None,
            "sample_size": 0,
            "watched_tmdb_ids": [],
            "watched_keys": [],
        }

    genre_counter: Counter[str] = Counter()
    media_counter: Counter[str] = Counter()
    years: list[int] = []
    anime_hits = 0
    watched_tmdb_ids: set[str] = set()
    watched_keys: set[str] = set()

    for row in rows:
        item_type = str(row.get("Type") or "").strip().lower()
        if item_type == "movie":
            media_counter["movie"] += 1
        elif item_type == "series":
            media_counter["tv"] += 1

        genres = row.get("Genres") or []
        if isinstance(genres, list):
            for g in genres:
                gs = str(g or "").strip()
                if gs:
                    genre_counter[gs] += 1

        py = row.get("ProductionYear")
        if isinstance(py, int) and 1900 <= py <= 2100:
            years.append(py)

        name = str(row.get("Name") or "").strip()
        if name:
            watched_keys.add(_watch_key(name, py))

        provider_ids = row.get("ProviderIds") or {}
        if isinstance(provider_ids, dict):
            tmdb_id = str(provider_ids.get("Tmdb") or "").strip()
            if tmdb_id:
                watched_tmdb_ids.add(tmdb_id)

        path = str(row.get("Path") or "")
        tags = row.get("Tags") or []
        genres_lower = {str(g).strip().lower() for g in genres if str(g).strip()}
        tags_lower = {str(t).strip().lower() for t in tags if str(t).strip()}

        is_anime = (
            "anime" in path.lower()
            or "anime" in genres_lower
            or "anime" in tags_lower
            or "animation" in genres_lower
        )
        if is_anime:
            anime_hits += 1

    total_genres = sum(genre_counter.values()) or 1
    genre_weights = {
        k: round(v / total_genres, 4)
        for k, v in genre_counter.most_common(12)
    }

    movie_count = media_counter.get("movie", 0)
    tv_count = media_counter.get("tv", 0)

    preferred_media = "all"
    if movie_count > tv_count * 1.25:
        preferred_media = "movie"
    elif tv_count > movie_count * 1.25:
        preferred_media = "tv"

    anime_affinity = round(anime_hits / max(len(rows), 1), 4)
    year_center = int(round(mean(years))) if years else None

    return {
        "ok": True,
        "reason": "",
        "genre_weights": genre_weights,
        "preferred_media": preferred_media,
        "anime_affinity": anime_affinity,
        "year_center": year_center,
        "sample_size": len(rows),
        "watched_tmdb_ids": sorted(watched_tmdb_ids),
        "watched_keys": sorted(watched_keys),
    }
