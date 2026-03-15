from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any, Dict, List

import requests

from ..engine import clamp01

TVMAZE_API_BASE = "https://api.tvmaze.com"


def _get(path: str, params: dict | None = None) -> list | dict:
    r = requests.get(
        f"{TVMAZE_API_BASE}{path}",
        params=params or {},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def _strip_html(s: str) -> str:
    s = str(s or "")
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return " ".join(s.split())


def _norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        elif ch in (" ", "-", "_", ".", ":"):
            out.append(" ")
    return " ".join("".join(out).split())


def _tvmaze_schedule_score(ep: dict, offset_days: int, source_kind: str = "mixed") -> float:
    show = ep.get("show") or {}
    rating_avg = float((show.get("rating") or {}).get("average") or 0.0)
    weight = float(show.get("weight") or 0.0)

    day_score = max(0.0, 1.0 - (offset_days * 0.18))
    rating_score = min(rating_avg / 10.0, 1.0)
    weight_score = min(weight / 100.0, 1.0)

    source_bonus = 0.0
    if source_kind == "web":
        source_bonus = 0.03
    elif source_kind == "schedule":
        source_bonus = 0.02

    score = (
        day_score * 0.55 +
        rating_score * 0.25 +
        weight_score * 0.20 +
        source_bonus
    )
    return round(clamp01(score), 4)


def _build_item(ep: dict, offset: int, source_kind: str) -> dict | None:
    if not isinstance(ep, dict):
        return None

    show = ep.get("show") or {}
    show_id = int(show.get("id") or 0)
    if not show_id:
        return None

    title = str(show.get("name") or "").strip()
    if not title:
        return None

    premiered = str(show.get("premiered") or "").strip()
    year = premiered[:4] if premiered[:4] else ""

    image = show.get("image") or {}
    poster_url = (
        str(image.get("medium") or "").strip()
        or str(image.get("original") or "").strip()
    )

    externals = show.get("externals") or {}
    network = show.get("network") or {}
    web_channel = show.get("webChannel") or {}

    return {
        "tmdb_id": "",
        "tvmaze_id": show_id,
        "imdb_id": externals.get("imdb") or "",
        "tvdb_id": externals.get("thetvdb") or "",
        "media_type": "tv",
        "title": title,
        "year": year,
        "overview": _strip_html(show.get("summary") or ""),
        "poster_url": poster_url,
        "vote_average": (show.get("rating") or {}).get("average"),
        "vote_count": None,
        "popularity": show.get("weight"),
        "genre_ids": [],
        "genres": [{"name": g} for g in (show.get("genres") or []) if g],
        "external_url": str(show.get("url") or "").strip(),
        "source": "TVMaze Airing",
        "network_name": str(network.get("name") or web_channel.get("name") or "").strip(),
        "provider_scores": {
            "tvmaze_airing": _tvmaze_schedule_score(ep, offset, source_kind=source_kind),
        },
        "_match_key": f"{_norm_title(title)}::{year}",
    }


def fetch_tvmaze_airing(media: str, page: int, days_per_page: int = 3) -> list[dict]:
    """
    TV-only source combining:
      - /schedule      (linear/country schedule)
      - /schedule/web  (web/streaming schedule)

    page=1 => today + next N days
    page=2 => following N days
    """
    media = (media or "all").strip().lower()
    if media == "movie":
        return []
    if media not in ("all", "tv"):
        media = "tv"

    try:
        page_num = max(1, int(page))
    except Exception:
        page_num = 1

    try:
        days_per_page = max(1, min(int(days_per_page), 7))
    except Exception:
        days_per_page = 3

    start = date.today() + timedelta(days=(page_num - 1) * days_per_page)
    out: list[dict] = []
    seen_show_ids: set[int] = set()

    for offset in range(days_per_page):
        d = start + timedelta(days=offset)

        batches: list[tuple[str, Any]] = []

        try:
            linear_raw = _get("/schedule", {"date": d.isoformat(), "country": "US"})
            batches.append(("schedule", linear_raw))
        except Exception:
            pass

        try:
            web_raw = _get("/schedule/web", {"date": d.isoformat()})
            batches.append(("web", web_raw))
        except Exception:
            pass

        for source_kind, raw in batches:
            if not isinstance(raw, list):
                continue

            for ep in raw:
                item = _build_item(ep, offset, source_kind=source_kind)
                if not item:
                    continue

                show_id = int(item.get("tvmaze_id") or 0)
                if not show_id or show_id in seen_show_ids:
                    continue

                out.append(item)
                seen_show_ids.add(show_id)

    return out
