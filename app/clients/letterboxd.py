from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

import requests

from app.db import get_db

TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/movie"

_LETTERBOXD_RAW_CACHE: Dict[str, Dict[str, Any]] = {}
_LETTERBOXD_RAW_CACHE_TTL = 900

_LETTERBOXD_TMDB_SEARCH_CACHE: Dict[str, Dict[str, Any]] = {}
_LETTERBOXD_TMDB_SEARCH_CACHE_TTL = 43200  # 12 hours


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


def _letterboxd_rss_url() -> str:
    return (_app_setting("letterboxd_rss_url", "") or "").strip()


def _letterboxd_feed_entries() -> List[Dict[str, str]]:
    raw_multi = (_app_setting("letterboxd_rss_urls", "") or "").strip()
    raw_single = (_app_setting("letterboxd_rss_url", "") or "").strip()

    out: List[Dict[str, str]] = []

    if raw_multi:
        for idx, line in enumerate(raw_multi.splitlines(), start=1):
            line = str(line or "").strip()
            if not line:
                continue

            label = f"Feed {idx}"
            url = line

            if "|" in line:
                left, right = line.split("|", 1)
                label = str(left or "").strip() or label
                url = str(right or "").strip()

            if url:
                out.append({
                    "key": f"feed_{len(out)+1}",
                    "label": label,
                    "url": url,
                })

    elif raw_single:
        out.append({
            "key": "feed_1",
            "label": "Feed 1",
            "url": raw_single,
        })

    return out


def get_letterboxd_feed_sources() -> List[Dict[str, str]]:
    return _letterboxd_feed_entries()


def _tmdb_headers() -> dict:
    from app.routes_discover import _tmdb_auth_headers
    return _tmdb_auth_headers()


def _tmdb_params(extra: dict | None = None) -> dict:
    from app.routes_discover import _tmdb_auth_params
    params = dict(_tmdb_auth_params())
    if extra:
        params.update(extra)
    return params


def _clean_title(raw: str) -> str:
    s = html.unescape(str(raw or "")).strip()
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip(" -–—:|")
    return s.strip()


def _split_title_year(raw: str) -> tuple[str, str]:
    s = _clean_title(raw)

    m = re.match(r"^(.*?)\s*\((\d{4})\)\s*$", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    m = re.match(r"^(.*?),\s*(\d{4})\s*-\s*[★½]+\s*$", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    m = re.match(r"^(.*?),\s*(\d{4})\s*$", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    s = re.sub(r"\s*-\s*[★½]+\s*$", "", s).strip()
    return s, ""


def _extract_em_titles(desc_html: str) -> list[str]:
    text = html.unescape(desc_html or "")
    found = re.findall(r"<em>(.*?)</em>", text, flags=re.I | re.S)
    out: list[str] = []
    seen: set[str] = set()

    for raw in found:
        t = _clean_title(raw)
        if not t or len(t) < 2:
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)

    return out


def _candidate_titles(item) -> list[tuple[str, str]]:
    raw_title = item.findtext("title", default="").strip()
    raw_desc = item.findtext("description", default="").strip()

    candidates: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    t, y = _split_title_year(raw_title)
    if t:
        pair = (t, y)
        if pair not in seen:
            seen.add(pair)
            candidates.append(pair)

    for em_title in _extract_em_titles(raw_desc):
        pair = _split_title_year(em_title)
        if pair not in seen:
            seen.add(pair)
            candidates.append(pair)

    if ":" in raw_title:
        tail = raw_title.split(":", 1)[1].strip()
        tail = re.sub(r"\s+at\s+\d+\s*$", "", tail, flags=re.I)
        tail = re.sub(r"\s+on\s+.*$", "", tail, flags=re.I)
        tail = re.sub(r"\s+while\s+.*$", "", tail, flags=re.I)
        tail = re.sub(r"\s+and\s+beyond\s*$", "", tail, flags=re.I)
        pair = _split_title_year(tail)
        if pair[0] and pair not in seen:
            seen.add(pair)
            candidates.append(pair)

    return candidates


def _search_tmdb_movie(title: str, year: str = "", source_label: str = "Letterboxd") -> dict | None:
    if not title:
        return None

    import time

    cache_key = f"{str(title).strip().lower()}||{str(year).strip()}||{str(source_label).strip().lower()}"
    cached = _LETTERBOXD_TMDB_SEARCH_CACHE.get(cache_key)
    if cached:
        age = time.time() - float(cached.get("ts") or 0)
        if age <= _LETTERBOXD_TMDB_SEARCH_CACHE_TTL:
            return cached.get("payload")

    params = _tmdb_params({
        "query": title,
        "page": 1,
        "include_adult": "false",
    })
    if year:
        params["year"] = year

    r = requests.get(
        TMDB_SEARCH_URL,
        headers=_tmdb_headers(),
        params=params,
        timeout=20,
    )
    r.raise_for_status()

    data = r.json() or {}
    results = data.get("results") or []
    if not results:
        _LETTERBOXD_TMDB_SEARCH_CACHE[cache_key] = {
            "ts": time.time(),
            "payload": None,
        }
        return None

    best = results[0]
    tmdb_id = best.get("id")
    if not tmdb_id:
        _LETTERBOXD_TMDB_SEARCH_CACHE[cache_key] = {
            "ts": time.time(),
            "payload": None,
        }
        return None

    poster_path = str(best.get("poster_path") or "").strip()
    poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else ""

    release_date = str(best.get("release_date") or "").strip()
    release_year = release_date[:4] if len(release_date) >= 4 else (year or "")

    payload = {
        "tmdb_id": int(tmdb_id),
        "media_type": "movie",
        "title": str(best.get("title") or title).strip(),
        "year": release_year,
        "overview": str(best.get("overview") or "").strip(),
        "poster_url": poster_url,
        "vote_average": best.get("vote_average"),
        "vote_count": best.get("vote_count"),
        "popularity": best.get("popularity") or 0,
        "genre_ids": best.get("genre_ids") or [],
        "source": source_label,
        "provider_scores": {
            "letterboxd": 0.58,
        },
    }

    _LETTERBOXD_TMDB_SEARCH_CACHE[cache_key] = {
        "ts": time.time(),
        "payload": payload,
    }
    return payload


def _get_letterboxd_popular_for_url(rss_url: str, page: int = 1, source_label: str = "Letterboxd") -> List[Dict[str, Any]]:
    try:
        page = int(page or 1)
    except Exception:
        page = 1

    if page < 1:
        page = 1

    rss_url = str(rss_url or "").strip()
    if not rss_url:
        print("[letterboxd] no RSS URL configured", flush=True)
        return []

    import time

    cached = _LETTERBOXD_RAW_CACHE.get(rss_url)
    if cached:
        age = time.time() - float(cached.get("ts") or 0)
        if age <= _LETTERBOXD_RAW_CACHE_TTL:
            try:
                root = ET.fromstring(str(cached.get("text") or ""))
            except Exception:
                root = None
        else:
            root = None
    else:
        root = None

    if root is None:
        try:
            r = requests.get(
                rss_url,
                timeout=20,
                headers={
                    "User-Agent": "Mozilla/5.0 (QueueDeck Letterboxd RSS Fetch)"
                },
            )
            r.raise_for_status()
            raw_text = r.text
            root = ET.fromstring(raw_text)
            _LETTERBOXD_RAW_CACHE[rss_url] = {
                "ts": time.time(),
                "text": raw_text,
            }
        except Exception as e:
            print(f"[letterboxd] feed fetch failed: {e}", flush=True)

            if cached and cached.get("text"):
                try:
                    root = ET.fromstring(str(cached.get("text") or ""))
                    print("[letterboxd] using stale cached feed after fetch failure", flush=True)
                except Exception:
                    return []
            else:
                return []

    channel = root.find("channel")
    if channel is None:
        print("[letterboxd] no channel in RSS", flush=True)
        return []

    items = channel.findall("item") or []
    print(f"[letterboxd] feed_items={len(items)} rss_url={rss_url}", flush=True)

    per_page = 20
    start = (page - 1) * per_page
    end = start + per_page
    window = items[start:end]

    out: List[Dict[str, Any]] = []
    seen_tmdb: set[int] = set()

    total = max(len(window), 1)

    for idx, item in enumerate(window, start=1):
        raw_title = item.findtext("title", default="").strip()
        candidates = _candidate_titles(item)

        print(f"[letterboxd] raw_title={raw_title!r} candidates={candidates!r}", flush=True)

        hit = None
        for title, year in candidates[:5]:
            try:
                hit = _search_tmdb_movie(title=title, year=year, source_label=source_label)
            except Exception as e:
                print(f"[letterboxd] tmdb search failed for {title!r}: {e}", flush=True)
                hit = None

            if hit:
                break

        if not hit:
            continue

        tmdb_id = int(hit.get("tmdb_id") or 0)
        if not tmdb_id or tmdb_id in seen_tmdb:
            continue

        seen_tmdb.add(tmdb_id)

        rank_score = round(max(0.35, 1.0 - ((idx - 1) / total) * 0.60), 4)
        ps = dict(hit.get("provider_scores") or {})
        ps["letterboxd"] = rank_score
        hit["provider_scores"] = ps
        hit["letterboxd_rank"] = idx

        out.append(hit)

    print(f"[letterboxd] returned_items={len(out)}", flush=True)
    return out


def get_letterboxd_popular(page: int = 1) -> List[Dict[str, Any]]:
    feeds = _letterboxd_feed_entries()
    if not feeds:
        return []

    first = feeds[0]
    return _get_letterboxd_popular_for_url(
        rss_url=first.get("url", ""),
        page=page,
        source_label=f"Letterboxd — {first.get('label', 'Feed 1')}",
    )


def get_letterboxd_popular_feed(feed_key: str, page: int = 1) -> List[Dict[str, Any]]:
    feed_key = str(feed_key or "").strip()
    feeds = _letterboxd_feed_entries()

    for feed in feeds:
        if str(feed.get("key") or "") == feed_key:
            return _get_letterboxd_popular_for_url(
                rss_url=feed.get("url", ""),
                page=page,
                source_label=f"Letterboxd — {feed.get('label', feed_key)}",
            )

    return []


def get_letterboxd_popular_aggregate(page: int = 1) -> List[Dict[str, Any]]:
    feeds = _letterboxd_feed_entries()
    if not feeds:
        return []

    merged: Dict[Any, Dict[str, Any]] = {}
    ordered_keys: List[Any] = []

    for feed in feeds:
        label = str(feed.get("label") or "Feed").strip()
        items = _get_letterboxd_popular_for_url(
            rss_url=feed.get("url", ""),
            page=page,
            source_label=f"Letterboxd — {label}",
        )

        for item in items:
            tmdb_id = item.get("tmdb_id")
            key = tmdb_id or f"title::{item.get('title','')}::{item.get('year','')}"

            if key not in merged:
                merged[key] = dict(item)
                merged[key]["source"] = "Letterboxd Aggregate"
                merged[key]["_letterboxd_labels"] = [label]
                ordered_keys.append(key)
            else:
                existing = merged[key]
                labels = existing.get("_letterboxd_labels") or []
                if label not in labels:
                    labels.append(label)
                existing["_letterboxd_labels"] = labels

                ps = dict(existing.get("provider_scores") or {})
                current_score = float(ps.get("letterboxd") or 0.0)
                incoming_score = float((item.get("provider_scores") or {}).get("letterboxd") or 0.0)

                labels = existing.get("_letterboxd_labels") or []
                overlap_count = max(1, len(labels))

                bonus = 0.12 + max(0, overlap_count - 1) * 0.06
                ps["letterboxd"] = min(1.0, max(current_score, incoming_score) + bonus)
                existing["provider_scores"] = ps

    out: List[Dict[str, Any]] = []
    for key in ordered_keys:
        item = merged[key]
        labels = item.pop("_letterboxd_labels", [])
        if labels:
            item["letterboxd_labels"] = labels
        out.append(item)

    return out
