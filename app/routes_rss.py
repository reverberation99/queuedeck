from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape as xml_escape

import requests
from flask import Blueprint, Response, abort, request

from .db import get_db
from .clients.jellyfin import find_user_id_by_name, get_recent_unwatched_movies
from .models_settings import get_user_setting_scoped

bp = Blueprint("rss", __name__)


def _user_setting_for(user_id: int, key: str, default: str = "") -> str:
    try:
        return get_user_setting_scoped(user_id, key, default=default)
    except Exception:
        return default


def _cfg_for(user_id: int, key: str) -> str:
    return _user_setting_for(user_id, key, default="")


def _find_user_id_by_token(token: str) -> int:
    t = str(token or "").strip()
    if not t:
        return 0

    db = get_db()
    row = db.execute(
        """
        SELECT user_id
        FROM user_settings
        WHERE key = 'rss_feed_token' AND value = ?
        LIMIT 1
        """,
        (t,),
    ).fetchone()

    return int(row["user_id"] or 0) if row else 0


def _ensure_rss_feed_state_table():
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS rss_feed_state (
            user_id INTEGER NOT NULL,
            feed_key TEXT NOT NULL,
            item_key TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (user_id, feed_key, item_key)
        )
        """
    )
    db.commit()


def _apply_event_feed_state(user_id: int, feed_key: str, items: list[dict], key_fields: list[str]) -> list[dict]:
    _ensure_rss_feed_state_table()
    db = get_db()

    now_iso = datetime.now(timezone.utc).isoformat()

    def build_key(item: dict) -> str:
        parts = []
        for f in key_fields:
            v = item.get(f)
            parts.append("" if v is None else str(v).strip())
        return "||".join(parts)

    current_keys = []
    for item in items:
        item_key = build_key(item)
        if not item_key.strip("|"):
            item_key = str(item.get("item_id") or item.get("title") or "").strip()
        if not item_key:
            continue

        current_keys.append(item_key)

        row = db.execute(
            """
            SELECT first_seen_at
            FROM rss_feed_state
            WHERE user_id = ? AND feed_key = ? AND item_key = ?
            """,
            (user_id, feed_key, item_key),
        ).fetchone()

        if row:
            first_seen_at = str(row["first_seen_at"] or now_iso)
            db.execute(
                """
                UPDATE rss_feed_state
                SET last_seen_at = ?, is_active = 1
                WHERE user_id = ? AND feed_key = ? AND item_key = ?
                """,
                (now_iso, user_id, feed_key, item_key),
            )
        else:
            first_seen_at = now_iso
            db.execute(
                """
                INSERT INTO rss_feed_state (user_id, feed_key, item_key, first_seen_at, last_seen_at, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (user_id, feed_key, item_key, now_iso, now_iso),
            )

        item["_rss_guid"] = f"{feed_key}:{item_key}"
        item["_rss_pub_date"] = first_seen_at

    db.execute(
        """
        UPDATE rss_feed_state
        SET is_active = 0
        WHERE user_id = ? AND feed_key = ?
        """,
        (user_id, feed_key),
    )

    for item_key in current_keys:
        db.execute(
            """
            UPDATE rss_feed_state
            SET is_active = 1, last_seen_at = ?
            WHERE user_id = ? AND feed_key = ? AND item_key = ?
            """,
            (now_iso, user_id, feed_key, item_key),
        )

    db.commit()
    return items


def _jf_get(base: str, api_key: str, path: str, params: dict | None = None, timeout: int = 25):
    r = requests.get(
        f"{base}{path}",
        headers={"X-Emby-Token": api_key},
        params=params or {},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json() or {}


def _build_nextup_split_for_user(user_id: int, limit: int):
    base = _cfg_for(user_id, "jellyfin_url").rstrip("/")
    api_key = _cfg_for(user_id, "jellyfin_api_key")
    username = _cfg_for(user_id, "jellyfin_user")
    view_id = _cfg_for(user_id, "mytv_view_id").strip()

    if not base or not api_key or not username:
        return {"tv": [], "anime": []}

    jf_user_id = find_user_id_by_name(username, base_url=base, api_key=api_key)

    merged = []

    try:
        data = _jf_get(
            base,
            api_key,
            "/Shows/NextUp",
            params={
                "UserId": jf_user_id,
                "Limit": str(max(limit, 200)),
                "Fields": "PrimaryImageAspectRatio,ImageTags,UserData,Path,DateCreated,PremiereDate",
            },
            timeout=25,
        )
        merged.extend(data.get("Items", []) or [])
    except Exception:
        pass

    if view_id:
        try:
            series_data = _jf_get(
                base,
                api_key,
                f"/Users/{jf_user_id}/Items",
                params={
                    "ParentId": view_id,
                    "IncludeItemTypes": "Series",
                    "Recursive": "true",
                    "Limit": "500",
                    "Fields": "Path",
                },
                timeout=30,
            )
            series_items = series_data.get("Items", []) or []

            for s in series_items:
                sid = s.get("Id")
                if not sid:
                    continue

                try:
                    ep_data = _jf_get(
                        base,
                        api_key,
                        "/Shows/NextUp",
                        params={
                            "UserId": jf_user_id,
                            "SeriesId": sid,
                            "Limit": "1",
                            "Fields": "PrimaryImageAspectRatio,ImageTags,Path,UserData,DateCreated,PremiereDate",
                        },
                        timeout=25,
                    )
                    ep_items = ep_data.get("Items", []) or []
                    if ep_items:
                        merged.append(ep_items[0])
                except Exception:
                    pass
        except Exception:
            pass

    by_series = {}
    deduped = []
    for ep in merged:
        series_id = str(ep.get("SeriesId") or "").strip()
        series_name = str(ep.get("SeriesName") or ep.get("Series") or ep.get("Name") or "Series").strip()
        key = series_id or series_name
        if not key or key in by_series:
            continue
        by_series[key] = 1
        deduped.append(ep)

    raw_paths = _cfg_for(user_id, "anime_paths")
    anime_paths = [p.strip().rstrip("/") for p in str(raw_paths or "").split(",") if p.strip()]

    anime = []
    tv = []

    for it in deduped:
        item_path = str(it.get("Path") or "")
        is_anime = any(item_path.startswith(ap) for ap in anime_paths)

        ud = it.get("UserData") or {}
        primary_tag = (it.get("ImageTags") or {}).get("Primary")
        series_id = it.get("SeriesId")
        series_tag = it.get("SeriesPrimaryImageTag")

        cleaned = {
            "series": it.get("SeriesName") or "",
            "season": it.get("SeasonName") or "",
            "episode": it.get("IndexNumber"),
            "title": it.get("Name") or "",
            "premiere_date": it.get("PremiereDate") or "",
            "path": item_path,
            "progress_percent": round(ud.get("PlayedPercentage") or 0, 1),
            "item_id": str(it.get("Id") or ""),
            "primary_image_url": (
                f"/img/jellyfin/series/{series_id}?tag={series_tag}"
                if series_id and series_tag else
                f"/img/jellyfin/primary/{it.get('Id')}?tag={primary_tag}"
            ),
            "jellyfin_web_url": f"{base}/web/index.html#!/details?id={it.get('Id')}" if base else "",
        }

        (anime if is_anime else tv).append(cleaned)

    return {"tv": tv[:limit], "anime": anime[:limit]}


def _fmt_rss_dt(val: str) -> str:
    s = str(val or "").strip()
    if not s:
        return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    except Exception:
        return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def _episode_label(item: dict) -> str:
    forced = str(item.get("_rss_title") or "").strip()
    if forced:
        return forced

    series = str(item.get("series") or "").strip()
    title = str(item.get("title") or "").strip()
    season_name = str(item.get("season") or "").strip()
    ep_num = item.get("episode")

    ep_part = f"E{int(ep_num):02d}" if isinstance(ep_num, int) else ""
    if series and season_name and ep_part and title:
        return f"{series} • {season_name} {ep_part} • {title}"
    if series and title:
        return f"{series} • {title}"
    return series or title or "Next Up"


def _rss_response(title: str, link: str, description: str, items: list[dict]) -> Response:
    item_xml = []

    for it in items:
        item_title = _episode_label(it)
        item_link = str(it.get("jellyfin_web_url") or "").strip() or link
        pub_date = _fmt_rss_dt(str(it.get("_rss_pub_date") or it.get("premiere_date") or ""))
        guid = str(it.get("_rss_guid") or it.get("item_id") or item_link or item_title)

        desc_parts = []
        if it.get("series"):
            desc_parts.append(f"Series: {it.get('series')}")
        if it.get("season"):
            desc_parts.append(f"Season: {it.get('season')}")
        if it.get("episode") is not None:
            desc_parts.append(f"Episode: {it.get('episode')}")
        if it.get("progress_percent") is not None:
            desc_parts.append(f"Progress: {it.get('progress_percent')}%")

        forced_desc = str(it.get("_rss_desc") or "").strip()
        desc = forced_desc or " • ".join(desc_parts)

        item_xml.append(
            f"""
    <item>
      <title>{xml_escape(item_title)}</title>
      <link>{xml_escape(item_link)}</link>
      <guid isPermaLink="false">{xml_escape(guid)}</guid>
      <pubDate>{xml_escape(pub_date)}</pubDate>
      <description>{xml_escape(desc)}</description>
    </item>""".rstrip()
        )

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>{xml_escape(title)}</title>
    <link>{xml_escape(link)}</link>
    <description>{xml_escape(description)}</description>
    <lastBuildDate>{xml_escape(datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT"))}</lastBuildDate>
{''.join(item_xml)}
  </channel>
</rss>
"""
    return Response(xml, mimetype="application/rss+xml; charset=utf-8")


def _sonarr_cfg_for(user_id: int):
    return (
        _cfg_for(user_id, "sonarr_url").rstrip("/"),
        _cfg_for(user_id, "sonarr_api_key"),
    )


def _radarr_cfg_for(user_id: int):
    return (
        _cfg_for(user_id, "radarr_url").rstrip("/"),
        _cfg_for(user_id, "radarr_api_key"),
    )


def _is_airing_tonight(date_str: str) -> bool:
    s = str(date_str or "").strip()
    if not s:
        return False

    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

        now_local = datetime.now().astimezone()
        dt_local = dt.astimezone(now_local.tzinfo)
        return dt_local.date() == now_local.date()
    except Exception:
        return False


def _build_airing_tonight_missing_for_user(user_id: int, limit: int):
    sonarr_url, sonarr_api_key = _sonarr_cfg_for(user_id)
    if not sonarr_url or not sonarr_api_key:
        return []

    headers = {"X-Api-Key": sonarr_api_key}
    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=14)).date().isoformat()
    end_date = (now + timedelta(days=14)).date().isoformat()

    try:
        cal_resp = requests.get(
            f"{sonarr_url}/api/v3/calendar",
            headers=headers,
            params={"start": start_date, "end": end_date, "includeSeries": "true"},
            timeout=30,
        )
        cal_resp.raise_for_status()
        cal_rows = cal_resp.json() or []
    except Exception:
        cal_rows = []

    try:
        queue_resp = requests.get(
            f"{sonarr_url}/api/v3/queue",
            headers=headers,
            params={"page": 1, "pageSize": 200, "includeSeries": "true", "includeEpisode": "true"},
            timeout=30,
        )
        queue_resp.raise_for_status()
        queue_data = queue_resp.json() or {}
        queue_rows = queue_data.get("records") or []
    except Exception:
        queue_rows = []

    out = []
    seen = set()
    queued_keys = set()

    for rec in queue_rows:
        ep = rec.get("episode") or {}
        series = rec.get("series") or {}
        key = (
            series.get("id"),
            ep.get("seasonNumber"),
            ep.get("episodeNumber"),
        )
        queued_keys.add(key)

    def add_item(series_obj: dict, ep_obj: dict, air_utc: str, missing: bool):
        series_id = series_obj.get("id")
        season_number = ep_obj.get("seasonNumber")
        episode_number = ep_obj.get("episodeNumber")

        key = (series_id, season_number, episode_number)
        if key in seen:
            return
        seen.add(key)

        if not _is_airing_tonight(air_utc):
            return

        series_title = str(series_obj.get("title") or "").strip()
        episode_title = str(ep_obj.get("title") or "").strip()
        episode_id = str(ep_obj.get("id") or ep_obj.get("episodeId") or "").strip()

        season_part = f"S{int(season_number):02d}" if isinstance(season_number, int) else ""
        ep_part = f"E{int(episode_number):02d}" if isinstance(episode_number, int) else ""
        se = f"{season_part}{ep_part}".strip()

        label = (
            f"{series_title} • {se} • {episode_title}"
            if series_title and se and episode_title else
            (f"{series_title} • {episode_title}" if series_title and episode_title else (series_title or episode_title or "Airing Tonight"))
        )

        status_bits = ["Airs tonight"]
        if missing:
            status_bits.append("Missing")
        elif key in queued_keys:
            status_bits.append("In queue")

        out.append({
            "series": series_title,
            "season": f"Season {season_number}" if isinstance(season_number, int) else "",
            "episode": episode_number if isinstance(episode_number, int) else None,
            "title": episode_title,
            "premiere_date": air_utc,
            "item_id": episode_id or label,
            "jellyfin_web_url": f"{sonarr_url}/calendar" if sonarr_url else "",
            "_rss_title": label,
            "_rss_desc": " • ".join(status_bits),
        })

    for ep in cal_rows:
        air_utc = ep.get("airDateUtc") or ep.get("airDate")
        if not air_utc:
            continue

        series = ep.get("series") or {}
        if ep.get("monitored") is False:
            continue
        if series.get("monitored") is False:
            continue

        missing = bool(ep.get("hasFile") is not True)
        add_item(series, ep, str(air_utc), missing)

    out.sort(key=lambda x: x.get("premiere_date") or "")
    return out[:limit]

def _build_latest_unwatched_movies_for_user(user_id: int, limit: int):
    radarr_url, radarr_api_key = _radarr_cfg_for(user_id)
    jf_base = _cfg_for(user_id, "jellyfin_url").rstrip("/")
    jf_api_key = _cfg_for(user_id, "jellyfin_api_key")
    jf_username = _cfg_for(user_id, "jellyfin_user")

    if not radarr_url or not radarr_api_key or not jf_base or not jf_api_key or not jf_username:
        return []

    try:
        jf_user_id = find_user_id_by_name(jf_username)
        data = _jf_get(
            jf_base,
            jf_api_key,
            f"/Users/{jf_user_id}/Items",
            params={
                "IncludeItemTypes": "Movie",
                "SortBy": "DateCreated",
                "SortOrder": "Descending",
                "Limit": str(limit),
                "Recursive": "true",
                "Fields": "PrimaryImageAspectRatio,ImageTags,DateCreated,UserData,ProductionYear",
            },
            timeout=30,
        )
        rows = data.get("Items") or []
    except Exception:
        rows = []

    out = []
    for it in rows[:limit]:
        ud = it.get("UserData") or {}
        if ud.get("Played") is True:
            continue
        try:
            if int(ud.get("PlayCount") or 0) > 0:
                continue
        except Exception:
            pass

        item_id = str(it.get("Id") or "").strip()
        title = str(it.get("Name") or "").strip()
        year = str(it.get("ProductionYear") or "").strip()
        date_added = str(it.get("DateCreated") or "").strip()

        label = f"{title} ({year})" if title and year else (title or "Latest Unwatched Movie")
        link = f"{jf_base}/web/index.html#!/details?id={item_id}" if item_id and jf_base else (f"{radarr_url}/movie" if radarr_url else "")

        out.append({
            "series": "",
            "season": "",
            "episode": None,
            "title": label,
            "premiere_date": date_added,
            "date_added": date_added,
            "item_id": item_id or label,
            "jellyfin_web_url": link,
            "_rss_title": label,
            "_rss_desc": "Latest unwatched movie",
        })

    return out


@bp.get("/rss/nextup-tv")
def rss_nextup_tv():
    token = str(request.args.get("token") or "").strip()
    user_id = _find_user_id_by_token(token)
    if user_id <= 0:
        abort(404)

    limit = 60
    try:
        limit = int(_cfg_for(user_id, "limit_nextup_tv") or "60")
    except Exception:
        limit = 60
    limit = max(1, min(limit, 120))

    data = _build_nextup_split_for_user(user_id, limit=max(limit, 60))
    items = (data.get("tv") or [])[:limit]
    items = _apply_event_feed_state(
        user_id,
        "nextup-tv",
        items,
        key_fields=["item_id", "series", "season", "episode", "title"],
    )

    host = request.host_url.rstrip("/")
    return _rss_response(
        title="QueueDeck — Next Up TV",
        link=f"{host}/",
        description="QueueDeck per-user RSS feed: Next Up TV",
        items=items,
    )



@bp.get("/rss/nextup-anime")
def rss_nextup_anime():
    token = str(request.args.get("token") or "").strip()
    user_id = _find_user_id_by_token(token)
    if user_id <= 0:
        abort(404)

    limit = 60
    try:
        limit = int(_cfg_for(user_id, "limit_nextup_anime") or "60")
    except Exception:
        limit = 60
    limit = max(1, min(limit, 120))

    data = _build_nextup_split_for_user(user_id, limit=max(limit, 60))
    items = (data.get("anime") or [])[:limit]
    items = _apply_event_feed_state(
        user_id,
        "nextup-anime",
        items,
        key_fields=["item_id", "series", "season", "episode", "title"],
    )

    host = request.host_url.rstrip("/")
    return _rss_response(
        title="QueueDeck — Next Up Anime",
        link=f"{host}/",
        description="QueueDeck per-user RSS feed: Next Up Anime",
        items=items,
    )


@bp.get("/rss/airing-tonight-missing")
def rss_airing_tonight_missing():
    token = str(request.args.get("token") or "").strip()
    user_id = _find_user_id_by_token(token)
    if user_id <= 0:
        abort(404)

    limit = 40
    items = _build_airing_tonight_missing_for_user(user_id, limit=limit)

    host = request.host_url.rstrip("/")
    return _rss_response(
        title="QueueDeck — Airing Tonight",
        link=f"{host}/",
        description="QueueDeck per-user RSS feed: Airing Tonight",
        items=items,
    )


@bp.get("/rss/latest-unwatched-movies")
def rss_latest_unwatched_movies():
    token = str(request.args.get("token") or "").strip()
    user_id = _find_user_id_by_token(token)
    if user_id <= 0:
        abort(404)

    limit = 10
    try:
        limit = int(_cfg_for(user_id, "limit_radarr_recent") or "10")
    except Exception:
        limit = 10
    limit = max(1, min(limit, 120))

    items = _build_latest_unwatched_movies_for_user(user_id, limit=limit)
    items = _apply_event_feed_state(
        user_id,
        "latest-unwatched-movies",
        items,
        key_fields=["item_id", "date_added"],
    )

    host = request.host_url.rstrip("/")
    return _rss_response(
        title="QueueDeck — Latest Unwatched Movies",
        link=f"{host}/",
        description="QueueDeck per-user RSS feed: Latest Unwatched Movies",
        items=items,
    )

