import os
from datetime import datetime, timedelta, timezone

import requests
from flask import Blueprint, jsonify, request, render_template

from .utils.auth import login_required, current_user
from .db import get_db
from .routes_actions import _get_state_all
from .models_settings import get_user_setting_scoped

from .clients.jellyfin import (
    get_next_up,
    find_user_id_by_name,
    get_series_remaining_from_nextup,
)

from .clients.sonarr import get_upcoming, get_calendar, get_series_slug_map, get_queue
from .clients.radarr import get_queue as get_radarr_queue

# IMPORTANT: pull config from DB settings first, then ENV
from .models_settings import get_setting

bp = Blueprint("dashboard", __name__)


def _current_user_id() -> int:
    me = current_user() or {}
    return int(me.get("user_id") or 0)


def _user_setting(key: str) -> str:
    user_id = _current_user_id()
    if user_id <= 0:
        return ""
    try:
        return get_user_setting_scoped(user_id, key, default="")
    except Exception:
        return ""


def _cfg(key: str, env: str, default: str = "") -> str:
    """Per-user setting only. No env fallback in multi-user mode."""
    return _user_setting(key)


def _hide_future_nextup_for_hidden_series_enabled() -> bool:
    raw = str(_user_setting("hide_future_nextup_for_hidden_series") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _hidden_series_matchers_for_nextup() -> tuple[set[str], set[str]]:
    """
    Returns:
      (
        hidden_jellyfin_series_ids,
        hidden_series_title_keys
      )

    We suppress Next Up TV when a user has hidden the series from:
      - Series Remaining (kind=remaining_series, item_id is Jellyfin SeriesId)
      - Sonarr Upcoming / Missing (kind=sonarr_series, item_id is Sonarr numeric id)
        -> we try to resolve the Sonarr series title and match by title
      - jellyfin_series (future-proof support if used elsewhere)
    """
    user_id = _current_user_id()
    if user_id <= 0:
        return set(), set()

    hidden_series_ids = set()
    hidden_title_keys = set()

    try:
        state = _get_state_all(user_id) or {}
        hidden_sonarr_ids = []

        for _, row in state.items():
            if not bool(row.get("hidden")):
                continue

            kind = str(row.get("kind") or "").strip()
            item_id = str(row.get("item_id") or "").strip()

            if not item_id:
                continue

            if kind in {"remaining_series", "jellyfin_series"}:
                hidden_series_ids.add(item_id)

            elif kind == "sonarr_series":
                hidden_sonarr_ids.append(item_id)

        # Resolve hidden Sonarr series IDs -> titles so we can suppress Jellyfin Next Up by series name
        if hidden_sonarr_ids:
            base = _cfg("sonarr_url", "SONARR_URL", "").rstrip("/")
            api_key = _cfg("sonarr_api_key", "SONARR_API_KEY", "").strip()

            if base and api_key:
                headers = {"X-Api-Key": api_key}
                for sid in hidden_sonarr_ids:
                    try:
                        r = requests.get(f"{base}/api/v3/series/{sid}", headers=headers, timeout=10)
                        r.raise_for_status()
                        j = r.json() or {}
                        title = str(j.get("title") or "").strip().lower()
                        if title:
                            hidden_title_keys.add(title)
                    except Exception:
                        pass

    except Exception:
        pass

    return hidden_series_ids, hidden_title_keys


# --------------------------------------------------
# Root (Dashboard)
# --------------------------------------------------

@bp.get("/")
@login_required
def root():
    return render_template("index.html", me=(current_user() or {}))


# --------------------------------------------------
# Jellyfin helpers (for season progress enrichment)
# --------------------------------------------------

def _jf_get(base: str, api_key: str, path: str, params: dict | None = None, timeout: int = 25):
    r = requests.get(
        f"{base}{path}",
        headers={"X-Emby-Token": api_key},
        params=params or {},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json() or {}


def _season_progress_for_series(base: str, api_key: str, user_id: str, series_id: str):
    """
    Returns:
      {
        "season_number": int|None,
        "next_episode_number": int|None,
        "season_watched_episodes": int|None,
        "season_total_episodes": int|None,
        "season_progress_pct": int|None
      }
    Uses Jellyfin:
      /Shows/NextUp?UserId=..&SeriesId=..&Limit=1
      /Shows/{SeriesId}/Episodes?UserId=..&SeasonId=..
    """
    try:
        nextup = _jf_get(
            base, api_key, "/Shows/NextUp",
            params={"UserId": user_id, "SeriesId": series_id, "Limit": 1, "Fields": "UserData"},
            timeout=20,
        )
        items = (nextup.get("Items") or [])
        if not items:
            return {}

        ep = items[0]
        season_id = str(ep.get("SeasonId") or "").strip()
        season_number = ep.get("ParentIndexNumber")
        next_episode_number = ep.get("IndexNumber")

        if not season_id:
            return {}

        eps = _jf_get(
            base, api_key, f"/Shows/{series_id}/Episodes",
            params={"UserId": user_id, "SeasonId": season_id, "Fields": "UserData", "Limit": 400},
            timeout=25,
        )
        ep_items = (eps.get("Items") or [])
        total = len(ep_items)
        watched = 0
        for e in ep_items:
            ud = e.get("UserData") or {}
            if ud.get("Played") is True:
                watched += 1

        pct = None
        if total > 0:
            pct = int(round((watched / float(total)) * 100))

        return {
            "season_number": season_number,
            "next_episode_number": next_episode_number,
            "season_watched_episodes": watched,
            "season_total_episodes": total,
            "season_progress_pct": pct,
        }
    except Exception:
        return {}


def _series_progress_for_series(base: str, api_key: str, user_id: str, series_id: str):
    """
    Series-wide progress (all seasons).
    Uses Jellyfin:
      /Shows/{SeriesId}/Episodes?UserId=..  (NO SeasonId)
    Returns:
      {
        "series_watched_episodes": int|None,
        "series_total_episodes": int|None,
        "series_progress_pct": int|None
      }
    """
    try:
        eps = _jf_get(
            base, api_key, f"/Shows/{series_id}/Episodes",
            params={"UserId": user_id, "Fields": "UserData", "Limit": 400},
            timeout=35,
        )
        ep_items = (eps.get("Items") or [])
        total = len(ep_items)
        watched = 0
        for e in ep_items:
            ud = e.get("UserData") or {}
            if ud.get("Played") is True:
                watched += 1

        pct = None
        if total > 0:
            pct = int(round((watched / float(total)) * 100))

        return {
            "series_watched_episodes": watched,
            "series_total_episodes": total,
            "series_progress_pct": pct,
        }
    except Exception:
        return {}


# --------------------------------------------------
# Jellyfin: Active Sessions
# --------------------------------------------------

@bp.get("/api/jellyfin/active-sessions")
@login_required
def api_jellyfin_active_sessions():
    me = current_user() or {}
    if not bool(me.get("is_admin")):
        return jsonify(ok=True, items=[])

    try:
        limit = int(request.args.get("limit", "2"))
        limit = max(1, min(limit, 10))

        base = _cfg("jellyfin_url", "JELLYFIN_URL").rstrip("/")
        api_key = _cfg("jellyfin_api_key", "JELLYFIN_API_KEY")

        if not base or not api_key:
            return jsonify(ok=True, items=[])

        sessions = _jf_get(
            base,
            api_key,
            "/Sessions",
            params={},
            timeout=20,
        )

        out = []
        for s in sessions if isinstance(sessions, list) else []:
            now_playing = s.get("NowPlayingItem") or {}
            if not now_playing:
                continue

            user_name = str(s.get("UserName") or "").strip() or "Unknown user"
            title = str(now_playing.get("Name") or "").strip() or "Unknown title"
            media_type = str(now_playing.get("Type") or "").strip().lower()

            playback = s.get("PlayState") or {}
            is_paused = bool(playback.get("IsPaused", False))
            state_label = "Paused" if is_paused else "Playing"

            position_ticks = int(playback.get("PositionTicks") or 0)
            run_time_ticks = int(now_playing.get("RunTimeTicks") or 0)

            progress_pct = 0
            if run_time_ticks > 0 and position_ticks > 0:
                try:
                    progress_pct = int(round((position_ticks / float(run_time_ticks)) * 100))
                    progress_pct = max(0, min(100, progress_pct))
                except Exception:
                    progress_pct = 0

            season_num = now_playing.get("ParentIndexNumber")
            episode_num = now_playing.get("IndexNumber")
            series_name = str(now_playing.get("SeriesName") or "").strip()

            subtitle = ""
            if media_type == "episode":
                parts = []
                if series_name:
                    parts.append(series_name)
                if season_num is not None and episode_num is not None:
                    try:
                        parts.append(f"S{int(season_num):02d}E{int(episode_num):02d}")
                    except Exception:
                        pass
                subtitle = " • ".join(parts)
            else:
                year = str(now_playing.get("ProductionYear") or "").strip()
                subtitle = year or (media_type.title() if media_type else "Video")

            out.append({
                "user": user_name,
                "title": title,
                "subtitle": subtitle,
                "state": state_label,
                "progress_pct": progress_pct,
            })

        return jsonify(ok=True, items=out[:limit])
    except Exception as e:
        return jsonify(ok=False, error=str(e), items=[]), 500



# --------------------------------------------------
# Jellyfin: Continue Watching
# --------------------------------------------------

@bp.get("/api/jellyfin/continue-watching")
@login_required
def api_jellyfin_continue_watching():
    try:
        limit = int(request.args.get("limit", "12"))
        limit = max(1, min(limit, 100))

        base = _cfg("jellyfin_url", "JELLYFIN_URL").rstrip("/")
        api_key = _cfg("jellyfin_api_key", "JELLYFIN_API_KEY")
        username = _cfg("jellyfin_user", "JELLYFIN_USER")

        if not base or not api_key or not username:
            return jsonify(
                Items=[],
                TotalRecordCount=0,
                items=[],
                count=0,
            )

        user_id = find_user_id_by_name(username)

        r = requests.get(
            f"{base}/Users/{user_id}/Items/Resume",
            headers={"X-Emby-Token": api_key},
            params={
                "Limit": str(limit),
                "Fields": "PrimaryImageAspectRatio,ImageTags,DateCreated,Path,UserData",
            },
            timeout=20,
        )
        r.raise_for_status()
        raw = r.json() or {}
        raw_items = raw.get("Items", []) or []

        cleaned = []
        for it in raw_items:
            ud = it.get("UserData") or {}
            primary_tag = (it.get("ImageTags") or {}).get("Primary")

            series_id = it.get("SeriesId")
            series_tag = it.get("SeriesPrimaryImageTag")

            cleaned.append({
                "series": it.get("SeriesName") or "",
                "season": it.get("SeasonName") or "",
                "episode": it.get("IndexNumber"),
                "title": it.get("Name") or "",
                "premiere_date": it.get("PremiereDate"),
                "path": it.get("Path") or "",
                "progress_percent": round(ud.get("PlayedPercentage") or 0, 1),
                "item_id": it.get("Id"),
                "primary_image_url": (
                    f"/img/jellyfin/series/{series_id}?tag={series_tag}"
                    if series_id and series_tag else
                    f"/img/jellyfin/primary/{it.get('Id')}?tag={primary_tag}"
                ),
                "jellyfin_web_url": f"{base}/web/index.html#!/details?id={it.get('Id')}",
            })

        return jsonify(
            Items=cleaned,
            TotalRecordCount=raw.get("TotalRecordCount", len(cleaned)),
            items=cleaned,
            count=len(cleaned),
        )

    except Exception as e:
        return jsonify(error=str(e)), 500


# --------------------------------------------------
# Jellyfin: Next Up Split
# --------------------------------------------------

@bp.get("/api/jellyfin/nextup/split")
@login_required
def api_jellyfin_nextup_split():
    try:
        limit = int(request.args.get("limit", "60"))
        limit = max(1, min(limit, 200))

        if not _cfg("jellyfin_url", "JELLYFIN_URL") or not _cfg("jellyfin_api_key", "JELLYFIN_API_KEY") or not _cfg("jellyfin_user", "JELLYFIN_USER"):
            return jsonify(
                anime=[],
                tv=[],
                all=[],
                meta={
                    "total": 0,
                    "anime": 0,
                    "tv": 0,
                    "anime_paths": [],
                },
            )

        data = get_next_up(limit=limit)
        items = data.get("Items", []) or []

        raw_paths = _cfg("anime_paths", "ANIME_PATHS", "")
        anime_paths = [p.strip() for p in raw_paths.split(",") if p.strip()]

        base = _cfg("jellyfin_url", "JELLYFIN_URL").rstrip("/")

        anime = []
        tv = []

        hide_future = _hide_future_nextup_for_hidden_series_enabled()
        hidden_series_ids, hidden_series_title_keys = _hidden_series_matchers_for_nextup() if hide_future else (set(), set())

        for it in items:
            p = it.get("Path") or ""
            is_anime = any(p.startswith(ap) for ap in anime_paths)

            series_name = str(it.get("SeriesName") or "").strip()
            series_id = str(it.get("SeriesId") or "").strip()

            if hide_future and not is_anime:
                series_name_key = series_name.lower()
                if (series_id and series_id in hidden_series_ids) or (series_name_key and series_name_key in hidden_series_title_keys):
                    continue

            ud = it.get("UserData") or {}
            primary_tag = (it.get("ImageTags") or {}).get("Primary")

            series_id = it.get("SeriesId")
            series_tag = it.get("SeriesPrimaryImageTag")

            cleaned = {
                "series": it.get("SeriesName"),
                "season": it.get("SeasonName"),
                "episode": it.get("IndexNumber"),
                "title": it.get("Name"),
                "premiere_date": it.get("PremiereDate"),
                "path": p,
                "progress_percent": round(ud.get("PlayedPercentage") or 0, 1),
                "item_id": it.get("Id"),
                "primary_image_url": (
                    f"/img/jellyfin/series/{series_id}?tag={series_tag}"
                    if series_id and series_tag else
                    f"/img/jellyfin/primary/{it.get('Id')}?tag={primary_tag}"
                ),
                "jellyfin_web_url": f"{base}/web/index.html#!/details?id={it.get('Id')}" if base else "",
            }

            (anime if is_anime else tv).append(cleaned)

        return jsonify(
            anime=anime,
            tv=tv,
            all=(anime + tv),
            meta={
                "total": len(anime) + len(tv),
                "anime": len(anime),
                "tv": len(tv),
                "anime_paths": anime_paths,
            },
        )

    except Exception as e:
        return jsonify(error=str(e)), 500


# --------------------------------------------------
# Jellyfin: Series Remaining (NOW with season progress)
# --------------------------------------------------

@bp.get("/api/jellyfin/series-remaining")
@login_required
def api_jellyfin_series_remaining():
    try:
        limit = int(request.args.get("limit", "30"))
        limit = max(1, min(limit, 80))

        if not _cfg("jellyfin_url", "JELLYFIN_URL") or not _cfg("jellyfin_api_key", "JELLYFIN_API_KEY") or not _cfg("jellyfin_user", "JELLYFIN_USER"):
            return jsonify(count=0, items=[])

        # ✅ IMPORTANT: this is your function’s real signature
        items = get_series_remaining_from_nextup(
            limit_series=limit,
            nextup_limit=250
        )

        # Enrich with season progress (safe: if anything fails, items still return)
        base = _cfg("jellyfin_url", "JELLYFIN_URL").rstrip("/")
        api_key = _cfg("jellyfin_api_key", "JELLYFIN_API_KEY")
        username = _cfg("jellyfin_user", "JELLYFIN_USER")

        if base and api_key and username:
            user_id = find_user_id_by_name(username)

            enriched = []
            for it in items:
                try:
                    # try common keys, depending on what your client returns
                    series_id = (
                        it.get("series_id")
                        or it.get("SeriesId")
                        or it.get("id")
                        or it.get("item_id")
                    )
                    series_id = str(series_id or "").strip()

                    if not series_id:
                        enriched.append(it)
                        continue

                    prog = _season_progress_for_series(base, api_key, user_id, series_id)

                    series_prog = _series_progress_for_series(base, api_key, user_id, series_id)
                    # don’t stomp existing keys; just add new ones
                    out = dict(it)
                    for k, v in prog.items():
                        if k not in out or out.get(k) in (None, "", 0):
                            out[k] = v

                    for k, v in series_prog.items():
                        if k not in out or out.get(k) in (None, "", 0):
                            out[k] = v

                    enriched.append(out)
                except Exception:
                    enriched.append(it)

            items = enriched

        return jsonify(count=len(items), items=items)

    except Exception as e:
        return jsonify(error=str(e)), 500




# --------------------------------------------------
# Jellyfin: Latest Unwatched TV / Anime
# --------------------------------------------------

def _latest_unwatched_series_split(tv_limit: int = 10, anime_limit: int = 10):
    tv_limit = max(1, min(int(tv_limit or 10), 120))
    anime_limit = max(1, min(int(anime_limit or 10), 120))

    base = _cfg("jellyfin_url", "JELLYFIN_URL").rstrip("/")
    api_key = _cfg("jellyfin_api_key", "JELLYFIN_API_KEY")
    username = _cfg("jellyfin_user", "JELLYFIN_USER")

    if not base or not api_key or not username:
        return {"tv_items": [], "anime_items": [], "tv_count": 0, "anime_count": 0}

    user_id = find_user_id_by_name(username)

    raw_paths = _cfg("anime_paths", "ANIME_PATHS", "") or ""
    anime_paths = [p.strip() for p in str(raw_paths).split(",") if str(p).strip()]

    def _is_anime_path(path_val: str) -> bool:
        p = str(path_val or "").strip()
        if not p:
            return False
        return any(p.startswith(ap) for ap in anime_paths)

    def _jf_get(path: str, params: dict | None = None, timeout: int = 30):
        r = requests.get(
            f"{base}{path}",
            headers={"X-Emby-Token": api_key},
            params=params or {},
            timeout=timeout,
        )
        r.raise_for_status()
        return r.json() or {}

    raw = _jf_get(
        f"/Users/{user_id}/Items",
        params={
            "Recursive": "true",
            "IncludeItemTypes": "Episode",
            "Filters": "IsUnplayed",
            "SortBy": "DateCreated",
            "SortOrder": "Descending",
            "Limit": "500",
            "Fields": "PrimaryImageAspectRatio,ImageTags,SeriesPrimaryImageTag,DateCreated,Path,UserData,PremiereDate,SeriesId,SeriesName,ParentIndexNumber,IndexNumber",
        },
        timeout=35,
    )
    raw_items = raw.get("Items", []) or []

    grouped = {}

    for it in raw_items:
        series_id = str(it.get("SeriesId") or "").strip()
        if not series_id:
            continue

        series_name = str(it.get("SeriesName") or "").strip()
        if not series_name:
            continue

        dt = it.get("DateCreated") or ""
        season_num = it.get("ParentIndexNumber")
        episode_num = it.get("IndexNumber")
        primary_tag = (it.get("ImageTags") or {}).get("Primary")
        series_tag = it.get("SeriesPrimaryImageTag")
        path_val = it.get("Path") or ""

        row = grouped.get(series_id)
        if not row:
            grouped[series_id] = {
                "series": series_name,
                "series_id": series_id,
                "item_id": series_id,
                "latest_added": dt,
                "latest_path": path_val,
                "unwatched_count": 1,
                "latest_season_number": season_num,
                "latest_episode_number": episode_num,
                "season": f"Season {season_num}" if season_num is not None else "",
                "episode": episode_num,
                "primary_image_url": (
                    f"/img/jellyfin/series/{series_id}?tag={series_tag}"
                    if series_tag else
                    f"/img/jellyfin/primary/{it.get('Id')}?tag={primary_tag}"
                ),
                "jellyfin_web_url": f"{base}/web/index.html#!/details?id={series_id}",
            }
        else:
            row["unwatched_count"] = int(row.get("unwatched_count") or 0) + 1
            if dt and str(dt) > str(row.get("latest_added") or ""):
                row["latest_added"] = dt
                row["latest_path"] = path_val
                row["latest_season_number"] = season_num
                row["latest_episode_number"] = episode_num
                row["season"] = f"Season {season_num}" if season_num is not None else ""
                row["episode"] = episode_num
                if not row.get("primary_image_url"):
                    row["primary_image_url"] = (
                        f"/img/jellyfin/series/{series_id}?tag={series_tag}"
                        if series_tag else
                        f"/img/jellyfin/primary/{it.get('Id')}?tag={primary_tag}"
                    )

    candidates = list(grouped.values())
    candidates.sort(key=lambda x: x.get("latest_added") or "", reverse=True)

    started_series_ids = set()

    try:
        nextup = get_next_up(limit=250) or {}
        for it in (nextup.get("Items") or []):
            sid = str(it.get("SeriesId") or "").strip()
            if sid:
                started_series_ids.add(sid)
    except Exception:
        pass

    try:
        rem = get_series_remaining_from_nextup(limit_series=250, nextup_limit=250) or []
        for it in rem:
            sid = str(
                it.get("series_id")
                or it.get("SeriesId")
                or it.get("id")
                or it.get("item_id")
                or ""
            ).strip()
            if sid:
                started_series_ids.add(sid)
    except Exception:
        pass

    def _series_has_any_played_episode(series_id: str) -> bool:
        try:
            data = _jf_get(
                f"/Shows/{series_id}/Episodes",
                params={
                    "UserId": user_id,
                    "Fields": "UserData",
                    "Limit": "500",
                },
                timeout=35,
            )
            eps = data.get("Items") or []
            for ep in eps:
                ud = ep.get("UserData") or {}
                if ud.get("Played") is True:
                    return True
                played_pct = ud.get("PlayedPercentage")
                try:
                    if played_pct is not None and float(played_pct) > 0:
                        return True
                except Exception:
                    pass
            return False
        except Exception:
            return False

    tv_items = []
    anime_items = []

    for row in candidates:
        sid = str(row.get("series_id") or "").strip()
        if not sid:
            continue

        if sid in started_series_ids:
            continue

        if _series_has_any_played_episode(sid):
            continue

        is_anime = _is_anime_path(row.get("latest_path") or "")

        out = dict(row)
        out.pop("latest_path", None)

        if is_anime:
            if len(anime_items) < anime_limit:
                anime_items.append(out)
        else:
            if len(tv_items) < tv_limit:
                tv_items.append(out)

        if len(tv_items) >= tv_limit and len(anime_items) >= anime_limit:
            break

    return {
        "tv_items": tv_items,
        "anime_items": anime_items,
        "tv_count": len(tv_items),
        "anime_count": len(anime_items),
    }


@bp.get("/api/jellyfin/latest-unwatched-tv")
@login_required
def api_jellyfin_latest_unwatched_tv():
    try:
        limit = int(request.args.get("limit", "10"))
        limit = max(1, min(limit, 120))
        data = _latest_unwatched_series_split(tv_limit=limit, anime_limit=120)
        items = data.get("tv_items") or []
        return jsonify(items=items, count=len(items))
    except Exception as e:
        return jsonify(error=str(e)), 500


@bp.get("/api/jellyfin/latest-unwatched-split")
@login_required
def api_jellyfin_latest_unwatched_split():
    try:
        tv_limit = int(request.args.get("tv_limit", "10"))
        anime_limit = int(request.args.get("anime_limit", "10"))
        data = _latest_unwatched_series_split(tv_limit=tv_limit, anime_limit=anime_limit)
        return jsonify(
            tv_items=data.get("tv_items") or [],
            anime_items=data.get("anime_items") or [],
            tv_count=data.get("tv_count") or 0,
            anime_count=data.get("anime_count") or 0,
        )
    except Exception as e:
        return jsonify(error=str(e)), 500


# --------------------------------------------------
# Sonarr: Upcoming
# --------------------------------------------------

@bp.get("/api/sonarr/upcoming")
@login_required
def sonarr_upcoming():
    try:
        days = int(request.args.get("days", "14"))
        days = max(1, min(days, 30))

        limit = int(request.args.get("limit", "60"))
        limit = max(1, min(limit, 120))

        if not _cfg("sonarr_url", "SONARR_URL") or not _cfg("sonarr_api_key", "SONARR_API_KEY"):
            return jsonify(count=0, items=[])

        items = get_upcoming(days=days)

        slug_map = get_series_slug_map()
        cleaned = []
        for it in items:
            series = it.get("series") or {}

            cleaned.append({
                "series": series.get("title"),
                "season_number": it.get("seasonNumber"),
                "episode_number": it.get("episodeNumber"),
                "title": it.get("title"),
                "air_date_utc": it.get("airDateUtc"),
                "episode_id": it.get("id"),
                "series_id": series.get("id"),
                "series_slug": (series.get("slug") or slug_map.get(series.get("id"))),
                "poster_url": (
                    f"/img/sonarr/series/{series.get('tvdbId')}.jpg"
                    if series.get("tvdbId") else None
                ),
            })

        cleaned.sort(key=lambda x: x.get("air_date_utc") or "")
        cleaned = cleaned[:limit]

        return jsonify(count=len(cleaned), items=cleaned)

    except Exception as e:
        return jsonify(error=str(e)), 500


# --------------------------------------------------
# Sonarr: Missing (aired but not downloaded)
#   Fixes:
#   - include all of "today" by using end = tomorrow
#   - union: calendar missing + queue records (grabbed/pending)
# --------------------------------------------------

def _build_sonarr_missing_items(days: int = 14, limit: int = 60):
    days = max(1, min(int(days or 14), 60))
    limit = max(1, min(int(limit or 60), 200))
    now = datetime.now(timezone.utc)

    start = (now - timedelta(days=days)).date().isoformat()
    end = (now + timedelta(days=1)).date().isoformat()

    if not _cfg("sonarr_url", "SONARR_URL") or not _cfg("sonarr_api_key", "SONARR_API_KEY"):
        return []

    cal = get_calendar(start=start, end=end)
    queue_records = get_queue(page_size=400)

    slug_map = get_series_slug_map()
    items = []
    seen = set()  # (seriesId, season, episode)

    def add_item(series_obj: dict, ep_obj: dict, air_utc: str):
        series_id = series_obj.get("id")
        season_number = ep_obj.get("seasonNumber")
        episode_number = ep_obj.get("episodeNumber")

        key = (series_id, season_number, episode_number)
        if key in seen:
            return
        seen.add(key)

        tvdb_id = series_obj.get("tvdbId")
        poster_url = f"/img/sonarr/series/{tvdb_id}.jpg" if tvdb_id else None

        items.append({
            "series": series_obj.get("title") or "",
            "title": ep_obj.get("title") or "",
            "season_number": season_number,
            "episode_number": episode_number,
            "air_date_utc": air_utc,
            "episode_id": ep_obj.get("id") or ep_obj.get("episodeId"),
            "series_id": series_id,
            "series_slug": (series_obj.get("slug") or series_obj.get("titleSlug") or slug_map.get(series_id)),
            "poster_url": poster_url,
        })

    for ep in cal:
        air_utc = ep.get("airDateUtc") or ep.get("airDate")
        if not air_utc:
            continue

        try:
            aired = datetime.fromisoformat(air_utc.replace("Z", "+00:00"))
        except Exception:
            continue

        if aired > now:
            continue
        if ep.get("hasFile") is True:
            continue
        if ep.get("monitored") is False:
            continue

        series = ep.get("series") or {}
        if series.get("monitored") is False:
            continue

        add_item(series, ep, air_utc)

    for rec in queue_records:
        ep = rec.get("episode") or {}
        series = rec.get("series") or {}

        air_utc = ep.get("airDateUtc") or ep.get("airDate")
        if not air_utc:
            continue

        try:
            aired = datetime.fromisoformat(air_utc.replace("Z", "+00:00"))
        except Exception:
            continue

        if aired > now:
            continue
        if ep.get("hasFile") is True:
            continue
        if ep.get("monitored") is False:
            continue
        if series.get("monitored") is False:
            continue

        add_item(series, ep, air_utc)

    items.sort(key=lambda x: x.get("air_date_utc") or "", reverse=True)
    return items[:limit]


# --------------------------------------------------
# Sonarr: Queue Summary
# --------------------------------------------------

@bp.get("/api/sonarr/queue-summary")
@login_required
def api_sonarr_queue_summary():
    me = current_user() or {}
    if not bool(me.get("is_admin")):
        return jsonify(items=[])

    try:
        page_size = int(request.args.get("page_size", "50"))
        page_size = max(1, min(page_size, 200))

        if not _cfg("sonarr_url", "SONARR_URL") or not _cfg("sonarr_api_key", "SONARR_API_KEY"):
            return jsonify(count=0, items=[])

        records = get_queue(page_size=page_size) or []
        raw_items = []

        for rec in records:
            ep = rec.get("episode") or {}
            eps = rec.get("episodes") or []
            series = rec.get("series") or {}

            if not isinstance(eps, list):
                eps = []

            title = (series.get("title") or "").strip()

            season_number = ep.get("seasonNumber")
            episode_number = ep.get("episodeNumber")
            ep_title = (ep.get("title") or "").strip()

            season_numbers = []
            episode_numbers = []
            episode_titles = []

            if season_number is not None:
                season_numbers.append(season_number)
            if episode_number is not None:
                episode_numbers.append(episode_number)
            if ep_title:
                episode_titles.append(ep_title)

            for ep_obj in eps:
                if not isinstance(ep_obj, dict):
                    continue
                s = ep_obj.get("seasonNumber")
                e = ep_obj.get("episodeNumber")
                t = str(ep_obj.get("title") or "").strip()

                if s is not None and s not in season_numbers:
                    season_numbers.append(s)
                if e is not None and e not in episode_numbers:
                    episode_numbers.append(e)
                if t and t not in episode_titles:
                    episode_titles.append(t)

            # Fallback: if single episode fields were empty, use the first episode from episodes[]
            if season_number is None and season_numbers:
                season_number = season_numbers[0]
            if episode_number is None and episode_numbers:
                episode_number = episode_numbers[0]
            if not ep_title and episode_titles:
                ep_title = episode_titles[0]

            status = (
                rec.get("status")
                or rec.get("trackedDownloadState")
                or rec.get("trackedDownloadStatus")
                or ""
            )

            sizeleft = rec.get("sizeleft") or rec.get("sizeLeft") or 0
            size = rec.get("size") or 0
            progress = None
            try:
                size = float(size or 0)
                sizeleft = float(sizeleft or 0)
                if size > 0:
                    progress = int(round(((size - sizeleft) / size) * 100))
            except Exception:
                progress = None

            tvdb_id = series.get("tvdbId")
            poster_url = f"/img/sonarr/series/{tvdb_id}.jpg" if tvdb_id else None

            raw_items.append({
                "series": title,
                "season_number": season_number,
                "episode_number": episode_number,
                "title": ep_title,
                "status": str(status),
                "progress": progress,
                "poster_url": poster_url,
                "_download_id": str(rec.get("downloadId") or "").strip(),
                "_queue_title": str(rec.get("title") or "").strip(),
                "_season_numbers": season_numbers,
                "_episode_numbers": episode_numbers,
                "_episode_titles": episode_titles,
                "_group_count": max(1, len(eps) if eps else 1),
            })

        grouped = {}
        ordered = []

        for item in raw_items:
            download_id = item.get("_download_id") or ""
            if not download_id:
                key = f'row::{len(ordered)}'
                grouped[key] = item
                ordered.append(key)
                continue

            key = f'dlid::{download_id}'
            if key not in grouped:
                grouped[key] = item
                ordered.append(key)
            else:
                g = grouped[key]

                # Keep best-known title/series/poster
                if not g.get("series") and item.get("series"):
                    g["series"] = item["series"]
                if not g.get("poster_url") and item.get("poster_url"):
                    g["poster_url"] = item["poster_url"]

                # Prefer the most active status wording
                active_states = {"downloading": 3, "importing": 2, "completed": 1}
                gs = str(g.get("status") or "").lower()
                is_ = str(item.get("status") or "").lower()
                if active_states.get(is_, 0) > active_states.get(gs, 0):
                    g["status"] = item.get("status")

                # Prefer higher progress if available
                gp = g.get("progress")
                ip = item.get("progress")
                if isinstance(ip, int) and (not isinstance(gp, int) or ip > gp):
                    g["progress"] = ip

                # Merge grouped metadata
                g["_group_count"] = int(g.get("_group_count") or 1) + 1

                for s in item.get("_season_numbers") or []:
                    if s is not None and s not in (g.get("_season_numbers") or []):
                        g.setdefault("_season_numbers", []).append(s)

                for e in item.get("_episode_numbers") or []:
                    if e is not None and e not in (g.get("_episode_numbers") or []):
                        g.setdefault("_episode_numbers", []).append(e)

                for t in item.get("_episode_titles") or []:
                    if t and t not in (g.get("_episode_titles") or []):
                        g.setdefault("_episode_titles", []).append(t)

        items = []
        for key in ordered:
            it = grouped[key]

            seasons = sorted([x for x in (it.get("_season_numbers") or []) if x is not None])
            episodes = sorted([x for x in (it.get("_episode_numbers") or []) if x is not None])
            episode_titles = [x for x in (it.get("_episode_titles") or []) if x]
            count = int(it.get("_group_count") or 1)

            if count <= 1:
                if seasons and episodes and not it.get("display_code"):
                    it["display_code"] = f"S{int(seasons[0]):02d}E{int(episodes[0]):02d}"
                if not it.get("title") and episode_titles:
                    it["title"] = episode_titles[0]
            else:
                if len(set(seasons)) == 1 and seasons:
                    s = int(seasons[0])
                    it["season_number"] = s
                    it["display_code"] = f"Season {s}"
                    it["title"] = f"{count} episodes"
                elif seasons:
                    it["display_code"] = f"{len(set(seasons))} seasons"
                    it["title"] = f"{count} episodes"
                else:
                    it["display_code"] = ""
                    it["title"] = f"{count} queue items"

            # Extra fallback from Sonarr queue title if episode metadata was sparse
            if not it.get("title"):
                qtitle = str(it.get("_queue_title") or "").strip()
                if qtitle:
                    it["title"] = qtitle

            it.pop("_download_id", None)
            it.pop("_queue_title", None)
            it.pop("_group_count", None)
            it.pop("_season_numbers", None)
            it.pop("_episode_numbers", None)
            it.pop("_episode_titles", None)
            items.append(it)

        return jsonify(count=len(items), items=items)

    except Exception as e:
        return jsonify(error=str(e)), 500


@bp.get("/api/radarr/queue-summary")
@login_required
def api_radarr_queue_summary():
    me = current_user() or {}
    if not bool(me.get("is_admin")):
        return jsonify(items=[])

    try:
        page_size = int(request.args.get("page_size", "50"))
        page_size = max(1, min(page_size, 200))

        if not _cfg("radarr_url", "RADARR_URL") or not _cfg("radarr_api_key", "RADARR_API_KEY"):
            return jsonify(count=0, items=[])

        records = get_radarr_queue(page_size=page_size) or []
        items = []

        for rec in records:
            movie = rec.get("movie") or {}
            title = (movie.get("title") or rec.get("title") or "").strip()
            year = movie.get("year") or rec.get("year")
            tmdb_id = movie.get("tmdbId") or rec.get("tmdbId")

            status = (
                rec.get("trackedDownloadState")
                or rec.get("trackedDownloadStatus")
                or rec.get("status")
                or ""
            )

            sizeleft = rec.get("sizeleft") or rec.get("sizeLeft") or 0
            size = rec.get("size") or 0
            progress = None
            try:
                size = float(size or 0)
                sizeleft = float(sizeleft or 0)
                if size > 0:
                    progress = int(round(((size - sizeleft) / size) * 100))
                    progress = max(0, min(100, progress))
            except Exception:
                progress = None

            poster_url = f"/img/radarr/movie/{tmdb_id}.jpg" if tmdb_id else None

            items.append({
                "kind": "movie",
                "series": title,
                "title": title,
                "year": year,
                "status": str(status),
                "progress": progress,
                "poster_url": poster_url,
                "tmdb_id": tmdb_id,
            })

        return jsonify(count=len(items), items=items)

    except Exception as e:
        return jsonify(error=str(e)), 500


@bp.get("/api/sonarr/missing")
@login_required
def api_sonarr_missing():
    try:
        days = int(request.args.get("days", "14"))
        days = max(1, min(days, 60))

        limit = int(request.args.get("limit", "60"))
        limit = max(1, min(limit, 200))

        items = _build_sonarr_missing_items(days=days, limit=limit)
        return jsonify(count=len(items), items=items)

    except Exception as e:
        return jsonify(error=str(e)), 500
