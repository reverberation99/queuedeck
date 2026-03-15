from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, render_template, request

from .db import get_db
from .models_settings import get_user_setting_scoped
from .routes_actions import _get_state_all

from .utils.auth import login_required, current_user
from .clients.jellyfin import (
    find_user_id_by_name,
    get_next_up,
    get_recent_unwatched_movies,
    get_series_remaining_from_nextup,
    _base,
    _username,
    _get,
    _items_from,
    _anime_paths,
)
from .clients.sonarr import get_upcoming
from .clients.radarr import get_upcoming_missing

bp = Blueprint("stats", __name__)


def _utc_now():
    return datetime.now(timezone.utc)


def _parse_dt(val):
    s = str(val or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _stats_window_key():
    raw = str(request.args.get("window") or "lifetime").strip().lower()
    allowed = {"lifetime", "365d", "90d", "30d"}
    return raw if raw in allowed else "lifetime"


def _window_start(window_key: str):
    now = _utc_now()
    if window_key == "365d":
        return now - timedelta(days=365)
    if window_key == "90d":
        return now - timedelta(days=90)
    if window_key == "30d":
        return now - timedelta(days=30)
    return None


def _played_dt(item: dict):
    ud = item.get("UserData") or {}
    return (
        _parse_dt(ud.get("LastPlayedDate"))
        or _parse_dt(item.get("DatePlayed"))
        or _parse_dt(item.get("LastPlayedDate"))
    )


def _in_window(item: dict, window_start):
    if window_start is None:
        return True
    dt = _played_dt(item)
    return bool(dt and dt >= window_start)


def _runtime_minutes(item: dict) -> int:
    ticks = item.get("RunTimeTicks") or 0
    try:
        ticks = int(ticks or 0)
    except Exception:
        ticks = 0
    if ticks <= 0:
        return 0
    # Jellyfin ticks are 10,000,000 per second
    seconds = ticks / 10000000
    return int(round(seconds / 60))


def _day_key(dt):
    if not dt:
        return ""
    return dt.date().isoformat()


def _weekday_name(dt):
    if not dt:
        return ""
    return dt.strftime("%A")


def _build_heatmap_and_streaks(play_dates):
    dates = sorted({d.date() for d in play_dates if d})
    if not dates:
        return {
            "current_streak": 0,
            "longest_streak": 0,
            "heatmap": [],
            "weekday_counts": {
                "Monday": 0, "Tuesday": 0, "Wednesday": 0, "Thursday": 0,
                "Friday": 0, "Saturday": 0, "Sunday": 0,
            },
            "best_day": "",
        }

    weekday_counts = {
        "Monday": 0, "Tuesday": 0, "Wednesday": 0, "Thursday": 0,
        "Friday": 0, "Saturday": 0, "Sunday": 0,
    }

    date_counts = Counter()
    for d in play_dates:
        if not d:
            continue
        date_counts[d.date().isoformat()] += 1
        weekday_counts[d.strftime("%A")] += 1

    longest = 1
    current = 0
    run = 1

    for i in range(1, len(dates)):
        diff = (dates[i] - dates[i - 1]).days
        if diff == 1:
            run += 1
        else:
            longest = max(longest, run)
            run = 1
    longest = max(longest, run)

    today = _utc_now().date()
    yesterday = today - timedelta(days=1)

    if dates[-1] in {today, yesterday}:
        current = 1
        for i in range(len(dates) - 1, 0, -1):
            diff = (dates[i] - dates[i - 1]).days
            if diff == 1:
                current += 1
            else:
                break

    # 18 weeks, github-ish
    start_day = today - timedelta(days=125)
    heatmap = []
    for i in range(126):
        d = start_day + timedelta(days=i)
        key = d.isoformat()
        count = int(date_counts.get(key, 0))
        level = 0
        if count >= 8:
            level = 4
        elif count >= 5:
            level = 3
        elif count >= 3:
            level = 2
        elif count >= 1:
            level = 1
        heatmap.append({
            "date": key,
            "count": count,
            "level": level,
            "weekday": d.strftime("%A"),
        })

    best_day = max(weekday_counts.items(), key=lambda kv: kv[1])[0] if any(weekday_counts.values()) else ""

    return {
        "current_streak": current,
        "longest_streak": longest,
        "heatmap": heatmap,
        "weekday_counts": weekday_counts,
        "best_day": best_day,
    }


def _is_played(item: dict) -> bool:
    ud = item.get("UserData") or {}
    if ud.get("Played") is True:
        return True
    try:
        if int(ud.get("PlayCount") or 0) > 0:
            return True
    except Exception:
        pass
    return False


def _is_anime_path(path_val: str, anime_paths: list[str]) -> bool:
    p = str(path_val or "").strip()
    if not p:
        return False
    return any(p.startswith(ap) for ap in anime_paths)


def _hidden_keys_for_current_user() -> set[str]:
    try:
        db = get_db()
        uid = _current_user_id_for_stats()
        rows = db.execute(
            """
            SELECT source, item_id, title
            FROM hidden_items
            WHERE user_id = ?
            """,
            (uid,),
        ).fetchall()

        out = set()
        for row in rows:
            source = str(row["source"] or "").strip().lower()
            item_id = str(row["item_id"] or "").strip()
            title = str(row["title"] or "").strip().lower()

            if item_id:
                out.add(f"{source}::{item_id}")
            if title:
                out.add(f"{source}::title::{title}")

        return out
    except Exception:
        return set()


def _is_hidden(hidden_keys: set[str], source: str, item_id: str = "", title: str = "") -> bool:
    src = str(source or "").strip().lower()
    iid = str(item_id or "").strip()
    ttl = str(title or "").strip().lower()

    if iid and f"{src}::{iid}" in hidden_keys:
        return True
    if ttl and f"{src}::title::{ttl}" in hidden_keys:
        return True
    return False


def _stats_hidden_ids() -> set[str]:
    try:
        state = _get_state_all(int(_current_user_id_for_stats())) or {}
    except Exception:
        return set()

    out = set()
    for _, row in state.items():
        kind = str(row.get("kind") or "")
        item_id = str(row.get("item_id") or "")
        hidden = bool(row.get("hidden"))
        if not hidden:
            continue
        if kind.startswith("stats_") and item_id:
            out.add(f"{kind}::{item_id}")
    return out


def _current_user_id_for_stats() -> int:
    try:
        me = current_user() or {}
        return int(me.get("user_id") or 0)
    except Exception:
        return 0


def _norm_stats_key(val: str) -> str:
    return "".join(ch.lower() for ch in str(val or "").strip() if ch.isalnum())


def _scoped_setting(key: str, default: str = "") -> str:
    try:
        user_id = _current_user_id_for_stats()
        if user_id <= 0:
            return str(default or "").strip()
        return str(get_user_setting_scoped(user_id, key, default=default) or "").strip()
    except Exception:
        return str(default or "").strip()


@bp.get("/stats")
@login_required
def stats_page():
    return render_template("stats.html", me=(current_user() or {}))


@bp.get("/api/stats/overview")
@login_required
def stats_overview():
    try:
        base = _base()
        if not base:
            return jsonify(error="Jellyfin is not configured for this user."), 400

        has_jellyfin = bool(
            _scoped_setting("jellyfin_url") and
            _scoped_setting("jellyfin_api_key") and
            _scoped_setting("jellyfin_user")
        )
        has_sonarr = bool(
            _scoped_setting("sonarr_url") and
            _scoped_setting("sonarr_api_key")
        )
        has_radarr = bool(
            _scoped_setting("radarr_url") and
            _scoped_setting("radarr_api_key")
        )

        user_id = find_user_id_by_name(_username())
        anime_paths = _anime_paths()
        has_anime = bool(has_jellyfin and anime_paths)
        window_key = _stats_window_key()
        window_start = _window_start(window_key)

        movies = _get(
            f"{base}/Users/{user_id}/Items",
            params={
                "IncludeItemTypes": "Movie",
                "Recursive": "true",
                "Fields": "UserData,Genres,Path,ProductionYear,RunTimeTicks",
                "Limit": "20000",
            },
        )

        episodes = _get(
            f"{base}/Users/{user_id}/Items",
            params={
                "IncludeItemTypes": "Episode",
                "Recursive": "true",
                "Fields": "UserData,SeriesId,SeriesName,Genres,Path,RunTimeTicks",
                "Limit": "20000",
            },
        )

        resume = _get(
            f"{base}/Users/{user_id}/Items/Resume",
            params={
                "Limit": "200",
                "Fields": "UserData,SeriesName,Path",
            },
        )

        movie_items = _items_from(movies)
        episode_items = _items_from(episodes)
        resume_items = _items_from(resume)

        watched_movies = 0
        watched_episodes = 0
        anime_watched_episodes = 0
        tv_watched_episodes = 0

        watched_series_ids = set()
        completed_series_ids = set()

        show_counter = Counter()
        anime_counter = Counter()
        genre_counter = Counter()

        episodes_by_series = defaultdict(list)

        filtered_movie_items = [m for m in movie_items if _is_played(m) and _in_window(m, window_start)]
        filtered_episode_items = [ep for ep in episode_items if _is_played(ep) and _in_window(ep, window_start)]

        play_dates = []

        movie_runtime_total = 0
        movie_runtime_count = 0
        episode_runtime_total = 0
        episode_runtime_count = 0

        for m in filtered_movie_items:
            watched_movies += 1
            dt = _played_dt(m)
            if dt:
                play_dates.append(dt)

            mins = _runtime_minutes(m)
            if mins > 0:
                movie_runtime_total += mins
                movie_runtime_count += 1

            for g in (m.get("Genres") or []):
                gs = str(g or "").strip()
                if gs:
                    genre_counter[gs] += 1

        for ep in episode_items:
            sid = str(ep.get("SeriesId") or "").strip()
            if sid:
                episodes_by_series[sid].append(ep)

        for ep in filtered_episode_items:
            sid = str(ep.get("SeriesId") or "").strip()

            watched_episodes += 1
            if sid:
                watched_series_ids.add(sid)

            dt = _played_dt(ep)
            if dt:
                play_dates.append(dt)

            mins = _runtime_minutes(ep)
            if mins > 0:
                episode_runtime_total += mins
                episode_runtime_count += 1

            series_name = str(ep.get("SeriesName") or "Unknown Series").strip()
            is_anime = _is_anime_path(ep.get("Path") or "", anime_paths)

            if is_anime:
                anime_watched_episodes += 1
                anime_counter[series_name] += 1
            else:
                tv_watched_episodes += 1
                show_counter[series_name] += 1

            for g in (ep.get("Genres") or []):
                gs = str(g or "").strip()
                if gs:
                    genre_counter[gs] += 1

        started_series_ids = set()
        finished_series_ids = set()

        for sid, eps in episodes_by_series.items():
            if not eps:
                continue

            played_any_lifetime = any(_is_played(ep) for ep in eps)
            played_any_window = any(_is_played(ep) and _in_window(ep, window_start) for ep in eps)
            all_played_lifetime = all(_is_played(ep) for ep in eps)

            if played_any_lifetime and all_played_lifetime:
                completed_series_ids.add(sid)

            if played_any_window:
                started_series_ids.add(sid)
                if all_played_lifetime:
                    finished_series_ids.add(sid)

        in_progress_series = len(watched_series_ids)
        completed_series = len(completed_series_ids)

        started_series_count = len(started_series_ids)
        finished_series_count = len(finished_series_ids)
        completion_rate = round((finished_series_count / started_series_count) * 100, 1) if started_series_count else 0.0

        avg_movie_runtime = round(movie_runtime_total / movie_runtime_count, 1) if movie_runtime_count else 0.0
        avg_episode_runtime = round(episode_runtime_total / episode_runtime_count, 1) if episode_runtime_count else 0.0

        streaks = _build_heatmap_and_streaks(play_dates)

        nextup = get_next_up(limit=250) or {}
        nextup_items = _items_from(nextup)

        nextup_tv_count = 0
        nextup_anime_count = 0
        for it in nextup_items:
            if _is_anime_path(it.get("Path") or "", anime_paths):
                nextup_anime_count += 1
            else:
                nextup_tv_count += 1

        if not has_anime:
            nextup_anime_count = 0

        remaining_items = get_series_remaining_from_nextup(limit_series=250, nextup_limit=250) or []
        remaining_series = len(remaining_items)

        recent_unwatched_movies = get_recent_unwatched_movies(limit=100) or []
        total_movies = len(movie_items)
        unwatched_movies = sum(
            1 for m in movie_items
            if not _is_played(m)
        )

        completion_percent = 0
        if total_movies > 0:
            completion_percent = round((watched_movies / total_movies) * 100)

        continue_watching_count = len(resume_items)

        if has_sonarr:
            try:
                airing_tonight_count = len([
                    x for x in (get_upcoming(days=1) or [])
                    if str(x.get("airDateUtc") or x.get("airDate") or "").strip()
                ])
            except Exception:
                airing_tonight_count = 0
        else:
            airing_tonight_count = 0

        if has_radarr:
            try:
                radarr_missing_count = len(get_upcoming_missing(days=365, limit=500) or [])
            except Exception:
                radarr_missing_count = 0
        else:
            radarr_missing_count = 0

        stats_hidden = _stats_hidden_ids()

        queue_health = {
            "continue_watching": continue_watching_count,
            "nextup_tv": nextup_tv_count,
            "nextup_anime": nextup_anime_count,
            "latest_unwatched_movies": unwatched_movies,
            "airing_tonight": airing_tonight_count,
            "series_remaining": remaining_series,
            "radarr_missing": radarr_missing_count,
        }

        queue_health_rows = [
            {"name": "Continue Watching", "value": continue_watching_count, "item_key": "continue_watching"},
            {"name": "Next Up — TV", "value": nextup_tv_count, "item_key": "nextup_tv"},
            {"name": "Next Up — Anime", "value": nextup_anime_count, "item_key": "nextup_anime"},
            {"name": "Latest Unwatched Movies", "value": unwatched_movies, "item_key": "latest_unwatched_movies"},
            {"name": "Airing Tonight", "value": airing_tonight_count, "item_key": "airing_tonight"},
            {"name": "Series Remaining", "value": remaining_series, "item_key": "series_remaining"},
            {"name": "Radarr Missing", "value": radarr_missing_count, "item_key": "radarr_missing"},
        ]

        queue_health_rows = [
            row for row in queue_health_rows
            if (
                (row["item_key"] != "airing_tonight" or has_sonarr) and
                (row["item_key"] != "radarr_missing" or has_radarr) and
                (row["item_key"] != "nextup_anime" or has_anime)
            )
        ]

        if not has_sonarr:
            airing_tonight_count = 0
        if not has_radarr:
            radarr_missing_count = 0
        if not has_anime:
            nextup_anime_count = 0

        queue_health_rows = [
            row for row in queue_health_rows
            if f"stats_queue_health::{row['item_key']}" not in stats_hidden
        ]

        top_shows = [
            {
                "name": name,
                "count": count,
                "item_key": f"show::{_norm_stats_key(name)}::{name}",
            }
            for name, count in show_counter.most_common(50)
            if f"stats_top_show::show::{_norm_stats_key(name)}::{name}" not in stats_hidden
        ][:10]

        top_anime = [
            {
                "name": name,
                "count": count,
                "item_key": f"anime::{_norm_stats_key(name)}::{name}",
            }
            for name, count in anime_counter.most_common(50)
            if f"stats_top_anime::anime::{_norm_stats_key(name)}::{name}" not in stats_hidden
        ][:10]

        top_genres = [
            {
                "name": name,
                "count": count,
                "item_key": f"genre::{_norm_stats_key(name)}::{name}",
            }
            for name, count in genre_counter.most_common(50)
            if f"stats_top_genre::genre::{_norm_stats_key(name)}::{name}" not in stats_hidden
        ][:10]

        hidden_keys = _hidden_keys_for_current_user()

        current_sections = {
            "continue_watching": [
                {
                    "title": str(x.get("Name") or x.get("SeriesName") or "Item"),
                    "item_id": str(x.get("Id") or ""),
                    "item_key": f"cw::{_norm_stats_key(str(x.get('Name') or x.get('SeriesName') or 'Item'))}::{str(x.get('Name') or x.get('SeriesName') or 'Item')}",
                }
                for x in resume_items
                if not _is_hidden(
                    hidden_keys,
                    "jellyfin",
                    str(x.get("Id") or ""),
                    str(x.get("Name") or x.get("SeriesName") or "Item"),
                )
            ],
            "nextup_tv": [
                {
                    "title": str(x.get("SeriesName") or x.get("Name") or "Item"),
                    "episode": str(x.get("Name") or "").strip(),
                    "item_id": str(x.get("Id") or ""),
                    "item_key": f"nextup_tv::{_norm_stats_key(str(x.get('SeriesName') or x.get('Name') or 'Item'))}::{str(x.get('SeriesName') or x.get('Name') or 'Item')}",
                }
                for x in nextup_items
                if not _is_anime_path(x.get("Path") or "", anime_paths)
                and not _is_hidden(
                    hidden_keys,
                    "jellyfin",
                    str(x.get("Id") or ""),
                    str(x.get("SeriesName") or x.get("Name") or "Item"),
                )
            ],
            "nextup_anime": [
                {
                    "title": str(x.get("SeriesName") or x.get("Name") or "Item"),
                    "episode": str(x.get("Name") or "").strip(),
                    "item_id": str(x.get("Id") or ""),
                    "item_key": f"nextup_anime::{_norm_stats_key(str(x.get('SeriesName') or x.get('Name') or 'Item'))}::{str(x.get('SeriesName') or x.get('Name') or 'Item')}",
                }
                for x in nextup_items
                if _is_anime_path(x.get("Path") or "", anime_paths)
                and not _is_hidden(
                    hidden_keys,
                    "jellyfin",
                    str(x.get("Id") or ""),
                    str(x.get("SeriesName") or x.get("Name") or "Item"),
                )
            ],
            "latest_unwatched_movies": [
                {
                    "title": str(x.get("title") or "Movie"),
                    "year": x.get("year"),
                    "item_id": str(x.get("item_id") or ""),
                    "item_key": f"movie::{_norm_stats_key(str(x.get('title') or 'Movie'))}::{str(x.get('title') or 'Movie')}",
                }
                for x in recent_unwatched_movies
                if not _is_hidden(
                    hidden_keys,
                    "jellyfin",
                    str(x.get("item_id") or ""),
                    str(x.get("title") or "Movie"),
                )
            ],
        }

        for k in list(current_sections.keys()):
            current_sections[k] = [
                row for row in current_sections[k]
                if f"stats_current_activity::{row['item_key']}" not in stats_hidden
            ][:8]

        return jsonify(
            overview={
                "window": window_key,
                "watched_movies": watched_movies,
                "watched_episodes": watched_episodes,
                "watched_tv_episodes": tv_watched_episodes,
                "watched_anime_episodes": anime_watched_episodes,
                "series_in_progress": in_progress_series,
                "completed_series": completed_series,
                "continue_watching_count": continue_watching_count,
                "nextup_tv_count": nextup_tv_count,
                "nextup_anime_count": nextup_anime_count,
                "series_remaining": remaining_series,
                "unwatched_movies": unwatched_movies,
                "total_movies": total_movies,
                "completion_percent": completion_percent,
                "airing_tonight_count": airing_tonight_count,
                "radarr_missing_count": radarr_missing_count,
            },
            time_stats={
                "window": window_key,
                "current_streak": streaks["current_streak"],
                "longest_streak": streaks["longest_streak"],
                "average_movie_runtime": avg_movie_runtime,
                "average_episode_runtime": avg_episode_runtime,
                "started_series": started_series_count,
                "finished_series": finished_series_count,
                "completion_rate": completion_rate,
                "best_day": streaks["best_day"],
            },
            heatmap=streaks["heatmap"],
            weekday_counts=streaks["weekday_counts"],
            top_shows=top_shows,
            top_anime=top_anime,
            top_genres=top_genres,
            queue_health=queue_health,
            queue_health_rows=queue_health_rows,
            current_sections=current_sections,
            availability={
                "jellyfin": has_jellyfin,
                "sonarr": has_sonarr,
                "radarr": has_radarr,
                "anime": has_anime,
            },
        )
    except Exception as e:
        return jsonify(error=str(e)), 500
