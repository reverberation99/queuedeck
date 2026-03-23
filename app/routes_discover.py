import time
import threading
import requests


def _qd_tmdb_debug(event: str, **kwargs):
    try:
        parts = []
        for k, v in kwargs.items():
            parts.append(f"{k}={repr(v)}")
        print(f"[discover-tmdb] {event} " + " ".join(parts), flush=True)
    except Exception:
        pass

from flask import Blueprint, jsonify, render_template, request, current_app, session


def _proxy_tmdb_url(url: str) -> str:
    url = str(url or "").strip()
    if not url:
        return url

    # already proxied or local
    if url.startswith("/img/tmdb/") or url.startswith("/img/") or url.startswith("/image"):
        return url

    # normalize direct TMDB CDN URLs
    if "image.tmdb.org" in url and "/t/p/" in url:
        try:
            tail = url.split("/t/p/", 1)[1]
            # tail looks like: w500/abc.jpg or original/abc.jpg
            parts = tail.split("/", 1)
            if len(parts) == 2 and parts[1]:
                return f"/img/tmdb/{parts[1]}"
        except Exception:
            return url

    return url

from .db import get_db
from .discover.engine import normalize_and_score_items
from .discover.providers.tmdb import (
    fetch_tmdb_trending,
    fetch_tmdb_popular, enrich_tmdb_item_by_id,
    fetch_tmdb_discover_by_genre,
    fetch_tmdb_discover_by_provider,
    consume_tmdb_enrich_stats
)

from .discover.providers.tvmaze import fetch_tvmaze_airing
from .discover.providers.trakt import (
    fetch_trakt_trending,
    fetch_trakt_popular,
)
from .discover.providers.anilist import (
    fetch_anilist_trending,
    fetch_anilist_popular,
    fetch_anilist_genre,
)
from .discover.providers.jikan import (
    fetch_jikan_anime_hot,
    fetch_jikan_anime_rising,
)

from .utils.auth import login_required, current_user
from .clients.jellyfin import find_in_library_batch
from .clients.sonarr import find_requested_series_batch
from .clients.radarr import find_requested_movies_batch
from app.clients.letterboxd import get_letterboxd_popular, get_letterboxd_popular_feed, get_letterboxd_popular_aggregate, get_letterboxd_feed_sources
from .clients.jellyfin_for_you import build_for_you_profile

bp = Blueprint("discover", __name__)

_DISCOVER_WARM_STATE = {
    "last_started": 0.0,
    "last_finished": 0.0,
    "last_elapsed": 0.0,
    "running": False,
    "phase": "",
    "message": "",
    "progress": 0,
    "total": 1,
    "percent": 0,
    "eta_bucket": "",
    "unresolved_anime_titles": [],
}
_DISCOVER_WARM_COOLDOWN_SEC = 900  # 15 minutes


def _discover_eta_bucket(progress: int, total: int) -> str:
    try:
        progress = int(progress or 0)
        total = max(1, int(total or 1))
    except Exception:
        return ""

    remaining = max(0, total - progress)
    if remaining >= 5:
        return "About 5–10 minutes remaining"
    if remaining >= 3:
        return "About 2–5 minutes remaining"
    if remaining >= 1:
        return "Less than 2 minutes remaining"
    return "Finalizing"


def _discover_collect_unresolved_anime_titles(items):
    out = []
    seen = set()

    for item in (items or []):
        title = str(item.get("title") or "").strip()
        tmdb_id = str(item.get("tmdb_id") or "").strip()
        media_type = str(item.get("media_type") or "").strip().lower()

        if not title or tmdb_id or media_type not in ("tv", "movie"):
            continue

        norm = " ".join(title.lower().split())
        if norm in seen:
            continue

        seen.add(norm)
        out.append(title)

    return out




def _ensure_user_discover_flags_table():
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS user_discover_flags (
            user_id INTEGER PRIMARY KEY,
            hide_nsfw_anime INTEGER NOT NULL DEFAULT 0
        )
    """)
    db.commit()


def _lookup_user_row_for_admin(identifier: str):
    db = get_db()
    ident = str(identifier or "").strip()
    if not ident:
        return None

    row = None
    try:
        row = db.execute(
            "SELECT id, username FROM users WHERE username = ?",
            (ident,),
        ).fetchone()
    except Exception:
        row = None

    if row:
        return row

    try:
        row = db.execute(
            "SELECT id, username FROM users WHERE email = ?",
            (ident,),
        ).fetchone()
    except Exception:
        row = None

    return row


def _get_user_hide_nsfw_anime_flag(user_id) -> bool:
    try:
        uid = int(user_id or 0)
    except Exception:
        return False

    if uid <= 0:
        return False

    _ensure_user_discover_flags_table()
    db = get_db()
    try:
        row = db.execute(
            "SELECT hide_nsfw_anime FROM user_discover_flags WHERE user_id = ?",
            (uid,),
        ).fetchone()
    except Exception:
        return False

    return bool(int(row["hide_nsfw_anime"] or 0)) if row else False


def _set_user_hide_nsfw_anime_flag(user_id, enabled: bool):
    uid = int(user_id or 0)
    if uid <= 0:
        return

    _ensure_user_discover_flags_table()
    db = get_db()
    db.execute(
        """
        INSERT INTO user_discover_flags (user_id, hide_nsfw_anime)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET hide_nsfw_anime = excluded.hide_nsfw_anime
        """,
        (uid, 1 if enabled else 0),
    )
    db.commit()


def _current_user_hide_nsfw_anime() -> bool:
    me = current_user() or {}

    user_id = me.get("id")
    if user_id:
        enabled = _get_user_hide_nsfw_anime_flag(user_id)
        print(f"[discover-nsfw-user] username={me.get('username')!r} id={user_id!r} enabled={enabled}", flush=True)
        return enabled

    username = str(me.get("username") or "").strip()
    if username:
        row = _lookup_user_row_for_admin(username)
        if row:
            enabled = _get_user_hide_nsfw_anime_flag(row["id"])
            print(f"[discover-nsfw-user] username={username!r} resolved_id={row['id']!r} enabled={enabled}", flush=True)
            return enabled

    print(f"[discover-nsfw-user] username={me.get('username')!r} id={me.get('id')!r} enabled=False reason='no_user_match'", flush=True)
    return False


def _is_nsfw_anime_item(item: dict) -> bool:
    import re

    if bool(item.get("is_adult")) or bool(item.get("adult")) or bool(item.get("nsfw")):
        return True

    rating = str(item.get("rating") or item.get("content_rating") or "").strip().lower()
    if rating:
        if "rx" in rating and "hentai" in rating:
            return True
        if "r+" in rating and ("nudity" in rating or "mild nudity" in rating):
            return True
        if "pg-13" not in rating and ("hentai" in rating or "nudity" in rating):
            return True
        if rating.startswith("r") and ("nudity" in rating or "suggestive" in rating):
            return True

    title = str(item.get("title") or "").strip()
    title_english = str(item.get("title_english") or "").strip()
    title_japanese = str(item.get("title_japanese") or "").strip()

    bits = [title, title_english, title_japanese]

    for key in ("overview", "description", "synopsis", "rating", "content_rating", "anime_type", "anime_source"):
        val = item.get(key)
        if val:
            bits.append(str(val))

    for key in ("genres", "genre_names", "tags", "themes", "demographics", "studios"):
        val = item.get(key)
        if isinstance(val, (list, tuple)):
            bits.extend([str(x) for x in val if x])
        elif val:
            bits.append(str(val))

    hay = " ".join(bits).lower()
    hay = re.sub(r"\s+", " ", hay).strip()
    title_hay = " ".join([title, title_english, title_japanese]).lower()

    hard_terms = [
        "hentai",
        "erotica",
        "ecchi",
        "softcore",
        "uncensored",
        "nsfw",
        "18+",
        "sexually explicit",
        "explicit sexual",
        "pornographic",
        "fuuzoku",
        "fuzoku",
        "breeder",
        "oppai",
        "paizuri",
        "milf",
        "big breasts",
        "large breasts",
        "immoral routine",
        "gal no tamariba",
        "bitch",
        "slut",
    ]
    if any(term in hay for term in hard_terms):
        return True

    genre_terms = [
        "hentai",
        "erotica",
        "ecchi",
        "adult cast",
        "love hotel",
        "sexual content",
        "nudity",
        "suggestive",
    ]
    if any(term in hay for term in genre_terms):
        return True

    combo_terms = ["the animation", "ova", "uncensored"]
    if any(flag in title_hay for flag in combo_terms):
        if any(term in hay for term in ["breeder", "fuuzoku", "immoral", "hentai", "ecchi", "nudity", "suggestive"]):
            return True

    return False


_DISCOVER_ANIME_TMDB_RESOLVE_CACHE: dict[tuple[str, str, str], dict] = {}
_DISCOVER_ANIME_TMDB_RESOLVE_CACHE_TTL_SEC = 43200  # 12 hours

_DISCOVER_ANIME_ENRICHED_ITEM_CACHE: dict[tuple[str, str, str], dict] = {}
_DISCOVER_ANIME_ENRICHED_ITEM_CACHE_TTL_SEC = 43200  # 12 hours

_DISCOVER_CACHE: dict[tuple[str, str, str, str, str, str, str, int, str], dict] = {}
def _discover_cache_ttl_sec() -> int:
    try:
        from app.routes_settings import _app_settings
        appcfg = _app_settings()
        minutes = int(str(appcfg.get("discover_cache_ttl_minutes", "30") or "30").strip())
    except Exception:
        minutes = 30

    if minutes < 1:
        minutes = 1
    if minutes > 240:
        minutes = 240

    return minutes * 60


_DISCOVER_SOURCE_CACHE: dict[tuple[str, str, int, str, str, str], dict] = {}
_DISCOVER_SOURCE_CACHE_TTL = 3600

_DISCOVER_BUILD_LOCK = threading.Lock()
_DISCOVER_BUILD_EVENTS: dict[tuple[str, str, str, str, str, str, int, str], threading.Event] = {}


def _discover_build_key(source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_from: str = "", year_to: str = "", hide_owned_requested: bool = False):
    return (
        str(source),
        str(media),
        str(genre),
        str(provider),
        str(year_from),
        str(year_to),
        "1" if hide_owned_requested else "0",
        int(page),
        _discover_settings_signature(),
    )


def _source_cache_clear():
    try:
        _DISCOVER_SOURCE_CACHE.clear()
    except Exception:
        pass


def _source_cache_get(source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_key: str = ""):
    key = (str(source), str(media), int(page), str(genre), str(provider), str(year_key))
    row = _DISCOVER_SOURCE_CACHE.get(key)
    if not row:
        return None

    ts = float(row.get("ts") or 0)
    if (time.time() - ts) > _DISCOVER_SOURCE_CACHE_TTL:
        _DISCOVER_SOURCE_CACHE.pop(key, None)
        return None

    return row.get("payload")


def _source_cache_set(source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_key: str = "", payload: dict | None = None):
    key = (str(source), str(media), int(page), str(genre), str(provider), str(year_key))
    _DISCOVER_SOURCE_CACHE[key] = {
        "ts": time.time(),
        "payload": payload or {},
    }


def _discover_settings_signature() -> str:
    parts = [
        _app_setting("discover_weight_tmdb", "1.0"),
        _app_setting("discover_weight_trakt", "1.0"),
        _app_setting("discover_weight_anilist", "0.92"),
        _app_setting("discover_bonus_2", "0.08"),
        _app_setting("discover_bonus_3", "0.18"),
        _app_setting("discover_bonus_4", "0.28"),
        _app_setting("discover_bonus_5", "0.34"),
        _app_setting("discover_bonus_6", "0.40"),
        _app_setting("discover_bonus_5", "0.34"),
        _app_setting("discover_bonus_6", "0.40"),
        _app_setting("discover_hot_threshold", "0.82"),
        _app_setting("discover_enrich_scale", "100"),
        _app_setting("tmdb_bearer_token", "")[:8],
        _app_setting("tmdb_api_key", "")[:8],
        _app_setting("trakt_client_id", "")[:8],
    ]
    return "|".join(parts)


def _cache_get(source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_from: str = "", year_to: str = "", hide_owned_requested: bool = False):
    key = (str(source), str(media), str(genre), str(provider), str(year_from), str(year_to), "1" if hide_owned_requested else "0", int(page), _discover_settings_signature())
    row = _DISCOVER_CACHE.get(key)
    if not row:
        return None

    ts = float(row.get("ts") or 0)
    if (time.time() - ts) > _discover_cache_ttl_sec():
        _DISCOVER_CACHE.pop(key, None)
        return None

    return row.get("payload")


def _cache_set(source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_from: str = "", year_to: str = "", hide_owned_requested: bool = False, payload: dict | None = None):
    key = (str(source), str(media), str(genre), str(provider), str(year_from), str(year_to), "1" if hide_owned_requested else "0", int(page), _discover_settings_signature())
    _DISCOVER_CACHE[key] = {
        "ts": time.time(),
        "payload": payload,
    }


_DISCOVER_NEXT_PAGE_WARM_INFLIGHT: set[tuple[str, str, int, str, str, str, str]] = set()


def _should_background_warm_next_page(source: str, media: str, page: int) -> bool:
    try:
        page = int(page or 1)
    except Exception:
        page = 1

    s = str(source or "").strip()
    if page >= 3:
        return False

    if s in ("anime_aggregate", "aggregate"):
        return True

    if s.startswith("letterboxd_"):
        return True

    return False


def _background_warm_discover_page(app, source: str, media: str, page: int, genre: str = "all", provider: str = "all", year_from: str = "", year_to: str = "", user_ctx: dict | None = None):
    import threading
    import urllib.parse

    try:
        page = int(page or 1)
    except Exception:
        page = 1

    warm_key = (str(source), str(media), page, str(genre), str(provider), str(year_from), str(year_to))
    if warm_key in _DISCOVER_NEXT_PAGE_WARM_INFLIGHT:
        return

    if _cache_get(source, media, page, genre, provider, year_from, year_to) is not None:
        return

    _DISCOVER_NEXT_PAGE_WARM_INFLIGHT.add(warm_key)

    def _run():
        try:
            with app.test_request_context(
                "/api/discover/items?" + urllib.parse.urlencode({
                    "source": source,
                    "media": media,
                    "genre": genre,
                    "provider": provider,
                    "year_from": year_from,
                    "year_to": year_to,
                    "page": page,
                })
            ):
                try:
                    if isinstance(user_ctx, dict):
                        session["logged_in"] = bool(user_ctx.get("logged_in"))
                        session["user_id"] = int(user_ctx.get("user_id") or 0)
                        session["username"] = str(user_ctx.get("username") or "")
                        session["is_admin"] = int(user_ctx.get("is_admin") or 0)
                except Exception:
                    pass

                try:
                    print(
                        f"[discover-nextpage-warm] start source={source} media={media} page={page} genre={genre} provider={provider} year_from={year_from} year_to={year_to}",
                        flush=True
                    )
                except Exception:
                    pass

                api_discover_items()

                try:
                    print(
                        f"[discover-nextpage-warm] done source={source} media={media} page={page}",
                        flush=True
                    )
                except Exception:
                    pass
        except Exception as e:
            try:
                print(
                    f"[discover-nextpage-warm] error source={source} media={media} page={page} error={str(e)[:220]}",
                    flush=True
                )
            except Exception:
                pass
        finally:
            _DISCOVER_NEXT_PAGE_WARM_INFLIGHT.discard(warm_key)

    threading.Thread(target=_run, daemon=True).start()


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


def _tmdb_auth_headers() -> dict:
    bearer = (_app_setting("tmdb_bearer_token") or "").strip()
    if bearer:
        return {
            "Authorization": f"Bearer {bearer}",
            "accept": "application/json",
        }
    return {"accept": "application/json"}


def _tmdb_auth_params() -> dict:
    api_key = (_app_setting("tmdb_api_key") or "").strip()
    return {"api_key": api_key} if api_key else {}


def _tmdb_is_configured() -> bool:
    return bool(
        (_app_setting("tmdb_bearer_token") or "").strip()
        or (_app_setting("tmdb_api_key") or "").strip()
    )


def _trakt_is_configured() -> bool:
    return bool((_app_setting("trakt_client_id") or "").strip())


def _app_float(key: str, default: float) -> float:
    try:
        return float((_app_setting(key, str(default)) or str(default)).strip())
    except Exception:
        return float(default)


def _timed(label: str, fn):
    started = time.time()
    result = fn()
    elapsed = round(time.time() - started, 3)
    print(f"[discover-step] {label} took {elapsed}s", flush=True)
    return result


def _anime_discover_enabled() -> bool:
    return _app_float("discover_weight_anilist", 0.92) > 0


def _aggregate_enrich_limit(page: int) -> int:
    try:
        p = int(page or 1)
    except Exception:
        p = 1

    # Stronger enrichment for deeper pages so poster coverage doesn't collapse
    if p == 1:
        return 40
    elif p <= 5:
        return 25
    elif p <= 10:
        return 18
    else:
        return 14

    # Lower cold-start enrichment cost so fresh installs feel responsive.
    # We still enrich page 1 the most, but much more conservatively.
    if page == 1:
        return 40
    if page == 2:
        return 20
    if page == 3:
        return 10
    return 5

    if page <= 1:
        base = 80
    elif page == 2:
        base = 24
    elif page == 3:
        base = 8
    else:
        base = 0

    try:
        scale = max(0.0, min(1.0, float(_app_setting("discover_enrich_scale", "100")) / 100.0))
    except Exception:
        scale = 1.0

    return max(0, int(round(base * scale)))


def _prioritize_rich_aggregate_items(items: list[dict]) -> list[dict]:
    def sort_key(x: dict):
        provider_hits = int(x.get("provider_hits") or 0)
        has_poster = 1 if str(x.get("poster_url") or "").strip() else 0
        has_rating = 1 if x.get("vote_average") is not None else 0
        has_overview = 1 if str(x.get("overview") or "").strip() else 0
        composite = float(x.get("composite_score") or 0.0)

        return (
            has_poster,
            has_rating,
            min(provider_hits, 3),
            has_overview,
            composite,
        )

    return sorted(items or [], key=sort_key, reverse=True)


def _get_tmdb_trending_cached(media: str, page: int):
    cached = _source_cache_get("tmdb_trending", media, page)
    if cached is not None:
        return cached

    payload = fetch_tmdb_trending(
        media=media,
        page=page,
        headers=_tmdb_auth_headers(),
        auth_params=_tmdb_auth_params(),
    )
    _source_cache_set("tmdb_trending", media, page, payload=payload)
    return payload


def _get_tmdb_popular_cached(media: str, page: int):
    cached = _source_cache_get("tmdb_popular", media, page)
    if cached is not None:
        return cached

    payload = fetch_tmdb_popular(
        media=media,
        page=page,
        headers=_tmdb_auth_headers(),
        auth_params=_tmdb_auth_params(),
    )
    _source_cache_set("tmdb_popular", media, page, payload=payload)
    return payload


def _get_trakt_trending_cached(media: str, page: int):
    cached = _source_cache_get("trakt_trending", media, page)
    if cached is not None:
        return cached

    payload = fetch_trakt_trending(
        media=media,
        client_id=_app_setting("trakt_client_id"),
        limit=40,
        page=page,
    )
    _source_cache_set("trakt_trending", media, page, payload=payload)
    return payload


def _get_trakt_popular_cached(media: str, page: int):
    cached = _source_cache_get("trakt_popular", media, page)
    if cached is not None:
        return cached

    payload = fetch_trakt_popular(
        media=media,
        client_id=_app_setting("trakt_client_id"),
        limit=40,
        page=page,
    )
    _source_cache_set("trakt_popular", media, page, payload=payload)
    return payload


def _fill_missing_tmdb_artwork(items: list[dict], budget: int = 20) -> list[dict]:
    if not _tmdb_is_configured():
        return items or []

    out = []
    used = 0

    for raw in (items or []):
        item = dict(raw or {})
        tmdb_id = int(item.get("tmdb_id") or 0)
        media_type = str(item.get("media_type") or "").strip().lower()
        has_poster = bool(str(item.get("poster_url") or "").strip())

        if tmdb_id and media_type in ("movie", "tv") and not has_poster and used < max(0, int(budget or 0)):
            try:
                enriched = enrich_tmdb_item_by_id(
                    tmdb_id=tmdb_id,
                    media_type=media_type,
                    headers=_tmdb_auth_headers(),
                    auth_params=_tmdb_auth_params(),
                )
            except Exception:
                enriched = None

            used += 1

            if enriched:
                if not item.get("poster_url") and enriched.get("poster_url"):
                    item["poster_url"] = _proxy_tmdb_url(enriched.get("poster_url"))
                if not item.get("backdrop_url") and enriched.get("backdrop_url"):
                    item["backdrop_url"] = enriched.get("backdrop_url")
                if not item.get("overview") and enriched.get("overview"):
                    item["overview"] = enriched.get("overview")
                if not item.get("title") and enriched.get("title"):
                    item["title"] = enriched.get("title")
                if not item.get("year") and enriched.get("year"):
                    item["year"] = enriched.get("year")
                if not item.get("vote_average") and enriched.get("vote_average") is not None:
                    item["vote_average"] = enriched.get("vote_average")

        out.append(item)

    return out


def _get_tmdb_genre_cached(media: str, page: int, genre: str, year_from: str = "", year_to: str = ""):
    year_key = f"{year_from}:{year_to}"
    cached = _source_cache_get("tmdb_genre", media, page, genre=genre, year_key=year_key)
    if cached is not None:
        return cached

    payload = fetch_tmdb_discover_by_genre(
        media=media,
        genre_id=genre,
        page=page,
        headers=_tmdb_auth_headers(),
        auth_params=_tmdb_auth_params(),
        year_from=year_from,
        year_to=year_to,
        pages_deep=3,
    )
    _source_cache_set("tmdb_genre", media, page, genre=genre, year_key=year_key, payload=payload)
    return payload


def _get_tmdb_provider_cached(media: str, page: int, provider: str, year_from: str = "", year_to: str = ""):
    year_key = f"{year_from}:{year_to}"
    cached = _source_cache_get("tmdb_provider", media, page, provider=provider, year_key=year_key)
    if cached is not None:
        return cached

    payload = fetch_tmdb_discover_by_provider(
        media=media,
        provider_id=provider,
        page=page,
        headers=_tmdb_auth_headers(),
        auth_params=_tmdb_auth_params(),
        year_from=year_from,
        year_to=year_to,
        pages_deep=3,
    )
    _source_cache_set("tmdb_provider", media, page, provider=provider, year_key=year_key, payload=payload)
    return payload


def _get_tvmaze_airing_cached(media: str, page: int):
    cached = _source_cache_get("tvmaze_airing", media, page)
    if cached is not None:
        return cached

    payload = fetch_tvmaze_airing(
        media=media,
        page=page,
        days_per_page=3,
    )
    _source_cache_set("tvmaze_airing", media, page, payload=payload)
    return payload


def _get_anilist_trending_cached(page: int):
    cached = _source_cache_get("anilist_trending", "tv", page)
    if cached is not None:
        return cached

    payload = fetch_anilist_trending(page=page, per_page=25)
    _source_cache_set("anilist_trending", "tv", page, payload=payload)
    return payload


def _get_jikan_anime_hot_cached(page: int):
    cached = _source_cache_get("jikan_anime_hot", "tv", page)
    if cached is not None:
        return cached

    payload = fetch_jikan_anime_hot(page=page, per_page=25)
    _source_cache_set("jikan_anime_hot", "tv", page, payload=payload)
    return payload


def _get_jikan_anime_rising_cached(page: int):
    cached = _source_cache_get("jikan_anime_rising", "tv", page)
    if cached is not None:
        return cached

    payload = fetch_jikan_anime_rising(page=page, per_page=25)
    _source_cache_set("jikan_anime_rising", "tv", page, payload=payload)
    return payload


def _get_anilist_popular_cached(page: int):
    cached = _source_cache_get("anilist_popular", "tv", page)
    if cached is not None:
        return cached

    payload = fetch_anilist_popular(page=page, per_page=25)
    _source_cache_set("anilist_popular", "tv", page, payload=payload)
    return payload


def _get_anilist_genre_cached(page: int, genre: str):
    cached = _source_cache_get("anilist_genre", "tv", page, genre=genre)
    if cached is not None:
        return cached

    payload = fetch_anilist_genre(genre=genre, page=page, per_page=25)
    _source_cache_set("anilist_genre", "tv", page, genre=genre, payload=payload)
    return payload


def _normalize_anilist_genre_name(raw: str) -> str:
    val = str(raw or "").strip()
    if not val or val.lower() == "all":
        return "Action"

    tmdb_to_anilist = {
        "28": "Action",
        "12": "Adventure",
        "16": "Animation",
        "35": "Comedy",
        "80": "Crime",
        "99": "Documentary",
        "18": "Drama",
        "10751": "Fantasy",
        "14": "Fantasy",
        "36": "Mystery",
        "27": "Horror",
        "10402": "Music",
        "9648": "Mystery",
        "10749": "Romance",
        "878": "Sci-Fi",
        "10770": "Slice of Life",
        "53": "Thriller",
        "10752": "Action",
        "37": "Western",
    }

    if val in tmdb_to_anilist:
        return tmdb_to_anilist[val]

    valid = {
        "action": "Action",
        "adventure": "Adventure",
        "animation": "Animation",
        "comedy": "Comedy",
        "crime": "Crime",
        "drama": "Drama",
        "ecchi": "Ecchi",
        "fantasy": "Fantasy",
        "hentai": "Hentai",
        "horror": "Horror",
        "mahou shoujo": "Mahou Shoujo",
        "mecha": "Mecha",
        "music": "Music",
        "mystery": "Mystery",
        "psychological": "Psychological",
        "romance": "Romance",
        "sci-fi": "Sci-Fi",
        "slice of life": "Slice of Life",
        "sports": "Sports",
        "supernatural": "Supernatural",
        "thriller": "Thriller",
    }

    return valid.get(val.lower(), "Action")


def _qd_norm_lookup_title(s: str) -> str:
    return "".join(ch.lower() for ch in str(s or "") if ch.isalnum())


def _qd_strip_anime_season_suffix(title: str) -> str:
    s = str(title or "").strip()
    if not s:
        return s

    import re

    out = s

    # Handle season/subtitle style names like:
    # "JUJUTSU KAISEN Season 3: The Culling Game Part 1"
    # "[OSHI NO KO] Season 3"
    # "Show Name - Season 2"
    out = re.sub(r'\s*[:\-–—]\s*season\s+\d+.*$', '', out, flags=re.I).strip(' :-')
    out = re.sub(r'\s+season\s+\d+\s*[:\-–—].*$', '', out, flags=re.I).strip(' :-')

    patterns = [
        r'\s+season\s+\d+\s*part\s*\d+$',
        r'\s+season\s+\d+$',
        r'\s+\d+(st|nd|rd|th)\s+season$',
        r'\s+part\s+\d+$',
        r'\s+cour\s+\d+$',
    ]

    changed = True
    while changed:
        changed = False
        for pat in patterns:
            newer = re.sub(pat, '', out, flags=re.I).strip(' :-')
            if newer != out:
                out = newer
                changed = True

    # If a title still contains "Season N" in the middle, keep only the parent show portion.
    newer = re.sub(r'^(.*?)\s+season\s+\d+.*$', r'\1', out, flags=re.I).strip(' :-')
    if newer:
        out = newer

    return out or s




def _qd_get_title_override(title: str) -> str:
    try:
        raw = str(_app_setting("discover_title_overrides", "") or "")
    except Exception as e:
        _qd_tmdb_debug("override-read-error", title=title, error=str(e))
        return ""

    want = _qd_norm_lookup_title(title)
    _qd_tmdb_debug("override-check", title=title, want=want, raw=raw)

    for line in raw.splitlines():
        if "|" not in line:
            continue

        left, right = [x.strip() for x in line.split("|", 1)]
        if not left or not right:
            continue

        left_norm = _qd_norm_lookup_title(left)
        if left_norm == want:
            _qd_tmdb_debug("override-hit", title=title, left=left, right=right)
            return right

    _qd_tmdb_debug("override-miss", title=title, want=want)
    return ""

def _tmdb_search_id_for_item(title: str, media_type: str, year: str = "") -> str:
    if not _tmdb_is_configured():
        return ""

    import re
    import time

    original_title = str(title or "").strip()
    title = original_title

    override = _qd_get_title_override(original_title)
    override_used = bool(override)
    if override:
        title = override

    media_type = str(media_type or "").strip().lower()
    year = str(year or "").strip()[:4]

    norm_title = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", original_title.lower())).strip()
    cache_key = (norm_title, media_type, year)

    cached = _DISCOVER_ANIME_TMDB_RESOLVE_CACHE.get(cache_key)
    if cached:
        age = time.time() - float(cached.get("ts") or 0)
        if age <= _DISCOVER_ANIME_TMDB_RESOLVE_CACHE_TTL_SEC:
            tmdb_id = str(cached.get("tmdb_id") or "").strip()
            if tmdb_id:
                print(f"[anime-resolve-cache] HIT title={original_title!r} year={year!r} id={tmdb_id!r}", flush=True)
            else:
                print(f"[anime-resolve-cache] NEGATIVE-HIT title={original_title!r} year={year!r}", flush=True)
            return tmdb_id

    # If an admin override is being used, trust the override title and do not
    # constrain the TMDb search by AniList's season/year value.
    if override_used:
        year = ""

    _qd_tmdb_debug(
        "override-state",
        original_title=original_title,
        effective_title=title,
        override=override,
        override_used=override_used,
    )

    if not title or media_type not in ("movie", "tv"):
        return ""

    def _dedupe_queries(values):
        out = []
        seen = set()
        for v in values:
            s = str(v or "").strip()
            if not s:
                continue
            k = s.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(s)
        return out

    def _strip_duplicate_year_suffix(s: str) -> str:
        out = str(s or "").strip()
        out = re.sub(r'\s*\((\d{4})\)\s*\(\1\)\s*$', r' (\1)', out).strip()
        out = re.sub(r'\s*\(\d{4}\)\s*$', '', out).strip()
        return out

    def _strip_final_season_terms(s: str) -> str:
        out = str(s or "").strip()
        out = re.sub(r'\s+final\s+season(\s+part\s+\d+)?$', '', out, flags=re.I).strip(' :-')
        out = re.sub(r'\s+season\s+\d+(\s+part\s+\d+)?$', '', out, flags=re.I).strip(' :-')
        out = re.sub(r'\s+part\s+\d+$', '', out, flags=re.I).strip(' :-')
        return out

    def _split_title_variants(s: str) -> list[str]:
        s = str(s or "").strip()
        if not s:
            return []

        variants = [s]

        stripped = _qd_strip_anime_season_suffix(s)
        variants.append(stripped)

        deduped = _strip_duplicate_year_suffix(s)
        variants.append(deduped)

        final_clean = _strip_final_season_terms(stripped or s)
        variants.append(final_clean)

        if ":" in s:
            left, right = s.split(":", 1)
            variants.append(left.strip())
            variants.append(right.strip())

        return _dedupe_queries(variants)

    def _search_kind(query_title: str, kind: str, year_val: str = "", allow_no_year: bool = False) -> str:
        query_param_sets = []

        params_with_year = dict(_tmdb_auth_params())
        params_with_year["query"] = query_title
        if year_val:
            if kind == "tv":
                params_with_year["first_air_date_year"] = year_val
            else:
                params_with_year["year"] = year_val
        query_param_sets.append(params_with_year)

        if allow_no_year or not year_val:
            params_no_year = dict(_tmdb_auth_params())
            params_no_year["query"] = query_title
            query_param_sets.append(params_no_year)

        want = _qd_norm_lookup_title(query_title)

        for params in query_param_sets:
            try:
                r = requests.get(
                    f"https://api.themoviedb.org/3/search/{kind}",
                    headers=_tmdb_auth_headers(),
                    params=params,
                    timeout=15,
                )
                r.raise_for_status()
                data = r.json() or {}
                results = data.get("results") or []
                _qd_tmdb_debug(
                    "search-results",
                    query=query_title,
                    kind=kind,
                    params=params,
                    result_count=len(results),
                    top_titles=[
                        (
                            str(row.get("name") or row.get("title") or row.get("original_name") or row.get("original_title") or "").strip(),
                            str(row.get("first_air_date") or row.get("release_date") or "")[:4],
                            str(row.get("id") or "").strip(),
                        )
                        for row in results[:5]
                    ],
                )
            except Exception as e:
                _qd_tmdb_debug(
                    "search-error",
                    query=query_title,
                    kind=kind,
                    params=params,
                    error=str(e),
                )
                continue

            for row in results[:8]:
                cand_title = (
                    str(row.get("name") or "").strip()
                    or str(row.get("title") or "").strip()
                    or str(row.get("original_name") or "").strip()
                    or str(row.get("original_title") or "").strip()
                )
                cand_norm = _qd_norm_lookup_title(cand_title)
                cand_date = str(row.get("first_air_date") or row.get("release_date") or "")[:4]
                cand_id = str(row.get("id") or "").strip()

                if not cand_id:
                    continue

                if want and cand_norm == want:
                    if "first_air_date_year" not in params and "year" not in params:
                        _qd_tmdb_debug("match-found", query=query_title, kind=kind, cand_title=cand_title, cand_date=cand_date, cand_id=cand_id, reason="exact-no-year")
                        _DISCOVER_ANIME_TMDB_RESOLVE_CACHE[cache_key] = {
                            "ts": time.time(),
                            "tmdb_id": str(cand_id or "").strip(),
                            "tmdb_title": str(cand_title or "").strip(),
                            "original_title": original_title,
                            "media_type": media_type,
                            "year": year,
                        }
                        return cand_id
                    if not year_val or not cand_date or cand_date == year_val:
                        _qd_tmdb_debug("match-found", query=query_title, kind=kind, cand_title=cand_title, cand_date=cand_date, cand_id=cand_id, reason="exact-with-year")
                        _DISCOVER_ANIME_TMDB_RESOLVE_CACHE[cache_key] = {
                            "ts": time.time(),
                            "tmdb_id": str(cand_id or "").strip(),
                            "tmdb_title": str(cand_title or "").strip(),
                            "original_title": original_title,
                            "media_type": media_type,
                            "year": year,
                        }
                        return cand_id

            _qd_tmdb_debug("no-match-for-query", query=query_title, kind=kind, year_val=year_val)
        return ""

    kind = "tv" if media_type == "tv" else "movie"
    candidate_queries = _split_title_variants(title)

    _qd_tmdb_debug(
        "resolve-start",
        original_title=original_title,
        effective_title=title,
        override_used=override_used,
        media_type=media_type,
        year=year,
        kind=kind,
        candidate_queries=candidate_queries,
    )

    for query_title in candidate_queries:
        found = _search_kind(
            query_title=query_title,
            kind=kind,
            year_val=year,
            allow_no_year=(media_type == "tv" and (query_title != title or override_used)),
        )
        if found:
            return found

    if media_type == "tv":
        for query_title in candidate_queries:
            found = _search_kind(
                query_title=query_title,
                kind="movie",
                year_val=year,
                allow_no_year=True,
            )
            if found:
                return found

    _qd_tmdb_debug("resolve-miss", original_title=original_title, effective_title=title, media_type=media_type, year=year)
    _DISCOVER_ANIME_TMDB_RESOLVE_CACHE[cache_key] = {
        "ts": time.time(),
        "tmdb_id": "",
        "original_title": original_title,
        "media_type": media_type,
        "year": year,
    }
    return ""

def _enrich_anilist_items_with_tmdb(items):
    import re

    out = []
    seen_request_keys = set()

    for raw in (items or []):
        item = dict(raw or {})

        req_title = str(item.get("title") or "").strip()
        req_media_type = str(item.get("media_type") or "").strip().lower() or "tv"
        req_year = str(item.get("year") or "").strip()[:4]
        req_norm_title = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", req_title.lower())).strip()
        request_key = (req_norm_title, req_media_type, req_year)

        if request_key in seen_request_keys:
            continue
        seen_request_keys.add(request_key)

        tmdb_id = str(item.get("tmdb_id") or "").strip()
        media_type = str(item.get("media_type") or "").strip().lower()

        if not tmdb_id:
            tmdb_id = _tmdb_search_id_for_item(
                title=str(item.get("title") or ""),
                media_type=media_type,
                year=str(item.get("year") or ""),
            )
            if tmdb_id:
                item["tmdb_id"] = tmdb_id

        enriched = None
        if tmdb_id and media_type in ("movie", "tv") and _tmdb_is_configured():
            try:
                enriched = enrich_tmdb_item_by_id(
                    tmdb_id=int(tmdb_id),
                    media_type=media_type,
                    headers=_tmdb_auth_headers(),
                    auth_params=_tmdb_auth_params(),
                )
            except Exception:
                enriched = None

        if enriched:
            merged = dict(enriched)

            # Keep original anime/provider signals alive
            merged_scores = dict(enriched.get("provider_scores") or {})
            merged_scores.update(item.get("provider_scores") or {})
            merged["provider_scores"] = merged_scores

            # Preserve source-hit strength at least as high as original
            merged["provider_hits"] = max(
                int(enriched.get("provider_hits") or 0),
                int(item.get("provider_hits") or 0),
            )

            # Keep original fields when TMDb enrichment doesn't provide them
            for k in (
                "source",
                "title",
                "year",
                "poster_url",
                "external_url",
                "media_type",
                "tmdb_id",
                "imdb_id",
                "tvdb_id",
                "vote_average",
                "vote_count",
                "popularity",
            ):
                if not merged.get(k) and item.get(k):
                    merged[k] = item.get(k)

            if not merged.get("external_url") and merged.get("tmdb_id"):
                merged["external_url"] = f"https://www.themoviedb.org/{'tv' if str(merged.get('media_type') or '') == 'tv' else 'movie'}/{merged.get('tmdb_id')}"

            out.append(merged)
        else:
            if tmdb_id and not item.get("external_url"):
                item["external_url"] = f"https://www.themoviedb.org/{'tv' if media_type == 'tv' else 'movie'}/{tmdb_id}"
            out.append(item)

    return out


@bp.get("/discover")
@login_required
def discover_page():
    return render_template("discover.html", me=(current_user() or {}))


@bp.get("/api/discover/for-you-profile")
@login_required
def api_discover_for_you_profile():
    try:
        profile = build_for_you_profile(limit=200)
        return jsonify(profile)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@bp.post("/api/discover/clear-cache")
@login_required
def api_discover_clear_cache():
    try:
        _DISCOVER_CACHE.clear()
        _DISCOVER_SOURCE_CACHE.clear()

        try:
            _DISCOVER_WARM_STATE["running"] = False
            _DISCOVER_WARM_STATE["last_started"] = 0.0
            _DISCOVER_WARM_STATE["last_finished"] = 0.0
            _DISCOVER_WARM_STATE["last_elapsed"] = 0.0
            _DISCOVER_WARM_STATE["phase"] = ""
            _DISCOVER_WARM_STATE["message"] = ""
            _DISCOVER_WARM_STATE["progress"] = 0
            _DISCOVER_WARM_STATE["total"] = 1
            _DISCOVER_WARM_STATE["percent"] = 0
        except Exception:
            pass

        print("[discover-cache] cleared", flush=True)
        return jsonify(ok=True, cleared=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@bp.post("/api/discover/library-state")
@login_required
def api_discover_library_state():
    try:
        payload = request.get_json(silent=True) or {}
        raw_items = payload.get("items") or []
        if not isinstance(raw_items, list):
            return jsonify(ok=False, error="invalid_items"), 400

        items = []
        for idx, raw in enumerate(raw_items[:500]):
            if not isinstance(raw, dict):
                continue
            items.append({
                "key": str(raw.get("key") or f"row-{idx}"),
                "title": str(raw.get("title") or "").strip(),
                "year": str(raw.get("year") or "").strip(),
                "media_type": str(raw.get("media_type") or "").strip().lower(),
                "tmdb_id": str(raw.get("tmdb_id") or "").strip(),
                "imdb_id": str(raw.get("imdb_id") or "").strip(),
                "tvdb_id": str(raw.get("tvdb_id") or "").strip(),
            })

        library_result = find_in_library_batch(items)

        try:
            sonarr_result = find_requested_series_batch(items)
        except Exception:
            sonarr_result = {}

        try:
            radarr_result = find_requested_movies_batch(items)
        except Exception:
            radarr_result = {}

        merged = {}
        for row in items:
            key = str(row.get("key") or "")
            lib = library_result.get(key) or {}
            son = sonarr_result.get(key) or {}
            rad = radarr_result.get(key) or {}

            in_library = bool(lib.get("in_library"))

            merged[key] = {
                "in_library": in_library,
                "item_id": str(lib.get("item_id") or ""),
                "in_sonarr": (False if in_library else bool(son.get("in_sonarr"))),
                "series_id": ("" if in_library else str(son.get("series_id") or "")),
                "in_radarr": (False if in_library else bool(rad.get("in_radarr"))),
                "movie_id": ("" if in_library else str(rad.get("movie_id") or "")),
            }

            print(
                f"[discover-library-state] key={key} "
                f"title={row.get('title')!r} year={row.get('year')!r} media={row.get('media_type')!r} "
                f"tmdb={row.get('tmdb_id')!r} imdb={row.get('imdb_id')!r} tvdb={row.get('tvdb_id')!r} "
                f"lib={bool(lib.get('in_library'))} lib_item={str(lib.get('item_id') or '')!r} "
                f"sonarr={bool(son.get('in_sonarr'))} sonarr_id={str(son.get('series_id') or '')!r} "
                f"radarr={bool(rad.get('in_radarr'))} radarr_id={str(rad.get('movie_id') or '')!r}",
                flush=True
            )

        return jsonify(ok=True, items=merged)
    except Exception as e:
        return jsonify(ok=False, error=str(e), items={}), 502


@bp.post("/api/discover/warm")
@login_required
def api_discover_warm():
    import time

    now = time.time()

    if bool(_DISCOVER_WARM_STATE.get("running")):
        elapsed_since_start = round(now - float(_DISCOVER_WARM_STATE.get("last_started") or now), 3)
        print(
            f"[discover-warm] skipped already_running elapsed_since_start={elapsed_since_start}s",
            flush=True
        )
        return jsonify(
            ok=True,
            skipped=True,
            reason="already_running",
            elapsed_since_start=elapsed_since_start,
            last_elapsed=round(float(_DISCOVER_WARM_STATE.get("last_elapsed") or 0.0), 3),
            warmed=[],
            errors=[],
        )

    last_finished = float(_DISCOVER_WARM_STATE.get("last_finished") or 0.0)
    cooldown_remaining = max(0.0, _DISCOVER_WARM_COOLDOWN_SEC - (now - last_finished))

    if last_finished > 0 and cooldown_remaining > 0:
        elapsed_since_finish = round(now - last_finished, 3)
        print(
            f"[discover-warm] skipped cooldown_remaining={round(cooldown_remaining, 3)}s elapsed_since_finish={elapsed_since_finish}s",
            flush=True
        )
        return jsonify(
            ok=True,
            skipped=True,
            reason="cooldown_active",
            cooldown_remaining=round(cooldown_remaining, 3),
            last_elapsed=round(float(_DISCOVER_WARM_STATE.get("last_elapsed") or 0.0), 3),
            warmed=[],
            errors=[],
        )

    started = time.time()
    _DISCOVER_WARM_STATE["last_started"] = started
    _DISCOVER_WARM_STATE["running"] = True
    _DISCOVER_WARM_STATE["phase"] = "start"
    _DISCOVER_WARM_STATE["message"] = "Preparing Discover data..."
    _DISCOVER_WARM_STATE["progress"] = 0
    _DISCOVER_WARM_STATE["total"] = 7
    _DISCOVER_WARM_STATE["percent"] = 0
    _DISCOVER_WARM_STATE["eta_bucket"] = _discover_eta_bucket(0, 7)
    _DISCOVER_WARM_STATE["unresolved_anime_titles"] = []

    warmed = []
    errors = []

    try:
        # -------------------------
        # Aggregate prewarm
        # -------------------------
        try:
            _DISCOVER_WARM_STATE["phase"] = "tmdb"
            _DISCOVER_WARM_STATE["message"] = "Preparing core sources (TMDb)..."
            _DISCOVER_WARM_STATE["progress"] = 1
            _DISCOVER_WARM_STATE["percent"] = 10
            _DISCOVER_WARM_STATE["eta_bucket"] = _discover_eta_bucket(1, 7)
            _get_tmdb_trending_cached(media="all", page=1)
            warmed.append("aggregate:tmdb_trending")
        except Exception as e:
            errors.append(f"aggregate:tmdb_trending:{str(e)[:180]}")

        aggregate_seed = []

        try:
            tmdb_items = _get_tmdb_trending_cached(media="all", page=1) or []
            aggregate_seed.extend(tmdb_items)
            warmed.append(f"aggregate:tmdb_trending:{len(tmdb_items)}")
        except Exception as e:
            errors.append(f"aggregate:tmdb_trending:{str(e)[:180]}")

        try:
            _DISCOVER_WARM_STATE["phase"] = "trakt"
            _DISCOVER_WARM_STATE["message"] = "Preparing core sources (Trakt)..."
            _DISCOVER_WARM_STATE["progress"] = 2
            _DISCOVER_WARM_STATE["percent"] = 22
            _DISCOVER_WARM_STATE["eta_bucket"] = _discover_eta_bucket(2, 7)
            if _trakt_is_configured():
                trakt_items = _get_trakt_trending_cached(media="all", page=1) or []
                aggregate_seed.extend(trakt_items)
                warmed.append(f"aggregate:trakt_trending:{len(trakt_items)}")
        except Exception as e:
            errors.append(f"aggregate:trakt_trending:{str(e)[:180]}")

        try:
            _DISCOVER_WARM_STATE["phase"] = "letterboxd"
            _DISCOVER_WARM_STATE["message"] = "Preparing core sources (Letterboxd aggregate)..."
            _DISCOVER_WARM_STATE["progress"] = 3
            _DISCOVER_WARM_STATE["percent"] = 34
            _DISCOVER_WARM_STATE["eta_bucket"] = _discover_eta_bucket(3, 7)
            lb_items = get_letterboxd_popular_aggregate(page=1) or []
            aggregate_seed.extend(lb_items)
            warmed.append(f"aggregate:letterboxd_aggregate:{len(lb_items)}")
        except Exception as e:
            errors.append(f"aggregate:letterboxd_aggregate:{str(e)[:180]}")

        # Prewarm TMDb enrich-by-id cache for aggregate page 1
        try:
            _DISCOVER_WARM_STATE["phase"] = "tmdb_enrich"
            _DISCOVER_WARM_STATE["message"] = "Enriching TMDb cache..."
            _DISCOVER_WARM_STATE["progress"] = 4
            _DISCOVER_WARM_STATE["percent"] = 52
            _DISCOVER_WARM_STATE["eta_bucket"] = _discover_eta_bucket(4, 7)
            seen = set()
            enrich_targets = []
            for item in aggregate_seed:
                tmdb_id = int(item.get("tmdb_id") or 0)
                media_type = str(item.get("media_type") or "").strip().lower()
                if not tmdb_id or media_type not in ("movie", "tv"):
                    continue
                key = (media_type, tmdb_id)
                if key in seen:
                    continue
                seen.add(key)
                enrich_targets.append(key)

            warmed_count = 0
            for media_type, tmdb_id in enrich_targets[:80]:
                try:
                    enriched = enrich_tmdb_item_by_id(
                        tmdb_id=tmdb_id,
                        media_type=media_type,
                        headers=_tmdb_auth_headers(),
                        auth_params=_tmdb_auth_params(),
                    )
                    if enriched:
                        warmed_count += 1
                except Exception:
                    pass

            warmed.append(f"aggregate:tmdb_enrich:{warmed_count}")
        except Exception as e:
            errors.append(f"aggregate:tmdb_enrich:{str(e)[:180]}")

        # -------------------------
        # Anime aggregate prewarm
        # -------------------------
        # Disabled intentionally:
        # Anime TMDb resolve is too expensive on cold starts and can monopolize
        # sync Gunicorn workers, making Discover feel hung on fresh installs.
        # Anime sources are loaded on-demand instead.
        _DISCOVER_WARM_STATE["phase"] = "anime"
        _DISCOVER_WARM_STATE["message"] = "Preparing anime discovery..."
        _DISCOVER_WARM_STATE["progress"] = 5
        _DISCOVER_WARM_STATE["percent"] = 70
        _DISCOVER_WARM_STATE["eta_bucket"] = _discover_eta_bucket(5, 7)
        warmed.append("anime:prewarm:disabled")

        try:
            _DISCOVER_WARM_STATE["phase"] = "anime_finalize"
            _DISCOVER_WARM_STATE["message"] = "Finalizing anime discovery..."
            _DISCOVER_WARM_STATE["progress"] = 6
            _DISCOVER_WARM_STATE["percent"] = 86
            _DISCOVER_WARM_STATE["eta_bucket"] = _discover_eta_bucket(6, 7)
            seen = set()
            enrich_targets = []
            for item in []:
                tmdb_id = int(item.get("tmdb_id") or 0)
                media_type = str(item.get("media_type") or "").strip().lower()
                if not tmdb_id or media_type not in ("movie", "tv"):
                    continue
                key = (media_type, tmdb_id)
                if key in seen:
                    continue
                seen.add(key)
                enrich_targets.append(key)

            warmed_count = 0
            for media_type, tmdb_id in enrich_targets[:80]:
                try:
                    enriched = enrich_tmdb_item_by_id(
                        tmdb_id=tmdb_id,
                        media_type=media_type,
                        headers=_tmdb_auth_headers(),
                        auth_params=_tmdb_auth_params(),
                    )
                    if enriched:
                        warmed_count += 1
                except Exception:
                    pass

            warmed.append(f"anime:tmdb_enrich:{warmed_count}")
        except Exception as e:
            errors.append(f"anime:tmdb_enrich:{str(e)[:180]}")

        elapsed = round(time.time() - started, 3)
        _DISCOVER_WARM_STATE["last_finished"] = time.time()
        _DISCOVER_WARM_STATE["last_elapsed"] = elapsed
        _DISCOVER_WARM_STATE["running"] = False
        _DISCOVER_WARM_STATE["phase"] = "done"
        _DISCOVER_WARM_STATE["message"] = "Discover ready."
        _DISCOVER_WARM_STATE["progress"] = 7
        _DISCOVER_WARM_STATE["total"] = 7
        _DISCOVER_WARM_STATE["percent"] = 100
        _DISCOVER_WARM_STATE["eta_bucket"] = "Finalizing"

        print(
            f"[discover-warm] elapsed={elapsed}s warmed={len(warmed)} errors={len(errors)} details={warmed}",
            flush=True
        )

        return jsonify(
            ok=True,
            skipped=False,
            warmed=warmed,
            errors=errors,
            elapsed=elapsed,
        )

    except Exception as e:
        _DISCOVER_WARM_STATE["running"] = False
        _DISCOVER_WARM_STATE["phase"] = "error"
        _DISCOVER_WARM_STATE["message"] = f"Discover warm failed: {str(e)[:180]}"
        _DISCOVER_WARM_STATE["percent"] = 100
        return jsonify(ok=False, error=str(e)[:300]), 500


@bp.post("/api/discover/generate-title-overrides")
@login_required
def api_discover_generate_title_overrides():
    import re
    import time

    if not _tmdb_is_configured():
        return jsonify(ok=False, error="tmdb_not_configured", suggestions=[]), 400

    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(s or "").lower())).strip()

    def _strip_season_terms(s: str) -> str:
        out = str(s or "").strip()
        out = re.sub(r"\s+final\s+season(\s+part\s+\d+)?$", "", out, flags=re.I).strip(" :-")
        out = re.sub(r"\s+season\s+\d+(\s+part\s+\d+)?$", "", out, flags=re.I).strip(" :-")
        out = re.sub(r"\s+\d+(st|nd|rd|th)\s+season$", "", out, flags=re.I).strip(" :-")
        out = re.sub(r"\s+part\s+\d+$", "", out, flags=re.I).strip(" :-")
        return out

    def _variants(title: str) -> list[str]:
        title = str(title or "").strip()
        vals = [title, _strip_season_terms(title)]
        if ":" in title:
            left, right = title.split(":", 1)
            vals.append(left.strip())
            vals.append(right.strip())

        out = []
        seen = set()
        for v in vals:
            v = str(v or "").strip()
            if not v:
                continue
            k = _norm(v)
            if k in seen:
                continue
            seen.add(k)
            out.append(v)
        return out

    def _search_best(title: str, media_type: str, year: str = "") -> str:
        kind_order = [media_type] if media_type in ("tv", "movie") else ["tv", "movie"]

        for query_title in _variants(title):
            for kind in kind_order:
                param_sets = []

                p1 = dict(_tmdb_auth_params())
                p1["query"] = query_title
                if year:
                    if kind == "tv":
                        p1["first_air_date_year"] = year
                    else:
                        p1["year"] = year
                param_sets.append(p1)

                p2 = dict(_tmdb_auth_params())
                p2["query"] = query_title
                param_sets.append(p2)

                for params in param_sets:
                    try:
                        r = requests.get(
                            f"https://api.themoviedb.org/3/search/{kind}",
                            headers=_tmdb_auth_headers(),
                            params=params,
                            timeout=15,
                        )
                        r.raise_for_status()
                        results = (r.json() or {}).get("results") or []
                    except Exception:
                        results = []

                    for row in results[:5]:
                        cand_title = (
                            str(row.get("name") or "").strip()
                            or str(row.get("title") or "").strip()
                            or str(row.get("original_name") or "").strip()
                            or str(row.get("original_title") or "").strip()
                        )
                        cand_id = str(row.get("id") or "").strip()
                        if cand_title and cand_id:
                            # store resolve result with title for suggestion generation
                            try:
                                _DISCOVER_ANIME_TMDB_RESOLVE_CACHE[cache_key] = {
                                    "tmdb_id": str(cand_id),
                                    "tmdb_title": str(cand_title),
                                    "original_title": original_title,
                                    "media_type": media_type,
                                    "year": year,
                                    "ts": time.time(),
                                }
                            except Exception:
                                pass
                            return cand_title
        return ""

    rows = []
    now = time.time()

    for key, payload in (_DISCOVER_ANIME_TMDB_RESOLVE_CACHE or {}).items():
        try:
            age = now - float(payload.get("ts") or 0)
        except Exception:
            age = 999999

        if age > _DISCOVER_ANIME_TMDB_RESOLVE_CACHE_TTL_SEC:
            continue

        original_title = str(payload.get("original_title") or "").strip()
        media_type = str(payload.get("media_type") or "tv").strip().lower()
        year = str(payload.get("year") or "").strip()[:4]
        cached_tmdb_id = str(payload.get("tmdb_id") or "").strip()
        cached_title = str(payload.get("resolved_title") or payload.get("tmdb_title") or "").strip()

        if not original_title:
            continue

        suggestion = ""

        if not cached_tmdb_id:
            suggestion = _search_best(original_title, media_type=media_type, year=year)
            if not suggestion:
                continue

            if _norm(original_title) == _norm(suggestion):
                continue
        else:
            suggestion = cached_title
            if not suggestion:
                continue

            orig_norm = _norm(original_title)
            sugg_norm = _norm(suggestion)
            orig_base = _norm(_strip_season_terms(original_title))
            sugg_base = _norm(_strip_season_terms(suggestion))

            if orig_norm == sugg_norm or orig_base == sugg_base:
                continue

        rows.append({
            "original_title": original_title,
            "override_title": suggestion,
            "media_type": media_type,
            "year": year,
        })

    deduped = []
    seen = set()
    for row in rows:
        k = (_norm(row["original_title"]), _norm(row["override_title"]))
        if k in seen:
            continue
        seen.add(k)
        deduped.append(row)

    deduped = deduped[:50]

    print(
        f"[discover-generate-title-overrides] cache_entries={len(_DISCOVER_ANIME_TMDB_RESOLVE_CACHE or {})} suggestions={len(deduped)}",
        flush=True
    )

    return jsonify(ok=True, suggestions=deduped)


@bp.get("/api/discover/admin-user-nsfw-anime/list")
@login_required
def api_discover_admin_user_nsfw_anime_list():
    me = current_user() or {}
    if not bool(me.get("is_admin")):
        return jsonify(ok=False, error="forbidden"), 403

    _ensure_user_discover_flags_table()
    db = get_db()

    rows = db.execute("""
        SELECT
            u.id,
            u.username,
            u.is_admin,
            COALESCE(f.hide_nsfw_anime, 0) AS hide_nsfw_anime
        FROM users u
        LEFT JOIN user_discover_flags f
          ON f.user_id = u.id
        ORDER BY lower(u.username) ASC
    """).fetchall()

    users = []
    for row in rows:
        users.append({
            "id": int(row["id"]),
            "username": str(row["username"] or ""),
            "is_admin": bool(row["is_admin"]),
            "hide_nsfw_anime": bool(int(row["hide_nsfw_anime"] or 0)),
        })

    return jsonify(ok=True, users=users)


@bp.get("/api/discover/admin-user-nsfw-anime")
@login_required
def api_discover_admin_user_nsfw_anime_get():
    me = current_user() or {}
    if not bool(me.get("is_admin")):
        return jsonify(ok=False, error="forbidden"), 403

    identifier = str(request.args.get("user") or "").strip()
    row = _lookup_user_row_for_admin(identifier)
    if not row:
        return jsonify(ok=False, error="user_not_found"), 404

    return jsonify(
        ok=True,
        user={
            "id": int(row["id"]),
            "username": str(row["username"] or ""),
            "email": "" ,
            "hide_nsfw_anime": _get_user_hide_nsfw_anime_flag(row["id"]),
        },
    )


@bp.post("/api/discover/admin-user-nsfw-anime")
@login_required
def api_discover_admin_user_nsfw_anime_set():
    me = current_user() or {}
    if not bool(me.get("is_admin")):
        return jsonify(ok=False, error="forbidden"), 403

    payload = request.get_json(silent=True) or {}
    identifier = str(payload.get("user") or "").strip()
    enabled = bool(payload.get("hide_nsfw_anime"))

    row = _lookup_user_row_for_admin(identifier)
    if not row:
        return jsonify(ok=False, error="user_not_found"), 404

    _set_user_hide_nsfw_anime_flag(row["id"], enabled)

    return jsonify(
        ok=True,
        user={
            "id": int(row["id"]),
            "username": str(row["username"] or ""),
            "email": "" ,
            "hide_nsfw_anime": enabled,
        },
    )



@bp.get("/api/discover/warm-status")
@login_required
def api_discover_warm_status():
    state = dict(_DISCOVER_WARM_STATE or {})
    running = bool(state.get("running"))

    last_started = float(state.get("last_started") or 0.0)
    last_finished = float(state.get("last_finished") or 0.0)
    last_elapsed = float(state.get("last_elapsed") or 0.0)

    phase = str(state.get("phase") or ("warming" if running else "idle"))
    message = str(state.get("message") or ("Preparing Discover..." if running else "Discover ready."))

    progress = int(state.get("progress") or 0)
    total = int(state.get("total") or 1)
    percent = int(state.get("percent") or 0)

    if running:
        if progress <= 0:
            progress = 1
        if total <= 1:
            total = 4
        if percent <= 0:
            percent = max(5, min(95, int((progress / max(total, 1)) * 100)))
    else:
        if total <= 0:
            total = 1
        if percent <= 0 and (last_finished > 0 or last_elapsed > 0):
            percent = 100

    return jsonify({
        "ok": True,
        "running": running,
        "phase": phase,
        "message": message,
        "progress": progress,
        "total": total,
        "percent": percent,
        "eta_bucket": str(state.get("eta_bucket") or ""),
        "unresolved_anime_count": len(list(state.get("unresolved_anime_titles") or [])),
        "last_started": last_started,
        "last_finished": last_finished,
        "last_elapsed": last_elapsed,
    })


@bp.post("/api/discover/warm-start")
@login_required
def api_discover_warm_start():
    started = start_discover_warm(force=False)
    return jsonify({
        "ok": True,
        "started": bool(started),
        "running": bool(_DISCOVER_WARM_STATE.get("running")),
        "state": dict(_DISCOVER_WARM_STATE),
    })


@bp.get("/api/discover/letterboxd-sources")
@login_required
def api_discover_letterboxd_sources():
    feeds = get_letterboxd_feed_sources() or []
    return jsonify(
        ok=True,
        items=[
            {
                "key": f"letterboxd_{str(feed.get('key') or '').strip()}",
                "label": str(feed.get("label") or "Feed").strip() or "Feed",
            }
            for feed in feeds
            if str(feed.get("key") or "").strip()
        ],
    )


@bp.get("/api/discover/items")
@login_required
def api_discover_items():
    hide_owned_requested = str(request.args.get("hide_owned_requested") or "0").strip() == "1"

    source = str(request.args.get("source") or "aggregate").strip().lower()
    media = str(request.args.get("media") or "all").strip().lower()
    genre = str(request.args.get("genre") or "all").strip().lower()
    provider = str(request.args.get("provider") or "all").strip().lower()
    min_rating_raw = str(request.args.get("min_rating") or "").strip()
    year_from = str(request.args.get("year_from") or "").strip()
    year_to = str(request.args.get("year_to") or "").strip()

    try:
        page = max(1, int(request.args.get("page") or 1))
    except Exception:
        page = 1

    allowed_sources = {"aggregate", "tmdb_trending", "tmdb_popular", "trakt_trending", "trakt_popular", "letterboxd", "letterboxd_aggregate", "tvmaze_airing", "anime_aggregate", "anilist_trending", "anilist_popular", "anilist_genre", "jikan_anime_hot", "jikan_anime_rising"}
    feed_sources = {f"letterboxd_{x.get('key')}" for x in (get_letterboxd_feed_sources() or [])}
    if source not in allowed_sources and source not in feed_sources:
        return jsonify(ok=False, error="unsupported_source"), 400

    if media not in ("all", "movie", "tv", "anime"):
        return jsonify(ok=False, error="unsupported_media"), 400

    min_rating = None
    if min_rating_raw:
        try:
            min_rating = float(min_rating_raw)
        except Exception:
            return jsonify(ok=False, error="invalid_min_rating"), 400

    if genre != "all":
        if source in ("anime_aggregate", "anilist_trending", "anilist_popular", "anilist_genre", "jikan_anime_hot", "jikan_anime_rising"):
            genre = str(genre).strip()
            if not genre:
                return jsonify(ok=False, error="unsupported_genre"), 400
        else:
            try:
                int(genre)
            except Exception:
                return jsonify(ok=False, error="unsupported_genre"), 400

    if source in ("anilist_trending", "anilist_popular"):
        genre = "all"

    if provider != "all":
        try:
            int(provider)
        except Exception:
            return jsonify(ok=False, error="unsupported_provider"), 400

    if year_from:
        try:
            int(year_from)
        except Exception:
            return jsonify(ok=False, error="unsupported_year_from"), 400

    if year_to:
        try:
            int(year_to)
        except Exception:
            return jsonify(ok=False, error="unsupported_year_to"), 400

    if source in ("aggregate", "tmdb_trending") and not _tmdb_is_configured():
        return jsonify(
            ok=False,
            configured=False,
            error="tmdb_not_configured",
            items=[],
        ), 400

    if source in ("trakt_trending", "trakt_popular") and not _trakt_is_configured():
        return jsonify(
            ok=False,
            configured=False,
            error="trakt_not_configured",
            items=[],
        ), 400

    cached = _cache_get(source, media, page, genre, provider, year_from, year_to)
    if cached is not None:
        return jsonify(cached)

    build_key = _discover_build_key(source, media, page, genre, provider, year_from, year_to)
    build_event = None
    build_owner = False

    with _DISCOVER_BUILD_LOCK:
        existing_event = _DISCOVER_BUILD_EVENTS.get(build_key)
        if existing_event is None:
            build_event = threading.Event()
            _DISCOVER_BUILD_EVENTS[build_key] = build_event
            build_owner = True
            try:
                print(
                    f"[discover-build-lock] owner source={source} media={media} page={page} genre={genre} provider={provider} year_from={year_from} year_to={year_to}",
                    flush=True
                )
            except Exception:
                pass
        else:
            build_event = existing_event
            try:
                print(
                    f"[discover-build-lock] wait source={source} media={media} page={page} genre={genre} provider={provider} year_from={year_from} year_to={year_to}",
                    flush=True
                )
            except Exception:
                pass

    if not build_owner:
        waited = build_event.wait(timeout=240)
        cached = _cache_get(source, media, page, genre, provider, year_from, year_to, hide_owned_requested=hide_owned_requested)
        if cached is not None:
            try:
                print(
                    f"[discover-build-lock] reused source={source} media={media} page={page} waited={waited}",
                    flush=True
                )
            except Exception:
                pass
            return jsonify(cached)

        try:
            print(
                f"[discover-build-lock] fallback-build source={source} media={media} page={page} waited={waited}",
                flush=True
            )
        except Exception:
            pass

        with _DISCOVER_BUILD_LOCK:
            existing_event = _DISCOVER_BUILD_EVENTS.get(build_key)
            if existing_event is None or existing_event.is_set():
                build_event = threading.Event()
                _DISCOVER_BUILD_EVENTS[build_key] = build_event
                build_owner = True

    try:
        req_started = time.time()
        items = []
        consume_tmdb_enrich_stats()
        print(f"[discover] source={source} media={media} genre={genre} provider={provider} year_from={year_from} year_to={year_to} page={page}", flush=True)

        if source in ("aggregate", "tmdb_trending"):
            enrich_budget = _aggregate_enrich_limit(page)
            enrich_used = 0

            if provider != "all":
                items = _timed(
                    f"tmdb_provider media={media} page={page} provider={provider}",
                    lambda: _get_tmdb_provider_cached(
                        media=media,
                        page=page,
                        provider=provider,
                        year_from=year_from,
                        year_to=year_to,
                    )
                )
                if genre != "all":
                    items = [
                        i for i in items
                        if str(genre) in [str(x) for x in (i.get("genre_ids") or [])]
                    ]
            elif genre != "all" or year_from or year_to:
                items = _timed(
                    f"tmdb_genre media={media} page={page} genre={genre}",
                    lambda: _get_tmdb_genre_cached(
                        media=media,
                        page=page,
                        genre=genre if genre != "all" else "",
                        year_from=year_from,
                        year_to=year_to,
                    )
                )
            else:
                items = _timed(
                    f"tmdb_trending media={media} page={page}",
                    lambda: _get_tmdb_trending_cached(
                        media=media,
                        page=page,
                    )
                )

        if source == "aggregate" and _trakt_is_configured():
            trakt_items = _timed(
                f"trakt_trending media={media} page={page}",
                lambda: _get_trakt_trending_cached(
                    media=media,
                    page=page,
                )
            )

            by_key = {
                (str(x.get("media_type") or ""), int(x.get("tmdb_id") or 0)): x
                for x in items
                if x.get("tmdb_id")
            }

            enrich_loop_started = time.time()
            for tr in trakt_items:
                key = (str(tr.get("media_type") or ""), int(tr.get("tmdb_id") or 0))
                if key in by_key:
                    existing = by_key[key]
                    merged_scores = dict(existing.get("provider_scores") or {})
                    merged_scores.update(tr.get("provider_scores") or {})
                    existing["provider_scores"] = merged_scores
                    existing["source"] = "TMDb + Trakt"

                    if tr.get("trakt_watchers") is not None:
                        existing["trakt_watchers"] = tr.get("trakt_watchers")
                else:
                    enriched = None
                    if enrich_used < enrich_budget:
                        enriched = enrich_tmdb_item_by_id(
                            tmdb_id=int(tr.get("tmdb_id") or 0),
                            media_type=str(tr.get("media_type") or ""),
                            headers=_tmdb_auth_headers(),
                            auth_params=_tmdb_auth_params(),
                        )
                        enrich_used += 1

                    if enriched:
                        merged = dict(enriched)
                        merged["source"] = "TMDb + Trakt"

                        merged_scores = dict(enriched.get("provider_scores") or {})
                        merged_scores.update(tr.get("provider_scores") or {})
                        merged["provider_scores"] = merged_scores

                        if tr.get("trakt_watchers") is not None:
                            merged["trakt_watchers"] = tr.get("trakt_watchers")
                        items.append(merged)
                    else:
                        items.append(tr)

            enrich_loop_elapsed = round(time.time() - enrich_loop_started, 3)
            print(f"[discover-enrich-loop] source={source} page={page} block=trakt_trending_merge elapsed={enrich_loop_elapsed}s", flush=True)

            letterboxd_items = []
            if media in ("all", "movie"):
                letterboxd_items = _timed(
                    f"letterboxd_aggregate media=movie page={page}",
                    lambda: get_letterboxd_popular_aggregate(page=page)
                )

                enrich_loop_started = time.time()
                for lb in letterboxd_items:
                    key = (str(lb.get("media_type") or ""), int(lb.get("tmdb_id") or 0))
                    if key in by_key:
                        existing = by_key[key]
                        merged_scores = dict(existing.get("provider_scores") or {})
                        merged_scores.update(lb.get("provider_scores") or {})
                        existing["provider_scores"] = merged_scores
                        existing["source"] = "TMDb + Trakt + Letterboxd"
                    else:
                        items.append(lb)
                        if key[1]:
                            by_key[key] = lb

                enrich_loop_elapsed = round(time.time() - enrich_loop_started, 3)
                print(f"[discover-enrich-loop] source={source} page={page} block=letterboxd_merge elapsed={enrich_loop_elapsed}s", flush=True)

            # Performance pass:
            # skip trakt_popular inside aggregate mode to reduce first-load latency.
            # Users can still access Trakt Popular from its dedicated source.
            trakt_pop_items = []

            if media == "tv" and int(page) == 1:
                # Performance pass: skip tvmaze_airing inside aggregate mode
                tvmaze_items = []

                def _mk_title_year_key(x):
                    title = str(x.get("title") or "").strip().lower()
                    year = str(x.get("year") or "").strip()[:4]
                    title_norm = "".join(ch if ch.isalnum() else " " for ch in title)
                    title_norm = " ".join(title_norm.split())
                    return f"{title_norm}::{year}"

                by_match = {
                    _mk_title_year_key(x): x
                    for x in items
                    if str(x.get("media_type") or "") == "tv"
                }

                for tvm in tvmaze_items:
                    mkey = str(tvm.get("_match_key") or "")
                    if mkey and mkey in by_match:
                        existing = by_match[mkey]
                        merged_scores = dict(existing.get("provider_scores") or {})
                        merged_scores.update(tvm.get("provider_scores") or {})
                        existing["provider_scores"] = merged_scores
                        existing["source"] = "TMDb + Trakt + TVMaze"
                    else:
                        items.append(tvm)

        elif source == "tmdb_popular":
            items = _timed(
                f"tmdb_popular media={media} page={page}",
                lambda: _get_tmdb_popular_cached(
                    media=media,
                    page=page,
                )
            )

        elif source == "trakt_trending":
            items = _timed(
                f"trakt_trending media={media} page={page}",
                lambda: _get_trakt_trending_cached(
                    media=media,
                    page=page,
                )
            )

            enriched_items = []
            for tr in items:
                enriched = enrich_tmdb_item_by_id(
                    tmdb_id=int(tr.get("tmdb_id") or 0),
                    media_type=str(tr.get("media_type") or ""),
                    headers=_tmdb_auth_headers(),
                    auth_params=_tmdb_auth_params(),
                ) if _tmdb_is_configured() else None

                if enriched:
                    merged = dict(enriched)
                    merged["source"] = "TMDb + Trakt"

                    merged_scores = dict(enriched.get("provider_scores") or {})
                    merged_scores.update(tr.get("provider_scores") or {})
                    merged["provider_scores"] = merged_scores

                    if tr.get("trakt_watchers") is not None:
                        merged["trakt_watchers"] = tr.get("trakt_watchers")
                    enriched_items.append(merged)
                else:
                    enriched_items.append(tr)

            items = enriched_items

        elif source == "trakt_popular":
            items = _timed(
                f"trakt_popular media={media} page={page}",
                lambda: _get_trakt_popular_cached(
                    media=media,
                    page=page,
                )
            )

            enriched_items = []
            for tr in items:
                enriched = enrich_tmdb_item_by_id(
                    tmdb_id=int(tr.get("tmdb_id") or 0),
                    media_type=str(tr.get("media_type") or ""),
                    headers=_tmdb_auth_headers(),
                    auth_params=_tmdb_auth_params(),
                ) if _tmdb_is_configured() else None

                if enriched:
                    merged = dict(enriched)
                    merged["source"] = "TMDb + Trakt"

                    merged_scores = dict(enriched.get("provider_scores") or {})
                    merged_scores.update(tr.get("provider_scores") or {})
                    merged["provider_scores"] = merged_scores
                    enriched_items.append(merged)
                else:
                    enriched_items.append(tr)

            items = enriched_items

        elif source == "letterboxd":
            if media not in ("all", "movie"):
                items = []
            else:
                items = _timed(
                    f"letterboxd media=movie page={page}",
                    lambda: get_letterboxd_popular(page=page)
                )

        elif source == "letterboxd_aggregate":
            if media not in ("all", "movie"):
                items = []
            else:
                items = _timed(
                    f"letterboxd_aggregate media=movie page={page}",
                    lambda: get_letterboxd_popular_aggregate(page=page)
                )

        elif source.startswith("letterboxd_feed_") or source.startswith("letterboxd_feed"):
            if media not in ("all", "movie"):
                items = []
            else:
                feed_key = source.replace("letterboxd_", "", 1)
                items = _timed(
                    f"{source} media=movie page={page}",
                    lambda: get_letterboxd_popular_feed(feed_key=feed_key, page=page)
                )

        elif source == "tvmaze_airing":
            items = _timed(
                f"tvmaze_airing media={media} page={page}",
                lambda: _get_tvmaze_airing_cached(
                    media=media,
                    page=page,
                )
            )

        elif source == "anime_aggregate":
            if not _anime_discover_enabled():
                print(f"[discover-anime] anime_aggregate using Jikan-only mode because discover_weight_anilist <= 0 (page={page})", flush=True)

                jikan_hot_items = _timed(
                    f"anime_aggregate jikan_anime_hot page={page}",
                    lambda: _get_jikan_anime_hot_cached(page=page)
                )

                jikan_rising_items = _timed(
                    f"anime_aggregate jikan_anime_rising page={page}",
                    lambda: _get_jikan_anime_rising_cached(page=page)
                )

                items = (jikan_hot_items or []) + (jikan_rising_items or [])
            else:
                anilist_items = _timed(
                    f"anime_aggregate anilist_trending page={page}",
                    lambda: _get_anilist_trending_cached(page=page)
                )
                anilist_items = _timed(
                    f"anime_aggregate anilist_trending tmdb_resolve page={page}",
                    lambda: _enrich_anilist_items_with_tmdb(anilist_items)
                )

                try:
                    jikan_hot_items = _timed(
                        f"anime_aggregate jikan_anime_hot page={page}",
                        lambda: _get_jikan_anime_hot_cached(page=page)
                    )
                    jikan_hot_items = _timed(
                        f"anime_aggregate jikan_anime_hot tmdb_resolve page={page}",
                        lambda: _enrich_anilist_items_with_tmdb(jikan_hot_items)
                    )
                except Exception as e:
                    print(f"[discover-anime] jikan_anime_hot failed page={page} error={str(e)[:220]}", flush=True)
                    jikan_hot_items = []

                try:
                    jikan_rising_items = _timed(
                        f"anime_aggregate jikan_anime_rising page={page}",
                        lambda: _get_jikan_anime_rising_cached(page=page)
                    )
                    jikan_rising_items = _timed(
                        f"anime_aggregate jikan_anime_rising tmdb_resolve page={page}",
                        lambda: _enrich_anilist_items_with_tmdb(jikan_rising_items)
                    )
                except Exception as e:
                    print(f"[discover-anime] jikan_anime_rising failed page={page} error={str(e)[:220]}", flush=True)
                    jikan_rising_items = []

                items = (anilist_items or []) + (jikan_hot_items or []) + (jikan_rising_items or [])

        elif source == "anilist_trending":
            if not _anime_discover_enabled():
                print(f"[discover-anime] anilist_trending skipped because discover_weight_anilist <= 0 (page={page})", flush=True)
                items = []
            else:
                items = _timed(
                    f"anilist_trending page={page}",
                    lambda: _get_anilist_trending_cached(page=page)
                )
                items = _timed(
                    f"anilist_trending tmdb_resolve page={page}",
                    lambda: _enrich_anilist_items_with_tmdb(items[:24]) + items[24:]
                )

        elif source == "jikan_anime_hot":
            items = _timed(
                f"jikan_anime_hot page={page}",
                lambda: _get_jikan_anime_hot_cached(page=page)
            )
            if _anime_discover_enabled():
                items = _timed(
                    f"jikan_anime_hot tmdb_resolve page={page}",
                    lambda: _enrich_anilist_items_with_tmdb(items[:24]) + items[24:]
                )
            else:
                print(f"[discover-anime] jikan_anime_hot using raw Jikan items because discover_weight_anilist <= 0 (page={page})", flush=True)

        elif source == "jikan_anime_rising":
            items = _timed(
                f"jikan_anime_rising page={page}",
                lambda: _get_jikan_anime_rising_cached(page=page)
            )
            if _anime_discover_enabled():
                items = _timed(
                    f"jikan_anime_rising tmdb_resolve page={page}",
                    lambda: _enrich_anilist_items_with_tmdb(items[:24]) + items[24:]
                )
            else:
                print(f"[discover-anime] jikan_anime_rising using raw Jikan items because discover_weight_anilist <= 0 (page={page})", flush=True)

        elif source == "anilist_popular":
            if not _anime_discover_enabled():
                print(f"[discover-anime] anilist_popular skipped because discover_weight_anilist <= 0 (page={page})", flush=True)
                items = []
            else:
                items = _timed(
                    f"anilist_popular page={page}",
                    lambda: _get_anilist_popular_cached(page=page)
                )
                items = _timed(
                    f"anilist_popular tmdb_resolve page={page}",
                    lambda: _enrich_anilist_items_with_tmdb(items)
                )

        elif source == "anilist_genre":
            if not _anime_discover_enabled():
                print(f"[discover-anime] anilist_genre skipped because discover_weight_anilist <= 0 (page={page})", flush=True)
                items = []
            else:
                anime_genre = _normalize_anilist_genre_name(genre)
                items = _timed(
                    f"anilist_genre genre={anime_genre} page={page}",
                    lambda: _get_anilist_genre_cached(page=page, genre=anime_genre)
                )
                items = _timed(
                    f"anilist_genre tmdb_resolve genre={anime_genre} page={page}",
                    lambda: _enrich_anilist_items_with_tmdb(items)
                )

        if source in ("anime_aggregate", "anilist_trending", "anilist_popular", "anilist_genre", "jikan_anime_hot", "jikan_anime_rising"):
            if _current_user_hide_nsfw_anime():
                before_items = list(items or [])
                filtered = []
                removed_titles = []

                for x in before_items:
                    if _is_nsfw_anime_item(x):
                        removed_titles.append(str(x.get("title") or "").strip())
                    else:
                        filtered.append(x)

                items = filtered

                print(f"[discover-nsfw] source={source} before={len(before_items)} after={len(items)} removed={len(removed_titles)}", flush=True)
                if removed_titles:
                    print(f"[discover-nsfw] removed_titles={removed_titles[:25]}", flush=True)

        enrich_stats = consume_tmdb_enrich_stats()
        try:
            print(
                f"[discover-enrich] source={source} page={page} budget={enrich_budget} used={enrich_used} "
                f"hits={enrich_stats.get('hits',0)} misses={enrich_stats.get('misses',0)} none_hits={enrich_stats.get('none_hits',0)}",
                flush=True
            )
        except Exception:
            print(
                f"[discover-enrich] source={source} page={page} "
                f"hits={enrich_stats.get('hits',0)} misses={enrich_stats.get('misses',0)} none_hits={enrich_stats.get('none_hits',0)}",
                flush=True
            )

        if source in ("aggregate", "trakt_trending", "trakt_popular"):
            artwork_budget = 0

            if source == "aggregate":
                # Favor poster consistency across deeper pages.
                # This is intentionally more generous so pages 8+ do not
                # collapse into lots of missing artwork.
                pnum = int(page or 1)
                if pnum == 1:
                    artwork_budget = 20
                elif pnum <= 5:
                    artwork_budget = 20
                elif pnum <= 10:
                    artwork_budget = 18
                else:
                    artwork_budget = 16
            else:
                artwork_budget = 20

            if artwork_budget > 0:
                items = _timed(
                    f"fill_missing_tmdb_artwork source={source} page={page}",
                    lambda: _fill_missing_tmdb_artwork(
                        items,
                        budget=artwork_budget,
                    )
                )
            else:
                print(
                    f"[discover-artwork] skipped fill_missing_tmdb_artwork source={source} page={page} budget=0",
                    flush=True
                )

        weights = {
            "tmdb_trending": _app_float("discover_weight_tmdb", 1.0),
            "tmdb_popular": _app_float("discover_weight_tmdb_popular", 0.95),
            "trakt_trending": _app_float("discover_weight_trakt", 1.0),
            "trakt_popular": _app_float("discover_weight_trakt_popular", 0.90),
            "letterboxd": _app_float("discover_weight_letterboxd", 0.58),
            "tvmaze_airing": _app_float("discover_weight_tvmaze", 0.80),
            "anilist_anime": _app_float("discover_weight_anilist", 0.92),
        }

        bonuses = {
            2: _app_float("discover_bonus_2", 0.08),
            3: _app_float("discover_bonus_3", 0.18),
            4: _app_float("discover_bonus_4", 0.28),
            5: _app_float("discover_bonus_5", 0.34),
            6: _app_float("discover_bonus_6", 0.40),
        }

        hot_threshold = _app_float("discover_hot_threshold", 0.82)

        items = _timed(
            f"normalize_and_score_items source={source} page={page}",
            lambda: normalize_and_score_items(
                items,
                weights=weights,
                bonuses=bonuses,
                hot_threshold=hot_threshold,
            )
        )

        try:
            from .routes_actions import _get_hidden_and_snoozed_rows, _current_user_id

            user_id = _current_user_id()
            hidden_rows, _snoozed_rows, _now = _get_hidden_and_snoozed_rows(user_id)

            hidden_discover = {
                str(r["item_id"] or "")
                for r in (hidden_rows or [])
                if str(r["kind"] or "") == "discover_item"
            }

            def _discover_hide_key(it):
                media_type = str(it.get("media_type") or "").strip().lower()
                tmdb_id = str(it.get("tmdb_id") or "").strip()
                title = str(it.get("title") or "").strip().lower()
                year = str(it.get("year") or "").strip()

                if tmdb_id:
                    return f"{media_type}:{tmdb_id}"

                key = "".join(ch for ch in title if ch.isalnum())
                return f"{media_type}:{key}::{year}"

            items = [it for it in (items or []) if _discover_hide_key(it) not in hidden_discover]

            
            # --------------------------------------------------
            # Hide owned / requested (server-side with top-up)
            # --------------------------------------------------
            if hide_owned_requested and items:
                try:
                    TARGET_COUNT = 20
                    MAX_EXTRA_PAGES = 3

                    collected = []
                    current_items = items
                    current_page = int(page or 1)
                    extra_fetches = 0

                    while True:
                        batch = []
                        for idx, it in enumerate(current_items):
                            batch.append({
                                "key": f"row-{idx}",
                                "title": str(it.get("title") or "").strip(),
                                "year": str(it.get("year") or "").strip(),
                                "media_type": str(it.get("media_type") or "").strip().lower(),
                                "tmdb_id": str(it.get("tmdb_id") or "").strip(),
                                "imdb_id": str(it.get("imdb_id") or "").strip(),
                                "tvdb_id": str(it.get("tvdb_id") or "").strip(),
                            })

                        lib_map = find_in_library_batch(batch) or {}
                        try:
                            son_map = find_requested_series_batch(batch) or {}
                        except Exception:
                            son_map = {}
                        try:
                            rad_map = find_requested_movies_batch(batch) or {}
                        except Exception:
                            rad_map = {}

                        for idx, it in enumerate(current_items):
                            key = f"row-{idx}"
                            lib = lib_map.get(key) or {}
                            son = son_map.get(key) or {}
                            rad = rad_map.get(key) or {}

                            owned = bool(lib.get("in_library"))
                            requested = bool(son.get("in_sonarr")) or bool(rad.get("in_radarr"))

                            if not (owned or requested):
                                collected.append(it)

                        print(f"[discover-hide-owned] pass page={current_page} collected={len(collected)}", flush=True)

                        if len(collected) >= TARGET_COUNT:
                            break

                        if extra_fetches >= MAX_EXTRA_PAGES:
                            break

                        # fetch next page
                        next_page = current_page + 1
                        more_payload = _cache_get(source, media, next_page, genre, provider, year_from, year_to)

                        if more_payload is None:
                            more_payload = _build_discover_payload(
                                source=source,
                                media=media,
                                page=next_page,
                                genre=genre,
                                provider=provider,
                                year_from=year_from,
                                year_to=year_to,
                            )

                        more_items = (more_payload or {}).get("items") or []
                        if not more_items:
                            break

                        current_items = more_items
                        current_page = next_page
                        extra_fetches += 1

                    print(f"[discover-hide-owned] final count={len(collected)}", flush=True)
                    items = collected

                except Exception as e:
                    print(f"[discover-hide-owned] error={e}", flush=True)


        except Exception:
            pass

        if min_rating is not None:
            items = [
                i for i in items
                if float(i.get("vote_average") or 0) >= float(min_rating)
            ]

        # Poster fallback pass:
        # lower-ranked anime / partially enriched items may still have usable
        # artwork from upstream providers even when TMDb poster_url is missing.
        fallback_poster_hits = 0
        for i in (items or []):
            poster_url = str(i.get("poster_url") or "").strip()
            if poster_url:
                continue

            fallback = (
                str(i.get("image") or "").strip()
                or str(i.get("cover_image") or "").strip()
                or str(i.get("cover") or "").strip()
                or str(i.get("thumbnail") or "").strip()
                or str(i.get("poster") or "").strip()
                or str(i.get("image_url") or "").strip()
                or str(i.get("coverImage") or "").strip()
            )

            if fallback:
                i["poster_url"] = _proxy_tmdb_url(fallback)
                fallback_poster_hits += 1

        if fallback_poster_hits:
            print(
                f"[discover-poster-fallback] source={source} page={page} filled={fallback_poster_hits}",
                flush=True
            )

        # Prefer poster-bearing items on deeper aggregate pages.
        # If deep pages are filled with valid TMDb items that simply have no poster,
        # top up from later pages so the UI does not turn into blank cards.
        if source == "aggregate":
            try:
                pnum = int(page or 1)
            except Exception:
                pnum = 1

            if pnum >= 8 and items:
                target_count = len(items)
                max_extra_pages = 3

                def _has_poster(it):
                    return bool(str((it or {}).get("poster_url") or "").strip())

                poster_items = [it for it in items if _has_poster(it)]
                seen_keys = set()
                deduped = []

                def _poster_key(it):
                    media_type = str(it.get("media_type") or "").strip().lower()
                    tmdb_id = str(it.get("tmdb_id") or "").strip()
                    title = str(it.get("title") or "").strip().lower()
                    year = str(it.get("year") or "").strip()
                    if tmdb_id:
                        return f"{media_type}:{tmdb_id}"
                    return f"{media_type}:{title}::{year}"

                for it in poster_items:
                    k = _poster_key(it)
                    if k in seen_keys:
                        continue
                    seen_keys.add(k)
                    deduped.append(it)

                collected = list(deduped)
                extra_used = 0
                next_page = pnum + 1

                while len(collected) < target_count and extra_used < max_extra_pages:
                    more_payload = _cache_get(
                        source, media, next_page, genre, provider, year_from, year_to,
                        hide_owned_requested=hide_owned_requested
                    )

                    # If not cached, trigger normal build path by calling cache again
                    if more_payload is None:
                        try:
                            # trigger build via normal pipeline
                            _ = _cache_get(source, media, next_page, genre, provider, year_from, year_to)
                            more_payload = _cache_get(
                                source, media, next_page, genre, provider, year_from, year_to,
                                hide_owned_requested=hide_owned_requested
                            )
                        except Exception as e:
                            print(f"[discover-deep-posters] build failed page={next_page} err={e}", flush=True)
                            break

                    if more_payload is None:
                        break

                    more_items = more_payload.get("items") or []
                    if not more_items:
                        break

                    for it in more_items:
                        if not _has_poster(it):
                            continue
                        k = _poster_key(it)
                        if k in seen_keys:
                            continue
                        seen_keys.add(k)
                        collected.append(it)
                        if len(collected) >= target_count:
                            break

                    extra_used += 1
                    next_page += 1

                if collected:
                    print(
                        f"[discover-deep-posters] source={source} page={pnum} "
                        f"before={len(items)} poster_items={len(deduped)} final={len(collected)} extra_pages={extra_used}",
                        flush=True
                    )
                    items = collected[:target_count]

        if source == "aggregate":
            items = _prioritize_rich_aggregate_items(items)

        # 🔥 EXCLUDE OWNED / REQUESTED FOR FOR_YOU
        try:
            sort_mode = str(request.args.get("sort") or "").strip().lower()

            if source in ("aggregate", "anime_aggregate", "anilist_trending", "anilist_popular", "jikan_anime_hot", "jikan_anime_rising") and sort_mode == "for_you" and items:
                batch = []
                for idx, it in enumerate(items):
                    batch.append({
                        "key": f"row-{idx}",
                        "title": str(it.get("title") or "").strip(),
                        "year": str(it.get("year") or "").strip(),
                        "media_type": str(it.get("media_type") or "").strip().lower(),
                        "tmdb_id": str(it.get("tmdb_id") or "").strip(),
                        "imdb_id": str(it.get("imdb_id") or "").strip(),
                        "tvdb_id": str(it.get("tvdb_id") or "").strip(),
                    })

                library_result = find_in_library_batch(batch)

                try:
                    sonarr_result = find_requested_series_batch(batch)
                except Exception:
                    sonarr_result = {}

                try:
                    radarr_result = find_requested_movies_batch(batch)
                except Exception:
                    radarr_result = {}

                filtered = []
                for row in batch:
                    key = row["key"]

                    lib = library_result.get(key) or {}
                    son = sonarr_result.get(key) or {}
                    rad = radarr_result.get(key) or {}

                    owned = bool(lib.get("in_library"))
                    requested = bool(son.get("in_sonarr")) or bool(rad.get("in_radarr"))

                    if not (owned or requested):
                        idx = int(key.split("-")[1])
                        if idx < len(items):
                            filtered.append(items[idx])

                if filtered:
                    print(f"[discover-filter] for_you filtered {len(items)} -> {len(filtered)}", flush=True)
                    items = filtered

        except Exception as e:
            print(f"[discover-filter] failed: {e}", flush=True)

        payload = {
            "ok": True,
            "configured": True,
            "source": source,
            "media": media,
            "genre": genre,
            "provider": provider,
            "year_from": year_from,
            "year_to": year_to,
            "page": page,
            "items": items,
        }
        elapsed = round(time.time() - req_started, 3)
        print(f"[discover-timing] source={source} page={page} items={len(items)} elapsed={elapsed}s", flush=True)
        _cache_set(source, media, page, genre, provider, year_from, year_to, hide_owned_requested=hide_owned_requested, payload=payload)

        try:
            current_page = int(page or 1)
        except Exception:
            current_page = 1

        if _should_background_warm_next_page(source, media, current_page):
            next_page = current_page + 1
            if _cache_get(source, media, next_page, genre, provider, year_from, year_to, hide_owned_requested=False) is None:
                _background_warm_discover_page(
                    current_app._get_current_object(),
                    source=source,
                    media=media,
                    page=next_page,
                    genre=genre,
                    provider=provider,
                    year_from=year_from,
                    year_to=year_to,
                    user_ctx={
                        "logged_in": bool(session.get("logged_in")),
                        "user_id": int(session.get("user_id") or 0),
                        "username": str(session.get("username") or ""),
                        "is_admin": int(session.get("is_admin") or 0),
                    },
                )

        return jsonify(payload)
    except Exception as e:
        try:
            elapsed = round(time.time() - req_started, 3)
            print(f"[discover-timing] source={source} page={page} failed_after={elapsed}s error={e}", flush=True)
        except Exception:
            pass
        return jsonify(
            ok=False,
            configured=True,
            error=str(e),
            items=[],
        ), 502
    finally:
        try:
            if build_owner and build_event is not None:
                build_event.set()
                with _DISCOVER_BUILD_LOCK:
                    cur = _DISCOVER_BUILD_EVENTS.get(build_key)
                    if cur is build_event:
                        _DISCOVER_BUILD_EVENTS.pop(build_key, None)
                try:
                    print(
                        f"[discover-build-lock] release source={source} media={media} page={page}",
                        flush=True
                    )
                except Exception:
                    pass
        except Exception:
            pass
