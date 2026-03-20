import os
from flask import Blueprint, Response
import requests

from .models_settings import get_setting, get_user_admin_settings
from .utils.auth import login_required_401, current_user

bp = Blueprint("images", __name__)


def _cfg(key_db: str, key_env: str, default: str = "") -> str:
    """
    Prefer DB settings, fallback to environment variables.
    Called inside request context, so get_setting() is safe here.
    """
    try:
        v = (get_setting(key_db) or "").strip()
    except Exception:
        v = ""
    if v:
        return v
    return (os.getenv(key_env, default) or "").strip()


def _user_cfg(key_name: str) -> str:
    """
    Prefer logged-in user's admin-managed connection settings.
    Fallback to empty string if unavailable.
    """
    try:
        me = current_user() or {}
        user_id = int(me.get("user_id") or 0)
        if not user_id:
            return ""
        settings = get_user_admin_settings(user_id) or {}
        return str(settings.get(key_name) or "").strip()
    except Exception:
        return ""


def _cfg_user_first(user_key: str, key_db: str, key_env: str, default: str = "") -> str:
    v = _user_cfg(user_key)
    if v:
        return v
    return _cfg(key_db, key_env, default)


def _proxy_image(url: str, headers: dict | None = None) -> Response:
    r = requests.get(url, headers=headers or {}, timeout=25)
    r.raise_for_status()
    ct = r.headers.get("Content-Type", "image/jpeg")
    resp = Response(r.content, status=200, content_type=ct)
    # cache a bit; Jellyfin/Sonarr/Radarr handle freshness via tags/ids
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


# ---------------------------
# TMDB images
# ---------------------------

@bp.get("/img/tmdb/<path:img_path>")
@login_required_401
def tmdb_image(img_path: str):
    clean = (img_path or "").lstrip("/")
    if not clean:
        return Response("Missing TMDB image path", status=400)

    # Keep this fixed to posters/backdrops hosted by TMDB CDN.
    url = f"https://image.tmdb.org/t/p/w500/{clean}"
    return _proxy_image(url)


# ---------------------------
# Jellyfin images
# ---------------------------

@bp.get("/img/jellyfin/primary/<item_id>")
@login_required_401
def jellyfin_primary(item_id: str):
    base = _cfg_user_first("jellyfin_url", "jellyfin_url", "JELLYFIN_URL", "").rstrip("/")
    api_key = _cfg_user_first("jellyfin_api_key", "jellyfin_api_key", "JELLYFIN_API_KEY", "")
    if not base:
        return Response("Missing Jellyfin URL", status=500)
    if not api_key:
        return Response("Missing Jellyfin API key", status=500)

    # Jellyfin supports Items/<id>/Images/Primary
    url = f"{base}/Items/{item_id}/Images/Primary?quality=90"
    return _proxy_image(url, headers={"X-Emby-Token": api_key})


@bp.get("/img/jellyfin/series/<series_id>")
@login_required_401
def jellyfin_series(series_id: str):
    base = _cfg_user_first("jellyfin_url", "jellyfin_url", "JELLYFIN_URL", "").rstrip("/")
    api_key = _cfg_user_first("jellyfin_api_key", "jellyfin_api_key", "JELLYFIN_API_KEY", "")
    if not base:
        return Response("Missing Jellyfin URL", status=500)
    if not api_key:
        return Response("Missing Jellyfin API key", status=500)

    url = f"{base}/Items/{series_id}/Images/Primary?quality=90"
    return _proxy_image(url, headers={"X-Emby-Token": api_key})


# ---------------------------
# Sonarr poster by TVDB id
# ---------------------------

@bp.get("/img/sonarr/series/<int:tvdb_id>.jpg")
@login_required_401
def sonarr_series_poster(tvdb_id: int):
    base = _cfg_user_first("sonarr_url", "sonarr_url", "SONARR_URL", "").rstrip("/")
    api_key = _cfg_user_first("sonarr_api_key", "sonarr_api_key", "SONARR_API_KEY", "")
    if not base:
        return Response("Missing Sonarr URL", status=500)
    if not api_key:
        return Response("Missing Sonarr API key", status=500)

    # Find Sonarr series ID by tvdbId
    s = requests.get(
        f"{base}/api/v3/series",
        headers={"X-Api-Key": api_key},
        timeout=25,
    )
    s.raise_for_status()
    series = next((x for x in (s.json() or []) if int(x.get("tvdbId") or 0) == tvdb_id), None)
    if not series:
        return Response("Series not found in Sonarr", status=404)

    series_id = series.get("id")
    # Standard Sonarr cover endpoint
    url = f"{base}/api/v3/MediaCover/{series_id}/poster.jpg"
    return _proxy_image(url, headers={"X-Api-Key": api_key})


# ---------------------------
# Radarr poster by TMDB id
# ---------------------------

@bp.get("/img/radarr/tmdb/<int:tmdb_id>.jpg")
@login_required_401
def radarr_movie_poster(tmdb_id: int):
    base = _cfg_user_first("radarr_url", "radarr_url", "RADARR_URL", "").rstrip("/")
    api_key = _cfg_user_first("radarr_api_key", "radarr_api_key", "RADARR_API_KEY", "")
    if not base:
        return Response("Missing Radarr URL", status=500)
    if not api_key:
        return Response("Missing Radarr API key", status=500)

    # Find Radarr movie ID by tmdbId
    m = requests.get(
        f"{base}/api/v3/movie",
        headers={"X-Api-Key": api_key},
        timeout=25,
    )
    m.raise_for_status()
    movie = next((x for x in (m.json() or []) if int(x.get("tmdbId") or 0) == tmdb_id), None)
    if not movie:
        return Response("Movie not found in Radarr", status=404)

    movie_id = movie.get("id")
    url = f"{base}/api/v3/MediaCover/{movie_id}/poster.jpg"
    return _proxy_image(url, headers={"X-Api-Key": api_key})
