import os
from flask import Blueprint, jsonify, request

from .utils.auth import login_required, current_user
from .db import get_db
from .models_settings import get_user_setting_scoped

from .clients.jellyfin import get_recent_unwatched_movies
from .clients.radarr import get_upcoming_missing

bp = Blueprint("radarr", __name__)


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


@bp.get("/api/radarr/recent-unwatched")
@login_required
def api_radarr_recent_unwatched():
    """
    "Radarr — Recently Downloaded (Unwatched Only)"

    We intentionally use Jellyfin here because:
    - Jellyfin knows per-user watched/unwatched state
    - We can sort by DateCreated (newest added)
    - We already have a poster proxy endpoint pattern

    NOTE: clients.jellyfin.get_recent_unwatched_movies() returns a LIST of cleaned dicts.
    """
    try:
        limit = int(request.args.get("limit", "10"))
        limit = max(1, min(limit, 50))

        if not _user_setting("jellyfin_url") or not _user_setting("jellyfin_api_key") or not _user_setting("jellyfin_user"):
            return jsonify(
                count=0,
                debug={"source": "jellyfin_recent_unplayed", "configured": False},
                items=[],
            )

        items = get_recent_unwatched_movies(limit=limit)  # <-- already cleaned list

        # Ensure shape is always list
        if not isinstance(items, list):
            return jsonify(error="Unexpected data type from Jellyfin client"), 500

        return jsonify(
            count=len(items),
            debug={"source": "jellyfin_recent_unplayed"},
            items=items,
        )

    except Exception as e:
        return jsonify(error=str(e)), 500


@bp.get("/api/radarr/upcoming-missing")
@login_required
def api_radarr_upcoming_missing():
    """
    Movies coming soon that are NOT downloaded yet (hasFile=false).
    """
    try:
        days = int(request.args.get("days", "90"))
        days = max(1, min(days, 365))

        limit = int(request.args.get("limit", "30"))
        limit = max(1, min(limit, 200))

        if not _user_setting("radarr_url") or not _user_setting("radarr_api_key"):
            return jsonify(count=0, items=[])

        items = get_upcoming_missing(days=days, limit=limit)

        # add poster url via your existing image proxy pattern (tmdb-based)
        cleaned = []
        for it in items or []:
            tmdb_id = it.get("tmdb_id") or it.get("tmdbId") or it.get("tmdbID")
            cleaned.append(
                {
                    **it,
                    "poster_url": f"/img/radarr/tmdb/{tmdb_id}.jpg" if tmdb_id else None,
                }
            )

        return jsonify(count=len(cleaned), items=cleaned)

    except Exception as e:
        return jsonify(error=str(e)), 500
