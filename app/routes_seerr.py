import requests
from urllib.parse import quote_plus
from flask import Blueprint, jsonify, request

from .db import get_db
from .models_settings import get_user_setting_scoped
from .utils.auth import login_required, current_user

bp = Blueprint("seerr", __name__)


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


def _seerr_debug(label: str, **vals):
    try:
        bits = " ".join(f"{k}={vals[k]!r}" for k in vals)
        print(f"[seerr-debug] {label} {bits}", flush=True)
    except Exception:
        pass


def _seerr_base() -> str:
    return _user_setting("seerr_url").rstrip("/")


def _seerr_api_key() -> str:
    return _user_setting("seerr_api_key").strip()


def _seerr_headers() -> dict:
    api_key = _seerr_api_key()
    return {"X-Api-Key": api_key} if api_key else {}


def _seerr_configured() -> bool:
    return bool(_seerr_base() and _seerr_api_key())


def _seerr_tv_details(tv_id: int) -> dict:
    r = requests.get(
        f"{_seerr_base()}/api/v1/tv/{tv_id}",
        headers=_seerr_headers(),
        timeout=15,
    )
    r.raise_for_status()
    return r.json() or {}


def _parse_seerr_tv_destinations(raw: str) -> list[dict]:
    text = str(raw or "").strip()
    if not text:
        return []

    out = []
    seen = set()

    for line in text.splitlines():
        row = str(line or "").strip()
        if not row:
            continue

        parts = [p.strip() for p in row.split("|")]
        if len(parts) < 4:
            continue

        label = parts[0]
        root_folder = parts[1]
        profile_id = parts[2]
        server_id = parts[3]

        if not label or not root_folder:
            continue

        key = label.strip().lower()
        if key in seen:
            continue
        seen.add(key)

        out.append({
            "key": key,
            "label": label,
            "rootFolder": root_folder,
            "profileId": profile_id or "4",
            "serverId": server_id or "0",
        })

    return out



def _normalize_search_result(item: dict) -> dict:
    media_type = str(item.get("mediaType") or "").strip().lower()

    title = (
        item.get("title")
        or item.get("name")
        or item.get("originalTitle")
        or item.get("originalName")
        or ""
    )

    date_val = item.get("releaseDate") or item.get("firstAirDate") or ""
    year = ""
    if isinstance(date_val, str) and len(date_val) >= 4:
        year = date_val[:4]

    media_info = item.get("mediaInfo") or {}
    request_info = item.get("request") or {}

    return {
        "media_type": media_type,
        "tmdb_id": item.get("id"),
        "tvdb_id": item.get("tvdbId"),
        "title": title,
        "year": year,
        "overview": item.get("overview") or "",
        "poster_path": item.get("posterPath") or "",
        "backdrop_path": item.get("backdropPath") or "",
        "poster_url": f"https://image.tmdb.org/t/p/w342{item.get('posterPath')}" if item.get("posterPath") else "",
        "backdrop_url": f"https://image.tmdb.org/t/p/w780{item.get('backdropPath')}" if item.get("backdropPath") else "",
        "status": media_info.get("status"),
        "status4k": media_info.get("status4k"),
        "media_info": media_info,
        "request": request_info,
    }


@bp.get("/api/seerr/config")
@login_required
def api_seerr_config():
    return jsonify(
        ok=True,
        configured=_seerr_configured(),
    )


@bp.get("/api/seerr/tv-destinations")
@login_required
def api_seerr_tv_destinations():
    if not _seerr_configured():
        return jsonify(ok=True, configured=False, destinations=[])

    me = current_user() or {}
    raw_destinations = _user_setting("seerr_tv_destinations")
    parsed = _parse_seerr_tv_destinations(raw_destinations)

    _seerr_debug(
        "tv-destinations user",
        queue_user_id=me.get("user_id"),
        queue_username=me.get("username"),
        seerr_user_id=_user_setting("seerr_user_id"),
        raw_destinations=raw_destinations,
        parsed_count=len(parsed),
    )

    if parsed:
        return jsonify(
            ok=True,
            configured=True,
            destinations=parsed,
        )

    # Fallback for legacy installs / missing config
    return jsonify(
        ok=True,
        configured=True,
        destinations=[
            {
                "key": "tv",
                "label": "Temporary TV",
                "rootFolder": "/tv",
                "profileId": "4",
                "serverId": "0",
            },
            {
                "key": "television",
                "label": "Television",
                "rootFolder": "/television",
                "profileId": "4",
                "serverId": "0",
            },
            {
                "key": "anime",
                "label": "Anime",
                "rootFolder": "/anime",
                "profileId": "4",
                "serverId": "0",
            },
        ],
    )



@bp.get("/api/seerr/users")
@login_required
def api_seerr_users():
    if not _seerr_configured():
        return jsonify(ok=True, configured=False, users=[])

    try:
        r = requests.get(
            f"{_seerr_base()}/api/v1/user",
            headers=_seerr_headers(),
            timeout=15,
        )
        r.raise_for_status()
        data = r.json() or {}
        raw = data.get("results") or data.get("users") or []
        users = []
        for u in raw:
            users.append({
                "id": u.get("id"),
                "displayName": u.get("displayName") or u.get("username") or u.get("email") or f'User {u.get("id")}',
                "email": u.get("email") or "",
            })
        return jsonify(ok=True, configured=True, users=users)
    except Exception as e:
        return jsonify(ok=False, configured=True, error=str(e), users=[]), 502


@bp.get("/api/seerr/search")
@login_required
def api_seerr_search():
    q = str(request.args.get("q") or "").strip()
    page = int(request.args.get("page") or 1)

    if not _seerr_configured():
        return jsonify(
            ok=True,
            configured=False,
            count=0,
            results=[],
        )

    if len(q) < 2:
        return jsonify(
            ok=True,
            configured=True,
            count=0,
            results=[],
        )

    try:
        r = requests.get(
            f"{_seerr_base()}/api/v1/search",
            headers=_seerr_headers(),
            params={
                "query": quote_plus(q),
                "page": page,
                "language": "en",
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json() or {}

        raw_results = data.get("results") or []
        results = []

        for item in raw_results:
            media_type = str(item.get("mediaType") or "").strip().lower()
            if media_type not in ("movie", "tv"):
                continue
            results.append(_normalize_search_result(item))

        return jsonify(
            ok=True,
            configured=True,
            count=len(results),
            page=page,
            results=results,
        )

    except Exception as e:
        return jsonify(
            ok=False,
            configured=True,
            error=str(e),
            count=0,
            results=[],
        ), 502



@bp.get("/api/seerr/pending-requests")
@login_required
def api_seerr_pending_requests():
    try:
        me = current_user() or {}
        if not me.get("is_admin"):
            return jsonify(ok=True, requests=[], admin=False)

        base = _seerr_base()
        key = _seerr_api_key()
        if not base or not key:
            return jsonify(ok=True, configured=False, requests=[], admin=True)

        take = max(1, min(int(request.args.get("take") or 12), 30))

        r = requests.get(
            f"{base}/api/v1/request",
            headers={"X-Api-Key": key},
            params={"filter": "pending", "take": take},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}

        rows = data.get("results") or data.get("requests") or []
        out = []

        for row in rows:
            if not isinstance(row, dict):
                continue

            media = row.get("media") or {}
            requested_by = row.get("requestedBy") or {}
            media_type = str(row.get("type") or media.get("mediaType") or "").strip().lower()

            title = (
                str(media.get("title") or "").strip()
                or str(media.get("name") or "").strip()
                or str(media.get("originalTitle") or "").strip()
                or "Untitled"
            )

            poster = str(media.get("posterPath") or "").strip()
            if poster and not poster.startswith("http"):
                poster = f"https://image.tmdb.org/t/p/w342{poster}"

            out.append({
                "id": int(row.get("id") or 0),
                "title": title,
                "media_type": "tv" if media_type == "tv" else "movie",
                "tmdb_id": str(media.get("tmdbId") or media.get("tmdb_id") or "").strip(),
                "poster_url": poster,
                "requested_by": (
                    str(requested_by.get("displayName") or "").strip()
                    or str(requested_by.get("username") or "").strip()
                    or str(requested_by.get("email") or "").strip()
                    or "Unknown"
                ),
                "status": str(row.get("status") or "").strip(),
            })

        return jsonify(ok=True, configured=True, admin=True, requests=out)
    except Exception as e:
        return jsonify(ok=False, error=str(e), requests=[]), 502


@bp.post("/api/seerr/request/<int:req_id>/approve")
@login_required
def api_seerr_approve_request(req_id: int):
    try:
        me = current_user() or {}
        if not me.get("is_admin"):
            return jsonify(ok=False, error="forbidden"), 403

        base = _seerr_base()
        key = _seerr_api_key()
        if not base or not key:
            return jsonify(ok=False, error="seerr_not_configured"), 400

        r = requests.post(
            f"{base}/api/v1/request/{req_id}/approve",
            headers={"X-Api-Key": key},
            timeout=20,
        )
        r.raise_for_status()
        return jsonify(ok=True, approved=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 502


@bp.post("/api/seerr/request/<int:req_id>/decline")
@login_required
def api_seerr_decline_request(req_id: int):
    try:
        me = current_user() or {}
        if not me.get("is_admin"):
            return jsonify(ok=False, error="forbidden"), 403

        base = _seerr_base()
        key = _seerr_api_key()
        if not base or not key:
            return jsonify(ok=False, error="seerr_not_configured"), 400

        r = requests.post(
            f"{base}/api/v1/request/{req_id}/decline",
            headers={"X-Api-Key": key},
            timeout=20,
        )
        r.raise_for_status()
        return jsonify(ok=True, declined=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 502


@bp.get("/api/seerr/my-requests")
@login_required
def api_seerr_my_requests():
    if not _seerr_configured():
        return jsonify(ok=True, configured=False, items=[])

    try:
        take = int(request.args.get("take") or 10)
    except Exception:
        take = 10
    take = max(1, min(take, 25))

    seerr_user_id = _user_setting("seerr_user_id") or ""
    if not str(seerr_user_id).strip():
        return jsonify(ok=True, configured=True, items=[])

    try:
        r = requests.get(
            f"{_seerr_base()}/api/v1/request",
            headers=_seerr_headers(),
            params={"take": max(take * 3, 20)},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}
        results = data.get("results") or []

        def _status_label(v):
            try:
                n = int(v)
            except Exception:
                return str(v or "")

            # QueueDeck currently uses API-key based Seerr requests, which are
            # effectively auto-approved. In practice, status 2 is what we see
            # for approved requests that are not yet available, so map it to
            # Approved instead of Pending.
            if n == 1:
                return "Pending"
            if n == 2:
                return "Approved"
            if n == 3:
                return "Processing"
            if n == 4:
                return "Partially Available"
            if n == 5:
                return "Available"
            return f"Status {n}"

        detail_cache = {}

        def _fetch_media_title(media_type, tmdb_id):
            cache_key = f"{media_type}:{tmdb_id}"
            if cache_key in detail_cache:
                return detail_cache[cache_key]

            if not tmdb_id:
                detail_cache[cache_key] = {}
                return {}

            try:
                if media_type == "movie":
                    rr = requests.get(
                        f"{_seerr_base()}/api/v1/movie/{tmdb_id}",
                        headers=_seerr_headers(),
                        timeout=15,
                    )
                else:
                    rr = requests.get(
                        f"{_seerr_base()}/api/v1/tv/{tmdb_id}",
                        headers=_seerr_headers(),
                        timeout=15,
                    )
                rr.raise_for_status()
                data = rr.json() or {}
                detail_cache[cache_key] = data
                return data
            except Exception:
                detail_cache[cache_key] = {}
                return {}

        out = []
        for item in results:
            req_by = item.get("requestedBy") or {}
            if str(req_by.get("id") or "") != str(seerr_user_id):
                continue

            media = item.get("media") or {}
            media_type = str(item.get("type") or media.get("mediaType") or "").strip().lower()

            tmdb_id = media.get("tmdbId")
            tvdb_id = media.get("tvdbId")
            title = ""
            service_url = media.get("serviceUrl") or ""
            poster_url = None

            details = _fetch_media_title(media_type, tmdb_id)

            if media_type == "movie":
                title = str(
                    item.get("subject")
                    or item.get("title")
                    or details.get("title")
                    or details.get("originalTitle")
                    or ""
                ).strip()
                if details.get("posterPath"):
                    poster_url = f"https://image.tmdb.org/t/p/w342{details.get('posterPath')}"
                elif tmdb_id:
                    poster_url = ""
            else:
                title = str(
                    item.get("subject")
                    or item.get("title")
                    or details.get("name")
                    or details.get("originalName")
                    or media.get("externalServiceSlug")
                    or ""
                ).strip()
                if details.get("posterPath"):
                    poster_url = f"https://image.tmdb.org/t/p/w342{details.get('posterPath')}"
                elif tvdb_id:
                    poster_url = f"/img/sonarr/series/{tvdb_id}.jpg"

            if not title:
                slug = media.get("externalServiceSlug")
                if slug:
                    title = str(slug).replace("-", " ").title()

            if not title:
                title = str(tmdb_id or "Unknown")

            seasons = item.get("seasons") or []
            season_text = ""
            if media_type == "tv" and seasons:
                nums = []
                for s in seasons:
                    try:
                        nums.append(int(s.get("seasonNumber")))
                    except Exception:
                        continue
                nums = sorted(set(nums))
                if nums:
                    if len(nums) == 1:
                        season_text = f"Season {nums[0]}"
                    else:
                        season_text = f"Seasons {nums[0]}-{nums[-1]}"

            out.append({
                "id": item.get("id"),
                "type": media_type,
                "title": title,
                "status": _status_label(item.get("status")),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
                "poster_url": poster_url,
                "service_url": service_url,
                "season_text": season_text,
                "requested_by": req_by.get("displayName") or "",
            })

        out = out[:take]

        return jsonify(ok=True, configured=True, items=out)

    except Exception as e:
        return jsonify(ok=False, configured=True, error=str(e), items=[]), 502


@bp.post("/api/seerr/request")
@login_required
def api_seerr_request():
    if not _seerr_configured():
        return jsonify(
            ok=False,
            configured=False,
            error="seerr_not_configured",
        ), 400

    me = current_user() or {}
    data = request.get_json(silent=True) or {}

    media_type = str(data.get("mediaType") or "").strip().lower()
    media_id = data.get("mediaId")
    is4k = bool(data.get("is4k") or False)
    seasons = data.get("seasons") or []
    destination = str(data.get("destination") or "").strip().lower()
    tvdb_id = data.get("tvdbId")
    seerr_user_id = _user_setting("seerr_user_id") or ""

    if media_type not in ("movie", "tv"):
        return jsonify(ok=False, error="invalid_media_type"), 400

    try:
        media_id = int(media_id)
    except Exception:
        return jsonify(ok=False, error="invalid_media_id"), 400

    payload = {
        "mediaType": media_type,
        "mediaId": media_id,
        "is4k": is4k,
    }

    try:
        if str(seerr_user_id).strip() != "":
            payload["userId"] = int(seerr_user_id)
    except Exception:
        pass

    _seerr_debug(
        "request pre-tv-logic",
        queue_user_id=me.get("user_id"),
        queue_username=me.get("username"),
        seerr_user_id=seerr_user_id,
        media_type=media_type,
        media_id=media_id,
        destination=destination,
        incoming_tvdb_id=tvdb_id,
        payload_user_id=payload.get("userId"),
    )

    # For TV, if the UI didn't send seasons, fetch them from Seerr and request
    # all standard seasons (skip season 0 specials).
    if media_type == "tv":
        clean_seasons = []
        if isinstance(seasons, list) and seasons:
            for s in seasons:
                try:
                    n = int(s)
                    if n > 0:
                        clean_seasons.append(n)
                except Exception:
                    continue

        if not clean_seasons:
            try:
                tv = _seerr_tv_details(media_id)
                for s in (tv.get("seasons") or []):
                    try:
                        n = int(s.get("seasonNumber"))
                    except Exception:
                        continue
                    if n > 0:
                        clean_seasons.append(n)
                if not tvdb_id:
                    tvdb_id = tv.get("externalIds", {}).get("tvdbId")
            except Exception:
                pass

        clean_seasons = sorted(set(clean_seasons))
        if clean_seasons:
            payload["seasons"] = clean_seasons

        # Hardcoded mapping from your Seerr captures
        payload["serverId"] = 0
        payload["profileId"] = 4

        # Load destination settings from user config
        dests = api_seerr_tv_destinations().json["destinations"]

        chosen = None
        dest_lower = str(destination).lower()

        for d in dests:
            key_lower = str(d.get("key", "")).lower()
            label_lower = str(d.get("label", "")).lower()

            if key_lower == dest_lower or label_lower == dest_lower:
                chosen = d
                break

        if not chosen and media_type == "tv":
            # Helpful aliasing for common QueueDeck request paths
            for d in dests:
                key_lower = str(d.get("key", "")).lower()
                label_lower = str(d.get("label", "")).lower()

                if dest_lower == "anime" and ("anime" in key_lower or "anime" in label_lower):
                    chosen = d
                    break

                if dest_lower in ("television", "tv"):
                    if any(tok in key_lower for tok in ("television", "tv")) or any(tok in label_lower for tok in ("television", "tv")):
                        chosen = d
                        break

        if chosen:
            payload["rootFolder"] = chosen.get("rootFolder", "/tv")
            payload["profileId"] = int(chosen.get("profileId", 4))
            payload["serverId"] = int(chosen.get("serverId", 0))
        elif dests:
            # fallback to first configured destination
            payload["rootFolder"] = dests[0].get("rootFolder", "/tv")
            payload["profileId"] = int(dests[0].get("profileId", 4))
            payload["serverId"] = int(dests[0].get("serverId", 0))
        else:
            payload["rootFolder"] = "/tv"

        _seerr_debug(
            "request post-destination",
            queue_user_id=me.get("user_id"),
            queue_username=me.get("username"),
            destination=destination,
            chosen_destination=(chosen.get("key") if chosen else ""),
            root_folder=payload.get("rootFolder"),
            profile_id=payload.get("profileId"),
            server_id=payload.get("serverId"),
        )

        try:
            if tvdb_id is not None and str(tvdb_id).strip() != "":
                payload["tvdbId"] = int(tvdb_id)
        except Exception:
            pass

    _seerr_debug(
        "request final payload",
        queue_user_id=me.get("user_id"),
        queue_username=me.get("username"),
        payload_user_id=payload.get("userId"),
        media_type=payload.get("mediaType"),
        media_id=payload.get("mediaId"),
        seasons=payload.get("seasons"),
        root_folder=payload.get("rootFolder"),
        profile_id=payload.get("profileId"),
        server_id=payload.get("serverId"),
        tvdb_id=payload.get("tvdbId"),
    )

    try:
        r = requests.post(
            f"{_seerr_base()}/api/v1/request",
            headers={
                **_seerr_headers(),
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=20,
        )

        if r.status_code >= 400:
            body = r.text[:1200]
            try:
                j = r.json() or {}
                return jsonify(
                    ok=False,
                    configured=True,
                    error=j.get("message") or j.get("error") or f"HTTP {r.status_code}",
                    body=body,
                    payload=payload,
                ), 502
            except Exception:
                return jsonify(
                    ok=False,
                    configured=True,
                    error=f"HTTP {r.status_code}",
                    body=body,
                    payload=payload,
                ), 502

        out = r.json() or {}

        return jsonify(
            ok=True,
            configured=True,
            request=out,
            payload=payload,
        )

    except Exception as e:
        return jsonify(
            ok=False,
            configured=True,
            error=str(e),
            payload=payload,
        ), 502
