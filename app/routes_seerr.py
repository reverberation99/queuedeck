import requests
from urllib.parse import quote_plus
from datetime import datetime, timedelta, timezone
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
        "poster_url": f"/img/tmdb{item.get('posterPath')}" if item.get("posterPath") else "",
        "backdrop_url": f"/img/tmdb{item.get('backdropPath')}" if item.get("backdropPath") else "",
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
                poster = f"/img/tmdb{poster}"

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
                    poster_url = f"/img/tmdb{details.get('posterPath')}"
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
                    poster_url = f"/img/tmdb{details.get('posterPath')}"
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


def _seerr_request_status_label(v) -> str:
    try:
        n = int(v)
    except Exception:
        return str(v or "").strip()

    if n == 1:
        return "Unknown"
    if n == 2:
        return "Pending"
    if n == 3:
        return "Processing"
    if n == 4:
        return "Partially Available"
    if n == 5:
        return "Available"
    return f"Status {n}"


def _parse_dt(raw) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _pick_movie_release_date(media: dict, radarr_row: dict | None) -> str:
    for src in (radarr_row or {}, media or {}):
        for key in ("digitalRelease", "physicalRelease", "inCinemas", "releaseDate", "serviceDate"):
            v = str(src.get(key) or "").strip()
            if v:
                return v
    return ""


def _movie_pipeline_status(radarr_row: dict | None, request_status: str) -> str:
    if not isinstance(radarr_row, dict) or not radarr_row:
        return request_status or "Requested"

    if bool(radarr_row.get("hasFile")):
        return "In Library"

    status = str(radarr_row.get("status") or "").strip().lower()
    if status == "announced":
        return "Announced"
    if status == "inCinemas".lower():
        return "In Cinemas"
    if status == "released":
        return "Released"
    if status:
        return status.replace("-", " ").title()

    return request_status or "Requested"


def _radarr_movie_index() -> tuple[dict[str, dict], dict[str, str]]:
    base = str(_user_setting("radarr_url") or "").strip().rstrip("/")
    api = str(_user_setting("radarr_api_key") or "").strip()
    if not base or not api:
        return {}, {}

    try:
        r = requests.get(
            f"{base}/api/v3/movie",
            headers={"X-Api-Key": api},
            params={"includeImages": "false"},
            timeout=25,
        )
        r.raise_for_status()
        rows = r.json() or []
    except Exception:
        return {}, {}

    by_tmdb: dict[str, dict] = {}
    url_by_tmdb: dict[str, str] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue

        tmdb_id = str(row.get("tmdbId") or "").strip()
        movie_id = str(row.get("id") or "").strip()
        if not tmdb_id:
            continue

        by_tmdb[tmdb_id] = row
        if movie_id:
            url_by_tmdb[tmdb_id] = f"{base}/movie/{movie_id}"
        else:
            url_by_tmdb[tmdb_id] = f"{base}/movie/{tmdb_id}"

    return by_tmdb, url_by_tmdb


def _sonarr_series_index() -> tuple[dict[str, dict], dict[str, dict], dict[str, str], dict[str, dict], dict[str, dict]]:
    base = str(_user_setting("sonarr_url") or "").strip().rstrip("/")
    api = str(_user_setting("sonarr_api_key") or "").strip()
    if not base or not api:
        return {}, {}, {}, {}, {}

    headers = {"X-Api-Key": api}

    try:
        rs = requests.get(f"{base}/api/v3/series", headers=headers, timeout=25)
        rs.raise_for_status()
        series_rows = rs.json() or []
    except Exception:
        return {}, {}, {}, {}, {}

    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=14)).date().isoformat()
    end = (now + timedelta(days=365)).date().isoformat()

    try:
        rc = requests.get(
            f"{base}/api/v3/calendar",
            headers=headers,
            params={
                "start": start,
                "end": end,
                "includeSeries": "true",
                "includeEpisodeFile": "true",
                "unmonitored": "false",
            },
            timeout=35,
        )
        rc.raise_for_status()
        cal_rows = rc.json() or []
    except Exception:
        cal_rows = []

    by_tvdb: dict[str, dict] = {}
    by_tmdb: dict[str, dict] = {}
    slug_by_series_id: dict[str, str] = {}

    for row in series_rows:
        if not isinstance(row, dict):
            continue

        tvdb_id = str(row.get("tvdbId") or "").strip()
        tmdb_id = str(row.get("tmdbId") or "").strip()
        sid = str(row.get("id") or "").strip()
        slug = str(row.get("titleSlug") or row.get("slug") or "").strip()

        if tvdb_id:
            by_tvdb[tvdb_id] = row
        if tmdb_id:
            by_tmdb[tmdb_id] = row
        if sid and slug:
            slug_by_series_id[sid] = slug

    next_by_series: dict[str, dict] = {}
    missing_by_series: dict[str, dict] = {}

    for ep in cal_rows:
        if not isinstance(ep, dict):
            continue

        series = ep.get("series") or {}
        sid = str(ep.get("seriesId") or series.get("id") or "").strip()
        if not sid:
            continue

        dt = _parse_dt(ep.get("airDateUtc") or ep.get("airDate"))
        if not dt:
            continue

        item = {
            "episode_title": str(ep.get("title") or "").strip(),
            "season_number": ep.get("seasonNumber"),
            "episode_number": ep.get("episodeNumber"),
            "air_date": dt.isoformat(),
            "has_file": bool(ep.get("hasFile")),
            "finale_type": str(ep.get("finaleType") or "").strip(),
        }

        if dt >= now:
            current = next_by_series.get(sid)
            if current is None or str(item["air_date"]) < str(current.get("air_date") or ""):
                next_by_series[sid] = item
        elif not bool(ep.get("hasFile")):
            current = missing_by_series.get(sid)
            if current is None or str(item["air_date"]) > str(current.get("air_date") or ""):
                missing_by_series[sid] = item

    return by_tvdb, by_tmdb, slug_by_series_id, next_by_series, missing_by_series


def _episode_tag(season_number, episode_number) -> str:
    try:
        s = int(season_number)
        e = int(episode_number)
        return f"S{s:02d}E{e:02d}"
    except Exception:
        return ""


def _tv_airing_soon(raw_dt: str) -> bool:
    dt = _parse_dt(raw_dt)
    if not dt:
        return False
    now = datetime.now(timezone.utc)
    return now <= dt <= (now + timedelta(hours=36))


def _tv_status_label(series_row: dict | None, fallback: str) -> str:
    if not isinstance(series_row, dict) or not series_row:
        return fallback or "Requested"

    status = str(series_row.get("status") or "").strip().lower()
    if status == "continuing":
        return "Continuing"
    if status == "ended":
        return "Ended"
    if status == "upcoming":
        return "Upcoming"
    if status:
        return status.replace("-", " ").title()
    return fallback or "Requested"


def _seerr_fetch_all_requests(max_pages: int = 8, take: int = 100) -> list[dict]:
    if not _seerr_configured():
        return []

    out = []
    skip = 0

    for _ in range(max_pages):
        r = requests.get(
            f"{_seerr_base()}/api/v1/request",
            headers=_seerr_headers(),
            params={"take": take, "skip": skip},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json() or {}

        if isinstance(data, dict):
            rows = data.get("results") or data.get("requests") or data.get("items") or []
            page_info = data.get("pageInfo") or {}
        elif isinstance(data, list):
            rows = data
            page_info = {}
        else:
            rows = []
            page_info = {}

        if not rows:
            break

        out.extend(rows)

        if len(rows) < take:
            break

        try:
            total = int(page_info.get("results") or 0)
            if total and len(out) >= total:
                break
        except Exception:
            pass

        skip += take

    return out


@bp.get("/api/seerr/watchlist")
@login_required
def api_seerr_watchlist():
    if not _seerr_configured():
        return jsonify(ok=True, configured=False, tv=[], movies=[])

    try:
        want_user_id = str(_user_setting("seerr_user_id") or "").strip()
        seerr_base = str(_seerr_base() or "").strip().rstrip("/")

        radarr_by_tmdb, radarr_url_by_tmdb = _radarr_movie_index()
        sonarr_by_tvdb, sonarr_by_tmdb, sonarr_slug_by_id, sonarr_next_by_id, sonarr_missing_by_id = _sonarr_series_index()

        rows = _seerr_fetch_all_requests()
        movies = []
        tv = []

        # First pass: try strict per-user matching
        filtered_rows = []
        for req in rows:
            requested_by = req.get("requestedBy") or {}
            req_user_id = str(
                requested_by.get("id")
                or requested_by.get("userId")
                or req.get("requestedById")
                or ""
            ).strip()

            if want_user_id:
                if req_user_id and req_user_id == want_user_id:
                    filtered_rows.append(req)
            else:
                filtered_rows.append(req)

        # Fallback: if strict user filtering produced nothing, show all rows
        # so the watchlist stays usable while we refine Seerr user matching.
        source_rows = filtered_rows if filtered_rows else rows

        try:
            print(
                f"[watchlist-debug] seerr_user_id={want_user_id!r} total_rows={len(rows)} filtered_rows={len(filtered_rows)} using_rows={len(source_rows)}",
                flush=True
            )
            if rows:
                sample = rows[0] or {}
                rb = sample.get("requestedBy") or {}
                print(
                    f"[watchlist-debug] sample_request id={sample.get('id')!r} type={sample.get('type')!r} requestedBy.id={rb.get('id')!r} requestedBy.userId={rb.get('userId')!r} requestedById={sample.get('requestedById')!r}",
                    flush=True
                )
        except Exception:
            pass

        for req in source_rows:
            media = req.get("media") or {}
            media_type = str(
                req.get("type")
                or req.get("mediaType")
                or media.get("mediaType")
                or ""
            ).strip().lower()

            tmdb_id = str(req.get("mediaTmdbId") or media.get("tmdbId") or media.get("id") or req.get("tmdbId") or "").strip()
            tvdb_id = str(req.get("mediaTvdbId") or media.get("tvdbId") or req.get("tvdbId") or "").strip()

            title = str(
                media.get("title")
                or media.get("name")
                or req.get("subject")
                or req.get("title")
                or ""
            ).strip()

            if not title:
                continue

            year = ""
            raw_year_src = (
                media.get("releaseDate")
                or media.get("firstAirDate")
                or req.get("createdAt")
                or ""
            )
            if isinstance(raw_year_src, str) and len(raw_year_src) >= 4:
                year = raw_year_src[:4]

            poster_path = media.get("posterPath") or ""
            poster_url = f"/img/tmdb{poster_path}" if poster_path else ""

            request_status = _seerr_request_status_label(req.get("status"))
            created_at = str(req.get("createdAt") or "").strip()

            if media_type == "movie":
                radarr_row = radarr_by_tmdb.get(tmdb_id) if tmdb_id else None
                pipeline_status = _movie_pipeline_status(radarr_row, request_status)
                release_date = _pick_movie_release_date(media, radarr_row)

                movies.append({
                    "request_id": req.get("id"),
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "year": year,
                    "poster_url": poster_url,
                    "request_status": request_status,
                    "status": pipeline_status,
                    "release_date": release_date,
                    "created_at": created_at,
                    "in_library": bool((radarr_row or {}).get("hasFile")),
                    "radarr_url": radarr_url_by_tmdb.get(tmdb_id, ""),
                    "seerr_url": f"{seerr_base}/movie/{tmdb_id}" if seerr_base and tmdb_id else "",
                })

            elif media_type == "tv":
                series_row = None
                if tvdb_id and tvdb_id in sonarr_by_tvdb:
                    series_row = sonarr_by_tvdb[tvdb_id]
                elif tmdb_id and tmdb_id in sonarr_by_tmdb:
                    series_row = sonarr_by_tmdb[tmdb_id]

                sid = str((series_row or {}).get("id") or "").strip()
                slug = sonarr_slug_by_id.get(sid, "")
                next_ep = sonarr_next_by_id.get(sid) if sid else None
                missing_ep = sonarr_missing_by_id.get(sid) if sid else None

                display_ep = next_ep or missing_ep or {}
                air_date = str(display_ep.get("air_date") or media.get("firstAirDate") or "").strip()
                ep_title = str(display_ep.get("episode_title") or "").strip()
                ep_tag = _episode_tag(display_ep.get("season_number"), display_ep.get("episode_number"))

                series_status = _tv_status_label(series_row, request_status)
                finale_type = str(display_ep.get("finale_type") or "").strip()
                is_season_finale = finale_type.lower() in {"seasonfinale", "seriesfinale", "midseasonfinale"}
                is_missing = bool(missing_ep and not next_ep)
                is_airing_soon = bool(next_ep) and _tv_airing_soon(str(next_ep.get("air_date") or ""))

                sonarr_base = str(_user_setting("sonarr_url") or "").strip().rstrip("/")
                sonarr_url = ""
                if sonarr_base and sid:
                    sonarr_url = f"{sonarr_base}/series/{slug or sid}"

                tv.append({
                    "request_id": req.get("id"),
                    "tmdb_id": tmdb_id,
                    "tvdb_id": tvdb_id,
                    "title": title,
                    "year": year,
                    "poster_url": poster_url,
                    "request_status": request_status,
                    "status": series_status,
                    "air_date": air_date,
                    "created_at": created_at,
                    "episode_title": ep_title,
                    "episode_tag": ep_tag,
                    "is_airing_soon": is_airing_soon,
                    "is_missing": is_missing,
                    "is_season_finale": is_season_finale,
                    "sonarr_url": sonarr_url,
                    "seerr_url": f"{seerr_base}/tv/{tmdb_id}" if seerr_base and tmdb_id else "",
                })

        def _sort_date_key(item, key):
            raw = str(item.get(key) or "").strip()
            if raw:
                return (0, raw)
            return (1, str(item.get("created_at") or ""))

        movies.sort(key=lambda x: _sort_date_key(x, "release_date"))
        tv.sort(key=lambda x: _sort_date_key(x, "air_date"))

        return jsonify(
            ok=True,
            configured=True,
            tv=tv,
            movies=movies,
        )
    except Exception as e:
        return jsonify(ok=False, configured=True, error=str(e), tv=[], movies=[]), 502

