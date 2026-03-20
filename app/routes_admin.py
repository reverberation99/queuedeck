import requests
import os
import shutil
import time
import json
import socket
from flask import Blueprint, render_template, request, redirect, flash, jsonify
from .db import get_db
from .models_settings import get_user_admin_settings, update_user_admin_settings
from .utils.auth import admin_required

bp = Blueprint("admin_connections", __name__)


def _norm_base(url: str) -> str:
    return (url or "").strip().rstrip("/")


def _admin_jellyfin_list_users(jellyfin_url: str, api_key: str) -> dict:
    base = _norm_base(jellyfin_url)

    if not base:
        return {"ok": False, "error": "jellyfin_url empty"}
    if not api_key:
        return {"ok": False, "error": "jellyfin_api_key empty"}

    try:
        r = requests.get(
            f"{base}/Users",
            headers={"X-Emby-Token": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()

        users = r.json() or []
        out = []
        for u in users:
            name = str(u.get("Name") or "").strip()
            uid = str(u.get("Id") or "").strip()
            if not name:
                continue
            out.append({
                "name": name,
                "id": uid,
                "is_disabled": bool((u.get("Policy") or {}).get("IsDisabled", False)),
            })

        out.sort(key=lambda x: x["name"].lower())
        return {"ok": True, "users": out}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def _admin_jellyfin_list_views(jellyfin_url: str, api_key: str, username: str) -> dict:
    base = _norm_base(jellyfin_url)

    if not base:
        return {"ok": False, "error": "jellyfin_url empty"}
    if not api_key:
        return {"ok": False, "error": "jellyfin_api_key empty"}
    if not username:
        return {"ok": False, "error": "jellyfin_user empty"}

    try:
        # resolve user id
        r = requests.get(
            f"{base}/Users",
            headers={"X-Emby-Token": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()
        users = r.json() or []

        user_id = ""
        for u in users:
            if (u.get("Name") or "").strip().lower() == username.strip().lower():
                user_id = str(u.get("Id") or "").strip()
                break

        if not user_id:
            return {"ok": False, "error": f"Jellyfin user not found: {username}"}

        # load user views
        r = requests.get(
            f"{base}/Users/{user_id}/Views",
            headers={"X-Emby-Token": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()

        raw = r.json() or {}
        items = raw.get("Items") or raw.get("items") or []
        out = []

        for v in items:
            vid = str(v.get("Id") or "").strip()
            name = str(v.get("Name") or "").strip()
            collection_type = str(v.get("CollectionType") or "").strip()
            item_type = str(v.get("Type") or "").strip()

            if not vid or not name:
                continue

            out.append({
                "id": vid,
                "name": name,
                "collection_type": collection_type,
                "type": item_type,
            })

        out.sort(key=lambda x: x["name"].lower())
        return {"ok": True, "views": out}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def _admin_seerr_list_users(seerr_url: str, api_key: str) -> dict:
    base = _norm_base(seerr_url)

    if not base:
        return {"ok": False, "error": "seerr_url empty"}
    if not api_key:
        return {"ok": False, "error": "seerr_api_key empty"}

    try:
        headers = {"X-Api-Key": api_key.strip()}
        users = []

        # Try paginated form first
        r = requests.get(
            f"{base}/api/v1/user",
            headers=headers,
            params={"take": 100, "skip": 0},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()

        if isinstance(data, dict):
            if isinstance(data.get("results"), list):
                users = data.get("results") or []
            elif isinstance(data.get("users"), list):
                users = data.get("users") or []
            elif isinstance(data.get("pageInfo"), dict) and isinstance(data.get("results"), list):
                users = data.get("results") or []
        elif isinstance(data, list):
            users = data

        # Fallback: try plain GET with no params
        if not users:
            r2 = requests.get(
                f"{base}/api/v1/user",
                headers=headers,
                timeout=15,
            )
            r2.raise_for_status()
            data2 = r2.json()

            if isinstance(data2, dict):
                if isinstance(data2.get("results"), list):
                    users = data2.get("results") or []
                elif isinstance(data2.get("users"), list):
                    users = data2.get("users") or []
                elif data2.get("id"):
                    users = [data2]
            elif isinstance(data2, list):
                users = data2

        out = []
        for u in users:
            uid = str(u.get("id") or "").strip()
            if not uid:
                continue

            display = (
                str(u.get("displayName") or "").strip()
                or str(u.get("username") or "").strip()
                or str(u.get("plexUsername") or "").strip()
                or f"User {uid}"
            )
            email = str(u.get("email") or "").strip()

            out.append({
                "id": uid,
                "displayName": display,
                "email": email,
            })

        # de-dupe
        dedup = {}
        for u in out:
            dedup[u["id"]] = u

        out = list(dedup.values())
        out.sort(key=lambda x: (x["displayName"] or "").lower())

        if not out:
            return {"ok": False, "error": "No Seerr users returned by API"}

        return {"ok": True, "users": out}
    except Exception as e:
        return {"ok": False, "error": str(e)[:500]}




def _ok_result(name: str, extra: dict | None = None) -> dict:
    out = {"ok": True, "service": name}
    if extra:
        out.update(extra)
    return out


def _fail_result(name: str, err: str) -> dict:
    return {"ok": False, "service": name, "error": (err or "unknown error")[:300]}


def _test_sonarr(sonarr_url: str, api_key: str) -> dict:
    base = _norm_base(sonarr_url)

    if not base:
        return _fail_result("sonarr", "sonarr_url empty")
    if not api_key:
        return _fail_result("sonarr", "sonarr_api_key empty")

    try:
        r = requests.get(
            f"{base}/api/v3/system/status",
            headers={"X-Api-Key": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json() or {}
        return _ok_result("sonarr", {"version": data.get("version")})
    except Exception as e:
        return _fail_result("sonarr", str(e))


def _test_radarr(radarr_url: str, api_key: str) -> dict:
    base = _norm_base(radarr_url)

    if not base:
        return _fail_result("radarr", "radarr_url empty")
    if not api_key:
        return _fail_result("radarr", "radarr_api_key empty")

    try:
        r = requests.get(
            f"{base}/api/v3/system/status",
            headers={"X-Api-Key": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json() or {}
        return _ok_result("radarr", {"version": data.get("version")})
    except Exception as e:
        return _fail_result("radarr", str(e))


def _test_seerr(seerr_url: str, api_key: str) -> dict:
    base = _norm_base(seerr_url)

    if not base:
        return _fail_result("seerr", "seerr_url empty")
    if not api_key:
        return _fail_result("seerr", "seerr_api_key empty")

    try:
        r = requests.get(
            f"{base}/api/v1/status",
            headers={"X-Api-Key": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json() or {}
        return _ok_result("seerr", {"version": data.get("version")})
    except Exception as e:
        return _fail_result("seerr", str(e))


def _test_jellyfin(jellyfin_url: str, api_key: str, username: str) -> dict:
    base = _norm_base(jellyfin_url)

    if not base:
        return _fail_result("jellyfin", "jellyfin_url empty")
    if not api_key:
        return _fail_result("jellyfin", "jellyfin_api_key empty")
    if not username:
        return _fail_result("jellyfin", "jellyfin_user empty")

    try:
        r = requests.get(
            f"{base}/Users",
            headers={"X-Emby-Token": api_key.strip()},
            timeout=10,
        )
        r.raise_for_status()

        users = r.json() or []
        for u in users:
            if (u.get("Name") or "").lower() == username.lower():
                return _ok_result("jellyfin", {"user": username})

        return _fail_result("jellyfin", f'user "{username}" not found')
    except Exception as e:
        return _fail_result("jellyfin", str(e))





@bp.get("/admin/users/<int:user_id>/connections")
@admin_required
def admin_user_connections(user_id: int):
    db = get_db()

    user = db.execute(
        "SELECT id, username, is_admin, is_active, created_at, last_login_at FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()

    if not user:
        flash("User not found.", "error")
        return redirect("/admin/users")

    settings = get_user_admin_settings(user_id)

    return render_template(
        "admin_user_connections.html",
        target_user=user,
        settings=settings,
    )


@bp.post("/admin/users/<int:user_id>/connections/jellyfin/users")
@admin_required
def admin_user_connections_jellyfin_users(user_id: int):
    db = get_db()

    user = db.execute(
        "SELECT id, username FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()

    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 404

    payload = request.get_json(silent=True) or {}
    saved = get_user_admin_settings(user_id)

    jellyfin_url = (payload.get("jellyfin_url") or saved.get("jellyfin_url") or "").strip()
    jellyfin_api_key = (payload.get("jellyfin_api_key") or saved.get("jellyfin_api_key") or "").strip()

    out = _admin_jellyfin_list_users(jellyfin_url, jellyfin_api_key)
    return jsonify(out), (200 if out.get("ok") else 400)


@bp.post("/admin/users/<int:user_id>/connections/jellyfin/views")
@admin_required
def admin_user_connections_jellyfin_views(user_id: int):
    db = get_db()

    user = db.execute(
        "SELECT id, username FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()

    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 404

    payload = request.get_json(silent=True) or {}
    saved = get_user_admin_settings(user_id)

    jellyfin_url = (payload.get("jellyfin_url") or saved.get("jellyfin_url") or "").strip()
    jellyfin_api_key = (payload.get("jellyfin_api_key") or saved.get("jellyfin_api_key") or "").strip()
    jellyfin_user = (payload.get("jellyfin_user") or saved.get("jellyfin_user") or "").strip()

    out = _admin_jellyfin_list_views(jellyfin_url, jellyfin_api_key, jellyfin_user)
    return jsonify(out), (200 if out.get("ok") else 400)


@bp.post("/admin/users/<int:user_id>/connections/seerr/users")
@admin_required
def admin_user_connections_seerr_users(user_id: int):
    db = get_db()

    user = db.execute(
        "SELECT id, username FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()

    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 404

    payload = request.get_json(silent=True) or {}
    saved = get_user_admin_settings(user_id)

    seerr_url = (payload.get("seerr_url") or saved.get("seerr_url") or "").strip()
    seerr_api_key = (payload.get("seerr_api_key") or saved.get("seerr_api_key") or "").strip()

    out = _admin_seerr_list_users(seerr_url, seerr_api_key)
    return jsonify(out), (200 if out.get("ok") else 400)



@bp.post("/admin/users/<int:user_id>/connections/test")
@admin_required
def admin_user_connections_test(user_id: int):
    db = get_db()

    user = db.execute(
        "SELECT id, username FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()

    if not user:
        return jsonify({"ok": False, "error": "User not found"}), 404

    cfg = get_user_admin_settings(user_id)
    results = []

    jellyfin_url = (cfg.get("jellyfin_url") or "").strip()
    jellyfin_api_key = (cfg.get("jellyfin_api_key") or "").strip()
    jellyfin_user = (cfg.get("jellyfin_user") or "").strip()

    sonarr_url = (cfg.get("sonarr_url") or "").strip()
    sonarr_api_key = (cfg.get("sonarr_api_key") or "").strip()

    radarr_url = (cfg.get("radarr_url") or "").strip()
    radarr_api_key = (cfg.get("radarr_api_key") or "").strip()

    seerr_url = (cfg.get("seerr_url") or "").strip()
    seerr_api_key = (cfg.get("seerr_api_key") or "").strip()

    if jellyfin_url or jellyfin_api_key or jellyfin_user:
        results.append(_test_jellyfin(jellyfin_url, jellyfin_api_key, jellyfin_user))

    if sonarr_url or sonarr_api_key:
        results.append(_test_sonarr(sonarr_url, sonarr_api_key))

    if radarr_url or radarr_api_key:
        results.append(_test_radarr(radarr_url, radarr_api_key))

    if seerr_url or seerr_api_key:
        results.append(_test_seerr(seerr_url, seerr_api_key))

    if not results:
        return jsonify({
            "ok": False,
            "error": "No configured connections found for this user."
        }), 400

    return jsonify({
        "ok": True,
        "results": results,
    })


@bp.post("/admin/users/<int:user_id>/connections")
@admin_required
def admin_user_connections_save(user_id: int):
    db = get_db()

    user = db.execute(
        "SELECT id, username FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()

    if not user:
        flash("User not found.", "error")
        return redirect("/admin/users")

    form = request.form

    keys = [
        "jellyfin_url",
        "jellyfin_play_base_url",
        "jellyfin_api_key",
        "jellyfin_user",
        "mytv_view_id",
        "anime_paths",
        "sonarr_url",
        "sonarr_api_key",
        "radarr_url",
        "radarr_api_key",
        "seerr_url",
        "seerr_api_key",
        "seerr_tv_destinations",
        "seerr_user_id",
    ]

    payload = {}

    for k in keys:
        payload[k] = (form.get(k) or "").strip()

    update_user_admin_settings(user_id, payload)

    flash(f"Connections saved for {user['username']}.", "success")

    return redirect(f"/admin/users/{user_id}/connections")


@bp.post("/admin/users/<int:user_id>/connections/clear-data")
@admin_required
def admin_user_connections_clear_data(user_id: int):

    db = get_db()

    user = db.execute(
        "SELECT id, username FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()

    if not user:
        flash("User not found.", "error")
        return redirect("/admin/users")

    db.execute("DELETE FROM user_settings WHERE user_id = ?", (user_id,))
    db.execute("DELETE FROM user_admin_settings WHERE user_id = ?", (user_id,))
    db.commit()

    flash(f"Cleared saved data for {user['username']}.", "success")

    return redirect(f"/admin/users/{user_id}/connections")

@bp.get("/api/admin/system-usage")
@admin_required
def api_admin_system_usage():
    try:
        sock_path = "/var/run/docker.sock"
        if not os.path.exists(sock_path):
            return jsonify(
                ok=False,
                error="docker_socket_not_mounted",
                hint='Add /var/run/docker.sock:/var/run/docker.sock:ro to the queuedeck-dev service volumes in docker-compose.yml, then restart the container.'
            ), 500

        def _docker_get(path: str) -> dict:
            req = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: docker\r\n"
                f"Connection: close\r\n\r\n"
            ).encode("utf-8")

            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(10)
            s.connect(sock_path)
            s.sendall(req)

            chunks = []
            while True:
                chunk = s.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
            s.close()

            raw = b"".join(chunks)
            if b"\r\n\r\n" not in raw:
                raise RuntimeError("Invalid Docker API response")

            header, body = raw.split(b"\r\n\r\n", 1)
            status_line = header.split(b"\r\n", 1)[0].decode("utf-8", "replace")
            if " 200 " not in status_line:
                raise RuntimeError(status_line)

            # Handle chunked transfer encoding from Docker API
            header_text = header.decode("utf-8", "replace").lower()
            if "transfer-encoding: chunked" in header_text:
                decoded = bytearray()
                rest = body
                while rest:
                    line_end = rest.find(b"\r\n")
                    if line_end == -1:
                        break
                    size_line = rest[:line_end].decode("utf-8", "replace").strip()
                    if not size_line:
                        break
                    size = int(size_line, 16)
                    rest = rest[line_end + 2:]
                    if size == 0:
                        break
                    decoded.extend(rest[:size])
                    rest = rest[size + 2:]
                body = bytes(decoded)

            return json.loads(body.decode("utf-8", "replace") or "{}")

        def _dir_size(path: str) -> int:
            total = 0
            if not os.path.exists(path):
                return 0
            for root, dirs, files in os.walk(path):
                for name in files:
                    fp = os.path.join(root, name)
                    try:
                        if not os.path.islink(fp):
                            total += os.path.getsize(fp)
                    except Exception:
                        pass
            return total

        try:
            with open("/etc/hostname", "r", encoding="utf-8") as f:
                container_id = f.read().strip()
        except Exception:
            container_id = str(os.environ.get("HOSTNAME") or "").strip()

        if not container_id:
            return jsonify(ok=False, error="container_id_unavailable"), 500

        stats = _docker_get(f"/containers/{container_id}/stats?stream=false")
        info = _docker_get(f"/containers/{container_id}/json?size=1")

        cpu_stats = stats.get("cpu_stats") or {}
        precpu = stats.get("precpu_stats") or {}

        cpu_percent = 0.0
        try:
            cpu_total = (cpu_stats.get("cpu_usage") or {}).get("total_usage", 0)
            precpu_total = (precpu.get("cpu_usage") or {}).get("total_usage", 0)
            system_total = cpu_stats.get("system_cpu_usage", 0)
            presystem_total = precpu.get("system_cpu_usage", 0)

            cpu_delta = cpu_total - precpu_total
            system_delta = system_total - presystem_total

            online_cpus = cpu_stats.get("online_cpus") or len((cpu_stats.get("cpu_usage") or {}).get("percpu_usage", [])) or 1

            if cpu_delta > 0 and system_delta > 0:
                cpu_percent = (cpu_delta / system_delta) * float(online_cpus) * 100.0
        except Exception:
            cpu_percent = 0.0

        mem = stats.get("memory_stats") or {}
        mem_used = int(mem.get("usage") or 0)
        mem_limit = int(mem.get("limit") or 0)
        mem_percent = (mem_used / mem_limit * 100.0) if mem_limit > 0 else 0.0

        size_rw = int(info.get("SizeRw") or 0)
        size_root_fs = int(info.get("SizeRootFs") or 0)

        app_size = _dir_size("/app")
        data_size = _dir_size("/data")

        name = str(info.get("Name") or "").strip().lstrip("/") or container_id[:12]
        image = str((info.get("Config") or {}).get("Image") or "").strip()
        state = info.get("State") or {}
        status_text = str(state.get("Status") or "").strip()
        running = bool(state.get("Running", False))
        started_at = str(state.get("StartedAt") or "").strip()
        finished_at = str(state.get("FinishedAt") or "").strip()
        restart_count = int(info.get("RestartCount") or 0)

        health_obj = state.get("Health") or {}
        health_status = str(health_obj.get("Status") or "").strip()

        return jsonify(
            ok=True,
            cpu_percent=round(max(0.0, cpu_percent), 2),
            mem_used=mem_used,
            mem_limit=mem_limit,
            mem_percent=round(max(0.0, mem_percent), 2),
            size_rw=size_rw,
            size_root_fs=size_root_fs,
            app_size=app_size,
            data_size=data_size,
            container_id=container_id,
            container_name=name,
            image=image,
            running=running,
            status=status_text,
            started_at=started_at,
            finished_at=finished_at,
            restart_count=restart_count,
            health_status=health_status,
        )
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500
