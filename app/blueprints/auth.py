import sqlite3
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
from werkzeug.security import check_password_hash, generate_password_hash

from ..db import get_db

bp = Blueprint("auth", __name__)

_LOGIN_WINDOW_SECONDS = 60
_LOGIN_MAX_ATTEMPTS = 10
_LOGIN_ATTEMPTS = defaultdict(deque)


def _login_attempt_key(username: str = "") -> str:
    forwarded = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    ip = forwarded or (request.remote_addr or "unknown")
    return f"{ip.lower()}|{(username or '').strip().lower()}"


def _prune_login_attempts(key: str) -> deque:
    now = time.time()
    dq = _LOGIN_ATTEMPTS[key]
    while dq and (now - dq[0]) > _LOGIN_WINDOW_SECONDS:
        dq.popleft()
    return dq


def _is_login_rate_limited(username: str = "") -> bool:
    key = _login_attempt_key(username)
    dq = _prune_login_attempts(key)
    return len(dq) >= _LOGIN_MAX_ATTEMPTS


def _record_login_failure(username: str = "") -> None:
    key = _login_attempt_key(username)
    dq = _prune_login_attempts(key)
    dq.append(time.time())


def _clear_login_failures(username: str = "") -> None:
    key = _login_attempt_key(username)
    _LOGIN_ATTEMPTS.pop(key, None)


def _ensure_login_audit_table() -> None:
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS login_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            attempted_username TEXT NOT NULL DEFAULT '',
            ip_address TEXT NOT NULL DEFAULT '',
            user_agent TEXT NOT NULL DEFAULT '',
            success INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_login_audit_created_at ON login_audit(created_at DESC)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_login_audit_success_created_at ON login_audit(success, created_at DESC)")
    db.commit()


def _login_audit_ip() -> str:
    forwarded = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    real = (request.headers.get("X-Real-IP") or "").strip()
    return forwarded or real or (request.remote_addr or "unknown")


def _record_login_audit(username: str = "", success: bool = False) -> None:
    try:
        _ensure_login_audit_table()
        db = get_db()
        db.execute(
            """
            INSERT INTO login_audit (attempted_username, ip_address, user_agent, success)
            VALUES (?, ?, ?, ?)
            """,
            (
                (username or "").strip(),
                _login_audit_ip(),
                str(request.headers.get("User-Agent") or "")[:300],
                1 if success else 0,
            ),
        )
        db.commit()
    except Exception:
        pass


def _admin_only():
    if not session.get("logged_in") or not session.get("user_id") or int(session.get("is_admin") or 0) != 1:
        return False
    return True


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _user_count() -> int:
    db = get_db()
    row = db.execute("SELECT COUNT(*) AS c FROM users").fetchone()
    return int(row["c"] or 0)


def _get_user_by_username(username: str):
    db = get_db()
    return db.execute(
        """
        SELECT id, username, password_hash, is_admin, is_active, last_login_at
        FROM users
        WHERE lower(username) = lower(?)
        """,
        (username.strip(),),
    ).fetchone()


def _create_user(username: str, password: str, is_admin: bool = False):
    db = get_db()
    now = _now_iso()
    db.execute(
        """
        INSERT INTO users (username, password_hash, is_admin, is_active, created_at, updated_at)
        VALUES (?, ?, ?, 1, ?, ?)
        """,
        (
            username.strip(),
            generate_password_hash(password),
            1 if is_admin else 0,
            now,
            now,
        ),
    )
    db.commit()

    return db.execute(
        """
        SELECT id, username, is_admin, is_active
        FROM users
        WHERE lower(username) = lower(?)
        """,
        (username.strip(),),
    ).fetchone()


def _login_user(user_row) -> None:
    session.clear()
    session["logged_in"] = True
    session["user_id"] = int(user_row["id"])
    session["username"] = str(user_row["username"])
    session["is_admin"] = int(user_row["is_admin"] or 0)


@bp.route("/login", methods=["GET", "POST"])
def login():
    if _user_count() == 0:
        return redirect(url_for("auth.setup"))

    if session.get("logged_in") and session.get("user_id"):
        return redirect(url_for("dashboard.root"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        if not username or not password:
            return render_template("login.html", error="Username and password are required.")

        if _is_login_rate_limited(username):
            return render_template("login.html", error="Too many login attempts. Please wait a minute and try again.")

        user = _get_user_by_username(username)

        if not user:
            _record_login_failure(username)
            _record_login_audit(username, False)
            return render_template("login.html", error="Invalid credentials.")

        if int(user["is_active"] or 0) != 1:
            _record_login_failure(username)
            _record_login_audit(username, False)
            return render_template("login.html", error="Invalid credentials.")

        if not check_password_hash(user["password_hash"], password):
            _record_login_failure(username)
            _record_login_audit(username, False)
            return render_template("login.html", error="Invalid credentials.")

        db = get_db()
        db.execute(
            "UPDATE users SET last_login_at = ?, updated_at = ? WHERE id = ?",
            (_now_iso(), _now_iso(), int(user["id"])),
        )
        db.commit()

        _clear_login_failures(username)
        _record_login_audit(username, True)
        _login_user(user)
        return redirect(url_for("dashboard.root"))

    return render_template("login.html")


@bp.route("/setup", methods=["GET", "POST"])
def setup():
    """
    First-run wizard:
    only available when there are zero users.
    The first created user becomes admin.
    """
    if _user_count() > 0:
        if session.get("logged_in") and session.get("user_id"):
            return redirect(url_for("dashboard.root"))
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        confirm_password = request.form.get("confirm_password") or ""

        if not username:
            return render_template("setup.html", error="Username is required.")

        if len(username) < 3:
            return render_template("setup.html", error="Username must be at least 3 characters.")

        if not password:
            return render_template("setup.html", error="Password is required.")

        if len(password) < 8:
            return render_template("setup.html", error="Password must be at least 8 characters.")

        if password != confirm_password:
            return render_template("setup.html", error="Passwords do not match.")

        try:
            user = _create_user(username=username, password=password, is_admin=True)
        except sqlite3.IntegrityError:
            return render_template("setup.html", error="That username already exists.")

        _login_user(user)
        return redirect(url_for("settings.settings_page", first_run=1))

    return render_template("setup.html")


@bp.route("/admin/api/login-audit/summary", methods=["GET"])
def login_audit_summary():
    if not _admin_only():
        return jsonify(ok=False, error="forbidden"), 403

    try:
        _ensure_login_audit_table()
        db = get_db()

        row = db.execute(
            """
            SELECT
              SUM(CASE WHEN success = 0 AND created_at >= strftime('%s','now') - 86400 THEN 1 ELSE 0 END) AS failed_24h,
              SUM(CASE WHEN success = 0 AND created_at >= strftime('%s','now') - 604800 THEN 1 ELSE 0 END) AS failed_7d,
              SUM(CASE WHEN success = 1 AND created_at >= strftime('%s','now') - 86400 THEN 1 ELSE 0 END) AS success_24h
            FROM login_audit
            """
        ).fetchone()

        top_rows = db.execute(
            """
            SELECT ip_address, COUNT(*) AS n
            FROM login_audit
            WHERE success = 0
              AND created_at >= strftime('%s','now') - 86400
              AND TRIM(COALESCE(ip_address,'')) <> ''
            GROUP BY ip_address
            ORDER BY n DESC, ip_address ASC
            LIMIT 10
            """
        ).fetchall()

        return jsonify(
            ok=True,
            failed_24h=int((row["failed_24h"] or 0) if row else 0),
            failed_7d=int((row["failed_7d"] or 0) if row else 0),
            success_24h=int((row["success_24h"] or 0) if row else 0),
            top_ips=[
                {"ip_address": str(r["ip_address"] or ""), "count": int(r["n"] or 0)}
                for r in (top_rows or [])
            ],
        )
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500


@bp.route("/admin/api/login-audit/recent", methods=["GET"])
def login_audit_recent():
    if not _admin_only():
        return jsonify(ok=False, error="forbidden"), 403

    try:
        _ensure_login_audit_table()
        db = get_db()

        try:
            limit = max(1, min(int(request.args.get("limit") or 20), 100))
        except Exception:
            limit = 20

        rows = db.execute(
            """
            SELECT attempted_username, ip_address, user_agent, success, created_at
            FROM login_audit
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        return jsonify(
            ok=True,
            items=[
                {
                    "attempted_username": str(r["attempted_username"] or ""),
                    "ip_address": str(r["ip_address"] or ""),
                    "user_agent": str(r["user_agent"] or ""),
                    "success": bool(int(r["success"] or 0)),
                    "created_at": int(r["created_at"] or 0),
                }
                for r in (rows or [])
            ],
        )
    except Exception as e:
        return jsonify(ok=False, error=str(e), items=[]), 500


@bp.route("/admin/api/login-audit/clear", methods=["POST"])
def login_audit_clear():
    if not _admin_only():
        return jsonify(ok=False, error="forbidden"), 403

    try:
        _ensure_login_audit_table()
        db = get_db()
        db.execute("DELETE FROM login_audit")
        db.commit()
        return jsonify(ok=True)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500




@bp.route("/logout", methods=["GET", "POST"])
def logout():
    session.clear()
    return redirect(url_for("auth.login"))
