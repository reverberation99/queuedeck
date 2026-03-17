import sqlite3
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, redirect, url_for, session
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
            return render_template("login.html", error="Invalid credentials.")

        if int(user["is_active"] or 0) != 1:
            _record_login_failure(username)
            return render_template("login.html", error="Invalid credentials.")

        if not check_password_hash(user["password_hash"], password):
            _record_login_failure(username)
            return render_template("login.html", error="Invalid credentials.")

        db = get_db()
        db.execute(
            "UPDATE users SET last_login_at = ?, updated_at = ? WHERE id = ?",
            (_now_iso(), _now_iso(), int(user["id"])),
        )
        db.commit()

        _clear_login_failures(username)
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


@bp.route("/logout", methods=["GET", "POST"])
def logout():
    session.clear()
    return redirect(url_for("auth.login"))
