from functools import wraps

from flask import redirect, session, url_for

from ..db import get_db


def users_exist() -> bool:
    db = get_db()
    row = db.execute("SELECT COUNT(*) AS c FROM users").fetchone()
    return bool(row and int(row["c"] or 0) > 0)


def is_logged_in() -> bool:
    return bool(session.get("logged_in") and session.get("user_id"))


def is_admin() -> bool:
    return bool(int(session.get("is_admin", 0) or 0) == 1)


def current_user():
    if not is_logged_in():
        return None
    return {
        "user_id": session.get("user_id"),
        "username": session.get("username"),
        "is_admin": bool(int(session.get("is_admin", 0) or 0) == 1),
    }


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not users_exist():
            return redirect(url_for("auth.setup"))

        if not is_logged_in():
            return redirect(url_for("auth.login"))

        return f(*args, **kwargs)
    return decorated


def login_required_401(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not users_exist():
            return ("Setup required", 503)

        if not is_logged_in():
            return ("Unauthorized", 401)

        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not users_exist():
            return redirect(url_for("auth.setup"))

        if not is_logged_in():
            return redirect(url_for("auth.login"))

        if not is_admin():
            return redirect(url_for("dashboard.root"))

        return f(*args, **kwargs)
    return decorated
