from flask import Blueprint, render_template, request
from werkzeug.security import generate_password_hash
import sqlite3

from ..db import get_db
from ..utils.auth import admin_required, current_user

bp = Blueprint("admin", __name__)


def _all_users():
    db = get_db()
    return db.execute(
        """
        SELECT id, username, is_admin, is_active, created_at, updated_at, last_login_at
        FROM users
        ORDER BY lower(username) ASC
        """
    ).fetchall()


def _admin_count() -> int:
    db = get_db()
    row = db.execute(
        "SELECT COUNT(*) AS c FROM users WHERE is_admin = 1 AND is_active = 1"
    ).fetchone()
    return int(row["c"] or 0)


def _get_user(user_id: int):
    db = get_db()
    return db.execute(
        """
        SELECT id, username, is_admin, is_active, created_at, updated_at, last_login_at
        FROM users
        WHERE id = ?
        """,
        (user_id,),
    ).fetchone()


def _render(error=None, success=None):
    me = current_user() or {}
    return render_template(
        "admin_users.html",
        users=_all_users(),
        error=error,
        success=success,
        me=me,
    )


@bp.get("/admin/users", strict_slashes=False)
@admin_required
def admin_users():
    return _render()


@bp.post("/admin/users/create", strict_slashes=False)
@admin_required
def admin_users_create():
    db = get_db()

    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    is_admin = 1 if request.form.get("is_admin") == "on" else 0
    is_active = 1 if request.form.get("is_active") == "on" else 0

    if not username:
        return _render(error="Username is required.")

    if len(username) < 3:
        return _render(error="Username must be at least 3 characters.")

    if not password:
        return _render(error="Password is required.")

    if len(password) < 8:
        return _render(error="Password must be at least 8 characters.")

    try:
        db.execute(
            """
            INSERT INTO users (username, password_hash, is_admin, is_active)
            VALUES (?, ?, ?, ?)
            """,
            (
                username,
                generate_password_hash(password),
                is_admin,
                is_active,
            ),
        )
        db.commit()
    except sqlite3.IntegrityError:
        return _render(error="That username already exists.")

    return _render(success=f'User "{username}" created successfully.')


@bp.post("/admin/users/reset-password", strict_slashes=False)
@admin_required
def admin_users_reset_password():
    db = get_db()

    try:
        user_id = int(request.form.get("user_id") or "0")
    except ValueError:
        return _render(error="Invalid user id.")

    new_password = request.form.get("new_password") or ""

    if user_id <= 0:
        return _render(error="Invalid user id.")

    if len(new_password) < 8:
        return _render(error="New password must be at least 8 characters.")

    user = _get_user(user_id)
    if not user:
        return _render(error="User not found.")

    db.execute(
        """
        UPDATE users
        SET password_hash = ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (generate_password_hash(new_password), user_id),
    )
    db.commit()

    return _render(success=f'Password reset for "{user["username"]}".')


@bp.post("/admin/users/toggle-active", strict_slashes=False)
@admin_required
def admin_users_toggle_active():
    db = get_db()
    me = current_user() or {}

    try:
        user_id = int(request.form.get("user_id") or "0")
    except ValueError:
        return _render(error="Invalid user id.")

    user = _get_user(user_id)
    if not user:
        return _render(error="User not found.")

    if int(me.get("user_id") or 0) == int(user["id"]):
        return _render(error="You cannot deactivate your own account from this page.")

    current_active = int(user["is_active"] or 0)
    new_active = 0 if current_active == 1 else 1

    # If disabling an admin, make sure at least one active admin remains
    if current_active == 1 and new_active == 0 and int(user["is_admin"] or 0) == 1:
        if _admin_count() <= 1:
            return _render(error="You cannot deactivate the last active admin.")

    db.execute(
        """
        UPDATE users
        SET is_active = ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (new_active, user_id),
    )
    db.commit()

    state = "activated" if new_active == 1 else "deactivated"
    return _render(success=f'User "{user["username"]}" {state}.')


@bp.post("/admin/users/toggle-admin", strict_slashes=False)
@admin_required
def admin_users_toggle_admin():
    db = get_db()
    me = current_user() or {}

    try:
        user_id = int(request.form.get("user_id") or "0")
    except ValueError:
        return _render(error="Invalid user id.")

    user = _get_user(user_id)
    if not user:
        return _render(error="User not found.")

    if int(me.get("user_id") or 0) == int(user["id"]):
        return _render(error="You cannot remove your own admin rights from this page.")

    current_admin = int(user["is_admin"] or 0)
    new_admin = 0 if current_admin == 1 else 1

    if current_admin == 1 and new_admin == 0:
        if _admin_count() <= 1 and int(user["is_active"] or 0) == 1:
            return _render(error="You cannot remove admin from the last active admin.")

    db.execute(
        """
        UPDATE users
        SET is_admin = ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (new_admin, user_id),
    )
    db.commit()

    state = "granted admin access to" if new_admin == 1 else "removed admin access from"
    return _render(success=f'You {state} "{user["username"]}".')
