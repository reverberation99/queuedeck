from flask import Blueprint, send_from_directory, current_app

bp = Blueprint("static_routes", __name__)

@bp.get("/favicon.svg")
def favicon_svg():
    """
    Serve the favicon from app/static/favicon.svg.

    This keeps all templates that reference /favicon.svg?v=... working,
    while letting you manage the icon as a real file (easy to replace).
    """
    return send_from_directory(current_app.static_folder, "favicon.svg")
