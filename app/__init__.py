import os
import importlib
from flask import Flask

from .db import init_app
from .utils.auth import current_user

# Modules that SHOULD expose a Flask blueprint named `bp`
BLUEPRINT_MODULES = [
    "app.routes_static",
    "app.routes_dashboard",
    "app.routes_rss",
    "app.routes_settings",
    "app.routes_images",
    "app.routes_radarr",
    "app.routes_actions",
    "app.routes_seerr",
    "app.routes_admin",
    "app.routes_discover",
    "app.routes_stats",
    "app.blueprints.auth",
    "app.blueprints.admin",
]


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )

    # sessions/login
    app.secret_key = os.getenv("SECRET_KEY", "dev-secret-change-me")

    # sqlite cleanup hooks
    init_app(app)

    @app.context_processor
    def inject_auth_user():
        return {
            "current_user": current_user(),
        }

    # simple health endpoint for docker healthcheck
    @app.get("/health")
    def health():
        return {"ok": True}

    # Register blueprints if present
    for modname in BLUEPRINT_MODULES:
        try:
            mod = importlib.import_module(modname)
            bp = getattr(mod, "bp", None)
            if bp is None:
                app.logger.warning("%s imported but has no `bp` blueprint", modname)
                continue
            app.register_blueprint(bp)
            app.logger.info("Registered blueprint: %s", modname)
        except Exception as e:
            app.logger.warning("Blueprint load failed for %s: %s", modname, e)

    return app
