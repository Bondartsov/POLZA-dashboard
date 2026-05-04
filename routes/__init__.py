from flask import Flask

from routes.misc import misc_bp
from routes.generations import gens_bp
from routes.summarize import summarize_bp
from routes.sessions import sessions_bp
from routes.analyze import analyze_bp
from routes.employee import employee_bp
from routes.provider import provider_bp
from routes.keys import keys_bp
from routes.sync import sync_bp
from routes.proxy import proxy_bp


def register_all(app: Flask):
    app.register_blueprint(misc_bp)
    app.register_blueprint(gens_bp)
    app.register_blueprint(summarize_bp)
    app.register_blueprint(sessions_bp)
    app.register_blueprint(analyze_bp)
    app.register_blueprint(employee_bp)
    app.register_blueprint(provider_bp)
    app.register_blueprint(keys_bp)
    app.register_blueprint(sync_bp)
    app.register_blueprint(proxy_bp)
