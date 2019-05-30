from flask import Flask
from wngchorses.extensions import db, debug_toolbar
from wngchorses.ccf.models import Track
from wngchorses.main.views import bp as bp_main
import wngchorses.commands as commands


def create_app():
    """
    Create a Flask application using the app factory pattern.
    :param settings_override: Override settings
    :return: Flask app
    """
    app = Flask(__name__)
    app.config.from_object('config.settings')

    register_extensions(app)
    register_blueprints(app)
    register_shellcontext(app)
    register_commands(app)

    return app


def register_extensions(app):
    """
    Register 0 or more extensions (mutates the app passed in).
    :param app: Flask application instance
    :return: None
    """
    db.init_app(app)
    debug_toolbar.init_app(app)

    return None


def register_blueprints(app):
    app.register_blueprint(bp_main)

    return None


def register_shellcontext(app):
    """
    Register shell context objects.
    """
    def shell_context():
        return {
            'db': db,
            'track': Track
        }

    app.shell_context_processor(shell_context)


def register_commands(app):
    """
    Register Click commands.
    """
    app.cli.add_command(commands.create_db)
    app.cli.add_command(commands.load_ccf)
    app.cli.add_command(commands.load_ccf_year)
