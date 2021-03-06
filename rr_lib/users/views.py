import logging

from flask import request, Blueprint, render_template, redirect, url_for
from flask import session
from flask_login import login_user, login_required, logout_user
from rr_lib.users.manage import UsersHandler, User

from rr_lib.cm import ConfigManager

usersHandler = UsersHandler()
cm = ConfigManager()
usersHandler.add_user(User(**cm.get("default_user", )))

users_app = Blueprint('users_api', __name__, template_folder="templates")

log = logging.getLogger("users_views")


@users_app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        login = request.form.get("name", )
        password = request.form.get("password", )
        remember_me = request.form.get("remember", ) == u"on"
        user = usersHandler.auth_user(login, password)
        if user:
            try:
                login_user(user, remember=remember_me)
                return redirect("/")
            except Exception as e:
                log.exception(e)

    return render_template("login.html")


@users_app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('users_api.login'))


@users_app.route('/callback')
def callback():
    user_id = session.get('user_id', )
    code = request.args.get('code', )
    log.info("Callback for user: %s\nCode: %s" % (user_id, code))

    usersHandler.db.update_user(user_id, {'callback_code': code})
    return redirect("/")
