import logging
import socket

from flask import request, Blueprint, render_template
from flask.json import jsonify
from wake_up import STEP_TIME

from wake_up.engine import WakeUp
from rr_lib.states.process_director import ProcessDirector

wake_up_app = Blueprint('wake_up_api', __name__, template_folder="templates")

log = logging
wu = WakeUp()
pd = ProcessDirector('wake_up')

aspect = "wake_up_%s" % socket.gethostbyname(socket.gethostname())
if not pd.is_aspect_work(aspect, timing_check=False):
    wu.start()
    pd.start_aspect(aspect, tick_time=STEP_TIME / 2, with_tracking=False)


@wake_up_app.route("/<salt>", methods=["POST"])
def wake_up(salt):
    return jsonify(**{"result": salt})


@wake_up_app.route("/check")
def wake_up_check():
    wu.check()
    urls = map(lambda x: {"url": x.get("url"), "state": x.get("state")}, wu.store.get_urls_info())
    return jsonify({"ok": True, "urls": urls})


@wake_up_app.route("/", methods=["GET", "POST"])
def wake_up_manage():
    if request.method == "POST":
        urls = request.form.get("urls")
        urls = filter(lambda x: x, map(lambda x: x.strip(), urls.split("\n")))

        stored_urls = wu.store.get_urls()
        to_delete = set(stored_urls).difference(urls)
        deleted_count = wu.store.delete_urls(to_delete)
        log.info("Delete %s urls" % deleted_count)
        for url in urls:
            wu.store.add_url(url)

    urls = wu.store.get_urls()

    return render_template("wake_up_.html", **{"urls": urls})
