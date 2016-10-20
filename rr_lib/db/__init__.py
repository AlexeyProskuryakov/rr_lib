import logging

from pymongo import MongoClient

from rr_lib.cm import ConfigManager

log = logging.getLogger("DB")

class DBHandler(object):
    def __init__(self, name="?", uri=None, db_name=None):
        cm = ConfigManager()
        _uri = uri or cm.get("mongo_uri")
        _db_name = db_name or cm.get("db_name")

        self.client = MongoClient(host=_uri, maxPoolSize=10, connect=False)
        self.db = self.client[_db_name]
        self.collection_names = self.db.collection_names(include_system_collections=False)

        log.info("Start DBHandler for [%s] to [%s/%s]" % (name, _uri, _db_name))