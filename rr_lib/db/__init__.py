import logging

from pymongo import MongoClient

from rr_lib.cm import ConfigManager

log = logging.getLogger("DB")


class DBHandler(object):
    def __init__(self, name="main", uri=None, db_name=None, connection_name=''):
        cm = ConfigManager()
        if not cm.get(name):
            raise ValueError("No %s config for connect to mongo:(" % name)

        _uri = uri or cm.get(name, ).get('mongo', ).get("uri", )
        _db_name = db_name or cm.get(name, ).get('mongo', ).get("db_name", )

        self.client = MongoClient(host=_uri, maxPoolSize=10, connect=False)
        self.db = self.client[_db_name]
        self.collection_names = self.db.collection_names(include_system_collections=False)

        log.info("Start DBHandler for [%s] to [%s/%s]" % (connection_name or '-', _uri, _db_name))
