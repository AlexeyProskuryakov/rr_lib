import logging

from pymongo import MongoClient

from rr_lib.cm import ConfigManager, Singleton

log = logging.getLogger("DB")


class DBHandler(object):
    __metaclass__ = Singleton

    def __init__(self, name="main", connection_name='', config_file_name=None):
        cm = ConfigManager(config_fn=config_file_name)
        _db_config = cm.get(name, ).get('mongo', )
        if not cm.get(name) or not _db_config:
            raise ValueError("No %s config for connect to mongo:(" % name)

        _uri = _db_config.get("uri", )
        _db_name = _db_config.get("db_name", )
        conn_params = {'host': _uri, 'maxPoolSize': 200, 'connect': False, 'connectTimeoutMS': 5000}

        self.client = MongoClient(**conn_params)

        self.db = self.client[_db_name]
        _u,_p = _db_config.get('user'), _db_config.get('pwd')
        if _u and _p:
            self.db.authenticate(_u, _p, 'admin')
        self.collection_names = self.db.collection_names(include_system_collections=False)

        log.info("Start DBHandler for [%s] to [%s@%s/%s]" % (connection_name or '-', _u, _uri, _db_name))
