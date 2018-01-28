import json
import logging
from functools import partial

import httplib2
from googleapiclient.discovery import build
from oauth2client.client import Storage, OAuth2WebServerFlow, OAuth2Credentials
from oauth2client.tools import run_flow, argparser

from rr_lib.cm import ConfigManager
from rr_lib.db import DBHandler

YOUTUBE_SCOPE = "https://www.googleapis.com/auth/youtube"
YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube.upload"
YOUTUBE_READ_WRITE_SSL_SCOPE = "https://www.googleapis.com/auth/youtube.force-ssl"
YOUTUBE_CHANNEL_AUDIT = "https://www.googleapis.com/auth/youtubepartner-channel-audit"
YOUTUBE_PARTNER = "https://www.googleapis.com/auth/youtubepartner"
YOUTUBE_READ_WRITE_SCOPE = "https://www.googleapis.com/auth/youtubepartner-channel-audit"

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

name = 'yt_auth'
log = logging.getLogger(name)
config = ConfigManager().get(name)
default_client = config.get('client_id', 1)


class _DatabaseConnector(DBHandler):
    def __init__(self):
        super(_DatabaseConnector, self).__init__(name=name, connection_name=name)
        self.apps = self.db.get_collection('app_creds')
        if not self.apps:
            self.apps = self.db.create_collection('app_creds')
            self.apps.create_index('type')
            self.apps.create_index('project_id')

        self.channels = self.db.get_collection('chan_creds')
        if not self.channels:
            self.channels = self.db.create_collection('chan_creds')
            self.channels.create_index('channel_id')
            self.channels.create_index('current')

    def add_app_credentials(self, project_id, type, data):
        q = {'project_id': project_id, 'type': type}
        self.apps.update_one(q, {'$set': dict(q, **{'data': data})}, upsert=True)

    def get_app_credentials(self, project_id, type):
        found = self.apps.find_one({'project_id': project_id, 'type': type})
        if found is not None:
            data = found.get('data')
            if data and type in data:
                return data[type]

    def add_channel_credentials(self, channel_id, data, client_id=default_client):
        q = {'channel_id': channel_id}
        self.channels.update_one(q, {'$set': dict(q, **{'data': data, 'added_by': client_id})}, upsert=True)

    def get_channel_credentials(self, channel_id):
        found = self.channels.find_one({'channel_id': channel_id})
        if found:
            return found.get('data')

    def delete_channel_credentials(self, channel_id):
        return self.channels.delete_one({'channel_id': channel_id})

    def set_current_channel(self, channel_id, client_id=default_client):
        self.channels.update_one({'current': client_id}, {'$unset': {'current': channel_id}})
        q = {'channel_id': channel_id}
        result = self.channels.update_one(q, {'$set': dict({'current': client_id}, **q)}, upsert=True)
        return result

    def prepare_channel(self, channel_id, title):
        return self.channels.update_one({'channel_id': channel_id},
                                        {'$set': {'channel_id': channel_id, 'title': title}},
                                        upsert=True)

    def get_current_channel_id(self, client_id=default_client):
        found = self.channels.find_one({'current': client_id})
        if found:
            return found.get('channel_id')

    def get_all_channels_creds(self):
        return list(self.channels.find())


auth_db = _DatabaseConnector()


class ChannelCredentialStorage(Storage):
    def __init__(self, channel_id):
        super(ChannelCredentialStorage, self).__init__()
        self.channel_id = channel_id

    def locked_get(self):
        data = auth_db.get_channel_credentials(self.channel_id)
        log.info("Found credentials for %s \n%s" % (self.channel_id, data))
        if not data:
            return None
        return OAuth2Credentials.from_json(json.dumps(data))

    def locked_put(self, credentials):
        raw_data = credentials.to_json()
        log.info('Will store credentials for channel %s\n%s' % (self.channel_id, raw_data))
        auth_db.add_channel_credentials(self.channel_id, json.loads(raw_data))


def authorise(channel_id, app_id, app_type):
    app_creds = auth_db.get_app_credentials(app_id, app_type)
    if not app_creds:
        raise Exception("You must create valid app creds.")
    scope = [YOUTUBE_SCOPE, YOUTUBE_READ_WRITE_SCOPE, YOUTUBE_UPLOAD_SCOPE, YOUTUBE_CHANNEL_AUDIT, YOUTUBE_PARTNER]

    flow = OAuth2WebServerFlow(
        app_creds['client_id'],
        app_creds['client_secret'],
        scope,
        **{
            'redirect_uri': 'http://localhost:9999',
            'auth_uri': app_creds['auth_uri'],
            'token_uri': app_creds['token_uri'],
            'login_hint': "LOGIN HINT",
        }
    )

    args = argparser.parse_args()
    storage = ChannelCredentialStorage(channel_id)
    channel_creds = run_flow(flow, storage, args)

    return channel_creds


def authenticate(app_id, app_type, channel_id):
    log.info('Start auth: %s %s for channel: [%s]' % (app_id, app_type, channel_id))
    storage = ChannelCredentialStorage(channel_id)
    credentials = storage.get()
    if not credentials:
        log.info('Channel credentials were not found, authorising')
        credentials = authorise(channel_id, app_id, app_type)
    else:
        credentials = OAuth2Credentials.from_json(credentials.to_json())
        credentials.set_store(storage)

        if credentials.invalid:
            credentials = authorise(channel_id, app_id, app_type)

        if credentials.access_token_expired:
            credentials.refresh(httplib2.Http())

    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                 http=credentials.authorize(httplib2.Http()))


default_app_config = config.get('default_app')
default_app_id, default_app_type = default_app_config.get('project_id'), default_app_config.get('type')
get_default_youtube_auth_engine = partial(authenticate, default_app_id, default_app_type)
