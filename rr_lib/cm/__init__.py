import json
import logging
import os
import sys

log = logging.getLogger("cm")

EV_TEST = 'RR_TEST'
EV_CONFIG_FILE_PATH = 'RR_CONFIG_PATH'


def is_test_mode():
    return os.environ.get(EV_TEST, "false").strip().lower() in ("true", "1", "yes")


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        key = (cls, hash("%s%s" % (args, kwargs)))
        saved = cls._instances.get(key)
        if not saved:
            cls._instances[key] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[key]

class ConfigException(Exception):
    pass

class ConfigManager(object):
    __metaclass__ = Singleton

    def __init__(self, config_f=None, group=0, **kwargs):
        config_path = os.environ.get(EV_CONFIG_FILE_PATH, )
        if not config_path:
            raise ConfigException('%s is undefined :('%EV_CONFIG_FILE_PATH)

        if is_test_mode():
            config_file = os.path.join(config_path, 'config_test.json')
        else:
            if config_path.endswith('.json'):
                config_file = config_path
            else:
                config_file = os.path.join(config_path, 'config.json')

        config_file = config_f or config_file
        try:
            f = open(config_file, )
            raw_data = '\n'.join(f.readlines())
        except Exception as e:
            log.exception(e)
            log.error("Can not read config file %s" % config_file)
            sys.exit(-1)

        self.config_data = json.loads(raw_data)
        self.config_file = config_file

        log.info(
            "LOAD CONFIG DATA FROM %s FOR GROUP %s:\n%s" % (
                config_file,
                group,
                "\n".join(["%s: %s" % (k, v) for k, v in self.config_data.iteritems()]))
        )

    def get(self, name, default_value=None):
        result = self.config_data.get(name, default_value)
        if not result:
            log.info("Not %s in %s :(" % (name, self.config_file))
        return result


if __name__ == '__main__':
    cm = ConfigManager()
    print cm.get('main')
    assert ConfigManager(group=1) != ConfigManager()
    assert ConfigManager(group=1) == ConfigManager(group=1)

    cm = ConfigManager(config_f='/home/alesha/Dropbox/rr/config_generators.json', group=2)
    print cm.get('sniffer', )
