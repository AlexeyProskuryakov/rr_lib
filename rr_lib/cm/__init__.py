import json
import logging
import os
import sys

log = logging.getLogger("wsgi")


def is_test_mode():
    return os.environ.get("RR_TEST", "false").strip().lower() in ("true", "1", "yes")


def module_path():
    if hasattr(sys, "frozen"):
        return os.path.dirname(
            sys.executable
        )
    return os.path.dirname(__file__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        key = (cls, hash("%s%s"%(args,kwargs)))
        saved = cls._instances.get(key)
        if not saved:
            cls._instances[key] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[key]


class ConfigManager(object):
    __metaclass__ = Singleton

    def __init__(self, config_f=None, group=0):
        if is_test_mode():
            config_file = "%s/config_test.json" % module_path()
        else:
            config_file = os.path.join(os.environ.get("OPENSHIFT_DATA_DIR", ""), os.environ.get("config_file", ""))
        config_file = config_f or config_file or 'config_test.json'
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

    def get(self, name):
        result = self.config_data.get(name)
        if not result:
            log.info("Not %s in %s :("%(name, self.config_file) )
        return result


if __name__ == '__main__':
    assert ConfigManager(group=1) != ConfigManager()
    assert ConfigManager(group=1) == ConfigManager(group=1)

    cm = ConfigManager(config_f='/home/alesha/Dropbox/rr/config_generators.json', group=2)
    print cm.get('sniffer')