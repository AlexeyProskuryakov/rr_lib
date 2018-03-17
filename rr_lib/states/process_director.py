# coding:utf-8
import os
import logging
import time
from threading import Thread, Event

import redis

from rr_lib.cm import ConfigManager

log = logging.getLogger("process_director")

PREFIX_ALLOC = lambda x: "PD_%s" % x

DEFAULT_TICK_TIME = 10

__all__ = [
    'ProcessDirector'
]

stop = 'stop'


class _ProcessTracker(object):
    def __init__(self, aspect, pd, tick_time):
        event = Event()
        self.tsh = Thread(target=_send_heart_beat, args=(aspect, pd, tick_time, event))
        self.tsh.setDaemon(True)
        self.tsh.start()

        self.stop_event = event

    def stop_track(self):
        log.info("will stop tracking")
        self.stop_event.set()
        self.tsh.join()


def _send_heart_beat(aspect, pd, tick_time, stop_event):
    log.info("start tracking [%s] every [%s]" % (aspect, tick_time))

    while not stop_event.isSet():
        try:
            state = pd._get_timed_state(aspect)
            if state is stop:
                stop_event.set()
                log.info('stop stracking %s' % aspect)
                return

            pd._set_timed_state(aspect, tick_time)
        except Exception as e:
            log.exception(e)
        time.sleep(tick_time / 2)

    log.info("stop tracking [%s]" % aspect)


class ProcessDirector(object):
    def __init__(self, name="?", clear=False, max_connections=5000):
        cm = ConfigManager()
        self.redis = redis.StrictRedis(host=cm.get('pd', ).get('host', ),
                                       port=int(cm.get('pd', ).get('port', )),
                                       password=cm.get('pd', ).get('pwd', ),
                                       db=0,
                                       max_connections=max_connections
                                       )
        if clear:
            self.redis.flushdb()
        log.info("Process director [%s] inited." % name)

    def start_aspect(self, aspect, tick_time=DEFAULT_TICK_TIME, with_tracking=True):
        alloc = self.redis.set(PREFIX_ALLOC(aspect), tick_time, ex=tick_time, nx=True)
        if alloc:
            if with_tracking:
                result = _ProcessTracker(aspect, self, tick_time)
                return result
            return alloc
        else:
            state = self.redis.get(PREFIX_ALLOC(aspect))
            if state == stop:
                self.redis.delete(PREFIX_ALLOC(aspect))
                self.redis.set(PREFIX_ALLOC(aspect), tick_time, ex=tick_time, nx=True)
                if with_tracking:
                    result = _ProcessTracker(aspect, self, tick_time)
                    return result
                return True

    def stop_aspect(self, aspect):
        data = self.redis.get(PREFIX_ALLOC(aspect))
        if data:
            self.redis.set(PREFIX_ALLOC(aspect), stop)
            return True
        return False

    def is_aspect_work(self, aspect, timing_check=True):
        tick_time = self.redis.get(PREFIX_ALLOC(aspect))
        if tick_time == stop:
            log.info('Stop by signal.')
            return False

        if not tick_time:
            log.info('Stop by time to update is expired')

        if tick_time and timing_check:
            time.sleep(int(tick_time))
            tick_time = self.redis.get(PREFIX_ALLOC(aspect))

        return tick_time is not None

    def _set_timed_state(self, aspect, ex):
        self.redis.set(PREFIX_ALLOC(aspect), ex, ex=ex)

    def _get_timed_state(self, aspect):
        return self.redis.get(PREFIX_ALLOC(aspect))

    def get_all_aspects(self):
        keys = self.redis.keys(PREFIX_ALLOC(''))
        result = {}
        for key in keys:
            k_res = self.redis.get(key)
            result[key] = k_res
        return result
