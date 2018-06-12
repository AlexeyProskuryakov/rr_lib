import logging
from logging import StreamHandler
from multiprocessing import Process

import time

from rr_lib.states.process_director import ProcessDirector, AspectDirector

log = logging.getLogger()


class TestProcess(Process):
    def __init__(self, aspect):
        super(TestProcess, self).__init__()
        self.pd = ProcessDirector()

        self.aspect = aspect

    def run(self):
        tracker = self.pd.start_aspect(self.aspect, tick_time=1)
        for i in range(5):
            print(i)
            time.sleep(1)
        print("stop")
        tracker.stop_track()
        print("stopped")


def test_posix():
    sp = TestProcess("test")

    pd = ProcessDirector("t")
    print("before start", pd.is_aspect_work("test"))
    sp.start()
    print("after start", pd.is_aspect_work("test"))

    time.sleep(5)
    sp.join()

    print("after join", pd.is_aspect_work("test"))
    time.sleep(4)
    print("after wait", pd.is_aspect_work("test"))


def test_nt():
    pd = ProcessDirector('t')
    assert pd.is_aspect_work('t') == True
    assert pd.start_aspect('t') == None


def test_sugar():
    aspect = 'test'
    ad = AspectDirector(aspect)
    ad.stop()

    @ad.aspect_checkable()
    def iterate(what):
        print(what)
        return True

    @ad.aspect_startable(sleep_time=2)
    def work():
        next = True
        i = 0
        while next:
            i += 1
            next, _ = iterate(i)
            if i > 100:
                ad.stop()

    work()


class AspectTest(object):
    ad = AspectDirector('')


log.setLevel('DEBUG')
log.addHandler(StreamHandler())

if __name__ == '__main__':
    test_sugar()
