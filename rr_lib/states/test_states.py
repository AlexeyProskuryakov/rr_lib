from multiprocessing import Process

import time

from rr_lib.states.process_director import ProcessDirector


class TestProcess(Process):
    def __init__(self, aspect):
        super(TestProcess, self).__init__()
        self.pd = ProcessDirector()

        self.aspect = aspect

    def run(self):
        tracker = self.pd.start_aspect(self.aspect, tick_time=1)
        for i in range(5):
            print i
            time.sleep(1)
        print "stop"
        tracker.stop_track()
        print "stopped"


if __name__ == '__main__':
    import logging
    from logging import StreamHandler
    logging.getLogger().addHandler(StreamHandler())
    sp = TestProcess("test")

    pd = ProcessDirector("t")
    print "before start", pd.is_aspect_work("test")
    sp.start()
    print "after start", pd.is_aspect_work("test")

    time.sleep(5)
    sp.join()

    print "after join", pd.is_aspect_work("test")
    time.sleep(4)
    print "after wait", pd.is_aspect_work("test")
