import replicates
class Master:

    def __init__(self, videos, keepers, lv, lk):
        self.videos = videos
        self.keepers = keepers
        self.lv = lv
        self.lk = lk
        self.replicas = replicates.Replicas()

    def run(self):
        pass


def init_master_process(videos, keepers, lv, lk):
    master = Master(videos, keepers, lv, lk)
    master.run()
