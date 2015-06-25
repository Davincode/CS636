#!/usr/bin/env python

import sys
from kazoo.client import KazooClient
import cPickle as pickle
import time

zk = KazooClient(hosts='bass01,bass02,bass03,bass04,bass05')
zk.start()


class Client():
    def __init__(self, filename):
        self.filename = filename
        self.path = "/clu10"
        zk.ensure_path(self.path)
        self.watcher = zk.DataWatch(self.path, func=self.watch_node)
        self.flag = True
        self.breaker = False

    def run(self):
        zk.create(self.path + "/filename", self.filename)
        while True:
            if self.flag:
                time.sleep(1)
            if self.breaker:
                break

    def watch_node(self, data, stat):
        self.flag = False
        if data:
            print pickle.loads(data)
            self.breaker = True
        else:
            self.flag = True

if __name__ == "__main__":
    filename = sys.argv[-1]
    client = Client(filename)
    client.run()