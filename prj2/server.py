#!/usr/bin/env python

from kazoo.client import KazooClient
from kazoo.client import KazooState
import os
import cPickle as pickle
import threading


def my_listener(state):
    if state == KazooState.LOST:
        server.counting.clear()
        server.index = -1
    elif state == KazooState.SUSPENDED:
        # pause the thread
        pass
    else:
        # notify the thread
        pass

zk = KazooClient(hosts='bass01,bass02,bass03,bass04,bass05')
zk.start()
zk.add_listener(my_listener)


class Server():
    def __init__(self):
        self.path = "/clu10"
        zk.ensure_path(self.path + "/lock")
        self.lock = zk.Lock(self.path + "/lock")

        self.counting = {}
        self.lists = []
        self.chunk_size = int(0.5 * 1024 * 1024 * 1024)
        self.index = -1

    def count(self, position):
        chunk_size = self.chunk_size - self.chunk_size % 100
        position *= chunk_size
        self.f.seek(position)
        self.lists = self.f.readlines(chunk_size)
        for line in self.lists:
            for char in line[46:98]:
                if char in self.counting:
                    self.counting[char] += 1
                else:
                    self.counting[char] = 1

    def merge(self, dic):
        for char in dic:
            if char in self.counting:
                self.counting[char] += dic[char]
            else:
                self.counting[char] = dic[char]

    def run(self):
        self.getName_thread = threading.Thread(target=self.getName)
        self.getName_thread.start()
        self.getName_thread.join()

        self.server_thread = threading.Thread(target=self.server)
        self.server_thread.start()

    def getName(self):
        while True:
            if zk.exists(os.path.join(self.path, "filename")):
                data, stat = zk.get(os.path.join(self.path, "filename"))
                if data:
                    self.filename = data
                    print self.filename
                    self.f = open(self.filename, 'r')
                    self.num_chunks = (os.stat(self.filename).st_size / self.chunk_size) + 1
                    return

    def server(self):
        while True:
            with self.lock:
                zk.ensure_path(self.path + "/in progress")
                children = zk.get_children(self.path + "/in progress")
                zk.ensure_path(self.path + "/results")
                results = zk.get_children(self.path + "/results")
                if len(results) == self.num_chunks:
                    self.summary(results)
                    return
                for k in range(self.num_chunks):
                    if str(k) not in set(children) | set(results):
                        self.index = k
                        break
                if self.index != -1:
                    zk.create(self.path + "/in progress/" + str(self.index), ephemeral=True)

            if self.index != -1:
                self.count(self.index)
                zk.create(self.path + "/results/" + str(self.index), pickle.dumps(self.counting))
                zk.delete(self.path + "/in progress/" + str(self.index))
                self.counting.clear()
                self.index = -1

    def summary(self, results):
        for child in results:
            data, stat = zk.get(self.path + "/results/" + child)
            count = pickle.loads(data)
            self.merge(count)
        zk.set(self.path, pickle.dumps(self.counting))

if __name__ == "__main__":
    server = Server()
    server.run()