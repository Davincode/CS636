#!/usr/bin/python
__author__ = 'madmen'

from kazoo.client import KazooClient
from kazoo.client import KazooState
import socket
import cPickle as pickle
import time
import threading
import string
import logging


def my_listener(state):
    if state == KazooState.LOST:
        worker.dictionary.clear()
    elif state == KazooState.SUSPENDED:
        # pause the thread
        pass
    else:
        # notify the thread
        pass

zk = KazooClient(hosts='bass01, bass02, bass03, bass04, bass05')
zk.start()
zk.add_listener(my_listener)


class worker():

    def __init__(self):
        self.path = "/madmen"
        self.dictionary = {}
        self.hostname = socket.gethostname()
        self.flag = True

        zk.ensure_path(self.path + "/worker")
        self.id = str(len(zk.get_children(self.path + "/worker")) + 1) + "," + self.hostname
        self.workerPath = self.path + "/worker/" + self.id
        zk.create(self.workerPath, ephemeral=True)
        self.watcher = zk.DataWatch(self.workerPath, func=self.switch)

        logging.basicConfig()

    def run(self):
        while True:
            if self.flag:
                time.sleep(1)

    def switch(self, data, stat):
        if not data:
            return
        self.flag = False

        if data[-3:] == "put":
            metaData = pickle.loads(data[:-3])
            self.port_number = metaData
            try:
                self.receive(self.port_number)
            except socket.timeout:
                print "timeout"

        elif data[-3:] == "get" or data[-3:] == "cat":
            metaData = pickle.loads(data[:-3])
            print metaData
            handles = metaData
            try:
                print self.port_number
                self.send(handles, self.port_number)
            except socket.error:
                print "socket fail"
                self.dictionary.clear()

        elif data[-4:] == "sort":
            metaData = pickle.loads(data[:-4])
            print metaData
            handles = metaData
            try:
                print self.port_number
                self.sort(handles, self.port_number)
            except socket.error:
                print "socket fail"
                self.dictionary.clear()

        elif data[-5:] == "count":
            metaData = pickle.loads(data[:-5])
            print metaData
            handles = metaData
            try:
                self.count(handles, self.port_number)
            except socket.timeout:
                print "timeout"
                self.dictionary.clear()

        elif data[-2:] == "rm":
            metaData = pickle.loads(data[:-2])
            for meta in metaData:
                handle = meta
                self.remove(handle)

        elif data[-2:] == "mv":
            metaData = pickle.loads(data[:-2])
            for meta in metaData:
                oldHandle, newHandle = meta.split(",")
                self.rename(oldHandle, newHandle)

        self.flag = True

    def receive(self, port_number):
        s = socket.socket()
        s.bind((self.hostname, int(port_number)))
        s.listen(5)

        c, address = s.accept()
        chunk = ""
        line = c.recv(4096)
        while line:
            chunk += line
            line = c.recv(4096)

        meta = pickle.loads(chunk)
        for key in meta:
            self.dictionary[key] = meta[key]
            print "receive one chunk"
        c.close()
        s.close()

    def send(self, handles, port_number):
        s = socket.socket()
        s.bind((self.hostname, int(port_number)))
        s.listen(5)
        c, address = s.accept()

        handle_data = {}
        for handle in handles:
            if handle in self.dictionary:
                handle_data[handle] = self.dictionary[handle]
        meta = pickle.dumps(handle_data)

        c.sendall(meta)
        c.sendall("")
        c.close()

    def sort(self, handles, port_number):
        s = socket.socket()
        s.bind((self.hostname, int(port_number)))
        s.listen(5)
        c, address = s.accept()

        mapResult = []
        for handle in handles:
            if handle in self.dictionary:
                for line in self.helper(self.dictionary[handle]):
                    mapResult.append((line[0:10], line))

        c.sendall(pickle.dumps(mapResult))
        c.sendall("")
        c.close()

    def remove(self, handle):
        if handle in self.dictionary:
            del self.dictionary[handle]
        print "remove one chunk"

    def rename(self, oldHandle, newHandle):
        if oldHandle in self.dictionary:
            self.dictionary[newHandle] = self.dictionary[oldHandle]
            del self.dictionary[oldHandle]
        print "rename one chunk"

    def count(self, handles, port_number):
        s = socket.socket()
        s.bind((self.hostname, int(port_number)))
        s.listen(5)
        c, address = s.accept()

        mapResult = {}
        for handle in handles:
            if handle in self.dictionary:
                for word in self.dictionary[handle].split():
                    word = word.strip(string.punctuation)
                    word = word.lower()
                    if len(word) < 20:
                        #mapResult.append((word, 1))
                        if word in mapResult:
                            mapResult[word] += 1
                        else:
                            mapResult[word] = 1

        c.sendall(pickle.dumps(mapResult))
        c.sendall("")
        c.close()

    def helper(self, chunk):
        lines = []
        cursor = 0
        while cursor < len(chunk):
            line = chunk[cursor: cursor + 100]
            lines.append(line)
            cursor += 100
        return lines


if __name__ == "__main__":
    worker = worker()
    worker.run()