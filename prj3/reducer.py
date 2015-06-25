#!/usr/bin/python
__author__ = 'madmen'

from kazoo.client import KazooClient
import socket
import cPickle as pickle
import sys
import time

zk = KazooClient(hosts='bass01, bass02, bass03, bass04, bass05')
zk.start()


class Reducer:

    def __init__(self, command):
        self.command = command
        self.path = "/madmen"
        self.hostname = socket.gethostname()
        self.map = {}
        self.counting = {}

        self.reducerPath = self.path + "/reducer"
        if zk.exists(self.reducerPath):
            zk.set(self.reducerPath, self.hostname)
        else:
            zk.create(self.reducerPath, self.hostname)

    def run(self):
        if self.command[0] == "count":
            if not self.checkArg(self.command, 2):
                return
            filename = self.command[1]
            if not zk.exists(self.path + "/fcl/" + filename):
                return

            path = zk.create(self.path + "/reducer/count", filename)

            while zk.exists(path):
                time.sleep(1)

            worker_port = {}
            for filename in zk.get_children(self.path + "/fcl"):
                for chunk in zk.get_children(self.path + "/fcl/" + filename):
                    for location in zk.get_children(self.path + "/fcl/" + filename + "/" + chunk):
                        worker, port_number = location.split(":")
                        if worker not in worker_port:
                            worker_port[worker] = port_number

            print worker_port

            failWorkers = []
            workers = zk.get_children(self.path + "/worker")
            workers.sort(key=lambda x: int(x[:x.find(",")]))
            for worker in workers:
                try:
                    print worker
                    print worker_port[worker]
                    ip_address = worker.split(",")[1]
                    self.count(ip_address, worker_port[worker])
                except socket.timeout:
                    failWorkers.append(worker)
                    print "timeout"

            if len(failWorkers) != 0:
                self.counting.clear()
            else:
                for key in sorted(self.counting):
                    print key, self.counting[key]

        elif self.command[0] == "sort":
            if not self.checkArg(self.command, 3):
                return
            filename = self.command[1]
            if not zk.exists(self.path + "/fcl/" + filename):
                return
            localFilename = self.command[2]

            path = zk.create(self.path + "/reducer/sort", filename)

            while zk.exists(path):
                time.sleep(1)

            worker_port = {}
            for filename in zk.get_children(self.path + "/fcl"):
                for chunk in zk.get_children(self.path + "/fcl/" + filename):
                    for location in zk.get_children(self.path + "/fcl/" + filename + "/" + chunk):
                        worker, port_number = location.split(":")
                        if worker not in worker_port:
                            worker_port[worker] = port_number

            print worker_port

            failWorkers = []
            workers = zk.get_children(self.path + "/worker")
            workers.sort(key=lambda x: int(x[:x.find(",")]))
            for worker in workers:
                try:
                    print worker
                    print worker_port[worker]
                    ip_address = worker.split(",")[1]
                    self.sort(ip_address, worker_port[worker])
                except socket.error:
                    failWorkers.append(worker)
                    print "node fails"

            if len(failWorkers) != 0:
                self.map.clear()
            else:
                self.write(localFilename)

    def sort(self, ip_address, port_number):
        s = socket.socket()
        s.connect((ip_address, int(port_number)))

        chunk = ""
        line = s.recv(4096)
        while line:
            chunk += line
            line = s.recv(4096)
        meta = pickle.loads(chunk)
        for elem in meta:
            self.map[elem[0]] = elem[1]
        s.close()

    def write(self, localFilename):
        f = open(localFilename, "w")
        for key in sorted(self.map):
            f.write(self.map[key])
        f.close()

    def count(self, ip_address, port_number):
        s = socket.socket()
        s.connect((ip_address, int(port_number)))

        chunk = ""
        line = s.recv(4096)
        while line:
            chunk += line
            line = s.recv(4096)
        meta = pickle.loads(chunk)
        for word in meta:
            if word in self.counting:
                self.counting[word] += meta[word]
            else:
                self.counting[word] = meta[word]
        s.close()

    def checkArg(self, command, number):
        return len(command) == number

if __name__ == "__main__":
    if len(sys.argv) == 0:
        sys.exit()
    reducer = Reducer(sys.argv[1:])
    reducer.run()