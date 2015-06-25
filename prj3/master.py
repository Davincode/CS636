#!/usr/bin/python
__author__ = 'madmen'

from kazoo.client import KazooClient
import cPickle as pickle
import time
import socket

zk = KazooClient(hosts='bass01, bass02, bass03, bass04, bass05')
zk.start()


class master():

    def __init__(self):
        self.path = "/madmen"
        self.dictionary = {}
        self.replicationFactor = 3
        self.curIndex = 0
        self.curPortNumber = 18000
        self.flag = True
        self.workers = []
        self.worker_port = {}

        zk.ensure_path(self.path + "/master")
        zk.ensure_path(self.path + "/client")
        zk.ensure_path(self.path + "/worker")
        zk.ensure_path(self.path + "/reducer")
        self.watcher = zk.ChildrenWatch(self.path + "/client", func=self.switch)
        self.detect = zk.ChildrenWatch(self.path + "/worker", func=self.reassign)
        self.map_reduce = zk.ChildrenWatch(self.path + "/reducer", func=self.mapReduce)

    def run(self):
        while True:
            if self.flag:
                time.sleep(1)

    def mapReduce(self, children):
        if len(children) == 0:
            return
        self.flag = False

        if children[0] == "count" or children[0] == "sort":
            data, stat = zk.get(self.path + "/reducer/" + children[0])
            filename = data
            if filename not in self.dictionary:
                return

            location_handles = {}
            for chunk in self.dictionary[filename]:
                location = self.dictionary[filename][chunk][0]
                handle = filename + ":" + chunk
                if location in location_handles:
                    location_handles[location].append(handle)
                else:
                    location_handles[location] = [handle]

            workers = zk.get_children(self.path + "/worker")
            workers.sort(key=lambda x: int(x[:x.find(",")]))
            for worker in workers:
                if zk.exists(self.path + "/worker/" + worker):
                    location = worker + ":" + self.worker_port[worker]
                    if children[0] == "count":
                        metaData = pickle.dumps(location_handles[location]) + "count"
                        zk.set(self.path + "/worker/" + worker, metaData)
                        print worker
                    elif children[0] == "sort":
                        metaData = pickle.dumps(location_handles[location]) + "sort"
                        zk.set(self.path + "/worker/" + worker, metaData)

            time.sleep(2)
            zk.delete(self.path + "/reducer/" + children[0])

        self.flag = True

    def reassign(self, children):
        if len(children) == 0:
            return
        self.flag = False
        if len(children) >= len(self.workers):
            self.workers = children
        else:
            failNodes = set(self.workers) - set(children)
            print failNodes

            for filename in zk.get_children(self.path + "/fcl"):
                for chunk in zk.get_children(self.path + "/fcl/" + filename):
                    for location in zk.get_children(self.path + "/fcl/" + filename + "/" + chunk):
                        if location.split(":")[0] in failNodes:
                            zk.delete(self.path + "/fcl/" + filename + "/" + chunk + "/" + location)
            for name in self.dictionary:
                for chunk in self.dictionary[name]:
                    for location in self.dictionary[name][chunk]:
                        if location.split(":")[0] in failNodes:
                            self.dictionary[name][chunk].remove(location)
        self.flag = True

    def switch(self, children):
        if len(children) == 0:
            return
        self.flag = False

        if children[0] == "put":
            data, stat = zk.get(self.path + "/client/put")
            filename = data
            self.dictionary[filename] = {}
            chunks = zk.get_children(self.path + "/fcl/" + filename)
            workers = zk.get_children(self.path + "/worker")
            if len(workers) == 0:
                return
            locations = []
            for worker in workers:
                location = worker + ":" + str(self.curPortNumber)
                self.worker_port[worker] = str(self.curPortNumber)
                self.curPortNumber += 1
                locations.append(location)

            # operate on file_chunk_location subtree, build the dictionary in master, act like job tracker
            for chunk in chunks:
                curList = []
                for i in range(self.replicationFactor):
                    self.curIndex %= len(locations)
                    location = locations[self.curIndex]
                    curList.append(location)
                    zk.create(self.path + "/fcl/" + filename + "/" + chunk + "/" + location)
                    self.curIndex += 1
                self.dictionary[filename][chunk] = curList

            # operate on worker subtree, inform the workers to begin receiving data from client
            for worker in workers:
                if zk.exists(self.path + "/worker/" + worker):
                    print worker
                    print self.worker_port[worker]
                    metaData = pickle.dumps(self.worker_port[worker]) + "put"
                    zk.set(self.path + "/worker/" + worker, metaData)

            # finish the job assigning, inform the client to begin sending data to workers
            time.sleep(3)
            zk.delete(self.path + "/client/put", recursive=True)

        elif children[0] == "get" or children[0] == "cat":
            data, stat = zk.get(self.path + "/client/" + children[0])
            filename = data
            if filename not in self.dictionary:
                return

            location_handles = {}
            for chunk in self.dictionary[filename]:
                location = self.dictionary[filename][chunk][0]
                handle = filename + ":" + chunk
                if location in location_handles:
                    location_handles[location].append(handle)
                else:
                    location_handles[location] = [handle]

            workers = zk.get_children(self.path + "/worker")
            workers.sort(key=lambda x: int(x[:x.find(",")]))
            for worker in workers:
                if zk.exists(self.path + "/worker/" + worker):
                    location = worker + ":" + self.worker_port[worker]
                    if children[0] == "get":
                        metaData = pickle.dumps(location_handles[location]) + "get"
                        zk.set(self.path + "/worker/" + worker, metaData)
                    elif children[0] == "cat":
                        metaData = pickle.dumps(location_handles[location]) + "cat"
                        zk.set(self.path + "/worker/" + worker, metaData)

            time.sleep(2)
            zk.delete(self.path + "/client/" + children[0])

        elif children[0] == "rm":
            data, stat = zk.get(self.path + "/client/rm")
            filename = data
            location_handle = {}
            if filename not in self.dictionary:
                return
            for chunk in self.dictionary[filename]:
                for location in self.dictionary[filename][chunk]:
                    handle = filename + ":" + chunk
                    worker, port_number = location.split(":")
                    if worker in location_handle:
                        location_handle[worker].append(handle)
                    else:
                        location_handle[worker] = [handle]
            for worker in location_handle:
                if zk.exists(self.path + "/worker/" + worker):
                    metaData = pickle.dumps(location_handle[worker]) + "rm"
                    zk.set(self.path + "/worker/" + worker, metaData)
            del self.dictionary[filename]
            zk.delete(self.path + "/client/rm")

        elif children[0] == "mv":
            data, stat = zk.get(self.path + "/client/mv")
            oldName, newName = data.split(",")
            location_handle = {}
            for chunk in self.dictionary[oldName]:
                for location in self.dictionary[oldName][chunk]:
                    oldHandle = oldName + ":" + chunk
                    newHandle = newName + ":" + chunk
                    worker, port_number = location.split(":")
                    item = oldHandle + "," + newHandle
                    if worker in location_handle:
                        location_handle[worker].append(item)
                    else:
                        location_handle[worker] = [item]
            for worker in location_handle:
                if zk.exists(self.path + "/worker/" + worker):
                    metaData = pickle.dumps(location_handle[worker]) + "mv"
                    zk.set(self.path + "/worker/" + worker, metaData)
            self.dictionary[newName] = self.dictionary.pop(oldName)
            zk.delete(self.path + "/client/mv")

        self.flag = True

if __name__ == "__main__":
    master = master()
    master.run()