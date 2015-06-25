#!/usr/bin/python
__author__ = 'madmen'

from kazoo.client import KazooClient
import os
import sys
import socket
import time
import cPickle as pickle

zk = KazooClient(hosts='bass01, bass02, bass03, bass04, bass05')
zk.start()


class client():
    def __init__(self, command):
        self.command = command
        self.path = "/madmen"
        self.hostname = socket.gethostname()
        self.replicationFactor = 3
        chunk_size = 8 * 1024 * 1024
        self.chunk_size = int(chunk_size - chunk_size % 100)
        self.chunk_data = {}

        zk.ensure_path(self.path + "/fcl")
        self.clientPath = self.path + "/client"
        if zk.exists(self.clientPath):
            zk.set(self.clientPath, self.hostname)
        else:
            zk.create(self.clientPath, self.hostname)

    def run(self):
        if self.command[0] == "put":
            if not self.checkArg(self.command, 2):
                return
            filename = self.command[1]
            num_chunks = (os.stat(filename).st_size / self.chunk_size) + 1
            zk.create(self.path + "/fcl/" + filename)
            for index in range(num_chunks):
                zk.create(self.path + "/fcl/" + filename + "/chunk" + str(index))

            # inform the master to begin assigning tasks
            path = zk.create(self.path + "/client/put", filename)

            # wait for workers to be ready for receiving data
            while zk.exists(path):
                time.sleep(1)

            location_handle = {}
            chunks = zk.get_children(self.path + "/fcl/" + filename)
            chunks.sort(key=lambda x: int(x[x.find("k")+1:]))
            for chunk in chunks:
                handle = filename + ":" + chunk
                locations = zk.get_children(self.path + "/fcl/" + filename + "/" + chunk)
                for location in locations:
                    if location in location_handle:
                        location_handle[location].append(handle)
                    else:
                        location_handle[location] = [handle]

            handle_data = {}
            f = open(filename, "r")
            for chunk in chunks:
                handle = filename + ":" + chunk
                handle_data[handle] = f.read(self.chunk_size)
            f.close()

            # send data to workers
            failLocation = []
            for location in location_handle:
                socketData = {}
                for handle in location_handle[location]:
                    socketData[handle] = handle_data[handle]
                worker, port_number = location.split(":")
                ip_address = worker.split(",")[1]
                try:
                    self.send(pickle.dumps(socketData), ip_address, port_number)
                except socket.timeout:
                    failLocation.append(location)
                    print "timeout"

        elif self.command[0] == "get":
            if not self.checkArg(self.command, 3):
                return
            if not zk.exists(self.path + "/fcl/" + self.command[1]):
                return
            filename = self.command[1]
            localFilename = self.command[2]

            path = zk.create(self.path + "/client/get", filename)

            while zk.exists(path):
                time.sleep(1)

            worker_port = {}
            for filename in zk.get_children(self.path + "/fcl"):
                for chunk in zk.get_children(self.path + "/fcl/" + filename):
                    locations = zk.get_children(self.path + "/fcl/" + filename + "/" + chunk)
                    if len(locations) == 0:
                        print "completely lose this data"
                        return
                    for location in locations:
                        worker, port_number = location.split(":")
                        if worker not in worker_port:
                            worker_port[worker] = port_number

            failWorkers = []
            workers = zk.get_children(self.path + "/worker")
            workers.sort(key=lambda x: int(x[:x.find(",")]))
            for worker in workers:
                try:
                    print worker
                    print worker_port[worker]
                    ip_address = worker.split(",")[1]
                    self.receive(ip_address, worker_port[worker])
                except socket.error:
                    failWorkers.append(worker)
                    print "node fails"

            if len(failWorkers) != 0:
                self.chunk_data.clear()
                #self.run()
            else:
                self.write(localFilename)

        elif self.command[0] == "ls":
            for filename in zk.get_children(self.path + "/fcl"):
                print filename

        elif self.command[0] == "cat":
            if not self.checkArg(self.command, 2):
                return
            filename = self.command[1]
            if not zk.exists(self.path + "/fcl/" + filename):
                return

            path = zk.create(self.path + "/client/cat", filename)

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

            workers = zk.get_children(self.path + "/worker")
            workers.sort(key=lambda x: int(x[:x.find(",")]))
            for worker in workers:
                try:
                    print worker
                    print worker_port[worker]
                    ip_address = worker.split(",")[1]
                    self.cat(ip_address, worker_port[worker])
                except socket.timeout:
                    print "timeout"

        elif self.command[0] == "rm":
            if not self.checkArg(self.command, 2):
                return
            filename = self.command[1]
            zk.delete(self.path + "/fcl/" + filename, recursive=True)
            zk.create(self.path + "/client/rm", filename)

        elif self.command[0] == "mv":
            if not self.checkArg(self.command, 3):
                return
            oldName = self.command[1]
            newName = self.command[2]
            chunk_location = {}
            if zk.exists(self.path + "/fcl/" + oldName):
                chunks = zk.get_children(self.path + "/fcl/" + oldName)
                if len(chunks) == 0:
                    return
                for chunk in chunks:
                    locations = zk.get_children(self.path + "/fcl/" + oldName + "/" + chunk)
                    if locations:
                        chunk_location[chunk] = locations
                zk.delete(self.path + "/fcl/" + oldName, recursive=True)
                zk.create(self.path + "/fcl/" + newName)
            for chunk in chunk_location:
                zk.create(self.path + "/fcl/" + newName + "/" + chunk)
                for location in chunk_location[chunk]:
                    zk.create(self.path + "/fcl/" + newName + "/" + chunk + "/" + location)
            zk.create(self.path + "/client/mv", oldName + "," + newName)

    def checkArg(self, command, number):
        return len(command) == number

    def receive(self, ip_address, port_number):
        s = socket.socket()
        s.connect((ip_address, int(port_number)))

        chunk = ""
        line = s.recv(4096)
        while line:
            chunk += line
            line = s.recv(4096)
        meta = pickle.loads(chunk)
        for key in meta:
            self.chunk_data[key] = meta[key]
            print key
        s.close()

    def send(self, chunk, ip_address, port_number):
        s = socket.socket()
        s.connect((ip_address, int(port_number)))
        s.sendall(chunk)
        s.sendall("")
        s.close()

    def cat(self, ip_address, port_number):
        s = socket.socket()
        s.connect((ip_address, int(port_number)))

        chunk = ""
        line = s.recv(4096)
        while line:
            chunk += line
            line = s.recv(4096)
        meta = pickle.loads(chunk)
        for key in meta:
            print key
        s.close()

    def write(self, localFilename):
        f = open(localFilename, "w")
        for chunk in sorted(self.chunk_data, key=lambda x: int(x[x.rfind("k")+1:])):
            print chunk
            f.write(self.chunk_data[chunk])
        f.close()

if __name__ == "__main__":
    if len(sys.argv) == 0:
        sys.exit()
    client = client(sys.argv[1:])
    client.run()