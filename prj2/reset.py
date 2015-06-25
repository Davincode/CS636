__author__ = 'clu10'

from kazoo.client import KazooClient

zk = KazooClient(hosts='bass01, bass02, bass03, bass04, bass05')
zk.start()


def t():
    zk.delete("/clu10", recursive=True)

if __name__ == "__main__":
    t()