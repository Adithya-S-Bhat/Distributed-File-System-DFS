import rpyc
import uuid
import threading 
import math
import random
import configparser 
import signal
import pickle
import sys
import os

from rpyc.utils.server import ThreadedServer

class MasterService(rpyc.Service):
  class exposed_Master():
    file2blocks = {}
    block2chunkservers = {}
    chunk_servers = {}

    block_size = 0
    replication_factor = 0

    def __init__(self, backup_interval = 60.0) -> None:
      super().__init__()
      threading.Timer(backup_interval, self.persistent_state).start()

    def persistent_state(self):
      pickle.dump((MasterService.exposed_Master.file2blocks,MasterService.exposed_Master.block2chunkservers), open('fs.img','wb'))

    def read(self,fname):
      mapping = self.file2blocks[fname]
      return mapping

    def write(self,dest,size):
      if self.exists(dest):
        pass

      self.file2blocks[dest]=[]

      num_blocks = self.calc_num_blocks(size)
      blocks = self.alloc_blocks(dest,num_blocks)
      return blocks
    
    def delete(self,fname):
      for block,node_ids in self.file2blocks[fname]:
        if block in self.block2chunkservers:
          del self.block2chunkservers[block]
      del self.file2blocks[fname]

    def exposed_get_file2blocks_entry(self,fname):
      if fname in self.file2blocks:
        return self.file2blocks[fname]
      else:
        return None

    def get_block_size(self):
      return self.block_size

    def get_minions(self):
      return self.chunk_servers

    def calc_num_blocks(self,size):
      return int(math.ceil(float(size)/self.block_size))

    def exists(self,file):
      return file in self.file2blocks

    def alloc_blocks(self,dest,num):
      blocks = []
      for i in range(0,num):
        block_uuid = uuid.uuid1()
        nodes_ids = random.sample(self.chunk_servers.keys(),self.replication_factor)
        blocks.append((block_uuid,nodes_ids))

        self.file2blocks[dest].append((block_uuid,nodes_ids))
        self.block2chunkservers[block_uuid] = nodes_ids

      return blocks


def signal_handler(signal, frame):
  pickle.dump((MasterService.exposed_Master.file2blocks,MasterService.exposed_Master.block2chunkservers), open('fs.img','wb'))
  sys.exit(0)


def set_conf():
  conf = configparser.ConfigParser()
  conf.readfp(open('dfs.conf'))
  MasterService.exposed_Master.block_size = int(conf.get('master','block_size'))
  MasterService.exposed_Master.replication_factor = int(conf.get('master','replication_factor'))
  chunk_servers = conf.get('master','chunk_servers').split(',')
  for m in chunk_servers:
    id,host,port=m.split(":")
    MasterService.exposed_Master.chunk_servers[id]=(host,port)

  if os.path.isfile('fs.img'):
    MasterService.exposed_Master.exposed_Master.file2blocks,MasterService.exposed_Master.block2chunkservers = pickle.load(open('fs.img','rb'))


if __name__ == "__main__":
  set_conf()

  signal.signal(signal.SIGINT,signal_handler)# On Interrupt
  signal.signal(signal.SIGSEGV,signal_handler)# On Segmentation Fault
  signal.signal(signal.SIGABRT,signal_handler)# On Abort 

  t = ThreadedServer(MasterService.exposed_Master, port = 2131)
  t.start()