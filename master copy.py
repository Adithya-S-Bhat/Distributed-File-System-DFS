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
import datetime
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

from rpyc.utils.server import ThreadedServer

class MasterService(rpyc.Service):
  class exposed_Master():
    file2blocks = {}
    block2chunkservers = {}
    chunkserver2heartbeat_time = {}
    chunk_servers = {}
    host2id = {}

    block_size = 0
    replication_factor = 0
    
    class RepeatingTimer(threading.Timer):
      def run(self):
        while not self.finished.is_set():
          self.function(*self.args, **self.kwargs)
          self.finished.wait(self.interval)
    
    def __init__(self, backup_interval = 60.0, threshold_seconds = 15) -> None:
      super().__init__()
      self.threshold_seconds = threshold_seconds
      self.RepeatingTimer(backup_interval, self.persistent_state).start()

      # for val in self.chunk_servers.values():
      #   port = str(val[1])
      #   self.chunkserver2heartbeat_time[val[0]+":"+port] = datetime.datetime.now()
      self.RepeatingTimer(threshold_seconds, self.handle_fail_over).start()


    def persistent_state(self):
      LOG.info("Persistent State is backedup")
      pickle.dump((MasterService.exposed_Master.file2blocks,MasterService.exposed_Master.block2chunkservers), open('fs.img','wb'))

    def exposed_heartbeat(self, host, port):
      # LOG.info(f"Heartbeat received from {port}")
      self.chunkserver2heartbeat_time[host+":"+str(port)] = datetime.datetime.now()

    def write_to_chunkserver(self, block_uuid,data,chunkserver):
      LOG.info("sending: " + str(block_uuid) + str(chunkserver))
      host,port = chunkserver

      con=rpyc.connect(host,port=port)
      chunkserver = con.root.ChunkServer()
      chunkserver.put(block_uuid,data)

    def read_from_chunkserver(self, block_uuid,chunkserver):
      host,port = chunkserver
      con=rpyc.connect(host,port=port)
      chunkserver = con.root.ChunkServer()
      return chunkserver.get(block_uuid)
    
    def get_block_data(self, block):
        for m in [self.get_chunkservers()[_] for _ in block[1]]:
          data = self.read_from_chunkserver(block[0],m)
          if data:
            return data
    
    def handle_fail_over(self):
      current_time = datetime.datetime.now()
      for chunkserver, heartbeat_time in self.chunkserver2heartbeat_time.items():
        if (current_time - heartbeat_time).total_seconds() > self.threshold_seconds:
          LOG.info(f"{chunkserver} has failed")
          # ------ Handling Failover-------
          del self.chunk_servers[self.host2id[chunkserver]]
          # Find the blocks that were contained in failed server and rewrite it
          chunkserver_id = self.host2id[chunkserver]
          for block,chunkservers in self.block2chunkservers.items():
            if chunkserver_id in chunkservers:
                # Allocate block
                block_uuid = uuid.uuid1()
                node_id = random.sample(self.chunk_servers.keys(),1)
                # Find the file to which the block belongs to and modify accordingly
                for file,blocks in self.file2blocks.items():
                  if blocks[1] == block:
                    self.file2blocks[file] = \
                      [block_pair for block_pair in blocks if block_pair[1] is not chunkserver_id]
                    self.file2blocks.append(block_uuid,node_id)

                chunkservers.remove(chunkserver_id)
                chunkservers.append(node_id)
                self.block2chunkservers[block] = chunkservers

                data = self.get_block_data(block)
                self.write_to_chunkserver(block_uuid, data, chunkserver.split(":"))


    def exposed_read(self,fname):
      mapping = self.file2blocks[fname]
      return mapping

    def exposed_write(self,dest,size):
      if self.exists(dest):
        pass

      self.file2blocks[dest]=[]

      num_blocks = self.calc_num_blocks(size)
      blocks = self.alloc_blocks(dest,num_blocks)
      return blocks
    
    def exposed_delete(self,fname):
      for block,node_ids in self.file2blocks[fname]:
        if block in self.block2chunkservers:
          del self.block2chunkservers[block]
      del self.file2blocks[fname]

    def exposed_get_file2blocks_entry(self,fname):
      if fname in self.file2blocks:
        return self.file2blocks[fname]
      else:
        return None

    def exposed_get_block_size(self):
      return self.block_size

    def exposed_get_chunkservers(self):
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
  conf.read_file(open('dfs copy.conf'))
  MasterService.exposed_Master.block_size = int(conf.get('master','block_size'))
  MasterService.exposed_Master.replication_factor = int(conf.get('master','replication_factor'))
  chunk_servers = conf.get('master','chunk_servers').split(',')
  for m in chunk_servers:
    id, host, port = m.split(":")
    MasterService.exposed_Master.chunk_servers[id] = (host, port)
    MasterService.exposed_Master.host2id[host + ":" + str(port)] = id

  if os.path.isfile('fs.img'):
    MasterService.exposed_Master.file2blocks, MasterService.exposed_Master.block2chunkservers = \
      pickle.load(open('fs.img','rb'))


if __name__ == "__main__":
  set_conf()

  signal.signal(signal.SIGINT, signal_handler)# On Interrupt
  signal.signal(signal.SIGSEGV, signal_handler)# On Segmentation Fault
  signal.signal(signal.SIGABRT, signal_handler)# On Abort 

  t = ThreadedServer(MasterService, port = 2131)
  t.start()
