import rpyc
import uuid
import os
import sys

from rpyc.utils.server import ThreadedServer

DATA_DIR="./data"

class ChunkServerService(rpyc.Service):
  class exposed_ChunkServer():
    blocks = {}

    def exposed_put(self,block_uuid,data,chunk_servers):
      with open(DATA_DIR+str(block_uuid),'w') as f:
        f.write(data)
      if len(chunk_servers)>0:
        self.forward(block_uuid,data,chunk_servers)


    def exposed_get(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      print(block_addr)
      if not os.path.isfile(block_addr):
        print("Path not found")
        return None
      with open(block_addr) as f:
        return f.read()   

    def exposed_delete(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      print(block_addr)
      if not os.path.isfile(block_addr):
        print("Path not found")
        return None
      os.remove(block_addr)
 
    def forward(self,block_uuid,data,chunk_servers):
      print("8888: forwaring to:")
      print(block_uuid, chunk_servers)
      chunk_server=chunk_servers[0]
      chunk_servers=chunk_servers[1:]
      host,port=chunk_server

      con=rpyc.connect(host,port=port)
      chunk_server = con.root.ChunkServer()
      chunk_server.put(block_uuid,data,chunk_servers)


if __name__ == "__main__":
  DATA_DIR = sys.argv[2]
  if not os.path.isdir(DATA_DIR): 
    os.mkdir(DATA_DIR)
    print(DATA_DIR)
  
  port_no = int(sys.argv[1])
  t = ThreadedServer(ChunkServerService, port = port_no)
  t.start()