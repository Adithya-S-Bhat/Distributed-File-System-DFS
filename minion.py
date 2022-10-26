import rpyc
import uuid
import os
import sys

from rpyc.utils.server import ThreadedServer

DATA_DIR="D:/College/Distributed_Systems/Project/data"

class MinionService(rpyc.Service):
  class exposed_Minion():
    blocks = {}

    def exposed_put(self,block_uuid,data,minions):
      with open(DATA_DIR+str(block_uuid),'w') as f:
        f.write(data)
      if len(minions)>0:
        self.forward(block_uuid,data,minions)


    def exposed_get(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      print(block_addr)
      if not os.path.isfile(block_addr):
        print("Path not found")
      if not os.path.isfile(block_addr):
        return None
      with open(block_addr) as f:
        return f.read()   
 
    def forward(self,block_uuid,data,minions):
      print("8888: forwaring to:")
      print(block_uuid, minions)
      minion=minions[0]
      minions=minions[1:]
      host,port=minion

      con=rpyc.connect(host,port=port)
      minion = con.root.Minion()
      minion.put(block_uuid,data,minions)

    def delete_block(self,uuid):
      pass

if __name__ == "__main__":
  DATA_DIR = sys.argv[2]
  if not os.path.isdir(DATA_DIR): 
    os.mkdir(DATA_DIR)
    print(DATA_DIR)
  
  port_no = int(sys.argv[1])
  t = ThreadedServer(MinionService, port = port_no)
  t.start()