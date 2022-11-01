import rpyc
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

def send_to_chunkserver(block_uuid,data,chunkservers):
  LOG.info("sending: " + str(block_uuid) + str(chunkservers))
  chunkserver=chunkservers[0]
  chunkservers=chunkservers[1:]
  host,port=chunkserver

  con=rpyc.connect(host,port=port)
  chunkserver = con.root.ChunkServer()
  chunkserver.put(block_uuid,data,chunkservers)


def read_from_chunkserver(block_uuid,chunkserver):
  host,port = chunkserver
  con=rpyc.connect(host,port=port)
  chunkserver = con.root.ChunkServer()
  return chunkserver.get(block_uuid)

def delete_from_chunkserver(block_uuid,chunkserver):
  host,port = chunkserver
  con=rpyc.connect(host,port=port)
  chunkserver = con.root.ChunkServer()
  chunkserver.delete(block_uuid)

def get(master,fname):
  blocks = master.get_file2blocks_entry(fname)
  if not blocks:
    LOG.info("404: File not found")
    return

  for block in blocks:
    for m in [master.get_chunkservers()[_] for _ in block[1]]:
      data = read_from_chunkserver(block[0],m)
      if data:
        sys.stdout.write(data)
        break
    else:
        LOG.info("No blocks found. Possibly a corrupt file")

def put(master,source,dest):
  size = os.path.getsize(source)
  blocks = master.write(dest,size)
  with open(source) as f:
    for b in blocks:
      data = f.read(master.get_block_size())
      block_uuid=b[0]
      chunkservers = [master.get_chunkservers()[_] for _ in b[1]]
      send_to_chunkserver(block_uuid,data,chunkservers)

def delete(master,fname):
  blocks = master.get_file2blocks_entry(fname)
  if not blocks:
    LOG.info("404: File not found")
    return

  master.delete(fname)
  for block in blocks:
    for m in [master.get_chunkservers()[_] for _ in block[1]]:
      delete_from_chunkserver(block[0],m)
    else:
        LOG.info("No blocks found. Possibly a corrupt file")
  print(f"Deleted {fname} successfully")


def main(args):
  con=rpyc.connect("localhost",port=2131)
  master=con.root.Master()
  
  if args[0] == "get":
    get(master,args[1])
  elif args[0] == "put":
    put(master,args[1],args[2])
  elif args[0] == "delete":
    delete(master, args[1])
  else:
    LOG.error("try 'put srcFile destFile OR get file OR delete file'")


if __name__ == "__main__":
  main(sys.argv[1:])