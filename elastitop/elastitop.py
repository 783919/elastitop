import subprocess
import os
import sys
import ctypes
import time
import logging
import signal

#############################################################################################
ES_INDEX="ntopng-%Y.%m.%d"
ES_CONNECT="http://localhost:9200/_bulk"
LOG_FOLDER="./logs"
#############################################################################################
def process_pcap_file(filename,port):
  if filename.endswith(".pcap") or filename.endswith(".pcapng"):
    logging.info("Feeding ntopng with file {0}".format(filename))
    es_string="es;doc;{0};{1};".format(ES_INDEX,ES_CONNECT)
    command=["ntopng", "-n", "1", "-w"]
    command.append(port)
    command.append("-i")
    command.append(filename)
    command.append("-F")
    command.append(es_string)
    p = subprocess.Popen(command,stdout=subprocess.PIPE,stdin=subprocess.PIPE,
      stderr=subprocess.PIPE,shell=False)
    while p.poll() is None:
      line=p.stdout.readline().decode()
      logging.info(line)
      if "Terminated packet polling" in line:
        time.sleep(1)
        os.kill(p.pid,signal.SIGINT)
      time.sleep(.2)
    error=p.stderr.readlines()
    if(len(error)>0):
      logging.error("An error occurred. {0}".format(error))
    logging.debug("output is {0}".format(p.stdout.readlines()))
    logging.info("Done processing file {0}".format(filename))


############################################################################################
#main
try:
  if len(sys.argv)!=3:
    raise Exception("Usage: {0} <path to pcap file> <ntopng http port>".format(sys.argv[0])) 
  path_to_pcap_file=sys.argv[1]
  if not(os.path.exists(path_to_pcap_file)):
    raise Exception("Path {0} is invalid".format(path_to_pcap_file))
  port=sys.argv[2]
  if not(os.path.exists(LOG_FOLDER)):
    os.makedirs(LOG_FOLDER)
  logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
      logging.FileHandler(
        os.path.join(LOG_FOLDER,
        "log-{0}.txt".format(os.path.basename(path_to_pcap_file))),'a'),#append is default anyway
      logging.StreamHandler()
    ])
  logging.Formatter.converter = time.gmtime
  process_pcap_file(path_to_pcap_file,port)
except Exception as ex:
  logging.error("An error occurred. {0}".format(ex.args))


