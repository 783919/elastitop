import subprocess
import os
import sys
import ctypes
import time
import logging
import signal
import shutil

#############################################################################################
ES_INDEX="ntopng-%Y.%m.%d"
ES_CONNECT="http://localhost:9200/_bulk"
LOG_FOLDER="./logs"
#############################################################################################
def process_pcap_file(filename,port):
  if filename.endswith(".pcap") or filename.endswith(".pcapng"):
    logging.info("Feeding ntopng with file {0}".format(filename))
    es_string="es;doc;{0};{1};".format(ES_INDEX,ES_CONNECT)
    #es_string="es;doc;{0}-{1};{2};".format(ES_INDEX,os.getpid(),ES_CONNECT)
    command=["ntopng", "-n", "1", "-w"]
    command.append(port)
    work_dir="ntopng-work-dir-{0}".format(os.getpid())
    os.mkdir(work_dir)
    command.append("-d")
    command.append(work_dir)
    command.append("-i")
    command.append(filename)
    command.append("-F")
    command.append(es_string)
    p = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=False)
    timeout=25
    while p.poll() is None:
      line=p.stdout.readline().decode()
      logging.info(line)
      if "Welcome to ntopng" in line:
        # it appears that ver >=3.9 are lazier in flushing flows onto ES. Let's allow some more time
        maj_ver=line.split("v.")[1][0]
        maj_ver_num=int(maj_ver)
        min_ver=line.split("v.")[1][2]
        min_ver_num=int(min_ver)
        if maj_ver_num <3:
            timeout=0
        elif maj_ver_num ==3:
          if min_ver_num <=8:
            timeout=0
        logging.info(
          "Ntopng major version is {0}. Wait before shutdown will be {1} seconds".format(maj_ver_num,timeout))
      elif "Terminated packet polling" in line:
        time.sleep(5)
        while timeout > 0:
          timeout-=1
          time.sleep(1)
        os.kill(p.pid,signal.SIGINT)
      time.sleep(.2)
    error=p.stderr.readlines()
    if(len(error)>0):
      logging.error("An error occurred. {0}".format(error))
    shutil.rmtree(work_dir,ignore_errors=True)
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


