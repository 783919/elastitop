import subprocess
import os
import sys
import ctypes
import time
import logging
import glob
import psutil

#############################################################################################
BANNER="Elastitop: Ntopng flows massive dumper to Elasticsearch rel. 1.3.2 Corrado Federici (corrado.federici@unibo.it). Times are in GMT"
LOG_FOLDER="./logs"
#############################################################################################
def spawn_process(pcap_file,port,proc_list):
    logging.info("Processing file {0}".format(pcap_file))
    command=["python3"]
    command.append("elastitop.py")
    command.append(pcap_file)
    command.append(port)
    p = subprocess.Popen(command,stderr=subprocess.PIPE)
    #error=p.stderr.readlines()
    #if(len(error)>0):
      #logging.error("An error occurred. {0}".format(error))
    proc_list[port]=p

#############################################################################################
def pick_a_free_port(ports_list):
    for p in ports_list:
        if(ports_list[p]==0):
            ports_list[p]=1
            return p
    return ""
#############################################################################################
def process_pcap_folder(pcap_files_folder):
    av_ports =	{
        #"4000": 0,
        #"4001": 0,
        #"4002": 0,
        #"4003": 0,
        #"4004": 0,
        #"4005": 0,
        #"4006": 0,
        #"4007": 0,
        "4008": 0,
        "4009": 0,
        "4010": 0,
        "4011": 0,
        "4012": 0,
        "4013": 0,
        "4014": 0,
        "4015": 0
    }
    process_list={}
    processed_pcaps=0
    logging.info("Using {0} concurrent ntopng processes".format(len(av_ports)))
    filenames = glob.glob(os.path.join(pcap_files_folder,"*"))
    filenames.sort(key=os.path.getmtime)
    for filename in filenames:
        if filename.endswith(".pcap") or filename.endswith(".pcapng"):
            processed_pcaps+=1
            #logging.info("Processing file {0}".format(filename))
            if(len(process_list)<len(av_ports)):
                port=pick_a_free_port(av_ports)
                if(len(port)==0):
                    logging.error("Internal error. No free ports available")
                    return
                spawn_process(os.path.join(pcap_files_folder,filename),port,process_list)
            else:#max num of processes already spawn. Wait for some to finish
                must_wait=True
                while(must_wait):
                    for id in process_list:
                        curr_proc=process_list[id]
                        if(curr_proc.poll() is not None):
                        #process is over
                            del process_list[id]
                            #free port
                            av_ports[id]=0
                            must_wait=False
                            port=pick_a_free_port(av_ports)
                            if(len(port)==0):
                                logging.error("Internal error. No free ports available")
                                return
                            spawn_process(os.path.join(pcap_files_folder,filename),port,process_list)
                            break
                    if(must_wait == True):
                        time.sleep(1)
        else:
            logging.info("Unexpected format for file. {0}".format(filename))
    logging.info("Processed {0} pcap files in folder {1}. Cleaning up...".format(processed_pcaps,pcap_files_folder))
    must_wait=True
    while(must_wait):
        must_wait=False
        for id in process_list:
            curr_proc=process_list[id]
            if(curr_proc.poll() is None):
                must_wait=True
                time.sleep(1)
                break
    logging.info("Done")
############################################################################################
#main
try:
    if not(os.path.exists(LOG_FOLDER)):
        os.makedirs(LOG_FOLDER)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
        logging.FileHandler(os.path.join(LOG_FOLDER,"logmngr.txt"),'a'),#append is default anyway
        logging.StreamHandler()
    ])
    logging.Formatter.converter = time.gmtime
    logging.info(BANNER)
    if len(sys.argv)!=2:
        raise Exception("Usage: {0} <path to pcap folder>".format(sys.argv[0])) 
    path_to_pcap_folder=sys.argv[1]
    if not(os.path.exists(path_to_pcap_folder)):
        raise Exception("Path {0} is invalid".format(path_to_pcap_folder))
    processName="ntopng"
    for proc in psutil.process_iter():
        if processName in proc.name().lower():
            raise Exception("{0} already running. Please stop it before launching me".format(processName))
    process_pcap_folder(path_to_pcap_folder)
except Exception as ex:
    logging.error("An error occurred. {0}".format(ex.args))


