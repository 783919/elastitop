import subprocess
import os
import sys
import ctypes
import time
import json
import re
import logging
import requests
import hashlib
import signal
import glob
import shutil
import psutil

#############################################################################################
BANNER="Capmerge : merge cap files up to chosen size rel. 0.0.0 Corrado Federici (corrado.federici@unibo.it). Times are in GMT"
LOG_FOLDER="./logs"
MERGED_FILE_SZ_MB=100
ONE_MB=1024*1024
MAX_NUM_OF_INSTANCES=8
#############################################################################################
def merge_files(proc_list,file_to_merge_list,dest_file):
    while(True):
        if(len(proc_list)<MAX_NUM_OF_INSTANCES):
            break
        for id in proc_list:
            curr_proc=proc_list[id]
            if(curr_proc.poll() is not None):
                del proc_list[id]#process is over
                break
        time.sleep(1)
    command=["mergecap", "-w"]
    command.append(dest_file)
    for item in file_to_merge_list:
        command.append(item)
    p = subprocess.Popen(command,stdout=subprocess.PIPE,stdin=subprocess.PIPE,
        stderr=subprocess.PIPE,shell=False)
    proc_list[p.pid]=p
#############################################################################################
def process_pcap_folder(pcap_source_files_folder,pcap_dest_files_folder,size_in_MB):
    files_to_merge_list=[]
    process_list={}
    processed_pcaps=0
    copied_file_id=0
    merged_file_size=0
    filenames = glob.glob(os.path.join(pcap_source_files_folder,"*"))
    filenames.sort(key=os.path.getmtime)
    for filename in filenames:
        processed_pcaps+=1
        logging.info("Processing file {0}".format(filename))
        size=os.path.getsize(filename)
        if(size/ONE_MB>=size_in_MB):
            copied_file_id+=1
            merge_name="merged-{0}.pcap".format(copied_file_id)
            shutil.copy(filename,os.path.join(pcap_dest_files_folder,merge_name))
            continue
        merged_file_size+=size
        files_to_merge_list.append(filename)
        if(merged_file_size/ONE_MB>=size_in_MB):
            copied_file_id+=1
            merge_name="merged-{0}.pcap".format(copied_file_id)
            merge_files(process_list,files_to_merge_list,os.path.join(pcap_dest_files_folder,merge_name))
            merged_file_size=0
            files_to_merge_list.clear()            
    logging.info("Processed {0} pcap files in folder {1}".format(processed_pcaps,
        pcap_source_files_folder))

############################################################################################
#main
try:
    if not(os.path.exists(LOG_FOLDER)):
        os.makedirs(LOG_FOLDER)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
        logging.FileHandler(os.path.join(LOG_FOLDER,"logcapmerge.txt"),'a'),#append is default anyway
        logging.StreamHandler()
    ])
    logging.Formatter.converter = time.gmtime
    logging.info(BANNER)
    if len(sys.argv)!=4:
        raise Exception("Usage: {0} <path to source pcap folder> <path to dest pcap folder> <size of merged caps in MB>".format(sys.argv[0])) 
    path_to_source_pcap_folder=sys.argv[1]
    if not(os.path.exists(path_to_source_pcap_folder)):
        raise Exception("Path {0} is invalid".format(path_to_source_pcap_folder))
    path_to_dest_pcap_folder=sys.argv[2]
    if not(os.path.exists(path_to_dest_pcap_folder)):
        raise Exception("Path {0} is invalid".format(path_to_dest_pcap_folder))
    size_in_MB=sys.argv[3]
    if (int(size_in_MB)>MERGED_FILE_SZ_MB):
        raise Exception("Merged caps file size needs to less than {0} (MB)".format(MERGED_FILE_SZ_MB))
    logging.info("Chosen merged file size is {0} MB".format(size_in_MB))
    logging.info("Max number of concurrent mergecap processes is {0}".format(MAX_NUM_OF_INSTANCES))
    process_pcap_folder(path_to_source_pcap_folder,path_to_dest_pcap_folder,int(size_in_MB))
except Exception as ex:
    logging.error("An error occurred. {0}".format(ex.args))


