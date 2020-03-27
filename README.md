# elastitop
ntopng flows massive uploader to Elasticsearch
Copyright (c) 2020 corrado federici (corrado.federici@unibo.it)


If your task is injecting into Elastisearch (ES) millions of Ntopng flows scattered in thousands of pcap files this simple project may help you. The combination of Ntopng Elastisearch Kibana is beneficial to network analysis with open source tools, but sometimes one may face the issue of dealing with a huge quantity of traffic which slows down the ingestion process to Elasticsearch. Elastitop is made of three python modules:

- elastitop_mngr.py: sorts pcap files folder according to modification time and spawns a ntopng process (handled by elastitop.py) for each file (up to an hardcoded limit of 8 concurrent processes). Feeding ES in parallel greatly reduces overall time needed to inject flows compared to a sequential approach.

  Usage: python3 elastitop_mngr.py "path to pcap files folder"
  
- capmerge.py: aggregates small sized pcaps into a large one according to a user defined size S, say from 20 to 50 GB (there is an hardcoded 100 MB limit). If a pcap il already larger than S it is only renamed. The module spawns at most 8 (also hardcoded) concurrent mergecap (wireshark common) processes to aggregate input pcaps. The use of capmerge.py is optional before invoking elastitop_mngr.py, but could be useful:
  - to reduce the overall number of spawned ntopng processes, as the shutdown phase (which flushes flows to ES in tested version 3.8) is            time consuming
  - to avoid polluting ES with many almost identical documents, in presence of small sized pcap files that ultimately store the same flow.
  
On the other hand, please consider that if size S overly increases, so does the chance of ES dropping flows  

  Usage: python3 capmerge.py "source pcap files folder" "destination pcap files folder" "size in MB"
 
Tested with: Ntopng rel 3.8.190813 community, Mergecap (Wireshark) 3.0.5 (Git v3.0.5 packaged as 3.0.5-1), Python 3.7.5,
Elastisearch 7.6.1
