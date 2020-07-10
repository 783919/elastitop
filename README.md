# elastitop
ntopng flows massive uploader to Elasticsearch and report generator.

Current version 1.3.2  July 10,2020

Copyright (c) 2020 corrado federici (corrado.federici@unibo.it)


If you need to get a quick insight into a vast amount of captured traffic this project can help you. Elastitop aggregates pcap files, leverages Ntopng to inject analyzed flows into Elastisearch (ES) then finally generates a report containing detected protocols and applications (e.g. mail, database, social media and so on). There are three python modules:

- elastitop/elastitop_mngr.py: sorts pcap files folder according to modification time and spawns a ntopng process (handled by elastitop.py) for each file (up to an hardcoded limit of 8 concurrent processes). Feeding ES in parallel greatly reduces overall time needed to inject flows compared to a sequential approach.

  Usage: sudo python3 elastitop_mngr.py "path to pcap files folder" (e.g. sudo python3 elastitop_mngr.py "/home/user/caps")
  
- capmerge/capmerge.py: aggregates small sized pcaps into a large one according to a user defined size S, say from 100 to 500 MB for ntopng versions 4+ (there is an hardcoded 1024 MB limit). If a pcap il already larger than S it is only renamed. The module spawns at most 8 (also hardcoded) concurrent mergecap (wireshark common) processes to aggregate input pcaps. The use of capmerge.py is optional before invoking elastitop_mngr.py, but could be useful:
  * to reduce the overall number of spawned ntopng processes, as the shutdown phase (which flushes flows to ES in tested version 3.8) is time consuming
  * to avoid polluting ES with many almost identical documents, in presence of small sized pcap files that ultimately store the same flow.
  
On the other hand, please consider that if size S overly increases, so does the chance of ES dropping flows, even if ntopng rel 4.1 has a default max capacity of 131072 active flows 

  Usage: python3 capmerge.py "source pcap files folder" "destination pcap files folder" "size in MB"
  (e.g. python3 capmerge.py "/home/user/caps-in" "/home/user/caps-merged" "20")

- metrics/es_metrics.py: performs queries on ES to detect the presence known application protocols (as per ntopng protocols definitions). A report with whois and nslookup data of ip addresses is finally generated as CSV file which contains most frequent dns resolutions, top talkers, top http/https hosts, http user-agents (leveraging ngrep), http top urls, communication protocols (e.g. STUN, RTP, WhatsApp/Skype calls), social media Apps (Twitter,Facebook,...) and many other (databases, file transfer, remote management, ..). Protocol detection can be easily enabled/disabled via python code modification (arrays COMM_PROTO and ALLOWED_PROTO)

  Usage: python3 es_metrics.py  "number of top scored records" "path to pcap files folder" (e.g. python3 es_metrics.py  "10" "/home/user/caps")
 
Run each tool in its own folder.

Tested with: Ubuntu 19.04, Ntopng rel 4.1.200507 community, Mergecap (Wireshark) 3.0.5 (Git v3.0.5 packaged as 3.0.5-1), Python 3.7.5, Elastisearch 7.6.1, Ngrep 1.47.1
