# elastitop
ntopng flows massive uploader to Elasticsearch

If your task is injecting into Elastisearch millions of Ntopng flows scattered in thousands of pcap files this simple project may help you. The combination of Ntopng-Elastisearch-Kibana is beneficial to network analysis with open source tools, but sometimes one may face the issue of dealing with a huge quantity of traffic which slows down the ingestion process to Elasticsearch. Elastitop is made of three python modules:

capmerge.py: aggregates small sized pcaps into a large one according to a user defined size S, say from 20 to 50 GB (there is an hardcoded 100 MB limit). If a pcap il already larger than S it is only renamed. The module spawns at most 8 (also hardcoded) concurrent mergecap (wireshark common) processes to aggregate input pcaps. The use of such an aggregator is optional of course, but could be useful in presence of small sized pcap files. Indeed, when a single flow is split into many more files,  
