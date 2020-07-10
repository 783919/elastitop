import subprocess
import os
import sys
import ctypes
import time
import json
import logging
import requests
import socket
#import glob

#############################################################################################
BANNER="Es metrics : calculate reports on Elasticsearch rel. 1.3.2 Corrado Federici (corrado.federici@unibo.it). Times are in GMT"
ES_CONNECT="http://localhost:9200/ntopng-*/_search"
LOG_FOLDER="./logs"
REPORT_FOLDER="./reports"
REPORT_FNAME="report.csv"
REPORT_SPACE_HDR="***\n"
MAX_HITS=100
REPORT_COL_SEP=","
NTOPNG_MAJ_VER=4
#
NDPI_PROTOS={
#"NDPI_PROTOCOL_UNKNOWN":"0",
"NDPI_PROTOCOL_FTP_CONTROL":"1", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_MAIL_POP":"2",
"NDPI_PROTOCOL_MAIL_SMTP":"3",
"NDPI_PROTOCOL_MAIL_IMAP":"4",
"NDPI_PROTOCOL_DNS":"5",
"NDPI_PROTOCOL_IPP":"6",
"NDPI_PROTOCOL_HTTP":"7",
"NDPI_PROTOCOL_MDNS":"8",
"NDPI_PROTOCOL_NTP":"9",
"NDPI_PROTOCOL_NETBIOS":"10",
"NDPI_PROTOCOL_NFS":"11",
"NDPI_PROTOCOL_SSDP":"12",
"NDPI_PROTOCOL_BGP":"13",
"NDPI_PROTOCOL_SNMP":"14",
"NDPI_PROTOCOL_XDMCP":"15",
"NDPI_PROTOCOL_SMBV1":"16", # SMB version 1 */
"NDPI_PROTOCOL_SYSLOG":"17",
"NDPI_PROTOCOL_DHCP":"18",
"NDPI_PROTOCOL_POSTGRES":"19",
"NDPI_PROTOCOL_MYSQL":"20",
"NDPI_PROTOCOL_HOTMAIL":"21",
"NDPI_PROTOCOL_DIRECT_DOWNLOAD_LINK":"22",
"NDPI_PROTOCOL_MAIL_POPS":"23",
"NDPI_PROTOCOL_APPLEJUICE":"24",
"NDPI_PROTOCOL_DIRECTCONNECT":"25",
"NDPI_PROTOCOL_NTOP":"26",
"NDPI_PROTOCOL_COAP":"27",
"NDPI_PROTOCOL_VMWARE":"28",
"NDPI_PROTOCOL_MAIL_SMTPS":"29",
"NDPI_PROTOCOL_FBZERO":"30",
"NDPI_PROTOCOL_UBNTAC2":"31", # Ubiquity UBNT AirControl":"2 - Thomas Fjellstrom <thomas+"NDPI@fjellstrom.ca> */
"NDPI_PROTOCOL_KONTIKI":"32",
"NDPI_PROTOCOL_OPENFT":"33",
"NDPI_PROTOCOL_FASTTRACK":"34",
"NDPI_PROTOCOL_GNUTELLA":"35",
"NDPI_PROTOCOL_EDONKEY":"36", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_BITTORRENT":"37",
"NDPI_PROTOCOL_SKYPE_CALL":"38", # Skype call and videocalls */
"NDPI_PROTOCOL_SIGNAL":"39",
"NDPI_PROTOCOL_MEMCACHED":"40", # Memcached - Darryl Sokoloski <darryl@egloo.ca> */
"NDPI_PROTOCOL_SMBV23":"41", # SMB version 2/3 */
"NDPI_PROTOCOL_MINING":"42", # Bitcoin", Ethereum", ZCash", Monero */
"NDPI_PROTOCOL_NEST_LOG_SINK":"43", # Nest Log Sink (Nest Protect) - Darryl Sokoloski <darryl@egloo.ca> */
"NDPI_PROTOCOL_MODBUS":"44", # Modbus */
"NDPI_PROTOCOL_WHATSAPP_CALL":"45", # WhatsApp video ad audio calls go here */
"NDPI_PROTOCOL_DATASAVER":"46", # Protocols used to save data on Internet communications */
"NDPI_PROTOCOL_XBOX":"47",
"NDPI_PROTOCOL_QQ":"48",
"NDPI_PROTOCOL_TIKTOK":"49",
"NDPI_PROTOCOL_RTSP":"50",
"NDPI_PROTOCOL_MAIL_IMAPS":"51",
"NDPI_PROTOCOL_ICECAST":"52",
"NDPI_PROTOCOL_PPLIVE":"53", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_PPSTREAM":"54",
"NDPI_PROTOCOL_ZATTOO":"55",
"NDPI_PROTOCOL_SHOUTCAST":"56",
"NDPI_PROTOCOL_SOPCAST":"57",
"NDPI_PROTOCOL_TVANTS":"58",
"NDPI_PROTOCOL_TVUPLAYER":"59",
"NDPI_PROTOCOL_HTTP_DOWNLOAD":"60",
"NDPI_PROTOCOL_QQLIVE":"61",
"NDPI_PROTOCOL_THUNDER":"62",
"NDPI_PROTOCOL_SOULSEEK":"63",
"NDPI_PROTOCOL_PS_VUE":"64",
"NDPI_PROTOCOL_IRC":"65",
"NDPI_PROTOCOL_AYIYA":"66",
"NDPI_PROTOCOL_UNENCRYPTED_JABBER":"67",
"NDPI_PROTOCOL_MSN":"68",
"NDPI_PROTOCOL_OSCAR":"69",
"NDPI_PROTOCOL_YAHOO":"70",
"NDPI_PROTOCOL_BATTLEFIELD":"71",
"NDPI_PROTOCOL_GOOGLE_PLUS":"72",
"NDPI_PROTOCOL_IP_VRRP":"73",
"NDPI_PROTOCOL_STEAM":"74", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_HALFLIFE2":"75",
"NDPI_PROTOCOL_WORLDOFWARCRAFT":"76",
"NDPI_PROTOCOL_TELNET":"77",
"NDPI_PROTOCOL_STUN":"78",
"NDPI_PROTOCOL_IP_IPSEC":"79",
"NDPI_PROTOCOL_IP_GRE":"80",
"NDPI_PROTOCOL_IP_ICMP":"81",
"NDPI_PROTOCOL_IP_IGMP":"82",
"NDPI_PROTOCOL_IP_EGP":"83",
"NDPI_PROTOCOL_IP_SCTP":"84",
"NDPI_PROTOCOL_IP_OSPF":"85",
"NDPI_PROTOCOL_IP_IP_IN_IP":"86",
"NDPI_PROTOCOL_RTP":"87",
"NDPI_PROTOCOL_RDP":"88",
"NDPI_PROTOCOL_VNC":"89",
"NDPI_PROTOCOL_PCANYWHERE":"90",
"NDPI_PROTOCOL_TLS":"91",
"NDPI_PROTOCOL_SSH":"92",
"NDPI_PROTOCOL_USENET":"93",
"NDPI_PROTOCOL_MGCP":"94",
"NDPI_PROTOCOL_IAX":"95",
"NDPI_PROTOCOL_TFTP":"96",
"NDPI_PROTOCOL_AFP":"97",
"NDPI_PROTOCOL_STEALTHNET":"98",
"NDPI_PROTOCOL_AIMINI":"99",
"NDPI_PROTOCOL_SIP":"100",
"NDPI_PROTOCOL_TRUPHONE":"101",
"NDPI_PROTOCOL_IP_ICMPV6":"102",
"NDPI_PROTOCOL_DHCPV6":"103",
"NDPI_PROTOCOL_ARMAGETRON":"104",
"NDPI_PROTOCOL_CROSSFIRE":"105",
"NDPI_PROTOCOL_DOFUS":"106",
"NDPI_PROTOCOL_FIESTA":"107",
"NDPI_PROTOCOL_FLORENSIA":"108",
"NDPI_PROTOCOL_GUILDWARS":"109",
"NDPI_PROTOCOL_HTTP_ACTIVESYNC":"110",
"NDPI_PROTOCOL_KERBEROS":"111",
"NDPI_PROTOCOL_LDAP":"112",
"NDPI_PROTOCOL_MAPLESTORY":"113",
"NDPI_PROTOCOL_MSSQL_TDS":"114",
"NDPI_PROTOCOL_PPTP":"115",
"NDPI_PROTOCOL_WARCRAFT3":"116",
"NDPI_PROTOCOL_WORLD_OF_KUNG_FU":"117",
"NDPI_PROTOCOL_SLACK":"118",
"NDPI_PROTOCOL_FACEBOOK":"119",
"NDPI_PROTOCOL_TWITTER":"120",
"NDPI_PROTOCOL_DROPBOX":"121",
"NDPI_PROTOCOL_GMAIL":"122",
"NDPI_PROTOCOL_GOOGLE_MAPS":"123",
"NDPI_PROTOCOL_YOUTUBE":"124",
"NDPI_PROTOCOL_SKYPE":"125",
"NDPI_PROTOCOL_GOOGLE":"126",
"NDPI_PROTOCOL_DCERPC":"127",
"NDPI_PROTOCOL_NETFLOW":"128",
"NDPI_PROTOCOL_SFLOW":"129",
"NDPI_PROTOCOL_HTTP_CONNECT":"130",
"NDPI_PROTOCOL_HTTP_PROXY":"131",
"NDPI_PROTOCOL_CITRIX":"132", # It also includes the old "NDPI_PROTOCOL_CITRIX_ONLINE */
"NDPI_PROTOCOL_NETFLIX":"133",
"NDPI_PROTOCOL_LASTFM":"134",
"NDPI_PROTOCOL_WAZE":"135",
"NDPI_PROTOCOL_YOUTUBE_UPLOAD":"136", # Upload files to youtube */
"NDPI_PROTOCOL_HULU":"137",
"NDPI_PROTOCOL_CHECKMK":"138",
"NDPI_PROTOCOL_AJP":"139", # Leonn Paiva <leonn.paiva@gmail.com> */
"NDPI_PROTOCOL_APPLE":"140",
"NDPI_PROTOCOL_WEBEX":"141",
"NDPI_PROTOCOL_WHATSAPP":"142",
"NDPI_PROTOCOL_APPLE_ICLOUD":"143",
"NDPI_PROTOCOL_VIBER":"144",
"NDPI_PROTOCOL_APPLE_ITUNES":"145",
"NDPI_PROTOCOL_RADIUS":"146",
"NDPI_PROTOCOL_WINDOWS_UPDATE":"147",
"NDPI_PROTOCOL_TEAMVIEWER":"148", # xplico.org */
"NDPI_PROTOCOL_TUENTI":"149",
"NDPI_PROTOCOL_LOTUS_NOTES":"150",
"NDPI_PROTOCOL_SAP":"151",
"NDPI_PROTOCOL_GTP":"152",
"NDPI_PROTOCOL_UPNP":"153",
"NDPI_PROTOCOL_LLMNR":"154",
"NDPI_PROTOCOL_REMOTE_SCAN":"155",
"NDPI_PROTOCOL_SPOTIFY":"156",
"NDPI_PROTOCOL_MESSENGER":"157",
"NDPI_PROTOCOL_H323":"158", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_OPENVPN":"159", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_NOE":"160", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_CISCOVPN":"161", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_TEAMSPEAK":"162", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_TOR":"163", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_SKINNY":"164", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_RTCP":"165", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_RSYNC":"166", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_ORACLE":"167", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_CORBA":"168", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_UBUNTUONE":"169", # Remy Mudingay <mudingay@ill.fr> */
"NDPI_PROTOCOL_WHOIS_DAS":"170",
"NDPI_PROTOCOL_COLLECTD":"171",
"NDPI_PROTOCOL_SOCKS":"172", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_NINTENDO":"173",
"NDPI_PROTOCOL_RTMP":"174", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_FTP_DATA":"175", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_WIKIPEDIA":"176", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_ZMQ":"177",
"NDPI_PROTOCOL_AMAZON":"178", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_EBAY":"179", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_CNN":"180", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_MEGACO":"181", # Gianluca Costa <g.costa@xplico.org> */
"NDPI_PROTOCOL_REDIS":"182",
"NDPI_PROTOCOL_PANDO":"183", # Tomasz Bujlow <tomasz@skatnet.dk> */
"NDPI_PROTOCOL_VHUA":"184",
"NDPI_PROTOCOL_TELEGRAM":"185", # Gianluca Costa <g.costa@xplico.org> */
"NDPI_PROTOCOL_VEVO":"186",
"NDPI_PROTOCOL_PANDORA":"187",
"NDPI_PROTOCOL_QUIC":"188", # Andrea Buscarinu <andrea.buscarinu@gmail.com> - Michele Campus <michelecampus5@gmail.com> */
"NDPI_PROTOCOL_ZOOM":"189", # Zoom video conference. */
"NDPI_PROTOCOL_EAQ":"190",
"NDPI_PROTOCOL_OOKLA":"191",
"NDPI_PROTOCOL_AMQP":"192",
"NDPI_PROTOCOL_KAKAOTALK":"193", # KakaoTalk Chat (no voice call) */
"NDPI_PROTOCOL_KAKAOTALK_VOICE":"194", # KakaoTalk Voice */
"NDPI_PROTOCOL_TWITCH":"195", # Edoardo Dominici <edoaramis@gmail.com> */
"NDPI_PROTOCOL_DOH_DOT":"196", # DoH (DNS over HTTPS)", DoT (DNS over TLS) */
"NDPI_PROTOCOL_WECHAT":"197",
"NDPI_PROTOCOL_MPEGTS":"198",
"NDPI_PROTOCOL_SNAPCHAT":"199",
"NDPI_PROTOCOL_SINA":"200",
"NDPI_PROTOCOL_HANGOUT_DUO":"201", # Google Hangout ad Duo (merged as they are very similar) */
"NDPI_PROTOCOL_IFLIX":"202", # www.vizuamatix.com R&D team & M.Mallawaarachchie <manoj_ws@yahoo.com> */
"NDPI_PROTOCOL_GITHUB":"203",
"NDPI_PROTOCOL_BJNP":"204",
"NDPI_PROTOCOL_FREE_205":"205",
"NDPI_PROTOCOL_WIREGUARD":"206",
"NDPI_PROTOCOL_SMPP":"207", # Damir Franusic <df@release14.org> */
"NDPI_PROTOCOL_DNSCRYPT":"208",
"NDPI_PROTOCOL_TINC":"209", # William Guglielmo <william@deselmo.com> */
"NDPI_PROTOCOL_DEEZER":"210",
"NDPI_PROTOCOL_INSTAGRAM":"211", # Andrea Buscarinu <andrea.buscarinu@gmail.com> */
"NDPI_PROTOCOL_MICROSOFT":"212",
"NDPI_PROTOCOL_STARCRAFT":"213", # Matteo Bracci <matteobracci1@gmail.com> */
"NDPI_PROTOCOL_TEREDO":"214",
"NDPI_PROTOCOL_HOTSPOT_SHIELD":"215",
"NDPI_PROTOCOL_IMO":"216",
"NDPI_PROTOCOL_GOOGLE_DRIVE":"217",
"NDPI_PROTOCOL_OCS":"218",
"NDPI_PROTOCOL_OFFICE_365":"219",
"NDPI_PROTOCOL_CLOUDFLARE":"220",
"NDPI_PROTOCOL_MS_ONE_DRIVE":"221",
"NDPI_PROTOCOL_MQTT":"222",
"NDPI_PROTOCOL_RX":"223",
"NDPI_PROTOCOL_APPLESTORE":"224",
"NDPI_PROTOCOL_OPENDNS":"225",
"NDPI_PROTOCOL_GIT":"226",
"NDPI_PROTOCOL_DRDA":"227",
"NDPI_PROTOCOL_PLAYSTORE":"228",
"NDPI_PROTOCOL_SOMEIP":"229",
"NDPI_PROTOCOL_FIX":"230",
"NDPI_PROTOCOL_PLAYSTATION":"231",
"NDPI_PROTOCOL_PASTEBIN":"232", # Paulo Angelo <pa@pauloangelo.com> */
"NDPI_PROTOCOL_LINKEDIN":"233", # Paulo Angelo <pa@pauloangelo.com> */
"NDPI_PROTOCOL_SOUNDCLOUD":"234",
"NDPI_PROTOCOL_CSGO":"235", # Counter-Strike Global Offensive", Dota":"2 */
"NDPI_PROTOCOL_LISP":"236",
"NDPI_PROTOCOL_DIAMETER":"237",
"NDPI_PROTOCOL_APPLE_PUSH":"238",
"NDPI_PROTOCOL_GOOGLE_SERVICES":"239",
"NDPI_PROTOCOL_AMAZON_VIDEO":"240",
"NDPI_PROTOCOL_GOOGLE_DOCS":"241",
"NDPI_PROTOCOL_WHATSAPP_FILES":"242", # Videos", pictures", voice messages... */
"NDPI_PROTOCOL_TARGUS_GETDATA":"243",
"NDPI_PROTOCOL_DNP3":"244",
"NDPI_PROTOCOL_IEC60870":"245", # https://en.wikipedia.org/wiki/IEC_60870-5 */
"NDPI_PROTOCOL_BLOOMBERG":"246",
"NDPI_PROTOCOL_CAPWAP":"247",
"NDPI_PROTOCOL_ZABBIX":"248",
"NDPI_PROTOCOL_S7COMM":"249"
}

COMM_PROTO=[
    "NDPI_PROTOCOL_SKYPE_CALL",
    "NDPI_PROTOCOL_WHATSAPP_CALL",
    "NDPI_PROTOCOL_STUN",
    "NDPI_PROTOCOL_RTP",
    "NDPI_PROTOCOL_TRUPHONE",
    "NDPI_PROTOCOL_SIP"
]
ALLOWED_PROTO_EMPTY=[
]
ALLOWED_PROTO=[
"NDPI_PROTOCOL_FTP_CONTROL",
"NDPI_PROTOCOL_MAIL_POP",
"NDPI_PROTOCOL_MAIL_SMTP",
"NDPI_PROTOCOL_MAIL_IMAP",
"NDPI_PROTOCOL_HTTP",
"NDPI_PROTOCOL_POSTGRES",
"NDPI_PROTOCOL_MYSQL",
"NDPI_PROTOCOL_HOTMAIL",
"NDPI_PROTOCOL_MAIL_POPS",
"NDPI_PROTOCOL_MAIL_SMTPS",
"NDPI_PROTOCOL_MINING",
"NDPI_PROTOCOL_GNUTELLA",
"NDPI_PROTOCOL_EDONKEY",
"NDPI_PROTOCOL_BITTORRENT",
"NDPI_PROTOCOL_MAIL_IMAPS",
"NDPI_PROTOCOL_HTTP_DOWNLOAD",
"NDPI_PROTOCOL_IRC",
"NDPI_PROTOCOL_UNENCRYPTED_JABBER",
"NDPI_PROTOCOL_TELNET",
"NDPI_PROTOCOL_IP_IPSEC",
"NDPI_PROTOCOL_IP_GRE",
"NDPI_PROTOCOL_IP_IP_IN_IP",
"NDPI_PROTOCOL_RDP",
"NDPI_PROTOCOL_VNC",
"NDPI_PROTOCOL_PCANYWHERE",
"NDPI_PROTOCOL_TLS",
"NDPI_PROTOCOL_SSH",
"NDPI_PROTOCOL_TFTP",
"NDPI_PROTOCOL_KERBEROS",
"NDPI_PROTOCOL_LDAP",
"NDPI_PROTOCOL_MSSQL_TDS",
"NDPI_PROTOCOL_PPTP",
"NDPI_PROTOCOL_FACEBOOK",
"NDPI_PROTOCOL_TWITTER",
"NDPI_PROTOCOL_DROPBOX",
"NDPI_PROTOCOL_GMAIL",
"NDPI_PROTOCOL_GOOGLE_MAPS",
"NDPI_PROTOCOL_YOUTUBE",
"NDPI_PROTOCOL_HTTP_PROXY",
"NDPI_PROTOCOL_WAZE",
"NDPI_PROTOCOL_YOUTUBE_UPLOAD",
"NDPI_PROTOCOL_APPLE",
"NDPI_PROTOCOL_WHATSAPP",
"NDPI_PROTOCOL_VIBER",
"NDPI_PROTOCOL_TEAMVIEWER",
"NDPI_PROTOCOL_LOTUS_NOTES",
"NDPI_PROTOCOL_H323",
"NDPI_PROTOCOL_OPENVPN",
"NDPI_PROTOCOL_TELEGRAM",
"NDPI_PROTOCOL_GOOGLE_DRIVE",
"NDPI_PROTOCOL_CLOUDFLARE",
"NDPI_PROTOCOL_WHATSAPP_FILES"
]

known_fqdn={}
known_orgs={}

#############################################################################################
def get_aggreg_data_from_es(data):
    success=False
    buckets={}
    r = requests.post(ES_CONNECT,json=data)
    if r.status_code==200:
        resp=r.json()
        buckets=resp["aggregations"]["es_query"]["buckets"]
        if(len(buckets)>0):
            success=True
        else:
            logging.info("Protocol not present")
    else:
        logging.error("Fatal error while talking to Elasticsearch. Status code:{0}".format(r.status_code))
    return success,buckets

#############################################################################################
def get_data_from_es(data):
    success=False
    hits={}
    r = requests.post(ES_CONNECT,json=data)
    if r.status_code==200:
        resp=r.json()
        hits=resp["hits"]["hits"]
        if(len(hits)>0):
            success=True
        else:
            logging.info("Protocol not present")
    else:
        logging.error("Fatal error while talking to Elasticsearch. Status code:{0}".format(r.status_code))
    return success,hits

#############################################################################################
def resolve_ipv4_address_fqdn(ipv4_addr):
    fqdn=""
    try:
        if sys.platform.startswith('linux'):
            if ipv4_addr in known_fqdn:
                fqdn=known_fqdn[ipv4_addr]
            else:
                socket.inet_aton(ipv4_addr)
                command=["nslookup"]
                command.append(ipv4_addr)
                p = subprocess.run(command,capture_output=True,shell=False,text=True)
                if len(p.stderr)==0 and len(p.stdout)>0:
                    if "name =" in p.stdout:
                        fqdn=p.stdout.split("name =")[1].split("\n")[0].rstrip('.').strip()
                        known_fqdn[ipv4_addr]=fqdn
                time.sleep(.5)
    except Exception as ex:
        logging.error("Nslookup error for ip address {0}. Error: {1}".format(ipv4_addr,ex.args))
    return fqdn

#############################################################################################
def resolve_ipv4_address_org(ipv4_addr):
    org=""
    try:
        if sys.platform.startswith('linux'):
            if ipv4_addr in known_orgs:
                org=known_orgs[ipv4_addr]
            else:
                socket.inet_aton(ipv4_addr)
                command=["whois"]
                command.append(ipv4_addr)
                p = subprocess.run(command,capture_output=True,shell=False)
                if len(p.stderr)==0 and len(p.stdout)>0:
                    for line in p.stdout.decode("ISO-8859-1").splitlines():
                        if line.startswith("org-name:") or line.startswith("netname:") or line.startswith("OrgName:"):
                            org=line.split(":")[1].strip("\n").strip()
                            known_orgs[ipv4_addr]=org
                            break
                time.sleep(2)#a delay is needed as it appears whois queries are rate limited
    except Exception as ex:
        logging.error("Whois error for ip address {0}. Error: {1}".format(ipv4_addr,ex.args))
    return org

#############################################################################################
def get_user_agent_string(path_to_pcap_folder,pcap_file_name,report_file,uas):
    #uas=[]
    try:
        command=["ngrep"]
        command.append("-I")
        command.append(os.path.join(path_to_pcap_folder,pcap_file_name))
        command.append("-W")
        command.append("byline")
        command.append("-q")
        command.append("User-Agent:")
        p = subprocess.run(command,capture_output=True,shell=False)
        if len(p.stderr)==0 and len(p.stdout)>0:
            socket_str_found=False
            ipv4s=""
            ports=""
            ipv4d=""
            portd=""
            for line in p.stdout.decode("ISO-8859-1").splitlines():
                if line.startswith("T "):
                    socket_str_found=True
                    ipv4s=line.split(" -> ")[0].strip("T ").split(":")[0]
                    ports=line.split(" -> ")[0].strip("T ").split(":")[1]
                    ipv4d=line.split(" -> ")[1].split(" ")[0].split(":")[0]
                    portd=line.split(" -> ")[1].split(" ")[0].split(":")[1]
                elif line.startswith("User-Agent:"):
                    if socket_str_found:
                        socket_str_found=False
                        ua=line.split("User-Agent:")[1].strip(".").strip()
                        if len(ua)>0 and ua not in uas:
                            uas.append(ua)
                            report_file.write(
                                "{0},{1},{2},{3},{4},{5}\n".format(pcap_file_name,ua,ipv4s,ports,ipv4d,portd))
                elif line.startswith("."):
                    socket_str_found=False
    except Exception as ex:
        logging.error("Error while launching ngrep utility. Error: {0}".format(ex.args))
    return uas

#############################################################################################
def process_protocols(num_of_hits):
    for proto in NDPI_PROTOS:
        if proto in COMM_PROTO:
            ES_QUERY_HITS ={
		        "query":{
			        "match":{
				        "L7_PROTO":NDPI_PROTOS[proto]
			        }
		        },
                "size":MAX_HITS,
           		"_source": ["IPV4_SRC_ADDR","IPV4_DST_ADDR","INTERFACE"]
            }
            logging.info("Generating report for protocol {0} top {1} hits".format(proto,MAX_HITS))
            success,hits=get_data_from_es(ES_QUERY_HITS)
            if success:
                f = open(os.path.join(REPORT_FOLDER,REPORT_FNAME),"a")
                f.write("*** {0} ***\n".format(proto))
                f.write("Interface" + REPORT_COL_SEP + "Source Ipv4" + REPORT_COL_SEP + "fqdn" + REPORT_COL_SEP + "organization" + REPORT_COL_SEP + "Dest Ipv4" + REPORT_COL_SEP + "fqdn" + REPORT_COL_SEP + "organization"+"\n")
                for hit in hits:
                    ipv4s=hit["_source"]["IPV4_SRC_ADDR"]
                    ipv4d=hit["_source"]["IPV4_DST_ADDR"]
                    intf=hit["_source"]["INTERFACE"]
                    fqdns=resolve_ipv4_address_fqdn(ipv4s)
                    orgs=resolve_ipv4_address_org(ipv4s)
                    fqdnd=resolve_ipv4_address_fqdn(ipv4d)
                    orgd=resolve_ipv4_address_org(ipv4d)
                    f.write("{0},{1},{2},{3},{4},{5},{6}\n".format(intf,ipv4s,fqdns,orgs,ipv4d,
                        fqdnd,orgd))
                f.write(REPORT_SPACE_HDR)
                f.close()
        elif proto in ALLOWED_PROTO:
            ES_QUERY_HITS ={
		        "query":{
			        "match":{
				        "L7_PROTO":NDPI_PROTOS[proto]
			        }
		        },
                "size":0,
                "aggs" : {
                    "es_query" : {
                        "terms" : {
                            "size":num_of_hits,
                            "field" : "IPV4_DST_ADDR.keyword"
                        }
                    }
                }
            }
            logging.info("Generating report for protocol {0} top {1} hits".format(proto,num_of_hits))
            success,buckets=get_aggreg_data_from_es(ES_QUERY_HITS)
            if success:
                f = open(os.path.join(REPORT_FOLDER,REPORT_FNAME),"a")
                f.write("*** {0} ***\n".format(proto))
                f.write("Ipv4"+REPORT_COL_SEP+"Count"+ REPORT_COL_SEP+"fqdn"+REPORT_COL_SEP+"organization"+"\n")
                for bucket in buckets:
                    ipv4=bucket["key"]
                    counts=bucket["doc_count"]
                    fqdn=resolve_ipv4_address_fqdn(ipv4)
                    org=resolve_ipv4_address_org(bucket["key"])
                    f.write("{0},{1},{2},{3}\n".format(ipv4,counts,fqdn,org))
                f.write(REPORT_SPACE_HDR)
                f.close()

#############################################################################################
def calculate_hits_dns(num_of_hits):
    ES_QUERY_HITS ={
        "size":0,
        "aggs" : {
            "es_query" : {
                "terms" : {
                    "size":num_of_hits,
                    "field" : "DNS_QUERY.keyword"
                }
            }
        }
    }
    logging.info("Generating report: dns query  {0} hits".format(num_of_hits))
    success,buckets=get_aggreg_data_from_es(ES_QUERY_HITS)
    if success:
        f = open(os.path.join(REPORT_FOLDER,REPORT_FNAME),"a")
        f.write("*** {0} ***\n".format("DNS TOP QUERIES"))
        f.write("Fqdn"+ REPORT_COL_SEP +"Count"+"\n")
        for bucket in buckets:
            ipv4=bucket["key"]
            count=bucket["doc_count"]
            f.write("{0},{1}\n".format(ipv4,count))
        f.write(REPORT_SPACE_HDR)
        f.close()

#############################################################################################
def calculate_hits_http_urls(num_of_hits):
    ES_QUERY_HITS ={
        "size":0,
        "aggs" : {
            "es_query" : {
                "terms" : {
                    "size":num_of_hits,
                    "field" : "HTTP_URL.keyword"
                }
            }
        }
    }
    logging.info("Generating report: http top {0} urls".format(num_of_hits))
    success,buckets=get_aggreg_data_from_es(ES_QUERY_HITS)
    if success:
        f = open(os.path.join(REPORT_FOLDER,REPORT_FNAME),"a")
        f.write("*** {0} ***\n".format("TOP HTTP URLS"))
        f.write("Http Urls"+ REPORT_COL_SEP + "Count"+"\n")
        for bucket in buckets:
            ipv4=bucket["key"]
            count=bucket["doc_count"]
            f.write("{0},{1}\n".format(ipv4,count))
        f.write(REPORT_SPACE_HDR)
        f.close()

#############################################################################################
def calculate_http_user_agent(path_to_pcap_folder):
    ES_QUERY_HITS ={
        "query": {
		    "bool": {
	            "must": [
    	            {"match": { "L7_PROTO":7}}
                ]
            }
        },      
	    "size":1,
        "_source": ["IPV4_SRC_ADDR","L4_SRC_PORT","IPV4_DST_ADDR","L4_DST_PORT","INTERFACE"]
    }
    success,hits=get_data_from_es(ES_QUERY_HITS)
    if success:
        #http is present
        logging.info("Generating report for http user agents")
        f = open(os.path.join(REPORT_FOLDER,REPORT_FNAME),"a")
        f.write("*** {0} ***\n".format("HTTP USER AGENTS"))
        f.write("Interface" + REPORT_COL_SEP + "User Agent" + REPORT_COL_SEP + "Source Ipv4" + REPORT_COL_SEP + "Source Port" + REPORT_COL_SEP + "Dest Ipv4" + REPORT_COL_SEP + "Dest Port" + "\n")
        uas=[]
        for filename in os.listdir(path_to_pcap_folder):
            uas=get_user_agent_string(path_to_pcap_folder,filename,f,uas)
        f.write(REPORT_SPACE_HDR)
        f.close()
    else:
        logging.info("Http protocol not present. Cannot look for user agents")

#############################################################################################
def calculate_hits_http_hosts(num_of_hits):
    ES_QUERY_HITS ={
        "size":0,
        "aggs" : {
            "es_query" : {
                "terms" : {
                    "size":num_of_hits,
                    "field" : "HTTP_HOST.keyword"
                }
            }
        }
    }
    logging.info("Generating report: http top {0} hosts".format(num_of_hits))
    success,buckets=get_aggreg_data_from_es(ES_QUERY_HITS)
    if success:
        f = open(os.path.join(REPORT_FOLDER,REPORT_FNAME),"a")
        f.write("*** {0} ***\n".format("TOP HTTP HOSTS"))
        f.write("Http Server Name"+ REPORT_COL_SEP + "Count"+"\n")
        for bucket in buckets:
            ipv4=bucket["key"]
            count=bucket["doc_count"]
            f.write("{0},{1}\n".format(ipv4,count))
        f.write(REPORT_SPACE_HDR)
        f.close()

#############################################################################################
def calculate_hits_https_hosts(num_of_hits):
    FLD_SRV_NAME=""
    if NTOPNG_MAJ_VER >=4:
        FLD_SRV_NAME="TLS_SERVER_NAME.keyword"
    else:
        FLD_SRV_NAME="SSL_SERVER_NAME.keyword"
    ES_QUERY_HITS ={
        "size":0,
        "aggs" : {
            "es_query" : {
                "terms" : {
                    "size":num_of_hits,
                    "field" : FLD_SRV_NAME
                }
            }
        }
    }
    logging.info("Generating report: https top {0} hosts".format(num_of_hits))
    success,buckets=get_aggreg_data_from_es(ES_QUERY_HITS)
    if success:
        f = open(os.path.join(REPORT_FOLDER,REPORT_FNAME),"a")
        f.write("*** {0} ***\n".format("TOP HTTPS HOSTS"))
        f.write("Https Server Name" + REPORT_COL_SEP + "Count"+"\n")
        for bucket in buckets:
            ipv4=bucket["key"]
            count=bucket["doc_count"]
            f.write("{0},{1}\n".format(ipv4,count))
        f.write(REPORT_SPACE_HDR)
        f.close()

#############################################################################################
def calculate_hits_ipv4dest_out_bytes(num_of_hits):
    ES_QUERY_HITS ={
        "size":0,
        "aggs" : {
            "es_query" : {
                "terms" : {
                    "size":num_of_hits,
                    "field" : "IPV4_DST_ADDR",
					"order" : {
					    "sum_out_bytes":"desc"
					}
                },
				"aggs":{
				    "sum_out_bytes":{
					"sum":{"field":"OUT_BYTES"}
					}						
				}
            }
        }
    }
    logging.info("Generating report: destination ipv4 top {0} talkers by out bytes".format(num_of_hits))
    success,buckets=get_aggreg_data_from_es(ES_QUERY_HITS)
    if success:
        f = open(os.path.join(REPORT_FOLDER,REPORT_FNAME),"a")
        f.write("*** {0} ***\n".format("IPV4 TOP TALKERS BY OUT BYTES"))
        f.write("Ipv4" + REPORT_COL_SEP + "out bytes" + REPORT_COL_SEP + "fqdn" + REPORT_COL_SEP + "organization"+"\n")
        for bucket in buckets:
            ipv4=bucket["key"]
            sum=bucket["sum_out_bytes"]["value"]
            fqdn=resolve_ipv4_address_fqdn(ipv4)
            org=resolve_ipv4_address_org(ipv4)
            f.write("{0},{1},{2},{3}\n".format(ipv4,sum,fqdn,org))
        f.write(REPORT_SPACE_HDR)
        f.close()

#############################################################################################
def calculate_hits_ipv4dest_in_bytes(num_of_hits):
    ES_QUERY_HITS ={
        "size":0,
        "aggs" : {
            "es_query" : {
                "terms" : {
                    "size":num_of_hits,
                    "field" : "IPV4_DST_ADDR",
					"order" : {
					    "sum_in_bytes":"desc"
					}
                },
				"aggs":{
				    "sum_in_bytes":{
					"sum":{"field":"IN_BYTES"}
					}						
				}
            }
        }
    }
    logging.info("Generating report: destination ipv4 top {0} talkers by in bytes".format(num_of_hits))
    success,buckets=get_aggreg_data_from_es(ES_QUERY_HITS)
    if success:
        f = open(os.path.join(REPORT_FOLDER,REPORT_FNAME),"a")
        f.write("*** {0} ***\n".format("IPV4 TOP TALKERS BY IN BYTES"))
        f.write("Ipv4" + REPORT_COL_SEP + "in bytes" + REPORT_COL_SEP + "fqdn" + REPORT_COL_SEP + "organization" + "\n")
        for bucket in buckets:
            ipv4=bucket["key"]
            sum=bucket["sum_in_bytes"]["value"]
            fqdn=resolve_ipv4_address_fqdn(ipv4)
            org=resolve_ipv4_address_org(ipv4)
            f.write("{0},{1},{2},{3}\n".format(ipv4,sum,fqdn,org))
        f.write(REPORT_SPACE_HDR)
        f.close()

############################################################################################
#main
try:
    if not(os.path.exists(LOG_FOLDER)):
        os.makedirs(LOG_FOLDER)
    if not(os.path.exists(REPORT_FOLDER)):
        os.makedirs(REPORT_FOLDER)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
        logging.FileHandler(os.path.join(LOG_FOLDER,"logmetrics.txt"),'a'),#append is default anyway
        logging.StreamHandler()
    ])
    logging.Formatter.converter = time.gmtime
    logging.info(BANNER)
    if len(sys.argv)!=3:
        raise Exception("Usage: {0} <Number of hits> <path to pcap folder>".format(sys.argv[0]))
    path_to_pcap_folder=sys.argv[2]
    if not(os.path.exists(path_to_pcap_folder)):
        raise Exception("Path {0} is invalid".format(path_to_pcap_folder))
    num_of_hits=int(sys.argv[1])
    if os.path.exists(os.path.join(REPORT_FOLDER,REPORT_FNAME)):
        os.remove(os.path.join(REPORT_FOLDER,REPORT_FNAME))
    calculate_hits_dns(num_of_hits)
    calculate_http_user_agent(path_to_pcap_folder)
    calculate_hits_http_urls(num_of_hits)
    calculate_hits_http_hosts(num_of_hits)
    calculate_hits_https_hosts(num_of_hits)
    calculate_hits_ipv4dest_out_bytes(num_of_hits)
    calculate_hits_ipv4dest_in_bytes(num_of_hits)
    process_protocols(num_of_hits)
    logging.info("Done")
except Exception as ex:
    logging.error("An error occurred. {0}".format(ex.args))


