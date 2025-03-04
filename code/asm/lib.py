# Define file extensions
import re
import os
import enum
import pathlib
import pandas
import yaml
import json
import argparse
import ipaddress
import shutil
import urllib.parse

from itertools import zip_longest

CAP = ".pcap"
BOT = ".csv"
HAR = ".har"
REB = ".txt"

# Tstat configuration files and elf
TSTAT_BINARY = "tstat/tstat/tstat"
TSTAT_CONFIG = "tstat/tstat-conf/runtime.conf"
TSTAT_GLOBAL = "tstat/tstat-conf/globals.conf"

# Define filenames
LOG_TCP_COMPLETE   = "log_tcp_complete"
LOG_TCP_PERIODIC   = "log_tcp_periodic"
LOG_UDP_COMPLETE   = "log_udp_complete"
LOG_UDP_PERIODIC   = "log_udp_periodic"
LOG_HAR_COMPLETE   = "log_har_complete"
LOG_BOT_COMPLETE   = "log_bot_complete"
LOG_NET_COMPLETE   = "log_net_complete"
LOG_REB_COMPLETE   = "log_reb_complete"
STREAMING_EVENT    = "streaming_event"

# Useful Tstat names
LAYER_7_PROTOCOL_TCP     = "con_t"
LAYER_7_PROTOCOL_UDP     = "c_type"
SNI_CLIENT_HELLO_TCP     = "c_tls_SNI"
SNI_CLIENT_HELLO_UDP     = "quic_SNI"
HTTP_SERVER_HOSTNAME     = "http_hostname"
FULLY_QUALIFIED_NAME_TCP = "fqdn"
FULLY_QUALIFIED_NAME_UDP = "fqdn"

# Define current directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define the folder where manifest are stored
MANIFESTS = {
    "dazn":   os.path.join(current_dir, "dazn",   "manifest.yaml"),
    "amazon": os.path.join(current_dir, "amazon", "manifest.yaml"),    
}

class Protocol(enum.Enum):
    TCP = 1
    UDP = 2

def load_yaml_file(path: str):
    with open(path, "r") as file:
        content = yaml.safe_load(file)
        return content
    return {}
    
def get_events(frame: pandas.DataFrame):
    ## Define a function for extracting all streaming events
    frame = frame[~frame["event"].str.contains("sniffer|browser|origin|net|app", case=False, na=False)]
    return [(frame.iloc[i, frame.columns.get_loc("rel")], frame.iloc[i + 1, frame.columns.get_loc("rel")]) 
            for i in range(0, len(frame) - 1, 2)]
    
def fetch_files(folder: str, prefix: str, suffix: str) -> list[str]:
    ## Define a function to get files sorted using name-number.extension
    order = lambda name: int(name.rsplit('-', 1)[-1].split('.')[0])
    return sorted([str(f) for f in pathlib.Path(folder).glob(f"{prefix}*{suffix}")], key=order)

def fetch_tests(folder: str) -> list[str]:
    ## Define a function to get tests
    order = lambda name: int(name.split("-")[1]) if "-" in name else 0
    return sorted([test for test in os.listdir(folder)], key=order) 


def cname(record: pandas.Series, protocol: Protocol) -> str:
    result = "-"
    
    https = 8192
    http  = 1
    quic  = 27
    
    if protocol == Protocol.TCP:
        layer_7 = record.get(LAYER_7_PROTOCOL_TCP, 0)
        # if layer_7 == https:
        #     result = record.get(SNI_CLIENT_HELLO_TCP, "-")
        # if layer_7 == http:
        #     result = record.get(HTTP_SERVER_HOSTNAME, "-")
        if result == "-":
            value  = str(record.get(FULLY_QUALIFIED_NAME_TCP, "-"))
            result = value if pandas.notna(value) else "-"

    if protocol == Protocol.UDP:
        layer_7 = record.get(LAYER_7_PROTOCOL_UDP, 0)
        # if layer_7 == quic:
        #     result = record.get(SNI_CLIENT_HELLO_UDP, "-")
        if result == "-":
            value  = str(record.get(FULLY_QUALIFIED_NAME_UDP, "-"))
            result = value if pandas.notna(value) else "-"
            
    return result