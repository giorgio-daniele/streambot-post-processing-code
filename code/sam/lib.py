# Define file extensions
import os
import enum
import re
import sys
import json
import shutil
import argparse

import numpy
import pandas
from   typing import Optional

# Define filenames
LOG_TCP_COMPLETE   = "log_tcp_complete"
LOG_TCP_PERIODIC   = "log_tcp_periodic"
LOG_UDP_COMPLETE   = "log_udp_complete"
LOG_UDP_PERIODIC   = "log_udp_periodic"
LOG_HAR_COMPLETE   = "log_har_complete"
LOG_BOT_COMPLETE   = "log_bot_complete"
LOG_NET_COMPLETE   = "log_net_complete"
STREAMING_EVENT    = "streaming_event"

# Define the current directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define the path to the files in the 'dazn' folder using os.path.join
STREAMING_SERVERS = {
    "dazn": r"(?=.*live)(?=.*dazn)"
}

class Protocol(enum.Enum):
    TCP = 1
    UDP = 2
    
def merge_intervals(intervals: list[list[float]]) -> list[list[float]]:
    intervals.sort(key=lambda x: x[0])
    result = []
    for interval in intervals:
        if not result or interval[0] > result[-1][1]:
            result.append(interval)
        else:
            result[-1][1] = max(result[-1][1], interval[1])
    return result

def process_media(request_data: pd.DataFrame, mime_type: str, ts: float):
    # Filter the data based on mime type (case-insensitive)
    filtered_data = request_data[request_data["mime"].str.contains(mime_type, case=False, na=False)]
    
    # print(filtered_data)
    # print()
    
    mean = float(0)
    seqs = "null"
    
    # Check if no data matches the specified mime type (e.g., no video requests)
    if filtered_data.empty:
        return mean, seqs

    # Process the media data if available
    if mime_type == "video/mp4":
        videorates = filtered_data["videorate"].dropna().astype(float)
        mean       = videorates.mean()
        sequence   = [str(rate) for rate in videorates]
        seqs       = "$".join(sequence) if sequence else "null"
        return mean, seqs
    
    return mean, seqs
    
# UDP_FEATURES = [
#     "idle", "avg_span", "std_span", "max_span", "min_span",
#     "c_bytes_all", "c_pkts_all",  # Client
#     "s_bytes_all", "s_pkts_all",  # Server
#     "video_rate"                  # Truth
# ]

UDP_FEATURES = {
    "temporal":   ["idle", "avg_span", "std_span", "max_span", "min_span"],
    "volumetric": ["c_bytes_all", "c_pkts_all", "s_bytes_all", "s_pkts_all"]
}


TCP_FEATURES = {
    "temporal":   ["idle", "avg_span", "std_span", "max_span", "min_span"],
    "volumetric": ["c_bytes_all", "c_ack_cnt", "c_ack_cnt_p", "c_pkts_all", "c_pkts_data",
                   "s_bytes_all", "s_ack_cnt", "s_ack_cnt_p", "s_pkts_all", "s_pkts_data"]
                   
}