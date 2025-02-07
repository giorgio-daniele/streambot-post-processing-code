import os
import pandas
import argparse
import json

from collections import Counter

# Output file names
STREAMING_PERIODS_OBSERVED    = "streambing_periods.dat"
SERVER_NAMES_OVER_TCP         = "server_names_tcp.dat"
SERVER_NAMES_OVER_UDP         = "server_names_udp.dat"
SERVER_NAMES_VIDEO_REQUESTS   = "server_names_video_requests.dat"

def read_data(file: str, dictionary: Counter, column: str, incrementally: bool):
    if os.path.exists(file):
        data = pandas.read_csv(file, sep=" ")
        if incrementally == True:
            for key, value in zip(data["cname"], data[column]):
                    dictionary[key] += value
        else:
            dictionary.update(dict(zip(data["cname"], data[column])))
            
def write_data(file: str, dictionary: Counter):
    with open(os.path.join(file), "w") as f:
        f.write("cname value\n")
        for key, value in dictionary.items():
            f.write(f"{key} {value}\n")
            
def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",    required=True)
    parser.add_argument("--provider", required=True, choices=["dazn", "amazon"])
    return parser.parse_args()

def main():
    # Extract the arguments
    arguments = args()

    # Access the argument values
    folder = arguments.input
    server = arguments.provider
    
    # Init new counters
    events = 0
    names_tcp = Counter()
    names_udp = Counter()
    video_cnt = Counter()

    meta = f"code/profiler/{server}"

    # Load existing statistics if available
    try:
        with open(os.path.join(meta, STREAMING_PERIODS_OBSERVED), "r") as f:
            events = int(f.readline().strip())
    except FileNotFoundError:
        pass

    read_data(file=os.path.join(meta, SERVER_NAMES_OVER_TCP), 
              dictionary=names_tcp, column="value", incrementally=False)
    
    read_data(file=os.path.join(meta, SERVER_NAMES_OVER_UDP), 
              dictionary=names_udp, column="value", incrementally=False)
    
    read_data(file=os.path.join(meta, SERVER_NAMES_VIDEO_REQUESTS), 
              dictionary=video_cnt, column="value", incrementally=True)
    
    events += len(os.listdir(folder))

    for _, event in enumerate(os.listdir(folder), start=1):
        # Get the period
        file = os.path.join(folder, event)
        
        # Get the JSON
        with open(file, 'r') as f:
            event = json.load(f)
        
        # Update CNAMEs over TCP
        cnames = set([flow["cname"] for flow in event["tcp"] if "cname" in flow])
        for cname in cnames:
            names_tcp[cname] += 1
        
        # Update CNAMEs over UDP
        cnames = set([flow["cname"] for flow in event["udp"] if "cname" in flow])
        for cname in cnames:
            names_udp[cname] += 1
            
        # Update video requests count
        for http in event["http"]:
            if "video" in http["mime"]:
                video_cnt[http["server"]] += 1

    # Save the results
    with open(os.path.join(meta, STREAMING_PERIODS_OBSERVED), "w") as f:
        f.write(f"{events}\n")
        
    write_data(file=os.path.join(meta, SERVER_NAMES_OVER_TCP),         dictionary=names_tcp)
    write_data(file=os.path.join(meta, SERVER_NAMES_OVER_UDP),         dictionary=names_udp)
    write_data(file=os.path.join(meta, SERVER_NAMES_VIDEO_REQUESTS),   dictionary=video_cnt)

if __name__ == "__main__":
    main()
