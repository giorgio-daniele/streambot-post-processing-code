from lib import *

def sample_bins(bins:    pandas.DataFrame, 
                http:    pandas.DataFrame, 
                play:    Optional[pandas.DataFrame], 
                proto:   Protocol, 
                ts:      float, 
                te:      float,
                winsize: float) -> pandas.DataFrame: 
    
    # Define the list of records
    windows = []
    
    # Define the set of features
    features = []
    if proto == Protocol.TCP:
        features = TCP_FEATURES
    elif proto == Protocol.UDP:
        features = UDP_FEATURES

    n_features = len(features["temporal"]) + len(features["volumetric"])

    for ti in range(int(ts), int(te - winsize), int(winsize)):
        tj = ti + winsize
        
        # Filter all bins falling in the current window
        data = bins[(bins["ts"] <= tj) & (bins["te"] >= ti)].copy()
        
        if data.empty:
            windows.append([ti, tj] + [0] * (n_features + 1))
            continue
        
        # Filter all HTTP falling in the current window
        meta = http[(http["ts"] <= tj) & (http["te"] >= ti)]
        
        # # Filter states in [ti; tj]
        # status = None
        # if play is not None and not play.empty:
        #     status = play[(play["ti"] <= tj) & (play["ti"] >= ti)]
        
        # Compute the real active time for each bin
        data["rel_ts"] = numpy.maximum(data["ts"], ti)
        data["rel_te"] = numpy.minimum(data["te"], tj)
        data["abs_sz"] = data["te"] - data["ts"]
        data["rel_sz"] = data["rel_te"] - data["rel_ts"]
        data["factor"] = data["rel_sz"] / data["abs_sz"].replace(0, 1)
        
        # Derive temporal metrics
        intervals = data[["rel_ts", "rel_te"]].values.tolist()
        win_idle  = merge_intervals(intervals=intervals, ti=ti, tj=tj)
        avg_span  = data["rel_sz"].mean()
        max_span  = data["rel_sz"].max()
        min_span  = data["rel_sz"].min()
        std_span  = data["rel_sz"].std()

        volumetric = {}
        
        # Derive volumetric metrics
        for col in features["volumetric"]:
            volumetric[col] = float((data[col] * data["factor"]).sum())
        
        # Derive ground-truth
        video_rate, _ = process_media(meta, "video/mp4", ts)
        
        # Add the new window
        windows.append([ti, tj, 
                        # Temporal  metrics
                        win_idle, avg_span, std_span, max_span, min_span] + 
                        # Volumetric metrics
                        [volumetric[feature] for feature in features["volumetric"]] + [video_rate])
        
        # if status is not None and not status.empty:
        #     record.append(len(status[status["status"] == "live"]))
        #     record.append(len(status[status["status"] == "dead"]))
    
    column_names = ["ts", "te"] + features["temporal"] + features["volumetric"] + ["videorate"]

    # if play is not None:
    #     column_names += ["nlive", "ndead"]
    
    # Generate the frame
    samples = pandas.DataFrame(windows, columns=column_names)

    # Rescale the time limits
    if not samples.empty:
        first_ts = float(samples["ts"].iloc[0])
        samples["ts"] -= first_ts
        samples["te"] -= first_ts
        
    return samples

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",    required=True)
    parser.add_argument("--samples",  required=True)
    parser.add_argument("--provider", required=True, choices=["dazn", "amazon"])
    parser.add_argument("--window",   required=True)
    return parser.parse_args()

def main():
    input_path:    str = args().input
    samples_path:  str = args().samples
    provider:      str = args().provider
    winsize:       str = args().window
    
    # Remove previous output
    shutil.rmtree(os.path.join(samples_path, "tcp", winsize), ignore_errors=True)
    shutil.rmtree(os.path.join(samples_path, "udp", winsize), ignore_errors=True)
    # Generate new one

    os.makedirs(os.path.join(samples_path, "tcp", winsize), exist_ok=True)
    os.makedirs(os.path.join(samples_path, "udp", winsize), exist_ok=True)
    
    # Load the regular expression
    names = re.compile(STREAMING_SERVERS[provider])
        
    # Lambda function for sorting the events
    events_path = sorted([file for file in os.listdir(input_path)], key=lambda name: int(name.rsplit('-', 1)[-1].split('.')[0]))   
    
    # Define counters
    tcp_based_cnt = 1
    udp_based_cnt = 1
     
    for num, event_path in enumerate(events_path, start=1):    
        with open(os.path.join(input_path, event_path), 'r') as f:
            event = json.load(f)
            
            # Select all TCP flows associated with the content provider with regular expression
            event["tcp"] = [item for item in event["tcp"] if names.search(item.get("cname", ""))]
            # Select all UDP flows associated with the content provider with regular expression
            event["udp"] = [item for item in event["udp"] if names.search(item.get("cname", ""))]

            # Select all TCP flow bins matching the 4-tuple
            tcp_bins = []
            for flow in event["tcp"]:
                tcp_bins.extend([bin for bin in flow["bins"] 
                                 if all(bin[key] == flow[key] for key in ["s_ip", "s_port", "c_ip", "c_port"])])
            
            # Select all UDP flow bins matching the 4-tuple
            udp_bins = []
            for flow in event["udp"]:
                udp_bins.extend([bin for bin in flow["bins"] 
                                 if all(bin[key] == flow[key] for key in ["s_ip", "s_port", "c_ip", "c_port"])])

            # Select any HTTP
            http = pandas.DataFrame(event["http"])
            
            # Select any status
            # play = pandas.DataFrame(event["status"]) if "status" in event else None

            # Determine protocol and data
            total_bins = len(tcp_bins) + len(udp_bins)
            if total_bins == 0:
                continue
            
            proto, data = None, None
            if len(tcp_bins) / total_bins >= 0.9:
                proto, data = Protocol.TCP, pandas.DataFrame(tcp_bins)
            elif len(udp_bins) / total_bins >= 0.9:
                proto, data = Protocol.UDP, pandas.DataFrame(udp_bins)

            if not proto:
                continue

            # Run the sampling/feature extraction procedure
            out = sample_bins(bins=data, 
                              http=http, 
                              play=None, proto=proto, winsize=float(winsize), ts=float(event["ts"]), te=float(event["te"]))
            
            print(f"{event_path} sampled with winsize={winsize}")
            
            try:
                if proto == Protocol.TCP:
                    layer, title = "tcp", f"sample-{tcp_based_cnt}"
                    tcp_based_cnt += 1
                elif proto == Protocol.UDP:
                    layer, title = "udp", f"sample-{udp_based_cnt}"
                    udp_based_cnt += 1
                # Save the output to CSV
                out.to_csv(os.path.join(samples_path, layer, str(winsize), title), sep=" ", index=False)
            except Exception as error:
                print(error)
                continue

if __name__ == "__main__":
    main()