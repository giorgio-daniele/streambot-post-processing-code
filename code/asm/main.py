from lib import *


def process_har(file: str, provider: str, start: float) -> pandas.DataFrame:
    
    records = []
    
    # Load the manifests file
    manifest = MANIFESTS.get(provider, "")
    template = load_yaml_file(path=manifest)
    
    with open(file) as f:
        data = json.load(f)
        
        for entry in data["log"]["entries"]:        
            url  = entry["request"]["url"]
            mime = entry["response"]["content"]["mimeType"]
            
            tot: float = 0
            for field in entry["timings"]:
                if field is None:
                    continue
                tot += max(0, float(entry["timings"][field]))

            # Get the instant when the client issues the request and
            # compute the overall time the response requires to be 
            # received from the user
            ts: pandas.DatetimeIndex = pandas.to_datetime(entry["startedDateTime"])
            ts: pandas.Timestamp = ts.timestamp() * 1000
            ts = ts - (float(start))
            te: pandas.Timestamp = ts + tot
            
            # Parse the URL
            data = urllib.parse.urlparse(url)
            
            # Get the name of the server and the resouce
            server  = data.netloc
            content = data.path
            
            video_rate = float(0) 
            audio_rate = float(0)
            
            for key, value in template.items():
                if key in content:
                    mime = value["mime_type"]
                    if mime == "video/mp4":
                        video_rate = float(value["bitrate"])
                    if mime == "audio/mp4":
                        audio_rate = float(value["bitrate"])
                
            # Generate the record
            record = [ts, te, server, content, mime, video_rate, audio_rate]
            records.append(record)
            
    return pandas.DataFrame(records, columns=["ts", "te", "server", "content", "mime", "video_rate", "audio_rate"])

def args():
    ## Define arguments parser
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",    required=True)
    parser.add_argument("--provider", required=True, choices=["dazn", "amazon"])
    parser.add_argument("--tests",    required=True)
    parser.add_argument("--events",   required=True)
    return parser.parse_args()

def main():
    input_path  = args().input
    provider    = args().provider
    tests_path  = args().tests
    events_path = args().events
    
    # Init variables for data processing
    cap_files = fetch_files(folder=input_path, prefix=LOG_NET_COMPLETE, suffix=CAP)
    bot_files = fetch_files(folder=input_path, prefix=LOG_BOT_COMPLETE, suffix=BOT)
    har_files = fetch_files(folder=input_path, prefix=LOG_HAR_COMPLETE, suffix=HAR)
    reb_files = fetch_files(folder=input_path, prefix=LOG_REB_COMPLETE, suffix=REB)
    
    # Align lists to the longest one, filling missing values with None
    aligned_lists = list(zip_longest(cap_files, 
                                     bot_files, 
                                     har_files, 
                                     reb_files, fillvalue=None))

    # Convert back to individual lists if needed
    cap_files, bot_files, har_files, reb_files = map(list, zip(*aligned_lists))
    
    # Remove any previous output
    shutil.rmtree(tests_path,  ignore_errors=True)
    shutil.rmtree(events_path, ignore_errors=True)

    # Genearate new output
    os.makedirs(tests_path,  exist_ok=True)
    os.makedirs(events_path, exist_ok=True)
    
    print("=" * 20)
    print(f"Processing input folder={input_path}")
    print("=" * 20)
    
    ###########################################################################
    #       Convert data into tests folders with data serialization           #
    ###########################################################################

    for num, (cap, bot, har, reb) in enumerate(zip(cap_files, bot_files, har_files, reb_files), start=1):

        print("Processing cap", cap)
        print("Processing bot", bot)
        print("Processing har", har)
        #print("Processing reb", reb)
        print(";;;")
        
        # Define the out folder
        out = os.path.join(tests_path, f"test-{num}")
        os.makedirs(out, exist_ok=True)
        
        # Run Tstat in out
        os.system(f"{TSTAT_BINARY} -G {TSTAT_GLOBAL} -T {TSTAT_CONFIG} {cap} -s {out} > /dev/null 2>&1")
        os.system(f"find {out} -mindepth 2 -type f -exec mv -t {out} {{}} +")
        os.system(f"find {out} -type d -empty -exec rmdir {{}} +")
        
        # Copy Streambot trace in out
        name = os.path.basename(bot).rsplit('-', 1)[0]
        os.system(f"cp {bot} {os.path.join(out, name)}")
        
        # Copy Puppeter trace in out
        name = os.path.basename(har).rsplit('-', 1)[0]
        os.system(f"cp {har} {os.path.join(out, name)}")
        
        # Copy Rebuffering trace in out
        # if reb is not None:
        #     name = os.path.basename(reb).rsplit('-', 1)[0]
        #     os.system(f"cp {reb} {os.path.join(out, name)}")
            
    ###########################################################################
    #       Convert tests into events object with data serialization          #
    ###########################################################################
    
    # Init variables for tests processing
    tests   = fetch_tests(tests_path)
    n_tests = len(tests)
    n_event = 1
    
    print("=" * 20)
    print(f"Converting tests into streaming event object...")
    print("=" * 20)
    
    for i, test_path in enumerate(tests, start=1):
        bcom = pandas.read_csv(os.path.join(tests_path, test_path, LOG_BOT_COMPLETE), sep=" ")
        tcom = pandas.read_csv(os.path.join(tests_path, test_path, LOG_TCP_COMPLETE), sep=" ")
        tper = pandas.read_csv(os.path.join(tests_path, test_path, LOG_TCP_PERIODIC), sep=" ")
        ucom = pandas.read_csv(os.path.join(tests_path, test_path, LOG_UDP_COMPLETE), sep=" ")
        uper = pandas.read_csv(os.path.join(tests_path, test_path, LOG_UDP_PERIODIC), sep=" ")
        
        # Process Tstat logs
        for frame in [tcom, tper, ucom, uper]:
            frame.columns = [re.sub(r'[#:0-9]', '', col) for col in frame.columns]
            frame.drop(frame[frame["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_private)].index,   inplace=True)
            frame.drop(frame[frame["s_ip"].apply(lambda ip: ipaddress.ip_address(ip).is_multicast)].index, inplace=True)

        tcom["id"] = tcom[["s_ip", "s_port", "c_ip", "c_port"]].astype(str).agg("-".join, axis=1)
        ucom["id"] = ucom[["s_ip", "s_port", "c_ip", "c_port"]].astype(str).agg("-".join, axis=1)
        tper["id"] = tper[["s_ip", "s_port", "c_ip", "c_port"]].astype(str).agg("-".join, axis=1)
        uper["id"] = uper[["s_ip", "s_port", "c_ip", "c_port"]].astype(str).agg("-".join, axis=1)
        
        tcom["cname"] = tcom.apply(lambda r: cname(record=r, protocol=Protocol.TCP), axis=1)
        ucom["cname"] = ucom.apply(lambda r: cname(record=r, protocol=Protocol.UDP), axis=1)
        
        tper = tper.merge(tcom[["id", "cname"]], on="id", how="left")
        uper = uper.merge(ucom[["id", "cname"]], on="id", how="left")
        
        tcom["ts"] = tcom["first"]       - bcom.iloc[0]["abs"]
        tcom["te"] = tcom["last"]        - bcom.iloc[0]["abs"]
        ucom["ts"] = ucom["s_first_abs"] - bcom.iloc[0]["abs"]
        ucom["te"] = ucom["s_first_abs"] - bcom.iloc[0]["abs"] + (ucom["s_durat"] * 1000)

        tper["ts"] = tper["time_abs_start"] - bcom.iloc[0]["abs"]
        tper["te"] = tper["time_abs_start"] - bcom.iloc[0]["abs"] + tper["bin_duration"]
        uper["ts"] = uper["time_abs_start"] - bcom.iloc[0]["abs"]
        uper["te"] = uper["time_abs_start"] - bcom.iloc[0]["abs"] + uper["bin_duration"]
        
        # Process Puppeter logs
        hcom = process_har(file=os.path.join(tests_path, test_path, LOG_HAR_COMPLETE), provider=provider, start=bcom.iloc[0]["abs"])
        
        # Process Buffering logs
        rcom = None
        path = os.path.join(tests_path, test_path, LOG_REB_COMPLETE)
        if os.path.exists(path):
            rcom       = pandas.read_csv(path, sep=" ", names=["ti", "status"])
            rcom["ti"] = rcom["ti"] - bcom.iloc[0]["abs"]
        
        # Save processed documents
        tcom.to_csv(os.path.join(tests_path, test_path, LOG_TCP_COMPLETE), sep=" ", index=False)
        ucom.to_csv(os.path.join(tests_path, test_path, LOG_UDP_COMPLETE), sep=" ", index=False)
        tper.to_csv(os.path.join(tests_path, test_path, LOG_TCP_PERIODIC), sep=" ", index=False)
        uper.to_csv(os.path.join(tests_path, test_path, LOG_UDP_PERIODIC), sep=" ", index=False)
        hcom.to_csv(os.path.join(tests_path, test_path, LOG_HAR_COMPLETE), sep=" ", index=False)
        
        # Rebuffering trace
        # path = os.path.join(tests_path, test_path, LOG_REB_COMPLETE)
        # if os.path.exists(path):
        #     rcom.to_csv(os.path.join(tests_path, test_path, LOG_HAR_COMPLETE), sep=" ", index=False)
    
        # Get all events
        periods = get_events(frame=bcom)
        for period in periods:
            ts = period[0]
            te = period[1]
            
            # Define the event data structure
            data = {
                "ts":   float(ts),
                "te":   float(te),
                "tcp":  tcom[(tcom["ts"] <= te) & (tcom["te"] >= ts)].to_dict(orient="records"),
                "udp":  ucom[(ucom["ts"] <= te) & (ucom["te"] >= ts)].to_dict(orient="records"),
                "http": hcom[(hcom["ts"] <= te) & (hcom["te"] >= ts)].to_dict(orient="records"),
            }
            
            # Edit the event data structure (if rebuffering is provided)
            # if reb is not None:
            #     data["status"] = rcom[(rcom["ti"] <= te) & 
            #                           (rcom["ti"] >= ts)].to_dict(orient="records")
        
            for flow in data["tcp"]:
                flow["bins"] = tper[
                    (tper["ts"] <= te) & 
                    (tper["te"] >= ts) &
                    (tper["c_ip"]   == flow["c_ip"])   &
                    (tper["s_ip"]   == flow["s_ip"])   &
                    (tper["c_port"] == flow["c_port"]) &
                    (tper["s_port"] == flow["s_port"])].to_dict(orient="records")
            
            for flow in data["udp"]:
                flow["bins"] = uper[
                    (uper["ts"] <= te) & 
                    (uper["te"] >= ts) &
                    (uper["c_ip"]   == flow["c_ip"])   &
                    (uper["s_ip"]   == flow["s_ip"])   &
                    (uper["c_port"] == flow["c_port"]) &
                    (uper["s_port"] == flow["s_port"])].to_dict(orient="records")
            
            # Write on disk streaming event
            event_path = os.path.join(events_path, f"event-{n_event}.json")
            with open(event_path, "w") as j:
                json.dump(data, j, indent=4)
            
            # Update the streaming events counter
            n_event += 1 

if __name__ == "__main__":
    main()
    print() 
