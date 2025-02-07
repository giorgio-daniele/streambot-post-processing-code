#!bin/bash

# Navigate to the directory containing the files
cd /home/giorgiodaniele/Desktop/thesis-code/amazon/2025-01-28_14-32-31

# Loop through the files that match the pattern and renumber them
count=113
for file in log_net_complete-*.pcap; do
    # Extract the extension (.pcap)
    extension="${file##*.}"
    
    # Rename the file with the new number
    mv "$file" "log_net_complete-$count.$extension"
    
    # Increment the counter
    ((count++))
done

# Loop through the files that match the pattern and renumber them
count=113
for file in log_bot_complete-*.csv; do
    # Extract the extension (.pcap)
    extension="${file##*.}"
    
    # Rename the file with the new number
    mv "$file" "log_bot_complete-$count.$extension"
    
    # Increment the counter
    ((count++))
done

# Loop through the files that match the pattern and renumber them
count=113
for file in log_har_complete-*.har; do
    # Extract the extension (.pcap)
    extension="${file##*.}"
    
    # Rename the file with the new number
    mv "$file" "log_har_complete-$count.$extension"
    
    # Increment the counter
    ((count++))
done

cd
cd Desktop/thesis-code
