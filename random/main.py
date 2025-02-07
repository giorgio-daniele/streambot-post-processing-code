import os
import shutil


caps = []
caps_cnt = 1
hars = []
hars_cnt = 1
csvs = []
csvs_cnt = 1

# Define the path where the files will be saved
path = os.path.join(os.getcwd(), "dataset3", "data")

# Create the output directory if it doesn't exist
os.makedirs(path, exist_ok=True)

# Loop over each folder in the current directory
for fold in os.listdir(os.getcwd()):
    if not os.path.isdir(os.path.join(os.getcwd(), fold)):
        continue
    
    # Loop over the files in the folder
    for file in os.listdir(os.path.join(os.getcwd(), fold)):
        # Check for 'log_net_complete-'
        if file.startswith('log_net_complete-'):
            name, ext = file.split(".")
            name, num = name.split("-")
            # Create a new file path for 'log_net_complete-'
            new_path = os.path.join(path, f"{name}-{caps_cnt}.{ext}")
            caps.append(new_path)
            # Copy the file to the new folder
            shutil.move(os.path.join(os.getcwd(), fold, file), new_path)
            caps_cnt += 1  # Increment the counter for unique renaming
        
        # Check for 'log_har_complete-'
        if file.startswith('log_har_complete-'):
            name, ext = file.split(".")
            name, num = name.split("-")
            # Create a new file path for 'log_har_complete-'
            new_path = os.path.join(path, f"{name}-{hars_cnt}.{ext}")
            hars.append(new_path)
            # Copy the file to the new folder
            shutil.move(os.path.join(os.getcwd(), fold, file), new_path)
            hars_cnt += 1  # Increment the counter for unique renaming
        
        # Check for 'log_bot_complete-'
        if file.startswith('log_bot_complete-'):
            name, ext = file.split(".")
            name, num = name.split("-")
            # Create a new file path for 'log_bot_complete-'
            new_path = os.path.join(path, f"{name}-{csvs_cnt}.{ext}")
            csvs.append(new_path)
            # Copy the file to the new folder
            shutil.move(os.path.join(os.getcwd(), fold, file), new_path)
            csvs_cnt += 1  # Increment the counter for unique renaming

print(f"Files have been merged and renamed in {path}")
