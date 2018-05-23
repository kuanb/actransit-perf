import subprocess
import os

# First, get all directories in busdata
main_dir = 'busdata'
all_day_directories = [g for g in os.walk(main_dir)][0][1]

# Craft a full list of paths to upload
to_upload = []
for day_dir in all_day_directories:
    full_day_dir_path = os.path.join(main_dir, day_dir)

    for filename in os.listdir(full_day_dir_path):
        if filename.endswith('.json'):
            print('filename', filename)
            # to_upload.append(to_upload)

# args = ['echo', 'Hello!']
# subprocess.Popen(args)