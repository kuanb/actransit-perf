import subprocess
import os

# First, get all directories in busdata
main_dir = 'busdata'
all_day_directories = [g for g in os.walk(main_dir)][0][1]

for day_dir in all_day_directories:
    full_day_dir_path = os.path.join(main_dir, day_dir)
    print('full_day_dir_path: ', full_day_dir_path)

# args = ['echo', 'Hello!']
# subprocess.Popen(args)