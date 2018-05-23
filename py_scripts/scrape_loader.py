import subprocess
import os

def _format_gcloud_bash(filepath, day_dir):
    template = 'sudo gsutil cp {} gs://ac-transit/traces/{}/'
    formatted = template.format(filepath, day_dir)
    return formatted

# First, get all directories in busdata
main_dir = 'busdata'
all_day_directories = [g for g in os.walk(main_dir)][0][1]

# Craft a full list of paths to upload
to_upload = []
for day_dir in all_day_directories:
    full_day_dir_path = os.path.join(main_dir, day_dir)

    for filename in os.listdir(full_day_dir_path):
        if filename.endswith('.json'):
            fpath = os.path.join(full_day_dir_path, filename)
            to_upload.append((fpath, day_dir))

upload_command = []
for fpath, day_dir in to_upload:
    # First we want to upload the file
    new_cmd = _format_gcloud_bash(fpath, day_dir)
    upload_command.append(new_cmd)

# Now concat them all to a single command
single_bash = ' && '.join(upload_command[:5])

# Now actually run the commands altogether
subprocess.Popen(['/bin/bash', '-c', single_bash])

print('This should happen afterwards')