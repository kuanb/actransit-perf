import glob
import json
import os
import requests
import time
import random

from google.protobuf import json_format
from google.transit import gtfs_realtime_pb2

# Globals
AC_BASE_URL = 'http://api.actransit.org/transit'

# Acquire the tokens from the .env file in root
token_env_var = 'ACT_GTFSRT_TOKENS'
if token_env_var not in os.environ:
    raise KeyError('No tokens set under {} in .env file'.format(token_env_var))
tokens = os.environ[token_env_var]


def convert_pb_to_json(content):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)
    return json.loads(json_format.MessageToJson(feed))


def get_daily_dir():
    curr_day = time.strftime('%Y%m%d')
    target_dir = 'busdata/{}'.format(curr_day)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    return target_dir


def get_vehicles_url(token):
    return '{}/gtfsrt/vehicles?token={}'.format(AC_BASE_URL, token)


while True:
    # AC Transit is finicky about hitting rate limits even though we
    # aren't even getting remotely close according to their TOU
    token = random.choice(tokens)

    # Pull down protobuf
    template = get_vehicles_url(token)
    resp = requests.get(template)
    try:
        res_json = convert_pb_to_json(resp.content)
    except Exception as e:
        print('Error occurred on this query: {}'.format(template))
        print('Error: {}'.format(e))
        res_json = False

    if res_json is not False:
        # Create output file location
        seconds = int(round(time.time(), 0))
        output_fname = '.'.join([str(seconds), 'json'])
        output_fpath = '/'.join([get_daily_dir(), output_fname])

        print('Got locations; saving to {}'.format(output_fpath))
        with open(output_fpath, 'w') as outfile:
            json.dump(res_json, outfile)

    time.sleep(30)
