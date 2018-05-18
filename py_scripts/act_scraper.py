import json
import os
import requests
import time
import random

# Globals
AC_BASE_URL = 'http://api.actransit.org/transit'

# TODO: These should be acquired through a .env variable passed
#       to the workers doing the data acquisition
tokens = ['***']


def get_daily_dir():
    curr_day = time.strftime('%Y%m%d')
    target_dir = 'busdata/{}'.format(curr_day)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    return target_dir


def get_routes_url(token):
    return '{}/routes?token={}'.format(AC_BASE_URL, token)


def get_vehicles_url(route_name, token):
    return '{}/route/{}/vehicles?token={}'.format(
        AC_BASE_URL, route_name, token)


while True:
    # AC Transit is finicky about hitting rate limits even though we
    # aren't even getting remotely close according to their TOU
    token = random.choice(tokens)

    output = []
    failed_routes = []
    for route in requests.get(get_routes_url(token)).json():
        try:
            template = get_vehicles_url(route['Name'], token)
            vehicles = requests.get(template)
            route['vehicles'] = vehicles.json()
            output.append(route)
        except Exception as e:
            failed_routes.append(route['Name'])

    if len(failed_routes):
        failed_routes_str = ','.join([str(x) for x in failed_routes])
        print('Error on querying routes {}'.format(failed_routes_str))

    seconds = int(round(time.time(), 0))
    output_fname = '.'.join([str(seconds), 'json'])
    output_fpath = '/'.join([get_daily_dir(), output_fname])

    output_packaged = {
        'timestamp': seconds,
        'data': output
    }
    
    print('Got locations; saving to {}'.format(output_fpath))
    with open(output_fpath, 'w') as outfile:
        json.dump(output_packaged, outfile)

    print('\n')
    time.sleep(30)
