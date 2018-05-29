import colorsys
import datetime
import json
import os
import random
import sys
import time

import pandas as pd

# TODO: Load source using GCloud utils, download
#       to local tempfile
day_dir = 'busdata_raw/'


def get_random_bright_color():
    h, s, l = random.random(), 0.5 + random.random()/2.0, 0.4 + random.random()/5.0
    r, g, b = [int(256*i) for i in colorsys.hls_to_rgb(h,l,s)]
    return '#%02x%02x%02x' % (r, g, b)


def add_colors_to_routes(vr: pd.DataFrame):
    route_color_lookup = []
    for vid in vr.route_id.unique():
        route_color_lookup.append({
            'route_id': vid,
            'color': get_random_bright_color()
        })
        
    # Convert color lookup to a dataframe
    color_lookup = pd.DataFrame(route_color_lookup)
    
    # Make sure the target column has the right type for both sides
    color_lookup['route_id'] = color_lookup['route_id'].astype(str)
    vr['route_id'] = vr['route_id'].astype(str)
    
    # Merge in the new column
    return pd.merge(vr, color_lookup, how='left', on='route_id')


# Helper functions
def summarize(entity):
    # Bail early if it is not parseable dict
    if not isinstance(entity, dict):
        print("1", entity)
        return None

    veh = entity['vehicle']
    lat = float(veh['position']['latitude'])
    lon = float(veh['position']['longitude'])

    if 'speed' in veh['position']:
        speed = float(veh['position']['speed'])
    else:
        speed = None

    timestamp = datetime.datetime.utcfromtimestamp(int(veh['timestamp']))
    route_id = veh['trip']['routeId']
    trip_id = veh['trip']['tripId']
    vehicle_id = veh['vehicle']['id']
    
    return {
        'route_id': route_id,
        'trip_id': trip_id,
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'lat': lat,
        'lon': lon,
        'speed': speed,
    }


def get_all_possible_jsons(target_dir):
    # Make sure that we only are reviewing the JSON files
    to_use = []
    for c in os.listdir(target_dir):
        fpath = '{}{}'.format(target_dir, c)
        if fpath.endswith('.json'):
            c2 = int(c.replace('.json', ''))
            t = datetime.datetime.fromtimestamp(c2)
            to_use.append(fpath)
    return to_use

            
def generate_vehicle_results_df(to_use: list):
    # We will be assembling 2 daily wrap up CSVs
    vehicle_results = []

    # Iterate through the days' data
    for fpath in to_use:
        # Try to load the vehicle locations as json
        data = None
        try:
            with open(fpath, mode='r') as f:
                data = json.load(f)
        # Though some requests returned invalid data, in
        # which case make a note of it and move on
        except Exception as e:
            print('Error opening {}'.format(fpath), e)
            continue

        # Make sure the data saved is what we expects
        data_ok = False
        if isinstance(data, dict):
            if 'entity' in data.keys():
                data_ok = True
            else:
                print('Data missing \'entity\' key: {}'.format(data))
        else:
            print('Data invalid format: {}'.format(data))

        # Skip if the data is not usable
        if not data_ok:
            continue

        # We make certain assumptions about the data structure scraped
        cleaned = []
        for d in data['entity']:
            try:
                sd = summarize(d)
                if sd is not None:
                    cleaned.append(sd)
            except Exception as e:
                print('Error parsing an entity: {}'.format(e))

        if len(cleaned):
            vehicle_results.extend(cleaned)
        else:
            print('{} had no valid location data'.format(path))

        # Probably unnecessary, but since we are dealing with
        # so much data, better to aggressively flush the data being read in
        data = None

    # At this point, we should be able to conver the results
    # lists into 2 dataframes
    vehicle_results = pd.DataFrame(vehicle_results)
    vr_trimmed = vehicle_results.drop_duplicates(subset=['trip_id', 'vehicle_id', 'timestamp'])
    print('Removed duplicates from vehicle trace count '
          '({} to {} rows)'.format(len(vehicle_results), len(vr_trimmed)))
    
    # Then add colors to the result before returning
    vr_with_colors = add_colors_to_routes(vr_trimmed)
    return vr_with_colors


def create_geometies(df):
    df = df.sort_values(by='timestamp')
    feature = {
        'type': 'Feature',
        'properties': {
            'route_id': df.route_id.values[0],
            'trip_id': df.trip_id.values[0],
            'vehicle_id': df.vehicle_id.values[0],
            'color': df.color.values[0],
        },
        'geometry': {
            'type': 'LineString',
            'coordinates': [list(ll) for ll in zip(df.lon, df.lat)]
        }
    }
    return feature


def nested_feature_geom_rollup(df):
    return df.groupby('vehicle_id').apply(create_geometies)


def generate_sorted_feature_collections(vehicle_results: pd.DataFrame):
    vr_sorted = vehicle_results.sort_values(by='timestamp')
    nested_features = vr_sorted.groupby(pd.Grouper(freq='10Min', key='timestamp')).apply(nested_feature_geom_rollup)

    unique_secs = []
    super_fc = {}
    for i, feat in nested_features.iteritems():
        # First get the time in seconds
        timelevel = i[0]
        epoch = datetime.datetime.utcfromtimestamp(0)
        secs = (timelevel.to_pydatetime() - epoch).total_seconds()
        secs = int(secs)
        secs = str(secs)

        # Other index is just the route
        route_id = i[1]

        # Catch initializer
        if secs not in super_fc.keys():
            super_fc[secs] = {
                'type': 'FeatureCollection',
                'features': []
            }

        super_fc[secs]['features'].append(feat)
        # Also used for next step
        unique_secs.append(secs)

    # Then convert the whole thing into a list instead of a nested dict
    as_int = [int(x) for x in list(set(unique_secs))]
    as_str = [str(x) for x in sorted(as_int)]

    super_fc_list = []
    for secs in as_str:
        super_fc_list.append(super_fc[secs])
        
    return super_fc_list


# Execution
list_of_jsons = get_all_possible_jsons(day_dir)
vehicle_results = generate_vehicle_results_df(list_of_jsons)
sorted_fcs = generate_sorted_feature_collections(vehicle_results)

import json
with open('daily.json', 'w') as outfile:
    json.dump(sorted_fcs, outfile)
