from datetime import datetime
import json
import os
import sys
import time

import pandas as pd

# TODO: Load source using GCloud utils, download
#       to local tempfile
day_dir = 'busdata2/'

# Make sure that we only are reviewing the JSON files
to_use = []
for c in os.listdir(day_dir):
    fpath = '{}{}'.format(day_dir, c)
    if fpath.endswith('.json'):
        c2 = int(c.replace('.json', ''))
        t = datetime.fromtimestamp(c2)
        to_use.append(fpath)

# We will be assembling 2 daily wrap up CSVs
routes_results = []
vehicle_res_tracker = {}
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
    
    # We make certain assumptions about the data structure scraped
    for d in data:
        route_id = d['RouteId']

        # Don't add unneeded route data (we just need metadata for a route once
        # for each discrete route)
        already_have = any(x['id'] == route_id for x in routes_results)
        if not already_have:
            routes_results.append({
                'id': route_id,
                'name': d['Name'],
                'description': d['Description']
            })
            
            # Also add to our "tracker" which helps prevent
            # redundant vehicle traces
            vehicle_res_tracker[route_id] = {}

        for v in d['vehicles']:
            try:
                parsed_time = datetime.strptime(v['TimeLastReported'], '%Y-%m-%dT%H:%M:%S')
                parsed_secs = time.mktime(parsed_time.timetuple())
                
                # TODO: Refactor this check blob
                # Now we need to first make sure that we have not already
                # added this vehicle trace into our lookup and, if we have,
                # we should skip it
                ADD_OK = True
                
                # First, it needs to have the trip
                trip_id = v['CurrentTripId']
                vid = v['VehicleId']
                
                vrt_route = vehicle_res_tracker[route_id]
                if trip_id in vrt_route.keys():
                    vrt_trip = vrt_route[trip_id]
                    
                    if vid in vrt_trip.keys():
                        # If it does, does it already have this time
                        # reported for this route-trip-vehicle combo?
                        if parsed_secs in vrt_trip:
                            ADD_OK = False
                    else:
                        vrt_route[trip_id][vid] = []
                else:
                    # Create the lookup component
                    vrt_route[trip_id] = {}
                    vrt_route[trip_id][vid] = []

                if ADD_OK:
                    # Update both the lookup and the final data array
                    vrt_route[trip_id][vid].append(parsed_time)
                    vehicle_results.append({
                        'trip_id': trip_id,
                        'route_id': route_id,
                        'heading': v['Heading'],
                        'lat': v['Latitude'],
                        'lon': v['Longitude'],
                        'id': vid,
                        'time': parsed_time
                    })
            except Exception as e:
                print('Something happened parsing vehicle {}: {}'.format(v, e))
    
    # Probably unnecessary, but since we are dealing with
    # so much data, better to aggressively flush the data being read in
    data = None

# At this point, we should be able to conver the results
# lists into 2 dataframes
routes_results = pd.DataFrame(routes_results)
vehicle_results = pd.DataFrame(vehicle_results)

# Some more housekeeping/notetaking logging
veh_size = sys.getsizeof(vehicle_results) / 1000000.0
print('Parsed vehicles dataset for the day is {} mb'.format(veh_size))

res = {}
for trip_id in vehicle_results.trip_id.unique():
    vrsub = vehicle_results[vehicle_results.trip_id == trip_id]
    vrsub = vrsub.sort_values('time')
    vrsub = vrsub.drop_duplicates(subset='time', keep='first')
    res[trip_id] = len(vrsub)

all_features = []

for target_trip_id in [i for i, v in res.items()]:
    vrsub = vehicle_results[vehicle_results.trip_id == target_trip_id]
    vrsub = vrsub.sort_values('time')
    vrsub = vrsub.drop_duplicates(subset='time', keep='first')
    
    geom = {
        'type': 'MultiPoint',
        'coordinates': [[round(float(row['lon']), 5),
                         round(float(row['lat']), 5)] for i, row in vrsub.iterrows()]
    }
    
    # Skip adding this if not endatetimough data
    if len(geom['coordinates']) < 10:
        continueSomething

    feature = {
          'type': 'Feature',
          'properties': {
              'route_id': str(vrsub.route_id.values[0]),
              'trip_id': str(target_trip_id)
          },
          'geometry': geom
    }

    all_features.append(feature)

fc = {
    'type': 'FeatureCollection',
    'features': all_features
}

import json
with open('daily.json', 'w') as outfile:
    json.dump(fc, outfile)
