import colorsys
import datetime
import json
import math
import os
import random
import shutil
import time

import dotenv
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

SECONDS_RESOLUTION = 10


def get_env_var(env_var):
    if env_var not in os.environ:
        raise KeyError('No tokens set under {} in .env file'.format(env_var))
    return str(os.environ[env_var])

dotenv.load()  # Make sure we load in the .env file
CONSUMER_KEY = get_env_var('CONSUMER_KEY')
CONSUMER_SECRET = get_env_var('CONSUMER_SECRET')
ACCESS_KEY = get_env_var('ACCESS_KEY')
ACCESS_SECRET = get_env_var('ACCESS_SECRET')


def parse_filename_as_datetime(filename):
    secs = filename.split('/')[-1].split('.')[0]
    secs = int(secs)
    return datetime.datetime.fromtimestamp(secs)


def get_counts_by_day_and_hour(target_files):
    # From this list of files, use the filenames to determine
    # which day-hour combo has the most datapoints associated
    dates_counter = {}
    for tf in target_files:
        dt = parse_filename_as_datetime(tf)
        
        # Pull out the day and hour values and
        # convert to string types
        day = str(dt.day)
        hour = str(dt.hour)

        # These two steps make sure that each
        # key-value is initialized
        if day not in dates_counter.keys():
            dates_counter[day] = {}

        if hour not in dates_counter[day].keys():
            dates_counter[day][hour] = 0

        # Now that we are have everything initialized, we
        # can read in the files to get the counts
        # for each file
        with open(tf) as f:
            traces = json.loads(f.read())

            # Sometimes you don't get any GTFS-RT
            # data in the protobuf response
            if 'entity' not in traces:
                continue

            # Otherwise, all we want for now is the length
            # of the entities array
            l = len(traces['entity'])
            
            # Now just increase the count by that value
            dates_counter[day][hour] += l
    
    return dates_counter


def get_busiest_hour_filepaths(target_directory):
    # First, read in all the files in the target directory
    target_files = []
    for f in os.listdir(target_directory):
        # Select only those that are .json format
        if f.endswith('.json'):
            target_files.append(os.path.join(target_directory, f))

    # Get a dictionary of datapoints sorted by day, hour
    dates_counter = get_counts_by_day_and_hour(target_files)

    # From this reference dictionary, we can now calculate the
    # peak hour (and day) from the available trace data
    peak = None
    for d in dates_counter.keys():
        for h in dates_counter[d].keys():
            val = dates_counter[d][h]
            if peak is None or peak['v'] < val:
                peak = {
                    'd': d,
                    'h': h,
                    'v': val
                }

    # Report back what the peak hour was
    print('Peak day ({}) hour ({}) count ({})'.format(peak['d'], peak['h'], peak['v']))

    # Now that we know the day and hour, go back through the
    # original potential .json files and subselect just the ones
    # that fall in our desired day-hour bracket of time
    keep_filepaths = []
    for tf in target_files:
        dt = parse_filename_as_datetime(tf)
        same_day = str(dt.day) == str(peak['d'])
        same_hour = str(dt.hour) == str(peak['h'])
        if same_day and same_hour:
            keep_filepaths.append(tf)

    # Return the subset of filepaths that
    # are in the timeframe we want to evaluate
    return keep_filepaths


def compile_trace_packages(keep_target_files):
    # Initialize tracking dict
    compiled_traces = {}

    # Read in each trace package JSON
    for target_file in keep_target_files:
        traces = None
        with open(target_file) as f:
            traces = json.loads(f.read())

        # Because we checked earlier, we should be able
        # to safely assume that these are all JSONs that are
        # both valid and contain entities
        for e in traces['entity']:
            # Extract the object contained vehicle data
            veh = e['vehicle']
            
            # And then also get the route's primary name
            trip = veh['trip']
            rid = str(trip['routeId']).split('-')[0]

            # Add to the dict if not yet initialized
            if rid not in compiled_traces.keys():
                compiled_traces[rid] = []

            # Update the reference dictionary
            compiled_traces[rid].append(veh)
        
    # Return compiles results object
    return compiled_traces


def generate_trace_dfs_reference(keep_target_files):
    # First, process in all the relevant
    # trace package filepaths
    compiled_traces = compile_trace_packages(keep_target_files)
    
    # Initialize the compiled trace dataframe the will
    # be used to hold dataframes describing each route's
    # composite trace data
    compiled_traces_dfs = {}

    for r in compiled_traces.keys():
        # Creating a matrix of n rows to be converted into
        # a pandas DataFrame at the end of the loop
        rows = []
        
        # For each trace extracted
        for ea in compiled_traces[r]:
            # Figure out the earliest timestamp, add a
            # new, formatted dictionary
            formatted = {
                'lat': float(ea['position']['latitude']),
                'lon': float(ea['position']['longitude']),
                'timestamp': int(ea['timestamp']),
                'trip_id': ea['trip']['tripId'],
                'rte_id': ea['trip']['routeId'],
                'veh_id': ea['vehicle']['id']}
            rows.append(formatted)

        # Then, conver the composite of all outputs to a DataFrame
        res_df = pd.DataFrame(rows)
        compiled_traces_dfs[r] = res_df
    
    # Return those summary results
    return compiled_traces_dfs


def interpolate_intermediaries(total_segment, time_frame=SECONDS_RESOLUTION):
    res = []
    for fr, to in zip(total_segment[:-1], total_segment[1:]):
        delta = to['timestamp'] - fr['timestamp']
        break_count = math.ceil(delta/float(time_frame))
        break_val = round(float(delta)/break_count, 3)

        line = LineString([fr['position'], to['position']])
        s = 0.000000001
        n = break_count + 1
        pts = [line.interpolate((i/n), normalized=True) for i in range(1, n)]

        # Iterates through and breaks each segment into components that are
        for i in range(len(pts)):
            res.append({
                'position': [pts[i].x, pts[i].y],
                'timestamp': fr['timestamp'] + (break_val * i),
            })
    return res


def calc_time_delta(df):
    sorted_df = df.sort_values('timestamp')
    deduped = sorted_df.drop_duplicates(subset=['timestamp'])
    deduped = deduped.reset_index()
    
    res = []
    for i, row in deduped.iterrows():
        res.append({
            'position': [row.lon, row.lat],
            'timestamp': row.timestamp,
        })
    if len(res) > 3:
        interpolated = interpolate_intermediaries(res)
        return interpolated
    else:
        return None


def clean_and_group_route_traces(compiled_traces_dfs):
    # Initialize processed compiled dict
    processed_traces = {}
    for key in compiled_traces_dfs.keys():
        # First pull out each route as a DataFrame
        df = compiled_traces_dfs[key]
        try:
            # Then try and perform the chained group operations
            parsed = df.groupby('veh_id').apply(lambda x: x.groupby('trip_id').apply(calc_time_delta))
            parsed = parsed[~parsed.isnull()]
            
            # Let's just drop marginally relevant routes
            # and not plot them here
            if len(parsed) > 0:
                processed_traces[key] = parsed
        
        # Some traces have no valid data for the timeframe
        # although they emitted a location or two; in such situations
        # just warn the user
        except Exception as e:
            print('Skipping {} because {}'.format(key, e))
    
    return processed_traces


def make_rand_col():
    h,s,l = random.random(), 0.5 + random.random()/2.0, 0.4 + random.random()/5.0
    r,g,b = [int(256*i) for i in colorsys.hls_to_rgb(h,l,s)]
    return '#%02x%02x%02x' % (r,g,b)


def generate_color_lookup(processed_traces):
    color_lookup = {}
    for key in processed_traces.keys():
        color_lookup[key] = make_rand_col()
    return color_lookup


def nameify(val):
    return str(val).zfill(3)


def get_plot_timeframe(compiled):
    minimum = None
    maximum = None
    for key in compiled.keys():
        mi = compiled[key].timestamp.min()
        if minimum is None or minimum > mi:
            minimum = mi

        ma = compiled[key].timestamp.max()
        if maximum is None or maximum < ma:
            maximum = ma
    return (minimum, maximum)


def plot_grouped_route_trace_results(start, end, grouped):
    color_lookup = generate_color_lookup(grouped)
    print('Start of analysis period: {}\nEnd of analysis period: {}'.format(start, end))

    curr_thresh = start + SECONDS_RESOLUTION
    count = 0
    while curr_thresh <= end:
        curr_thresh = start + (count * SECONDS_RESOLUTION)
        to_plot = []
        for key in grouped.keys():
            parsed = grouped[key]
            for p in parsed:
                if not isinstance(p, list):
                    continue
                filtered_p = list(filter(lambda x: x['timestamp'] <= curr_thresh, p))
                if len(filtered_p) > 0:
                    most_recent = max(filtered_p, key=lambda x: x['timestamp'])
                    to_plot.append({
                        'p': Point(most_recent['position']),
                        'color': color_lookup[key]
                    })

        # TODO: Clarify plotting structure
        # A vat of gobbledegook to poof out a matplotlib chart with little
        # care for reproducibility or legibility, just as MPL was intended (!)
        gdf = gpd.GeoDataFrame(to_plot, geometry=[x['p'] for x in to_plot])
        fig, ax = plt.subplots(figsize=(8,8), facecolor='black')
        gdf.plot(ax=ax, column='color', marker='.', markersize=16, cmap='cool')
        # Make the background black, both for the plot and all plots
        plt.style.use('dark_background')
        fig.set_facecolor('black')
        ax.set_facecolor('black')
        # Turn off the x and y axis
        ax.axes.get_xaxis().set_visible(False)
        ax.get_yaxis().set_visible(False)
        # This so happens to be the bounding box of the
        # AC Transit service area, roughly
        ax.set_xlim(-122.42515,-121.812707)
        ax.set_ylim(37.431498,37.998755)
        # Makes the buffer around the plot smaller
        plt.tight_layout()
        # Save the output
        plt.savefig('gif/{}.png'.format(nameify(count)))
        # Clear the state of the plot
        plt.close()

        # Update the count we are keeping track of
        count += 1


def tweet(gif_loc):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
    api = tweepy.API(auth)
    api.update_with_media(gif_loc, status='')


# Run when this script is invoked
if __name__ == '__main__':
    # Make sure that busdata_raw exists
    dest_dir = 'busdata_raw'
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # First pull down the previous day's images
    tod = datetime.date.today().isoformat().replace('-', '')
    # formatted_command = 'gsutil cp gs://ac-transit/traces/{}/* {}/'.format(tod, dest_dir)
    # ret = os.system(formatted_command)
    # if ret != 0 :
    #     print('The gustil command to pull down a day\'s worth of traces failed.')

    # Make sure that output_dir exists, so resulting files can be saved to
    # this director adn clear out previous outputs
    # output_dir = 'gif'
    # if os.path.exists(output_dir):
    #     shutil.rmtree(output_dir)
    # os.makedirs(output_dir)

    # target_filepaths = get_busiest_hour_filepaths('busdata_raw/')
    # compiled = generate_trace_dfs_reference(target_filepaths)
    # start, end = get_plot_timeframe(compiled)
    # grouped = clean_and_group_route_traces(compiled)
    # plot_grouped_route_trace_results(start, end, grouped)

    command = 'convert -limit memory 350MB -delay 20 -loop 0 gif/*.png  gif/animate.gif'
    ret = os.system(command)
    if ret != 0 :
        print('The convert imagemagick command to compile into gif failed.')
    else:
        tweet('gif/animate.gif')