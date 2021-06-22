import requests
from requests.adapters import HTTPAdapter
from requests.adapters import proxy_from_url
import fsspec
import xarray as xr
import pandas as pd
#from pandas.core.algorithms import isin
import numpy as np
from datetime import datetime
import json
from decimal import Decimal
import re
import errno
import copy
import os
import sys

# Do I need this here?
import logging


import get_variable_mappings as gvm
import rename_objects as rn
import check_if_ctd_file as ckctd
import filter_profiles as fp
import get_cruise_information as gi


# In order to use xarray open_dataset

# https://github.com/pydata/xarray/issues/3653
# pip install aiohttp
# pip install h5netcdf

# url = 'https://cchdo.ucsd.edu/data/16923/318M20130321_bottle.nc'
# with fsspec.open(url) as fobj:
#     ds = xr.open_dataset(fobj)


"""

Convert Go-Ship CTD and bottle CF netCDF files to ArgoVis JSON format

program by: Lynne Merchant
date: 2021

"""


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


def convert(o):

    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)


class fakefloat(float):
    def __init__(self, value):
        self._value = value

    def __repr__(self):
        return str(self._value)


# https://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object
def defaultencode(o):
    if isinstance(o, Decimal):
        # Subclass float with custom repr?
        return fakefloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")


def remove_file(filename, dir):
    filepath = os.path.join(dir, filename)

    try:
        os.remove(filepath)
    except OSError as e:
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occurred


def write_profile_goship_units(data_dict, logging_dir, type):

    filename = 'files_goship_units.txt'
    filepath = os.path.join(logging_dir, filename)

    if type == 'btl':
        goship_units = data_dict['goshipUnits']

        with open(filepath, 'a') as f:
            json.dump(goship_units, f, indent=4,
                      sort_keys=True, default=convert)

    if type == 'ctd':
        goship_units = data_dict['goshipUnits']

        with open(filepath, 'a') as f:
            json.dump(goship_units, f, indent=4,
                      sort_keys=True, default=convert)

    if type == 'btl_ctd':
        goship_units_btl = data_dict['goshipUnitsBtl']
        goship_units_ctd = data_dict['goshipUnitsCtd']
        goship_units = {**goship_units_btl, **goship_units_ctd}

        with open(filepath, 'a') as f:
            json.dump(goship_units_btl, f, indent=4,
                      sort_keys=True, default=convert)

        with open(filename, 'a') as f:
            json.dump(goship_units_ctd, f, indent=4,
                      sort_keys=True, default=convert)


def write_profile_json(json_dir, profile_dict, type):

    # Pop off meta key and use as start of data_dict
    meta_dict = profile_dict.pop('meta', None)

    if type == 'btl' or type == 'ctd':
        profile_dict.pop('profileNumber', None)
    elif type == 'btl_ctd':
        profile_dict.pop('profileNumberBtl', None)
        profile_dict.pop('profileNumberCtd', None)

    if type == 'btl' or type == 'ctd':
        profile_dict.pop('stationCast', None)
    elif type == 'btl_ctd':
        profile_dict.pop('stationCastBtl', None)
        profile_dict.pop('stationCastCtd', None)

    # Now combine with left over profile_dict
    data_dict = {**meta_dict, **profile_dict}

    id = data_dict['id']
    filename = f"{id}.json"
    expocode = data_dict['expocode']

    json_str = json.dumps(data_dict)

    # TODO
    # check did this earlier in program
    # _qc":2.0
    # If qc value in json_str matches '.0' at end, remove it to get an int qc
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

    if '/' in filename:
        filename = filename.replace('/', '_')

    if '/' in expocode:
        folder = expocode.replace('/', '_')
    else:
        folder = expocode

    path = os.path.join(json_dir, folder)

    if not os.path.exists(path):
        os.makedirs(path)

    file = os.path.join(json_dir, folder, filename)

    # TESTING
    # TODO Remove formatting when final

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable

    with open(file, 'w') as f:
        json.dump(data_dict, f, indent=4, sort_keys=True, default=convert)


def write_profiles_json(json_dir, profile_dicts, type):

    # Write profile_dict to as json to a profile file
    for profile_dict in profile_dicts:
        write_profile_json(json_dir, profile_dict, type)


def combine_bot_ctd_measurements(bot_measurements, ctd_measurements):

    # This is for one profile

    # 1) If temp_ctd and psal_ctd, exclude bottle objects
    # 2) If no temp_ctd but there is temp_btl, keep _btl objects,
    #    put nan in  temp_ctd of ctd objs and use btl objs  with temp_btl
    #   a) Keep _ctd object if there is  psal_ctd,
    #   b) otherwise delete object because just pressure variable
    # 3) If there is no psal_ctd, set to nan (keep objs), and use bottle
    #    objects, too. If no psal_btl, use salinity_btl. Still
    #    keep ctd objects if has temp_ctd (just missing psal_ctd)
    # Set measurements flag to 'CTD_BTL' if use btl BTL and CTD objs
    # If  just one  or the other, flag is CTD or BTL

    # First, filter ctd objects: keep only temp_ctd and psal_ctd
    # Question: What if some ctd objs missing psal, probably
    # just psal to nan and keep. Don't rely on btl because diff pressure
    # So check if min psal_ctd

    # Object will always have pres, psal_ctd, temp_ctd for ctd file
    # Object will always pres, temp_btl and psal_btl or salinity_btl in btl file

    use, use_elems = fp.find_measurements_hierarchy_bot_ctd(
        bot_measurements, ctd_measurements)

    combined_bot_ctd_measurements = fp.filter_bot_ctd_combined(
        bot_measurements, ctd_measurements, use_elems)

    # Get measurements flag
    if use['ctd'] and use['btl']:
        flag = 'BOT_CTD'
    elif use['ctd'] and not use['btl']:
        flag = 'CTD'
    elif not use['ctd'] and use['btl']:
        flag = 'BOT'
    else:
        flag = 'unknown'

    return combined_bot_ctd_measurements, flag


def combine_output_per_profile_bot_ctd(bot_renamed_dict, ctd_renamed_dict):

    profile_number_btl = None
    station_cast_btl = None
    bot_meta = {}
    bot_bgc_meas = []
    bot_measurements = []
    goship_argovis_name_mapping_bot = {}
    goship_ref_scale_mapping_bot = {}
    argovis_ref_scale_bot = {}
    goship_units_btl = {}
    goship_argovis_units_bot = {}

    profile_number_ctd = None
    station_cast_ctd = None
    ctd_meta = {}
    ctd_bgc_meas = []
    ctd_measurements = []
    goship_argovis_name_mapping_ctd = {}
    goship_ref_scale_mapping_ctd = {}
    argovis_ref_scale_ctd = {}
    goship_units_ctd = {}
    goship_argovis_units_ctd = {}

    # May have case where bot dict or ctd dict doesn't exist for same profile

    if bot_renamed_dict:

        profile_number_btl = bot_renamed_dict['profileNumber']
        station_cast_btl = bot_renamed_dict['stationCast']

        bot_meta = bot_renamed_dict['meta']

        bot_bgc_meas = bot_renamed_dict['bgcMeas']
        bot_measurements = bot_renamed_dict['measurements']

        goship_argovis_name_mapping_bot = bot_renamed_dict['goshipArgovisNameMapping']

        goship_ref_scale_mapping_bot = bot_renamed_dict['goshipReferenceScale']
        argovis_ref_scale_bot = bot_renamed_dict['argovisReferenceScale']
        goship_units_btl = bot_renamed_dict['goshipUnits']
        goship_argovis_units_bot = bot_renamed_dict['goshipArgovisUnitNameMapping']

    if ctd_renamed_dict:

        profile_number_ctd = ctd_renamed_dict['profileNumber']
        station_cast_ctd = ctd_renamed_dict['stationCast']

        # print(f"profile number ctd {profile_number_ctd}")

        ctd_meta = ctd_renamed_dict['meta']

        ctd_bgc_meas = ctd_renamed_dict['bgcMeas']
        ctd_measurements = ctd_renamed_dict['measurements']

        goship_argovis_name_mapping_ctd = ctd_renamed_dict['goshipArgovisNameMapping']

        goship_ref_scale_mapping_ctd = ctd_renamed_dict['goshipReferenceScale']
        argovis_ref_scale_ctd = ctd_renamed_dict['argovisReferenceScale']
        goship_units_ctd = ctd_renamed_dict['goshipUnits']
        goship_argovis_units_ctd = ctd_renamed_dict['goshipArgovisUnitNameMapping']

    if bot_renamed_dict and ctd_renamed_dict:

        bot_meta = rn.rename_bot_by_key_meta(bot_meta)

        # Add extension of '_btl' to lat/lon and cast in mapping
        new_obj = {}
        for key, val in goship_argovis_name_mapping_bot.items():
            if val == 'lat':
                new_obj[key] = 'lat_btl'
            elif val == 'lon':
                new_obj[key] = 'lon_btl'
            elif val == ['station_cast']:
                new_obj[key] = 'station_cast_btl'
            else:
                new_obj[key] = val

        goship_argovis_name_mapping_bot = new_obj

        # Remove _btl variables that are the same as CTD
        bot_meta.pop('expocode_btl', None)
        bot_meta.pop('cruise_url_btl', None)
        bot_meta.pop('DATA_CENTRE_btl', None)

    meta = {**ctd_meta, **bot_meta}

    bgc_meas = [*ctd_bgc_meas, *bot_bgc_meas]

    measurements, meas_flag = combine_bot_ctd_measurements(
        bot_measurements, ctd_measurements)

    measurements_source_qc = {"source": meas_flag, "qc": 2}

    goship_ref_scale_mapping = {
        **goship_ref_scale_mapping_ctd, **goship_ref_scale_mapping_bot}

    argovis_reference_scale = {
        **argovis_ref_scale_ctd, **argovis_ref_scale_bot}

    goship_argovis_units_mapping = {
        **goship_argovis_units_ctd, **goship_argovis_units_bot}

    combined_bot_ctd_dict = {}

    if profile_number_btl is not None and profile_number_ctd is not None:
        combined_bot_ctd_dict['profileNumberBtl'] = profile_number_btl
        combined_bot_ctd_dict['profileNumberCtd'] = profile_number_ctd
        combined_bot_ctd_dict['contains'] = 'btl_ctd'

    elif profile_number_btl is not None:
        combined_bot_ctd_dict['profileNumber'] = profile_number_btl
        combined_bot_ctd_dict['contains'] = 'btl'
    elif profile_number_ctd is not None:
        combined_bot_ctd_dict['profileNumber'] = profile_number_ctd
        combined_bot_ctd_dict['contains'] = 'ctd'

    if station_cast_btl is not None and station_cast_ctd is not None:
        combined_bot_ctd_dict['stationCastBtl'] = station_cast_btl
        combined_bot_ctd_dict['stationCastCtd'] = station_cast_ctd
    elif station_cast_btl is not None:
        combined_bot_ctd_dict['stationCast'] = station_cast_btl
    elif station_cast_ctd is not None:
        combined_bot_ctd_dict['stationCast'] = station_cast_ctd

    combined_bot_ctd_dict['meta'] = meta
    combined_bot_ctd_dict['measurements'] = measurements
    combined_bot_ctd_dict['bgcMeas'] = bgc_meas
    combined_bot_ctd_dict['measurementsSourceQC'] = measurements_source_qc

    if goship_argovis_name_mapping_bot and goship_argovis_name_mapping_ctd:
        combined_bot_ctd_dict['goshipArgovisNameMappingBtl'] = goship_argovis_name_mapping_bot
        combined_bot_ctd_dict['goshipArgovisNameMappingCtd'] = goship_argovis_name_mapping_ctd
    elif goship_argovis_name_mapping_bot:
        combined_bot_ctd_dict['goshipArgovisNameMapping'] = goship_argovis_name_mapping_bot
    elif goship_argovis_name_mapping_ctd:
        combined_bot_ctd_dict['goshipArgovisNameMapping'] = goship_argovis_name_mapping_ctd

    combined_bot_ctd_dict['goshipReferenceScale'] = goship_ref_scale_mapping
    combined_bot_ctd_dict['argovisReferenceScale'] = argovis_reference_scale
    combined_bot_ctd_dict['goshipArgovisUnitNameMapping'] = goship_argovis_units_mapping

    if goship_units_btl and goship_units_ctd:
        combined_bot_ctd_dict['goshipUnitsBtl'] = goship_units_btl
        combined_bot_ctd_dict['goshipUnitsCtd'] = goship_units_ctd
    elif goship_units_btl:
        combined_bot_ctd_dict['goshipUnits'] = goship_units_btl
    elif goship_units_ctd:
        combined_bot_ctd_dict['goshipUnits'] = goship_units_ctd

    return combined_bot_ctd_dict


def get_same_station_cast_profile_bot_ctd(bot_profile_dicts, ctd_profile_dicts):

    bot_num_profiles = range(len(bot_profile_dicts))
    ctd_num_profiles = range(len(ctd_profile_dicts))

    station_cast_btl = [bot_profile['stationCast']
                        for bot_profile in bot_profile_dicts]

    station_cast_ctd = [ctd_profile['stationCast']
                        for ctd_profile in ctd_profile_dicts]

    # To know which station_cast string matches to a profile
    station_cast_profile_btl = dict(zip(station_cast_btl, bot_num_profiles))
    station_cast_profile_ctd = dict(zip(station_cast_ctd, ctd_num_profiles))

    different_pairs_in_btl = set(station_cast_btl).difference(station_cast_ctd)
    different_pairs_in_ctd = set(station_cast_ctd).difference(station_cast_btl)

    index = len(ctd_profile_dicts) - 1
    for pair in different_pairs_in_btl:
        # Create matching but empty profiles for ctd
        # Create new profile number and same key
        # increment on the  last profile #
        station_cast_profile_ctd[pair] = index + 1
        ctd_profile_dicts.append({})
        index = index + 1

    index = len(bot_profile_dicts) - 1
    for pair in different_pairs_in_ctd:
        # Create matching but empty profiles for ctd
        # Create new profile number and same key
        # increment on the  last profile #
        station_cast_profile_btl[pair] = index+1
        bot_profile_dicts.append({})
        index = index + 1

    return station_cast_profile_btl, station_cast_profile_ctd, bot_profile_dicts, ctd_profile_dicts


def get_station_cast_profile(profile_dicts):

    num_profiles = range(len(profile_dicts))

    station_cast = [profile['station_cast'] for profile in profile_dicts]

    # Create a dictionary with tuple as key and profile num as value
    station_cast_profile = dict(zip(station_cast, num_profiles))

    return station_cast_profile


def combine_profile_dicts_bot_ctd(bot_profile_dicts, ctd_profile_dicts):

    # Get profile dicts so have the same number of profiles
    # one may be blank while the other exists at a cast
    station_cast_profile_btl, station_cast_profile_ctd, bot_profile_dicts, ctd_profile_dicts = get_same_station_cast_profile_bot_ctd(
        bot_profile_dicts, ctd_profile_dicts)

    #  bottle  and ctd have same keys, but  different values
    # which are the profile numbers

    profile_dicts_list_bot_ctd = []

    # for profile_number in range(num_profiles):
    for key in station_cast_profile_btl.keys():

        profile_number_btl = station_cast_profile_btl[key]
        bot_profile_dict = bot_profile_dicts[profile_number_btl]

        profile_number_ctd = station_cast_profile_ctd[key]
        ctd_profile_dict = ctd_profile_dicts[profile_number_ctd]

        combined_profile_dict_bot_ctd = combine_output_per_profile_bot_ctd(
            bot_profile_dict, ctd_profile_dict)

        profile_dicts_list_bot_ctd.append(combined_profile_dict_bot_ctd)

    return profile_dicts_list_bot_ctd, station_cast_profile_btl, station_cast_profile_ctd


def create_measurements_list(df_bgc_meas):

    df_meas = pd.DataFrame()

    # core values includes '_qc' vars
    core_values = gvm.get_goship_core_values()

    # First get subset of df_bgc_meas
    for val in core_values:

        try:
            df_meas[val] = df_bgc_meas[val].copy()
        except KeyError:
            pass

        try:
            val = f"{val}_qc"
            df_meas[val] = df_bgc_meas[val].copy()
        except KeyError:
            pass

    core_non_qc = [elem for elem in core_values if '_qc' not in elem]

    # If qc != 2, set corresponding value to np.nan
    for col in core_non_qc:
        qc_key = f"{col}_qc"

        if col == 'pressure' or col not in df_meas.columns:
            continue

        if qc_key not in df_meas.columns:
            df_meas[col] = np.nan
            continue

        try:
            df_meas[col] = df_meas.apply(lambda x: x[col] if pd.notnull(
                x[qc_key]) and int(x[qc_key]) == 2 else np.nan, axis=1)

        except KeyError:
            pass

    # drop qc columns now that have marked non_qc column values
    for col in df_meas.columns:
        if '_qc' in col:
            df_meas = df_meas.drop([col], axis=1)

    # If all core values have nan, drop row
    df_meas = df_meas.dropna(how='all')

    json_str = df_meas.to_json(orient='records')

    data_dict_list = json.loads(json_str)

    return data_dict_list


def create_bgc_meas_df(param_json_str):

    # Now split up param_json_str into multiple json dicts
    # And then only keep those that have a value not null for each key
    param_json_dict = json.loads(param_json_str)

    try:
        df = pd.DataFrame.from_dict(param_json_dict)
    except ValueError:
        df = pd.DataFrame.from_dict([param_json_dict])

    list_of_dicts = df.to_dict('records')

    new_list = []

    for one_dict in list_of_dicts:
        dict_len = len(one_dict)
        null_count = 0

        for key, val in one_dict.items():
            if pd.isnull(val) or val == '' or val == 'NaT':
                null_count = null_count + 1

        if null_count != dict_len:
            new_list.append(one_dict)

    df = pd.DataFrame.from_records(new_list)

    df = df.dropna(how='all')
    df = df.reset_index(drop=True)

    return df


def create_bgc_meas_list(df):

    json_str = df.to_json(orient='records')

    # _qc":2.0
    # If tgoship_argovis_name_mapping_bot is '.0' in qc value, remove it to get an int
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

    data_dict = json.loads(json_str)

    return data_dict


def create_geolocation_json_str(nc):

    # "geoLocation": {
    #     "coordinates": [
    #         -158.2927,
    #         21.3693
    #     ],
    #     "type": "Point"
    # },

    lat = nc.coords['latitude'].astype('str').values
    lon = nc.coords['longitude'].astype('str').values

    lat = Decimal(lat.item(0))
    lon = Decimal(lon.item(0))

    coordinates = [lon, lat]

    geo_dict = {}
    geo_dict['coordinates'] = coordinates
    geo_dict['type'] = 'Point'

    geolocation_dict = {}
    geolocation_dict['geoLocation'] = geo_dict

    json_str = json.dumps(geolocation_dict, default=defaultencode)

    return json_str


def create_json_profiles(profile_group, names):

    # Do the  following to keep precision of numbers
    # If had used pandas dataframe, it would
    # have added more decimal places

    # If NaN in column, int qc becomes float
    # Will fix this later by doing a regex
    # replace to remove ".0" from qc

    json_profile = {}

    for name in names:

        is_int = False
        is_float = False

        float_types = ['float64', 'float32']
        int_types = ['int8', 'int64']

        try:
            var = profile_group.coords[name]

            if var.dtype in float_types:
                vals = var.astype('str').values
                is_float = True
            elif var.dtype in int_types:
                vals = var.astype('str').values
                is_int = True
            else:
                vals = var.astype('str').values

        except KeyError:
            var = profile_group.data_vars[name]

            if var.dtype in float_types:
                vals = var.astype('str').values
                is_float = True
            elif var.dtype in int_types:
                vals = var.astype('str').values
                is_int = True
            else:
                vals = var.astype('str').values

        if vals.size == 1:
            val = vals.item(0)

            if is_float:
                result = Decimal(val)
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)
            elif is_int:
                result = int(val)
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)
            else:
                result = val
                name_dict = {name: result}
                json_str = json.dumps(name_dict)
        else:

            if is_float:
                result = [Decimal(x) for x in vals]
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)
            elif is_int:
                result = [int(x) for x in vals]
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)
            else:
                result = vals.tolist()
                name_dict = {name: result}
                json_str = json.dumps(name_dict)

        json_profile[name] = json_str

    json_profiles = ''

    for profile in json_profile.values():
        profile = profile.lstrip('{')
        bare_profile = profile.rstrip('}')

        json_profiles = json_profiles + ', ' + bare_profile

    # Strip off starting ', ' of string
    json_profiles = '{' + json_profiles.strip(', ') + '}'

    return json_profiles


def add_extra_coords(nc, data_obj):

    expocode = nc['expocode'].values
    station = nc['station'].values
    cast = nc['cast'].values
    filename = data_obj['filename']
    data_path = data_obj['data_path']

    print(f"expocode before is {expocode}")

    if '/' in expocode:
        expocode = expocode.replace('/', '_')
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"
    elif expocode == 'None':
        logging.info(filename)
        logging.info('expocode is None')
        cruise_url = ''
    else:
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

    new_coords = {}

    padded_station = str(station).zfill(3)
    padded_cast = str(cast).zfill(3)

    _id = f"{expocode}_{padded_station}_{padded_cast}"

    print(f"id is _id and type {data_obj['type']}")
    print(f"expocode after is {expocode}")

    new_coords['_id'] = _id
    new_coords['id'] = _id

    new_coords['POSITIONING_SYSTEM'] = 'GPS'
    new_coords['DATA_CENTRE'] = 'CCHDO'
    new_coords['cruise_url'] = cruise_url
    new_coords['netcdf_url'] = data_path

    new_coords['data_filename'] = filename

    datetime64 = nc['time'].values
    date = pd.to_datetime(datetime64)

    new_coords['date_formatted'] = date.strftime("%Y-%m-%d")

    # Create date coordiinate and convert date to iso
    new_coords['date'] = date.isoformat()

    latitude = nc['latitude'].values
    longitude = nc['longitude'].values

    roundLat = np.round(latitude, 3)
    roundLon = np.round(longitude, 3)

    strLat = f"{roundLat} N"
    strLon = f"{roundLon} E"

    new_coords['roundLat'] = roundLat
    new_coords['roundLon'] = roundLon

    new_coords['strLat'] = strLat
    new_coords['strLon'] = strLon

    nc = nc.assign_coords(new_coords)

    return nc


def create_meta_dict(profile_group, meta_names):

    meta_json_str = create_json_profiles(profile_group, meta_names)

    geolocation_json_str = create_geolocation_json_str(
        profile_group)

    # Include geolocation dict into meta json string
    meta_left_str = meta_json_str.rstrip('}')
    meta_geo_str = geolocation_json_str.lstrip('{')
    meta_json_str = f"{meta_left_str}, {meta_geo_str}"

    meta_dict = json.loads(meta_json_str)

    return meta_dict


def create_profile_dict(profile_group, data_obj):

    # don't rename variables yet
    profile_number = data_obj['profile_number']

    cast_number = str(profile_group['cast'].values)

    # The station number is a string like for 33RR20120218
    station = str(profile_group['station'].values)

    station_cast = f"{station}_{cast_number}"

    profile_group = add_extra_coords(profile_group, data_obj)

    meta_names, param_names = get_meta_param_names(profile_group)

    meta_dict = create_meta_dict(profile_group, meta_names)

    # Remove  time from meta since it was just used to create date variable
    meta_dict.pop('time', None)

    param_json = create_json_profiles(profile_group, param_names)

    df_bgc = create_bgc_meas_df(param_json)

    bgc_meas_dict_list = create_bgc_meas_list(df_bgc)

    measurements_dict_list = create_measurements_list(df_bgc)

    goship_units_dict = data_obj['goship_units']

    goship_ref_scale_mapping_dict = data_obj['goship_ref_scale']

    goship_names_list = [*meta_names, *param_names]

    # Save meta separate for renaming later
    profile_dict = {}
    profile_dict['profile_number'] = profile_number
    profile_dict['cast_number'] = cast_number
    profile_dict['station'] = station
    profile_dict['station_cast'] = station_cast
    profile_dict['meta'] = meta_dict
    profile_dict['bgc_meas'] = bgc_meas_dict_list
    profile_dict['measurements'] = measurements_dict_list
    profile_dict['goship_ref_scale'] = goship_ref_scale_mapping_dict
    profile_dict['goship_units'] = goship_units_dict
    profile_dict['goship_names'] = goship_names_list

    return profile_dict


def create_profile_dicts(data_obj):

    nc = data_obj['nc']

    type = data_obj['type']

    all_profiles_dict_list = []

    for nc_group in nc.groupby('N_PROF'):

        print(f"Processing {type} profile {nc_group[0] + 1}")

        profile_number = nc_group[0]
        profile_group = nc_group[1]

        data_obj['profile_number'] = profile_number

        profile_dict = create_profile_dict(profile_group, data_obj)

        all_profiles_dict_list.append(profile_dict)

    return all_profiles_dict_list


def convert_sea_water_temp(nc, var, var_goship_ref_scale):

    # Check sea_water_temperature to be degree_Celsius and
    # have goship_reference_scale be ITS-90

    # So look for ref_scale = IPTS-68 or ITS-90

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    # Change this to work for all temperature names

    if var_goship_ref_scale == 'IPTS-68':

        # Convert to ITS-90 scal
        temperature = nc[var].data

        converted_temperature = temperature/1.00024

        # Set nc var of temp to this value
        num_decimal_places = abs(
            Decimal(str(converted_temperature)).as_tuple().exponent)

        new_temperature = round(converted_temperature, num_decimal_places)

        # Set temperature value in nc because use it later to
        # create profile dict
        nc[var].data = new_temperature

        nc[var].attrs['reference_scale'] = 'ITS-90'

    return nc


def convert_goship_to_argovis_ref_scale(data_obj):

    nc = data_obj['nc']
    meta_vars = data_obj['meta']
    params = data_obj['param']

    # If argo ref scale not equal to goship ref scale, convert

    # So far, it's only the case for temperature

    argovis_ref_scale_per_type = gvm.get_argovis_reference_scale_per_type()

    for var in params:
        # Not all vars have a reference scale
        try:
            # Get goship reference scale of var
            var_goship_ref_scale = nc[var].attrrs['reference_scale']

            if 'temperature' in var:
                argovis_ref_scale = argovis_ref_scale_per_type['temperature']
                is_same_scale = var_goship_ref_scale == argovis_ref_scale
                is_IPTS68_scale = var_goship_ref_scale == 'IPTS-68'

                if is_IPTS68_scale and not is_same_scale:
                    nc = convert_sea_water_temp(nc, var, var_goship_ref_scale)

        except:
            pass

    data_obj['nc'] = nc

    return data_obj


def get_meta_param_names(nc):

    # Meta names have size N_PROF and no N_LEVELS
    # Parameter names have size N_PROF AND N_LEVELS

    meta_names = []
    param_names = []

    # check coords
    for name in list(nc.coords):
        size = nc[name].sizes

        try:
            size['N_LEVELS']
            param_names.append(name)
        except KeyError:
            meta_names.append(name)

    # check params
    for name in list(nc.keys()):
        size = nc[name].sizes

        try:
            size['N_LEVELS']
            param_names.append(name)
        except KeyError:
            meta_names.append(name)

    # Remove variables not wanted
    meta_names.remove('profile_type')
    meta_names.remove('geometry_container')

    return meta_names, param_names


def read_file_test(data_obj):

    data_path = data_path = data_obj['data_path']

    nc = xr.open_dataset(data_path)

    data_obj['nc'] = nc

    expocode = nc.coords['expocode'].data[0]

    meta_names, param_names = get_meta_param_names(nc)

    data_obj['meta'] = meta_names
    data_obj['param'] = param_names

    return data_obj, expocode


def read_file(data_obj):

    data_path = data_path = data_obj['data_path']

    data_url = f"https://cchdo.ucsd.edu{data_path}"

    with fsspec.open(data_url) as fobj:
        nc = xr.open_dataset(fobj)

    data_obj['nc'] = nc

    meta_names, param_names = get_meta_param_names(nc)

    data_obj['meta'] = meta_names
    data_obj['param'] = param_names

    return data_obj


def process_ctd(ctd_obj):

    print('---------------------------')
    print('Start processing ctd profiles')
    print('---------------------------')

    # Exclude before write to JSON
    # because may use temperature if combining

    # # Check if all ctd vars available: pressure and temperature
    # has_ctd_vars = check_if_all_ctd_vars(ctd_obj, logging, logging_dir)

    # if not has_ctd_vars:
    #     ctd_found = False

    ctd_obj = gvm.create_goship_unit_mapping(ctd_obj)
    ctd_obj = gvm.create_goship_ref_scale_mapping(ctd_obj)

    # TODO
    # Are there other variables to convert besides ctd_temperature?
    # And if I do, would need to note in argovis ref scale mapping
    # that this temperare is on a new scale.
    # I'm assuming goship will always refer to original vars before
    # values converted.

    # What if other temperatures not on ITS-90 scale?
    ctd_obj = convert_goship_to_argovis_ref_scale(ctd_obj)

    # Add convert units function

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    ctd_profile_dicts = create_profile_dicts(ctd_obj)

    station_cast_profile_ctd = get_station_cast_profile(ctd_profile_dicts)

    # Rename with _ctd suffix unless it is an Argovis variable
    # But no _ctd suffix to meta data
    renamed_ctd_profile_dicts = rn.rename_profile_dicts_to_argovis(
        ctd_profile_dicts, station_cast_profile_ctd, 'ctd')

    print('---------------------------')
    print('Processed ctd profiles')
    print('---------------------------')

    return renamed_ctd_profile_dicts, station_cast_profile_ctd


def process_bottle(bot_obj):

    print('---------------------------')
    print('Start processing bottle profiles')
    print('---------------------------')

    # Exclude before write to JSON
    # because may use temperature if combining

    # Check if all ctd vars available: pressure and temperature
    # has_ctd_vars = check_if_all_ctd_vars(bot_obj, logging, logging_dir)

    # if not has_ctd_vars:
    #     bot_found = False

    bot_obj = gvm.create_goship_unit_mapping(bot_obj)
    bot_obj = gvm.create_goship_ref_scale_mapping(bot_obj)

    # Only converting temperature so far
    bot_obj = convert_goship_to_argovis_ref_scale(bot_obj)

    # Add convert units function

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    bot_profile_dicts = create_profile_dicts(bot_obj)

    station_cast_profile_bot = get_station_cast_profile(bot_profile_dicts)

    # Rename with _btl suffix unless it is an Argovis variable
    # But no _btl suffix to meta data
    # Add _btl when combine files
    renamed_bot_profile_dicts = rn.rename_profile_dicts_to_argovis(
        bot_profile_dicts, station_cast_profile_bot, 'btl')

    print('---------------------------')
    print('Processed btl profiles')
    print('---------------------------')

    return renamed_bot_profile_dicts, station_cast_profile_bot


def setup_test_obj(dir, filename, type):

    if type == 'btl':
        bot_obj = {}
        bot_obj['found'] = True
        bot_obj['type'] = 'btl'
        bot_obj['data_path'] = os.path.join(dir, filename)
        bot_obj['filename'] = filename

        return bot_obj

    if type == 'ctd':
        ctd_obj = {}
        ctd_obj['found'] = True
        ctd_obj['type'] = 'ctd'
        ctd_obj['data_path'] = os.path.join(dir, filename)
        ctd_obj['filename'] = filename

        return ctd_obj


def setup_testing(bot_file, ctd_file, test_bot, test_ctd):

    input_dir = './testing_data/modify_data_for_testing'
    output_dir = './testing_output'
    os.makedirs(output_dir, exist_ok=True)

    bot_obj = {}
    ctd_obj = {}
    bot_obj['found'] = False
    ctd_obj['found'] = False

    cruise_info = {}
    cruise_info['btl'] = {}
    cruise_info['ctd'] = {}
    cruise_info['btl']['found'] = False
    cruise_info['ctd']['found'] = False
    cruise_info['expocode'] = 'testing'
    cruise_info['cruise_id'] = None

    all_cruises_info = []
    all_cruises_info.append(cruise_info)

    # Enter test files
    if test_bot:
        bot_obj = setup_test_obj(input_dir, bot_file, 'btl')

        print("======================\n")
        print(f"Processing btl test file {bot_file}")

    if test_ctd:
        ctd_obj = setup_test_obj(input_dir, ctd_file, 'ctd')

        print("======================\n")
        print(f"Processing ctd test file {ctd_file}")

    return output_dir, all_cruises_info, bot_obj, ctd_obj


def setup_logging(clear_old_logs):

    logging_dir = './logging'
    os.makedirs(logging_dir, exist_ok=True)

    if clear_old_logs:
        remove_file('files_goship_units.txt', logging_dir)
        remove_file('files_no_core_ctd_vars.txt', logging_dir)
        remove_file('files_w_ctd_temp_no_qc.txt', logging_dir)
        remove_file('files_no_ctd_temp_w_ref_scale.txt', logging_dir)
        remove_file('files_no_ctd_temp.txt', logging_dir)
        remove_file('files_no_expocode.txt', logging_dir)
        remove_file('files_no_pressure.txt', logging_dir)
        remove_file('output.log', logging_dir)
        remove_file('found_cruises_with_coords_netcdf.txt', logging_dir)

    filename = 'output.log'
    logging_path = os.path.join(logging_dir, filename)
    logging.root.handlers = []
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                        level=logging.INFO, filename=logging_path)

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

    return logging_dir, logging


def get_user_input():

    # TODO
    # Change to user click

    try:
        start_year = int(sys.argv[1])
        end_year = int(sys.argv[2])
    except:
        start_year = 1950
        end_year = 2021

    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)

    print(f"Converting years {start_year} to {end_year}")
    print(end_year)

    # overwrite or append  files
    clear_old_logs = False

    return start_date, end_date, clear_old_logs


def main():

    # First create a list of the cruises found
    # with netCDF files

    start_date, end_date, clear_old_logs = get_user_input()

    program_start_time = datetime.now()

    logging_dir, logging = setup_logging(clear_old_logs)

    session = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount('https://', a)

    json_directory = './converted_data'
    os.makedirs(json_directory, exist_ok=True)

    # TESTING
    testing = False
    test_bot_obj = {}
    test_ctd_obj = {}

    if testing:

        # Change this to do bot and ctd separate or combined
        test_bot = False
        test_ctd = True

        bot_file = 'modified_318M20130321_bottle_no_psal.nc'
        ctd_file = 'modified_318M20130321_ctd_core_bad_flag.nc'

        json_directory, all_cruises_info, test_bot_obj, test_ctd_obj = setup_testing(
            bot_file, ctd_file, test_bot, test_ctd)

        bot_found = test_bot_obj['found']
        ctd_found = test_ctd_obj['found']

    else:
        # Loop through all cruises and grap NetCDF files
        all_cruises_info = gi.get_cruise_information(session, logging_dir)

        # Sort cruises on date to process newest first
        try:
            all_cruises_info.sort(
                key=lambda item: item['start_datetime'], reverse=True)
        except:
            pass

        all_cruises_info = [obj for obj in all_cruises_info if obj['start_datetime']
                            >= start_date and obj['start_datetime'] <= end_date]

        if not all_cruises_info:
            print('No cruises within dates selected')
            exit(1)

    for cruise_info in all_cruises_info:

        if not testing:
            expocode = cruise_info['expocode']

            print("======================\n")
            # print(f"Processing {expocode}")
            # logging.info(f"Processing {expocode}")

            bot_found = cruise_info['btl']['found']
            ctd_found = cruise_info['ctd']['found']

        if bot_found:

            if testing:
                bot_obj, expocode = read_file_test(test_bot_obj)

            else:
                bot_obj = cruise_info['btl']
                bot_obj = read_file(bot_obj)

            renamed_bot_profile_dicts, station_cast_profile = process_bottle(
                bot_obj)

        if ctd_found:

            if testing:
                ctd_obj, expocode = read_file_test(test_ctd_obj)

            else:
                ctd_obj = cruise_info['ctd']
                ctd_obj = read_file(ctd_obj)

            renamed_ctd_profile_dicts, station_cast_profile = process_ctd(
                ctd_obj)

        if bot_found and ctd_found:

            # Combine and add _btl suffix to meta variables

            combined_bot_ctd_dicts, station_cast_profile_btl, station_cast_profile_ctd = combine_profile_dicts_bot_ctd(
                renamed_bot_profile_dicts, renamed_ctd_profile_dicts)

            print('---------------------------')
            print('Processed btl and ctd combined profiles')
            print('---------------------------')

            station_cast_profile = {
                **station_cast_profile_btl, **station_cast_profile_ctd}

            has_ctd_vars = ckctd.check_if_all_ctd_vars(
                combined_bot_ctd_dicts, station_cast_profile, logging, logging_dir, 'btl_ctd')

            for is_ctd in has_ctd_vars:
                profile_number = is_ctd['profile_number']
                has_vars = is_ctd['has_ctd_vars']

                combined_profile = combined_bot_ctd_dicts[profile_number]

                if has_vars:
                    write_profile_json(
                        json_directory, combined_profile, 'btl_ctd')

            write_profile_goship_units(
                combined_bot_ctd_dicts[0], logging_dir, 'btl_ctd')

        elif bot_found:
            renamed_bot_profile_dicts = fp.get_filtered_measurements(
                renamed_bot_profile_dicts, 'btl')

            has_ctd_vars = ckctd.check_if_all_ctd_vars(
                renamed_bot_profile_dicts, station_cast_profile, logging, logging_dir, 'btl')

            for is_ctd in has_ctd_vars:
                profile_number = is_ctd['profile_number']
                has_vars = is_ctd['has_ctd_vars']

                bot_profile = renamed_bot_profile_dicts[profile_number]

                if has_vars:
                    write_profile_json(
                        json_directory, bot_profile, 'btl')

            write_profile_goship_units(
                renamed_bot_profile_dicts[0], logging_dir, 'btl')

        elif ctd_found:
            renamed_ctd_profile_dicts = fp.get_filtered_measurements(
                renamed_ctd_profile_dicts, 'ctd')

            has_ctd_vars = ckctd.check_if_all_ctd_vars(
                renamed_ctd_profile_dicts, station_cast_profile, logging, logging_dir, 'ctd')

            for is_ctd in has_ctd_vars:
                profile_number = is_ctd['profile_number']
                has_vars = is_ctd['has_ctd_vars']

                ctd_profile = renamed_ctd_profile_dicts[profile_number]

                if has_vars:
                    write_profile_json(
                        json_directory, ctd_profile, 'ctd')

            write_profile_goship_units(
                renamed_ctd_profile_dicts[0], logging_dir, 'ctd')

        if bot_found or ctd_found:
            print('---------------------------')
            print(f"All casts written to files for cruise {expocode}")
            print('---------------------------')

            print("*****************************\n")

    logging.info("Time to run program")
    logging.info(datetime.now() - program_start_time)


if __name__ == '__main__':
    main()
