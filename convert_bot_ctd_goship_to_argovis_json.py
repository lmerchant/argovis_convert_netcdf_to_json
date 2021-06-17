from pandas.core.algorithms import isin
from requests.adapters import proxy_from_url
import xarray as xr
import pandas as pd
import numpy as np
import os
from datetime import datetime
import json
import logging
from decimal import Decimal
import requests
from requests.adapters import HTTPAdapter
import fsspec
import re
import copy

import get_variable_mappings as gvm
import rename_objects as rn

API_END_POINT = "https://cchdo.ucsd.edu/api/v1"

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


def write_profile_json(json_dir, profile_dict):

    # Pop off meta key and use as start of data_dict
    meta_dict = profile_dict.pop('meta', None)

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

    folder_name = expocode

    path = os.path.join(json_dir, folder_name)

    if not os.path.exists(path):
        os.makedirs(path)

    file = os.path.join(json_dir, folder_name, filename)

    # TESTING
    # TODO Remove formatting when final

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable

    with open(file, 'w') as f:
        json.dump(data_dict, f, indent=4, sort_keys=True, default=convert)


def write_profiles_json(json_dir, profile_dicts):

    # Write profile_dict to as json to a profile file

    for profile_dict in profile_dicts:
        write_profile_json(json_dir, profile_dict)


def combine_output_per_profile_bot_ctd(bot_renamed_dict, ctd_renamed_dict):

    bot_meta = bot_renamed_dict['meta']
    ctd_meta = ctd_renamed_dict['meta']

    bot_renamed_meta = rn.rename_bot_by_key_meta(bot_meta)

    meta = {**ctd_meta, **bot_renamed_meta}

    # Remove _btl variables that are the same as CTD
    meta.pop('expocode_btl', None)
    meta.pop('cruise_url_btl', None)
    meta.pop('DATA_CENTRE_btl', None)

    bot_bgc_meas = bot_renamed_dict['bgcMeas']
    ctd_bgc_meas = ctd_renamed_dict['bgcMeas']

    bgc_meas = [*ctd_bgc_meas, *bot_bgc_meas]

    bot_measurements = bot_renamed_dict['measurements']
    ctd_measurements = ctd_renamed_dict['measurements']

    measurement = [*ctd_measurements, *bot_measurements]

    goship_argovis_name_mapping_bot = bot_renamed_dict['goshipArgovisNameMapping']
    goship_argovis_name_mapping_ctd = ctd_renamed_dict['goshipArgovisNameMapping']

    goship_ref_scale_mapping_bot = bot_renamed_dict['goshipReferenceScale']
    goship_ref_scale_mapping_ctd = ctd_renamed_dict['goshipReferenceScale']

    goship_ref_scale_mapping = {
        **goship_ref_scale_mapping_ctd, **goship_ref_scale_mapping_bot}

    argovis_ref_scale_ctd = ctd_renamed_dict['argovisReferenceScale']
    argovis_ref_scale_bot = bot_renamed_dict['argovisReferenceScale']

    argovis_reference_scale = {
        **argovis_ref_scale_ctd, **argovis_ref_scale_bot}

    goship_argovis_units_mapping = {
        **ctd_renamed_dict['goshipArgovisUnitNameMapping'], **bot_renamed_dict['goshipArgovisUnitNameMapping']}

    combined_bot_ctd_dict = {}
    combined_bot_ctd_dict['meta'] = meta
    combined_bot_ctd_dict['measurements'] = measurement
    combined_bot_ctd_dict['bgcMeas'] = bgc_meas
    combined_bot_ctd_dict['goshipArgovisNameMappingBtl'] = goship_argovis_name_mapping_bot
    combined_bot_ctd_dict['goshipArgovisNameMappingCtd'] = goship_argovis_name_mapping_ctd
    combined_bot_ctd_dict['goshipReferenceScale'] = goship_ref_scale_mapping
    combined_bot_ctd_dict['argovisReferenceScale'] = argovis_reference_scale
    combined_bot_ctd_dict['goshipArgovisUnitNameMapping'] = goship_argovis_units_mapping

    return combined_bot_ctd_dict


def combine_profile_dicts_bot_ctd(bot_profile_dicts, ctd_profile_dicts):

    # TODO
    # Ask if this is still wanted
    # add following flags
    # isGOSHIPctd = true
    # isGOSHIPbottle = true
    # core_info = 1  # is ctd
    # core_info = 2  # is bottle (no ctd)
    # core_info = 12  # is ctd and tgoship_argovis_name_mapping_bot is bottle too (edited)

    bot_num_profiles = len(bot_profile_dicts)
    ctd_num_profiles = len(ctd_profile_dicts)

    num_profiles = max(bot_num_profiles, ctd_num_profiles)

    # TODO: what to do in the case when not equal when have both bot and ctd?
    #  Is there a case like this?

    if bot_num_profiles != ctd_num_profiles:
        print(
            f"bot profiles {bot_num_profiles} and ctd profiles {ctd_num_profiles} are different")

    profile_dicts_list_bot_ctd = []

    for profile_number in range(num_profiles):

        bot_profile_dict = bot_profile_dicts[profile_number]
        ctd_profile_dict = ctd_profile_dicts[profile_number]

        combined_profile_dict_bot_ctd = combine_output_per_profile_bot_ctd(
            bot_profile_dict, ctd_profile_dict)

        profile_dicts_list_bot_ctd.append(combined_profile_dict_bot_ctd)

    return profile_dicts_list_bot_ctd


def create_measurements_list(df_bgc_meas, data_obj):

    # TODO
    # If find qc != 2, do I throw away the value
    # or set it to nan?

    type = data_obj['type']

    df_meas = pd.DataFrame()

    # core values includes '_qc' vars
    core_values = gvm.get_goship_core_values()

    if type == 'bot':
        # add in bottle_salinity to get a subset of
        bottle_salinity_vars = ['bottle_salinity', 'bottle_salinity_qc']
        core_values.append(bottle_salinity_vars)

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

    for col in core_non_qc:
        try:
            qc_key = f"{col}_qc"
            # Set value to -222222 at qc !=2 so can find them
            # when convert df to dict list and then know
            # which valus to remove
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

    # If it is a bottle file, check to see if it
    # has ctd_salinity and bottle_salinity
    # Then if no ctd_salinity, use bottle_salinity
    # if it does, remove bottle_salinity entry

    new_data_list = []

    for obj in data_dict_list:

        if type == 'bot' and 'ctd_salinity' in obj.keys() and 'bottle_salinity' in obj.keys():
            del obj['bottle_salinity']

    return new_data_list


def create_bgc_meas_df(param_json_str):

    # Now split up param_json_str into multiple json dicts
    # And then only keep those that have a value not null for each key
    param_json_dict = json.loads(param_json_str)

    df = pd.DataFrame.from_dict(param_json_dict)

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

    # TODO
    # zero pad the station and cast to 3 places
    _id = f"{expocode}_{station}_{cast}"

    new_coords['_id'] = _id
    new_coords['id'] = _id

    new_coords['POSITIONING_SYSTEM'] = 'GPS'
    new_coords['DATA_CENTRE'] = 'CCHDO'
    new_coords['cruise_url'] = cruise_url
    new_coords['netcdf_url'] = data_path

    new_coords['data_filename'] = filename

    # TODO

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

    profile_group = add_extra_coords(profile_group, data_obj)

    meta_names, param_names = get_meta_param_names(profile_group)

    meta_dict = create_meta_dict(profile_group, meta_names)

    # Remove  time from meta since it was just used to create date variable
    meta_dict.pop('time', None)

    param_json = create_json_profiles(profile_group, param_names)

    df_bgc = create_bgc_meas_df(param_json)

    bgc_meas_dict_list = create_bgc_meas_list(df_bgc)

    measurements_dict_list = create_measurements_list(df_bgc, data_obj)

    goship_units_dict = data_obj['goship_units']
    goship_ref_scale_mapping_dict = data_obj['goship_ref_scale']

    goship_names_list = [*meta_names, *param_names]

    # Save meta separate for renaming later
    profile_dict = {}
    profile_dict['meta'] = meta_dict
    profile_dict['bgc_meas'] = bgc_meas_dict_list
    profile_dict['measurements'] = measurements_dict_list
    profile_dict['goship_ref_scale'] = goship_ref_scale_mapping_dict
    profile_dict['goship_units'] = goship_units_dict
    profile_dict['goship_names'] = goship_names_list

    return profile_dict


def check_if_all_ctd_vars(data_obj):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    logging_filename = 'logging.txt'

    nc = data_obj['nc']

    is_pres = False
    is_ctd_temp_w_refscale = False
    has_ctd_temperature = False

    coords = nc.coords

    for var in coords:

        if var == 'pressure':
            is_pres = True

    vars = nc.keys()

    temperature_vars = ['ctd_temperature', 'ctd_temperature_68']

    for var in vars:

        if var in temperature_vars:
            has_ctd_temperature = True

        try:
            var_ref_scale = nc[var].attrs['reference_scale']
        except:
            var_ref_scale = None

        if var in temperature_vars and var_ref_scale:
            is_ctd_temp_w_refscale = True
        elif var in temperature_vars and not var_ref_scale:
            is_ctd_temp_w_refscale = False

    expocode = nc.coords['expocode'].data[0]

    if is_pres and is_ctd_temp_w_refscale:
        has_ctd_vars = True
    else:
        has_ctd_vars = False

        print('**********************')
        print('No CTD variables found')
        print('**********************')

        logging.info('===========')
        logging.info('EXCEPTIONS FOUND')
        logging.info(expocode)
        logging.info(logging_filename)

    if not is_pres and not is_ctd_temp_w_refscale:
        logging.info('missing pres and ctd temperature')
        with open('files_no_core_ctd_vars.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    if not is_ctd_temp_w_refscale:
        logging.info('CTD temperature with no ref scale')
        # Write to file listing files without ctd variables
        with open('files_no_ctd_temp_w_ref_scale.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    if not has_ctd_temperature:
        logging.info('No ctd temperture')
        # Write to file listing files without ctd variables
        with open('files_no_ctd_temp.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    expocode = nc.coords['expocode'].data[0]

    if expocode == 'None':
        has_ctd_vars = False
        logging.info('No expocode')
        # Write to file listing files without ctd variables
        with open('files_no_expocode.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    return has_ctd_vars


def create_profile_dicts(data_obj):

    nc = data_obj['nc']

    type = data_obj['type']

    all_profiles_dict_list = []

    for nc_group in nc.groupby('N_PROF'):

        print(f"Processing {type} profile {nc_group[0] + 1}")

        profile_number = nc_group[0]
        profile_group = nc_group[1]

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


def move_pressure_to_vars():
    pass


def read_file(data_obj):

    data_path = data_obj['data_path']
    data_url = f"https://cchdo.ucsd.edu{data_path}"

    with fsspec.open(data_url) as fobj:
        nc = xr.open_dataset(fobj)

    data_obj['nc'] = nc

    meta_names, param_names = get_meta_param_names(nc)

    data_obj['meta'] = meta_names
    data_obj['param'] = param_names

    return data_obj


def find_bot_ctd_file_info(file_ids, session):

    # Get file meta for each file id to search for cruise doc id and bottle id

    file_info = {}

    file_info['bot_id'] = None
    file_info['bot_path'] = ''
    file_info['ctd_id'] = None
    file_info['ctd_path'] = ''
    file_info['bot_found'] = False
    file_info['ctd_found'] = False

    for file_id in file_ids:

        # Find all bottle ids and doc ids for each cruise
        # Following api only lists active files

        query = f"{API_END_POINT}/file/{file_id}"
        response = session.get(query)

        if response.status_code != 200:
            print('api not reached in function find_bot_ctd_file_info')
            print(response)
            exit(1)

        file_meta = response.json()

        file_role = file_meta['role']
        data_type = file_meta['data_type']
        data_format = file_meta['data_format']
        file_path = file_meta['file_path']

        # Only returning file info if both a bottle and doc for cruise
        if file_role == "dataset":
            if data_type == "bottle" and data_format == "cf_netcdf":
                file_info['bot_id'] = file_id
                file_info['bot_path'] = file_path
                file_info['bot_filename'] = file_path.split('/')[-1]
                file_info['bot_found'] = True

            if data_type == "ctd" and data_format == "cf_netcdf":
                file_info['ctd_id'] = file_id
                file_info['ctd_path'] = file_path
                file_info['ctd_filename'] = file_path.split('/')[-1]
                file_info['ctd_found'] = True

    return file_info


def get_all_file_ids(session):

    # Use api query to get all active file ids
    query = f"{API_END_POINT}/file/all"

    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_files')
        print(response)
        exit(1)

    all_files = response.json()

    all_file_ids = [file['id'] for file in all_files]

    return all_file_ids


def get_all_cruises(session):

    # Use api query to get all cruise id with their attached file ids

    query = f"{API_END_POINT}/cruise/all"

    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_cruises')
        print(response)
        exit(1)

    all_cruises = response.json()

    return all_cruises


def get_cruise_information(session):

    # To get expocodes and cruise ids, Use get cruise/all to get all cruise metadata
    # and search cruises to get Go-Ship cruises expocodes and cruise ids,
    # from attached file ids, Search file metadata from doc file id, bottle file id

    # Get all cruises and active files
    all_cruises = get_all_cruises(session)
    all_file_ids = get_all_file_ids(session)

    all_cruises_info = []

    # TESTING
    # For Testing. Use when wanting to limit the number of cruises processed
    cruise_count = 0

    for cruise in all_cruises:

        bot_found = False
        ctd_found = False

        programs = cruise['collections']['programs']
        programs = [x.lower() for x in programs]

        country = cruise['country']

        # TESTING
        # Find cruise 32MW9508 to check for ctd_temperature_68 case
        # It isn't a Go-Ship cruise but has ctd temp on 68 scale
        # And change check for Go-Ship to True
        # expocode = cruise['expocode']
        # if expocode != '32MW9508':
        #     continue

        cruise_info = {}
        cruise_info['bot'] = {}
        cruise_info['ctd'] = {}

        # Only want US Go-Ship

        # TESTING
        # Get non-goship cruise with True
        # if True:
        if 'go-ship' in programs and country == 'US':

            print(f"Finding cruise information for {cruise['expocode']}")

            bot_obj = {}
            ctd_obj = {}

            # Get files attached to the cruise
            # Could be deleted ones so check if exist in all_files
            file_ids = cruise['files']

            # Get only file_ids in all_file_ids (active files)
            active_file_ids = list(
                filter(lambda x: (x in all_file_ids), file_ids))

            # Get file meta for each file id to search for
            # cruise doc and bottle info
            file_info = find_bot_ctd_file_info(active_file_ids, session)

            bot_found = file_info['bot_found']
            ctd_found = file_info['ctd_found']

            # Only want cruises with both dataset bot and doc files
            if not len(file_info):
                continue

            # bot_url = https://cchdo.ucsd.edu/data/<file id>/<filename>

            cruise_info['expocode'] = cruise['expocode']
            cruise_info['cruise_id'] = cruise['id']

            bot_obj['found'] = False
            ctd_obj['found'] = False

            if bot_found:

                bot_obj['found'] = True
                bot_obj['type'] = 'bot'
                bot_obj['data_path'] = file_info['bot_path']
                bot_obj['filename'] = file_info['bot_filename']

                cruise_info['bot'] = bot_obj

            if ctd_found:
                ctd_obj['found'] = True
                ctd_obj['type'] = 'ctd'
                ctd_obj['data_path'] = file_info['ctd_path']
                ctd_obj['filename'] = file_info['ctd_filename']

                cruise_info['ctd'] = ctd_obj

        if cruise_info['bot'] or cruise_info['ctd']:

            all_cruises_info.append(cruise_info)

            # TESTING
            # Used to limit number of cruises processed
            cruise_count = cruise_count + 1

        # TESTING
        # Used to limit number of cruises processed
        # if cruise_count == 1:  # at count 5 gives one bottle, count 1 gives both
        #     return all_cruises_info

    return all_cruises_info


def main():

    start_time = datetime.now()

    logging.root.handlers = []
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                        level=logging.INFO, filename='output.log')

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

    json_directory = './converted_data'

    os.makedirs(json_directory, exist_ok=True)

    session = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount('https://', a)

    # Loop through all cruises and grap NetCDF files
    # from US Go-Ship
    all_cruises_info = get_cruise_information(session)

    for cruise_info in all_cruises_info:

        expocode = cruise_info['expocode']

        print("======================\n")
        print(f"Processing {expocode}")

        if cruise_info['bot']:
            bot_found = cruise_info['bot']['found']
        else:
            bot_found = False

        if cruise_info['ctd']:
            ctd_found = cruise_info['ctd']['found']
        else:
            ctd_found = False

        bot_profile_dicts = []
        ctd_profile_dicts = []

        expocode = cruise_info['expocode']

        # TODO
        # remove after testing. Using on a cruise with both bot and ctd
        # to see what each looks individually and combined
        #bot_found = False
        #ctd_found = False

        if bot_found:

            print('---------------------------')
            print('Start processing bottle profiles')
            print('---------------------------')

            bot_obj = cruise_info['bot']

            bot_obj = read_file(bot_obj)

            # Check if all ctd vars available: pressure and temperature
            has_ctd_vars = check_if_all_ctd_vars(bot_obj)

            if not has_ctd_vars:
                bot_found = False

            else:

                bot_obj = gvm.create_goship_unit_mapping(bot_obj)
                bot_obj = gvm.create_goship_ref_scale_mapping(bot_obj)

                # Only converting temperature so far
                bot_obj = convert_goship_to_argovis_ref_scale(bot_obj)

                # Rename converted temperature later.
                # Keep 68 in name and show it maps to temp_ctd
                # and ref scale show what scale it was converted to

                bot_profile_dicts = create_profile_dicts(bot_obj)

                # Rename with _btl suffix unless it is an Argovis variable
                # But no _btl suffix to meta data
                # Add _btl when combine files
                renamed_bot_profile_dicts = rn.rename_profile_dicts_to_argovis(
                    bot_profile_dicts, 'bot')

                print('---------------------------')
                print('Processed bot profiles')
                print('---------------------------')

        if ctd_found:

            print('---------------------------')
            print('Start processing ctd profiles')
            print('---------------------------')

            ctd_obj = cruise_info['ctd']

            ctd_obj = read_file(ctd_obj)

            # Check if all ctd vars available: pressure and temperature
            has_ctd_vars = check_if_all_ctd_vars(ctd_obj)

            if not has_ctd_vars:
                ctd_found = False

            else:

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

                # Rename converted temperature later.
                # Keep 68 in name and show it maps to temp_ctd
                # and ref scale show what scale it was converted to

                ctd_profile_dicts = create_profile_dicts(ctd_obj)

                # Rename with _ctd suffix unless it is an Argovis variable
                # But no _ctd suffix to meta data
                renamed_ctd_profile_dicts = rn.rename_profile_dicts_to_argovis(
                    ctd_profile_dicts, 'ctd')

                print('---------------------------')
                print('Processed ctd profiles')
                print('---------------------------')

        if bot_found and ctd_found:

            # Combine and add _btl suffix to meta variables
            combined_bot_ctd_dicts = combine_profile_dicts_bot_ctd(
                renamed_bot_profile_dicts, renamed_ctd_profile_dicts)

            print('---------------------------')
            print('Processed bot and ctd combined profiles')
            print('---------------------------')

        if bot_found and ctd_found:
            write_profiles_json(json_directory, combined_bot_ctd_dicts)

        elif bot_found:
            write_profiles_json(json_directory, renamed_bot_profile_dicts)

        elif ctd_found:
            write_profiles_json(json_directory, renamed_ctd_profile_dicts)

        if bot_found or ctd_found:
            print('---------------------------')
            print(f"All profiles written to files for cruise {expocode}")
            print('---------------------------')

            print("*****************************\n")

    logging.info(datetime.now() - start_time)


if __name__ == '__main__':
    main()
