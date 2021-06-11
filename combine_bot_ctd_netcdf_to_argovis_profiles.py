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

from config import Config


API_END_POINT = Config.API_END_POINT
API_KEY = Config.API_KEY
headers = {"X-Authentication-Token": API_KEY}

# In order to use xarray open_dataset

# https://github.com/pydata/xarray/issues/3653
# pip install aiohttp
# pip install h5netcdf

# url = 'https://cchdo.ucsd.edu/data/16923/318M20130321_bottle.nc'
# with fsspec.open(url) as fobj:
#     ds = xr.open_dataset(fobj)


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

    # Write profile_dict to as json to a profile file

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable

    id = profile_dict['id']

    filename = f"{id}.json"

    expocode = profile_dict['expocode']

    json_str = json.dumps(profile_dict)

    # _qc":2.0
    # If there is '.0' in qc value, remove it to get an int
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

    folder_name = expocode

    path = os.path.join(json_dir, folder_name)

    if not os.path.exists(path):
        os.makedirs(path)

    file = os.path.join(json_dir, folder_name, filename)

    # TODO Remove formatting when final
    with open(file, 'w') as f:
        json.dump(profile_dict, f, indent=4, sort_keys=True, default=convert)


# def filter_out_none_vals_meas(obj):

#     new_obj = {}
#     # Remove any obj val that is None
#     # Remove any key ending in _qc
#     for key, val in obj.items():
#         if '_qc' in key:
#             continue
#         if val is None:
#             continue
#         new_obj[key] = val

#     return new_obj


# def mark_not_qc2(obj):

#     new_obj = {}
#     for key, val in obj.items():

#         new_obj[key] = val
#         # qc comes after value, so change prev key
#         # Is there a better way? do I check if qc var exists
#         if '_qc' in key:

#             if np.isnan(val) or int(val) != 2:
#                 meas_key = key.replace('_qc', '')
#                 new_obj[meas_key] = None

#     return new_obj


# def filter_measurements_list(measurements_list):

#     # Keep qc = 2

#     # format start
#     # {
#     #     "pres": 48.0,
#     #     "psal": 34.7729,
#     #     "psal_qc": 2,
#     #     "temp": 24.9761,
#     #     "temp_qc": 2,
#     # }

#     # format end
#     # {
#     #     "pres": 48.0,
#     #     "psal": 34.7729,
#     #     "temp": 24.9761
#     # }

#     marked_list = list(map(mark_not_qc2, measurements_list))

#     no_qc_list = list(map(filter_out_none_vals_meas, marked_list))

#     return no_qc_list


# def create_measurements_array(profile_dict):

#     # Combine measurements lists
#     measurements_array = profile_dict['measurements']

#     # Only want qc=2, no qc flags and all same name
#     measurements_array = filter_measurements_list(measurements_array)

#     # sort measurements on pres value
#     # sorted_measurements_array = sorted(measurements_array,
#     #                                   key=lambda k: k['pres'])

#     return measurements_array


def rename_meta_bot(meta_bot):

    # Lon_btl,lat_btl,date_btl, geoLocation_btl
    new_obj = {}
    for key, val in meta_bot.items():
        new_key = f"{key}_btl"
        new_obj[new_key] = val

    return new_obj


def rename_ctd(name):

    if '_qc' in name:
        return name.replace('_qc', '_ctd_qc')
    else:
        return f"{name}_ctd"


def rename_bot(name):

    if 'bottle_salinity_qc' in name:
        return 'psal_btl_qc'
    elif 'bottle_salinity' in name:
        return 'psal_btl'
    elif '_qc' in name:
        return name.replace('_qc', '_btl_qc')

    return f"{name}_btl"


def rename_meta_bot(name):
    return f"{name}_btl"


def rename_core_bot(name):
    # Add suffix _ctd, and for _ctd_qc for all core
    # except temp for botttle
    if '_qc' in name and name == 'temp':
        return 'temp_btl_qc'
    elif name == 'temp':
        return 'temp_btl'
    elif '_qc' in name:
        return name.replace('_qc', '_ctd_qc')
    else:
        return f"{name}_ctd"


def rename_core_ctd(name):
    # Add suffix _ctd, and for _ctd_qc for all core
    if '_qc' in name:
        return name.replace('_qc', '_ctd_qc')
    else:
        return f"{name}_ctd"


def create_bgc_bot(bgc_array, core_values):
    # Common names are pres, temp, psal, and doxy will have suffix _ctd and _ctd_qc
    # All ctd vars will have suffix _ctd and _ctd_qc
    # All bot vars will have suffx _btl and _btl_qc
    # Special case is bottle_salinity to psal_btl

    new_bgc_array = []

    for obj in bgc_array:

        new_obj = {}

        for key, val in obj.items():
            if key in core_values:
                new_key = rename_core_bot(key)
                new_obj[new_key] = val
            else:
                new_key = rename_bot(key)
                new_obj[new_key] = val

        new_bgc_array.append(new_obj)

    return new_bgc_array


def create_bgc_ctd(bgc_array, core_values):
    # Common names are pres, temp, psal, and doxy will have suffix _ctd and _ctd_qc
    # All ctd vars will have suffix _ctd and _ctd_qc
    # All ctd vars will have suffx _btl and _btl_qc
    # Special case is ctdtle_salinity to psal_btl

    new_bgc_array = []

    for obj in bgc_array:

        new_obj = {}

        for key, val in obj.items():
            if key in core_values:
                new_key = rename_core_ctd(key)
                new_obj[new_key] = val
            else:
                new_key = rename_ctd(key)
                new_obj[new_key] = val

        new_bgc_array.append(new_obj)

    return new_bgc_array


def create_bgc_arrray(bot_profile_dict, ctd_profile_dict):

    # temp_ctd, temp_ctd_qc, psal_ctd, psal_ctd_qc, pres, pres_qc,  temp_btl(instead of temp_ctd_up), salinity_btl, press, salinity_btl_qc, temp_btl_qc, doxy_ctd, doxy_btl, doxy_ctd_qc, doxy_btl_qc,

    #  No profile_type stored at each pressure level since one variable has the same type at all pressures! (this info is stored in measurements type)

    # Combine bgc_meas dicts
    bot_bgc_meas_array = bot_profile_dict['bgc_meas']
    ctd_bgc_meas_array = ctd_profile_dict['bgc_meas']

    bgc_meas_array = [
        *ctd_bgc_meas_array, *bot_bgc_meas_array]

    return bgc_meas_array


def rename_mapping_bot_key(mapping, core_values):
    new_mapping = {}
    for key, val in mapping.items():
        if key == 'bottle_salinity':
            new_key = 'psal_btl'
        elif key == 'bottle_salinity_qc':
            new_key = 'psal_btl_qc'
        elif key in core_values and '_qc' in key:
            new_key = f"{key}_ctd_qc"
        elif key in core_values:
            new_key = f"{key}_ctd"
        elif '_qc' in key:
            new_key = key.replace('_qc', '_btl_qc')
        else:
            new_key = f"{key}_btl"

        new_mapping[new_key] = val

    return new_mapping


def rename_mapping_bot_value(mapping, core_values_mapping):
    new_mapping = {}
    for key in mapping:

        if key == 'bottle_salinity':
            new_val = 'psal_btl'
        elif key == 'bottle_salinity_qc':
            new_val = 'psal_btl_qc'
        elif '_qc' in key:
            new_val = key.replace('_qc', '_btl_qc')
        else:
            new_val = f"{key}_btl"

        # "ctd_oxygen_qc": "ctd_doxy_qc"
        if '_qc' in key:
            key_wo_qc = key.replace('_qc', '')
            if key_wo_qc in core_values_mapping.keys():
                new_val = f"{core_values_mapping[key_wo_qc]}_qc"
        elif key in core_values_mapping.keys():
            new_val = core_values_mapping[key]

        new_mapping[key] = new_val

    return new_mapping


def rename_mapping_ctd_key(mapping):
    print('mapping')
    print(mapping)

    new_mapping = {}
    for key, val in mapping.items():
        if '_qc' in key:
            new_key = key.replace('_qc', '_ctd_qc')
        else:
            new_key = f"{key}_ctd"

        new_mapping[new_key] = val

    return new_mapping


def rename_mapping_ctd_value(mapping):
    new_mapping = {}
    for key in mapping:
        if '_qc' in key:
            new_val = key.replace('_qc', '_ctd_qc')
        else:
            new_val = f"{key}_ctd"

        new_mapping[key] = new_val

    return new_mapping


def process_output_per_profile(core_values, core_values_mapping, profile_dict, type):

    meta = profile_dict['meta']

    measurements_list = profile_dict['measurements']
    bgc = profile_dict['bgc_meas']
    bgc_list = create_bgc_bot(bgc, core_values)

    names = profile_dict['goship_argovis_name_mappping']
    if type == 'bot':
        name_mapping = rename_mapping_bot_value(names, core_values_mapping)
    elif type == 'ctd':
        name_mapping = rename_mapping_ctd_value(names)

    goship_reference_scale_mappping = profile_dict['goship_reference_scale_mapping']

    if type == 'bot':
        argovis_reference_scale_mapping = rename_mapping_bot_key(profile_dict[
            'argovis_reference_scale_mapping'], core_values)
    elif type == 'ctd':
        argovis_reference_scale_mapping = rename_mapping_ctd_key(profile_dict[
            'argovis_reference_scale_mapping'])

    goship_argovis_unit_mapping = profile_dict['goship_argovis_unit_mappping']

    data_dict = meta
    data_dict['measurements'] = measurements_list
    data_dict['bgcMeas'] = bgc_list
    data_dict['goshipArgovisNameMapping'] = name_mapping
    data_dict['goshipReferenceScale'] = goship_reference_scale_mappping
    data_dict['argovisReferenceScale'] = argovis_reference_scale_mapping
    data_dict['goshipArgovisUnitsMapping'] = goship_argovis_unit_mapping

    return data_dict, meta


def process_output_per_profile_bot_ctd(bot_combined_dict, ctd_combined_dict, bot_meta, ctd_meta, json_data_directory):

    # Assuming bot was renamed with suffix _btl
    # Rename bot meta

    bot_renamed_meta = rename_meta_bot(bot_meta)

    #combined_bot_ctd_dict = {**ctd_meta, **bot_renamed_meta}

    combined_bot_ctd_dict['measurements'] = [
        *ctd_combined_dict['measurements'], *bot_combined_dict['measurements']]
    combined_bot_ctd_dict['bgcMeas'] = [
        *ctd_combined_dict['bgcMeas'], *bot_combined_dict['bgcMeas']]

    combined_bot_ctd_dict['goshipArgovisNameMapping'] = {
        **ctd_combined_dict['goshipArgovisNameMapping'], **bot_combined_dict['goshipArgovisNameMapping']}

    combined_bot_ctd_dict['goshipReferenceScale'] = {
        **ctd_combined_dict['goshipReferenceScale'], **bot_combined_dict['goshipReferenceScale']}

    combined_bot_ctd_dict['argovisReferenceScale'] = {
        **ctd_combined_dict['argovisReferenceScale'], **bot_combined_dict['argovisReferenceScale']}

    combined_bot_ctd_dict['goshipArgovisUnitsMapping'] = {
        **ctd_combined_dict['goshipArgovisUnitsMapping'], **bot_combined_dict['goshipArgovisUnitsMapping']}

    write_profile_json(json_data_directory, combined_bot_ctd_dict)


def process_output(nc_dict, bot_profile_dicts, ctd_profile_dicts, json_data_directory):

    # TODO
    # add following flags
    # isGOSHIPctd = true
    # isGOSHIPbottle = true
    # core_info = 1  # is ctd
    # core_info = 2  # is bottle (no ctd)
    # core_info = 12  # is ctd and there is bottle too (edited)

    bot_num_profiles = 0
    ctd_num_profiles = 0

    if nc_dict['bot']:
        bot_num_profiles = len(bot_profile_dicts)

    if nc_dict['ctd']:
        ctd_num_profiles = len(ctd_profile_dicts)

    num_profiles = max(bot_num_profiles, ctd_num_profiles)

    # TODO: what to do in the case when not equal when have both bot and ctd?
    #  Is there a case for this?
    if nc_dict['bot'] and nc_dict['ctd']:
        if bot_num_profiles != ctd_num_profiles:
            print(
                f"bot profiles {bot_num_profiles} and ctd profiles {ctd_num_profiles} are different")

    # For renaming, these are a special case
    core_values = ['pres', 'temp', 'psal',
                   'doxy', 'temp_qc', 'psal_qc', 'doxy_qc']

    core_values_mapping = {'pressure': 'pres',
                           'ctd_temperature': 'temp_ctd',
                           'ctd_salinity': 'psal_ctd',
                           'ctd_oxygen': 'doxy_ctd',
                           'latitude': 'lat',
                           'longitude': 'lon',
                           'time': 'date'}

    for profile_number in range(num_profiles):

        if nc_dict['bot']:
            bot_profile_dict = bot_profile_dicts[profile_number]
            bot_combined_dict, bot_meta = process_output_per_profile(
                core_values, core_values_mapping, bot_profile_dict, 'bot')

            write_profile_json(json_data_directory, bot_combined_dict)

        if nc_dict['ctd']:
            ctd_profile_dict = ctd_profile_dicts[profile_number]
            ctd_combined_dict, ctd_meta = process_output_per_profile(
                core_values, core_values_mapping, ctd_profile_dict, 'ctd')

            write_profile_json(json_data_directory, ctd_combined_dict)

        # if nc_dict['bot'] and nc_dict['ctd']:
        #     process_output_per_profile_bot_ctd(
        #         bot_combined_dict, ctd_combined_dict, bot_meta, ctd_meta, json_data_directory)

        if not nc_dict['bot'] and not nc_dict['ctd']:
            print("Didn't process bottle or ctd")


def create_goship_argovis_unit_mapping_json_str(df_mapping):

    # Create goship to new units mapping json string

    df_unit_mapping = df_mapping[['goship_unit', 'unit']].copy()
    # Remove rows if any NaN
    df_unit_mapping = df_unit_mapping.dropna(how='any')

    unit_mapping_dict = dict(
        zip(df_unit_mapping['goship_unit'], df_unit_mapping['unit']))

    json_str = json.dumps(unit_mapping_dict)

    return json_str


def create_goship_reference_scale_mapping_json_str(df_mapping):

    # Create goship to new names mapping json string
    df_ref_scale_mapping = df_mapping[[
        'goship_name', 'goship_reference_scale']].copy()

    # Remove rows if any NaN
    df_ref_scale_mapping = df_ref_scale_mapping.dropna(how='any')

    ref_scale_mapping_dict = dict(
        zip(df_ref_scale_mapping['goship_name'], df_ref_scale_mapping['goship_reference_scale']))

    json_str = json.dumps(ref_scale_mapping_dict)

    return json_str


def create_argovis_reference_scale_mapping_json_str(df_mapping):

    # Create new names to ref scale mapping json string
    df_ref_scale_mapping = df_mapping[[
        'name', 'reference_scale']].copy()

    # Remove rows if any NaN
    df_ref_scale_mapping = df_ref_scale_mapping.dropna(how='any')

    ref_scale_mapping_dict = dict(
        zip(df_ref_scale_mapping['name'], df_ref_scale_mapping['reference_scale']))

    json_str = json.dumps(ref_scale_mapping_dict)

    return json_str


def create_name_mapping_json_str(df_mapping):

    # Create goship to new names mapping json string
    df_name_mapping = df_mapping[['goship_name', 'name']].copy()
    # Remove rows if all NaN
    df_name_mapping = df_name_mapping.dropna(how='all')

    name_mapping_dict = dict(
        zip(df_name_mapping['goship_name'], df_name_mapping['name']))

    json_str = json.dumps(name_mapping_dict)

    return json_str


def create_measurements_json_str(df_bgc_meas, core_values):

    df_meas = pd.DataFrame()

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

    # Keep qc = 2
    def mark_not_qc_2(val):
        if pd.isnull(val).any():
            return np.nan
        elif val.astype(int).any() != 2:
            return np.nan
        else:
            return val

    for col in core_values:

        try:
            key = f"{col}_qc"
            df_meas[key].apply(lambda x: mark_not_qc_2(df_meas[key]))

            df_meas = df_meas[df_meas[key].notnull()]

        except KeyError:
            pass

    json_str = df_meas.to_json(orient='records')

    return json_str


def create_bgc_meas_df(param_entries):

    # Now split up param_entries into multiple json dicts

    json_dict = json.loads(param_entries)

    df = pd.DataFrame.from_dict(json_dict)

    # Replace '' with nan to filter on nan
    df = df.replace(r'^\s*$', np.nan, regex=True)

    # Drop all null rows
    df = df.dropna(how='all')

    # Change qc columns to integer
    qc_column_names = list(filter(lambda x: x.endswith('_qc'), df.columns))

    try:
        col_type_dict = {col_name: int for col_name in qc_column_names}

        # throws error if try to convert NaN to int
        # if still have NaN in qc, rest are float
        df = df.astype(col_type_dict)

    except:
        pass

    try:
        col_type_dict = {'sample': int}

        # throws error if try to convert NaN to int
        # if still have NaN in qc, rest are float
        df = df.astype(col_type_dict)

    except:
        pass

    return df


def create_bgc_meas_json_str(df):

    json_str = df.to_json(orient='records')

    # _qc":2.0
    # If there is '.0' in qc value, remove it to get an int
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

    return json_str


def create_geolocation_json_str(nc):

    # "geoLocation": {
    #     "coordinates": [
    #         -158.2927,
    #         21.3693
    #     ],
    #     "type": "Point"
    # },

    lat = nc.coords['lat'].astype('str').values
    lon = nc.coords['lon'].astype('str').values

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

    # If NaN in column, int qc becomes float

    json_profile = {}

    for name in names:

        # name either coord or data var
        # Test if float to convert to string and
        # keep precision

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


def add_extra_coords(nc, profile_number, filename):

    expocode = nc['expocode'].values
    station = nc['station'].values
    cast = nc['cast'].values

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

    _id = f"{expocode}_{station}_{cast}"
    new_coords['_id'] = _id
    new_coords['id'] = _id

    new_coords['POSITIONING_SYSTEM'] = 'GPS'
    new_coords['DATA_CENTRE'] = 'CCHDO'
    new_coords['cruise_url'] = cruise_url
    new_coords['netcdf_url'] = ''

    new_coords['data_filename'] = filename

    datetime64 = nc['date'].values
    date = pd.to_datetime(datetime64)

    new_coords['date_formatted'] = date.strftime("%Y-%m-%d")

    # Convert date to iso
    new_coords['date'] = date.isoformat()

    latitude = nc['lat'].values
    longitude = nc['lon'].values

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


def create_profile_json_and_dict(profile_number, profile_group, df_mapping, meta_param_names, filename):

    # Create json without translating to dataframe first

    # Consider this as way to get values out of the xarray
    # xr_strs = profile_group[name].astype('str')
    # np_arr = xr_strs.values

    name_mapping_dict = dict(
        zip(df_mapping['goship_name'], df_mapping['name']))

    profile_group = profile_group.rename(name_mapping_dict)

    profile_group = add_extra_coords(profile_group, profile_number, filename)

    meta_names, param_names = get_meta_param_names(profile_group)

    meta_param_names = {}
    meta_param_names['meta'] = meta_names
    meta_param_names['param'] = param_names

    meta_entries = create_json_profiles(profile_group, meta_names)
    param_entries = create_json_profiles(profile_group, param_names)

    geolocation_json_str = create_geolocation_json_str(profile_group)

    # Include geolocation dict into meta json string
    meta_left_str = meta_entries.rstrip('}')
    meta_geo_str = geolocation_json_str.lstrip('{')
    meta_json_str = f"{meta_left_str}, {meta_geo_str}"

    core_values = ['pres', 'temp', 'psal']

    df_bgc = create_bgc_meas_df(param_entries)
    bgc_meas_json_str = create_bgc_meas_json_str(df_bgc)
    measurements_json_str = create_measurements_json_str(df_bgc, core_values)

    goship_argovis_name_mapping_json_str = create_name_mapping_json_str(
        df_mapping)

    # Map argovis name to reference scale
    argovis_reference_scale_mapping_json_str = create_argovis_reference_scale_mapping_json_str(
        df_mapping)

    goship_reference_scale_mapping_json_str = create_goship_reference_scale_mapping_json_str(
        df_mapping)

    goship_argovis_unit_mapping_json_str = create_goship_argovis_unit_mapping_json_str(
        df_mapping)

    json_profile = {}
    json_profile['meta'] = meta_json_str
    json_profile['bgcMeas'] = bgc_meas_json_str
    json_profile['measurements'] = measurements_json_str
    json_profile['goshipArgovisNameMapping'] = goship_argovis_name_mapping_json_str
    json_profile['goshipArgovisUnitMapping'] = goship_argovis_unit_mapping_json_str
    json_profile['argovisRefScaleMapping'] = argovis_reference_scale_mapping_json_str
    json_profile['goshipRefScaleMapping'] = goship_reference_scale_mapping_json_str

    meta = json.loads(meta_json_str)
    bgc_meas = json.loads(bgc_meas_json_str)
    measurements = json.loads(measurements_json_str)
    goship_argovis_name_mappping = json.loads(
        goship_argovis_name_mapping_json_str)
    goship_argovis_unit_mappping = json.loads(
        goship_argovis_unit_mapping_json_str)
    argovis_reference_scale_mapping = json.loads(
        argovis_reference_scale_mapping_json_str)
    goship_reference_scale_mapping = json.loads(
        goship_reference_scale_mapping_json_str)

    profile_dict = {}
    profile_dict['meta'] = meta
    profile_dict['bgc_meas'] = bgc_meas
    profile_dict['measurements'] = measurements
    profile_dict['goship_argovis_name_mappping'] = goship_argovis_name_mappping
    profile_dict['goship_argovis_unit_mappping'] = goship_argovis_unit_mappping
    profile_dict['argovis_reference_scale_mapping'] = argovis_reference_scale_mapping
    profile_dict['goship_reference_scale_mapping'] = goship_reference_scale_mapping

    return profile_dict, json_profile


def check_if_all_ctd_vars(nc, df_mapping):

    # Has all ctd vars if have both ctd temperature and pressure

    logging_filename = 'logging.txt'

    is_pres = False
    is_ctd_temp_w_refscale = False
    is_ctd_temp_w_no_refscale = False

    has_ctd_vars = True

    names = df_mapping['name'].tolist()

    for name in names:

        # From name mapping earlier, goship pressure name mapped to Argo equivalent
        if name == 'pres':
            is_pres = True

        # From name mapping earlier, goship temperature name mapped to Argo equivalent
        if name == 'temp':
            is_ctd_temp_w_refscale = True

    expocode = nc.coords['expocode'].data[0]

    if not is_pres or not is_ctd_temp_w_refscale:
        logging.info('===========')
        logging.info('EXCEPTIONS FOUND')
        logging.info(expocode)
        logging.info(logging_filename)

    if not is_pres:
        logging.info('missing pres')
        with open('files_no_pressure.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    # If possiblity have both a ctd temperature with and without a refscale
    # Check what condition
    if not is_ctd_temp_w_refscale and is_ctd_temp_w_no_refscale:
        logging.info('CTD Temp with no ref scale')
        # Write to file listing files without ctd variables
        with open('files_ctd_temps_no_refscale.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    if not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale:
        logging.info('NO CTD Temp')
        # Write to file listing files without ctd variables
        with open('files_no_ctd_temps.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    # TODO log these exceptions

    # Skip making json if no expocode, no pressure or no ctd temp with ref scale
    if not is_pres:
        has_ctd_vars = False

    if not is_ctd_temp_w_refscale:
        has_ctd_vars = False

    # elif not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale:
    #     # files with no ctd temp
    #     return

    expocode = nc.coords['expocode'].data[0]

    if expocode == 'None':
        has_ctd_vars = False

    # return is_pres, is_ctd_temp_w_refscale

    return has_ctd_vars


def create_json_and_dict(nc, meta_param_names, df_mapping, filename):

    # Consider this as way to get values out of the xarray
    # xr_strs = nc[name].astype('str')
    # np_arr = xr_strs.values

    # Check if all ctd vars available: pressure and temperature
    has_ctd_vars = check_if_all_ctd_vars(nc, df_mapping)

    if not has_ctd_vars:
        return

    all_profile_dicts = []
    all_json_entries = []

    for nc_group in nc.groupby('N_PROF'):

        print(f"Processing profile {nc_group[0]}")

        profile_number = nc_group[0]
        profile_group = nc_group[1]

        profile_dict, json_profile = create_profile_json_and_dict(
            profile_number, profile_group, df_mapping, meta_param_names, filename)

        all_profile_dicts.append(profile_dict)
        all_json_entries.append(json_profile)

    return all_profile_dicts, all_json_entries


def convert_sea_water_temp(nc, var, goship_reference_scale):

    # Check sea_water_temperature to be degree_Celsius and
    # have goship_reference_scale be ITS-90

    # So look for ref_scale = IPTS-68 or ITS-90

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    # Change this to work for all temperature names

    reference_scale = np.nan

    try:
        temperature = nc[var].data

        if goship_reference_scale == 'IPTS-68':

            # Convert to ITS-90 scale
            temperature90 = temperature/1.00024

            # Set nc var of temp to this value

            # TODO: check if can set precision.
            # want the same as the orig temperature precision

            nc[var].data = temperature90

            reference_scale = 'IPT-90'

        else:
            print('temperature not IPTS-68')
            reference_scale = np.nan

        return nc, reference_scale

    except KeyError:

        return nc, reference_scale


def convert_units_add_ref_scale(nc, df_mapping, meta_param_names):

    # Create new column to hold new reference scales
    df_mapping['reference_scale'] = df_mapping['goship_reference_scale']

    # TODO
    # Is it correct only looking at params and not meta?

    for var in meta_param_names['param']:

        row = df_mapping.loc[df_mapping['goship_name'] == var]

        # new_ref_scale = row['goship_reference_scale']

        # If argo ref scale not equal to goship ref scale, convert
        # So far, it's only the case for temperature
        # row = df_mapping.loc[print(
        #     df_mapping['goship_reference_scale']) == var]

        try:
            goship_reference_scale = row['goship_reference_scale'].values[0]
        except IndexError:
            goship_reference_scale = np.nan

        try:
            argo_ref_scale = row['argo_reference_scale'].values[0]
        except IndexError:
            argo_ref_scale = np.nan

        goship_is_nan = pd.isnull(goship_reference_scale)
        argo_is_nan = pd.isnull(argo_ref_scale)

        if goship_is_nan and argo_is_nan:
            new_ref_scale = np.nan

        elif goship_reference_scale == argo_ref_scale:
            new_ref_scale = goship_reference_scale

        elif (goship_reference_scale != argo_ref_scale) and not argo_is_nan and not goship_is_nan:
            # Convert to argo ref scale
            # then save this to add to new reference_scale column

            # TODO: are there other ref scales to convert?

            if argo_ref_scale == 'IPT-90' and goship_reference_scale == 'IPTS-68':
                # convert seawater temperature
                # TODO
                # Use C-format or precision of IPT-90 to get same precision
                nc, new_ref_scale = convert_sea_water_temp(nc,
                                                           var, goship_reference_scale)

        elif not goship_is_nan and argo_is_nan:
            new_ref_scale = goship_reference_scale

        df_mapping.loc[df_mapping['name'] == var,
                       'reference_scale'] = new_ref_scale

    return nc, df_mapping


def get_new_unit_name(var, df_mapping, unit_mapping):

    # Unit mapping of names.

    # New unit name is argo unit, but if no argo unit, use goship unit
    # From name, get corresponding row to grab units information
    row_index = df_mapping.index[df_mapping['name'] == var].tolist()[0]

    argo_unit = df_mapping.loc[row_index, 'argo_unit']
    goship_unit = df_mapping.loc[row_index, 'goship_unit']

    try:
        new_goship_unit = unit_mapping[goship_unit]
    except:
        new_goship_unit = goship_unit

    if argo_unit == goship_unit:
        new_unit = argo_unit
    elif not pd.isnull(argo_unit) and not pd.isnull(new_goship_unit):
        new_unit = argo_unit
    elif pd.isnull(argo_unit) and not pd.isnull(new_goship_unit):
        new_unit = new_goship_unit
    elif not pd.isnull(argo_unit) and pd.isnull(new_goship_unit):
        new_unit = argo_unit
    else:
        new_unit = argo_unit

    return new_unit


def create_new_units_mapping(df_mapping, argo_units_mapping_file):

    # Mapping goship unit names to argo units
    # Since unit of 1 for Salinity maps to psu,
    # check reference scale to be PSS-78, otherwise
    # leave unit as 1

    # 3 columns
    # goship_unit, argo_unit, reference_scale
    df_mapping_unit_ref_scale = pd.read_csv(argo_units_mapping_file)

    # Use df_mapping_unit_ref_scale to map goship units to argo units
    # {'goship_unit': '1', 'argo_unit': 'psu', 'reference_scale': 'PSS-78'}
    argo_mapping = df_mapping_unit_ref_scale.to_dict(orient='records')

    unit_mapping = {argo_map['goship_unit']: argo_map['argo_unit']
                    for argo_map in argo_mapping}

    names = df_mapping['name'].tolist()

    new_units = {}
    for name in names:

        new_unit = get_new_unit_name(name, df_mapping, unit_mapping)

        new_units[name] = new_unit

    df_new_units = pd.DataFrame.from_dict(new_units.items())
    df_new_units.columns = ['name', 'unit']

    # Add name and corresponding unit mapping to df_mapping
    df_mapping = df_mapping.merge(
        df_new_units, how='left', left_on='name', right_on='name')

    return df_mapping


def create_new_names_mapping(df, meta_param_names):

    # Look at var names to rename temperature and salinity
    # depending on goship name because there are multiple types
    params = meta_param_names['param']

    is_ctd_temp = False
    is_ctd_temp_68 = False
    # is_ctd_temp_unknown = False

    for name in params:

        # if both ctd_temperature and ctd_temperature_68,
        # use ctd_temperature to temp only
        if name == 'ctd_temperature':
            is_ctd_temp = True
        if name == 'ctd_temperature_68':
            is_ctd_temp_68 = True

    # Create new column with new names. Start as argo names if exist
    df['name'] = df['argovis_name']

    # if argo name is nan, use goship name
    df['name'] = np.where(df['argovis_name'].isna(),
                          df['goship_name'], df['name'])

    # if both temp on ITS-90 scale and temp on IPTS-68 scale,
    # just change name of ctd_temperature (ITS-90) to temp, and
    # don't change name of ctd_temperature_68. So keep names as is.

    # If don't have an argo name of temp yet and do have a ctd temperature
    # on the 68 scale, call it temp
    if not is_ctd_temp and is_ctd_temp_68:
        # change name to temp and convert to ITS-90 scale later
        df.loc[df['goship_name'] == 'ctd_temperature_68', 'name'] = 'temp'
        df_qc = df.isin({'goship_name': ['ctd_temperature_68_qc']}).any()

        if df_qc.any(axis=None):
            df.loc[df['goship_name'] ==
                   'ctd_temperature_68_qc', 'name'] = 'temp_qc'

    return df


def add_qc_names_to_argo_names(df_mapping):

    # If goship name has a qc, rename corresponding argo name to argo name qc
    goship_qc_names = df_mapping.loc[df_mapping['goship_name'].str.contains(
        '_qc')]['goship_name'].tolist()

    for goship_qc_name in goship_qc_names:

        # find argo name of goship name without qc
        goship_base_name = goship_qc_name.replace('_qc', '')
        argo_name = df_mapping.loc[df_mapping['goship_name']
                                   == goship_base_name, 'argovis_name']

        # If argo_name not empty, add qc name
        if pd.notna(argo_name.values[0]):
            df_mapping.loc[df_mapping['goship_name'] == goship_qc_name,
                           'argovis_name'] = argo_name.values[0] + '_qc'

    return df_mapping


def rename_mapping_names_units(df_mapping, meta_param_names, argo_units_mapping_file):

    # Add qc names since Argo names don't have any
    # example: for argo name temp, add temp_qc if corresponding goship name has qc
    df_mapping = add_qc_names_to_argo_names(df_mapping)

    # Take mapping of goship to new Name and use Argo Names if exist
    # Otherwise, use goship name
    df_mapping = create_new_names_mapping(df_mapping, meta_param_names)

    # Convert goship unit names into argo unit names
    # Unit names rely on reference scale if unit = 1
    df_mapping = create_new_units_mapping(df_mapping, argo_units_mapping_file)

    # Later, compare ref scales and see if need to convert

    return df_mapping


def get_goship_mapping_df(nc):

    # Get units, reference scale and c_format from nc
    coord_names = list(nc.coords)
    var_names = list(nc.keys())

    name_to_units = {}
    name_to_ref_scale = {}
    name_to_c_format = {}

    for coord in coord_names:

        try:
            name_to_units[coord] = nc.coords[coord].attrs['units']
        except KeyError:
            name_to_units[coord] = np.nan

        try:
            name_to_ref_scale[coord] = nc.coords[coord].attrs['reference_scale']
        except KeyError:
            name_to_ref_scale[coord] = np.nan

        try:
            name_to_c_format[coord] = nc.coords[coord].attrs['C_format']
        except KeyError:
            name_to_c_format[coord] = np.nan

    for var in var_names:

        try:
            name_to_units[var] = nc[var].attrs['units']
        except KeyError:
            name_to_units[var] = np.nan

        try:
            name_to_ref_scale[var] = nc[var].attrs['reference_scale']
            # Replace 'unknown' with np.nan
            if name_to_ref_scale[var] == 'unknown':
                name_to_ref_scale[var] = np.nan
        except KeyError:
            name_to_ref_scale[var] = np.nan

        try:
            name_to_c_format[var] = nc[var].attrs['C_format']
        except KeyError:
            name_to_c_format[var] = np.nan

    df_dict = {}

    df_dict['goship_unit'] = name_to_units
    df_dict['goship_reference_scale'] = name_to_ref_scale
    df_dict['goship_c_format'] = name_to_c_format

    df = pd.DataFrame.from_dict(df_dict)
    df.index.name = 'goship_name'
    df = df.reset_index()

    return df


def get_argo_mapping_df(argo_name_mapping_file):

    df = pd.read_csv(argo_name_mapping_file)

    # Only want part of the csv file
    df = df[['argovis_name', 'argo_unit',
             'argo_reference_scale', 'goship_name']].copy()

    return df


def get_argo_goship_mapping(nc, argo_name_mapping_file):

    df_argo_mapping = get_argo_mapping_df(argo_name_mapping_file)

    df_goship_mapping = get_goship_mapping_df(nc)

    # Any empty cells are filled with nan
    df_all_mapping = df_goship_mapping.merge(
        df_argo_mapping, how='left', left_on='goship_name', right_on='goship_name')

    # Rename index created when merging on goship_name
    df_all_mapping = df_all_mapping.rename(
        columns={'index': 'goship_name'})

    # Remove profile_type from mapping
    indices = df_all_mapping[df_all_mapping['goship_name']
                             == 'profile_type'].index
    df_all_mapping.drop(indices, inplace=True)

    return df_all_mapping


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

    # Remove profile_type
    meta_names.remove('profile_type')

    return meta_names, param_names


def process_cruise(cruise_info, bot_found, ctd_found):

    nc_dict = {}
    nc_dict['bot'] = None
    nc_dict['ctd'] = None
    bot_names = {}
    ctd_names = {}
    filenames = {}

    if bot_found:
        bot_path = cruise_info['bot_path']
        bot_url = f"https://cchdo.ucsd.edu{bot_path}"

        with fsspec.open(bot_url) as fobj:
            nc_dict['bot'] = xr.open_dataset(fobj)

        filenames['bot'] = cruise_info['bot_filename']

        bot_meta_names, bot_param_names = get_meta_param_names(nc_dict['bot'])

        bot_names['meta'] = bot_meta_names
        bot_names['param'] = bot_param_names

    if ctd_found:
        ctd_path = cruise_info['ctd_path']
        ctd_url = f"https://cchdo.ucsd.edu{ctd_path}"

        with fsspec.open(ctd_url) as fobj:
            nc_dict['ctd'] = xr.open_dataset(fobj)

        filenames['ctd'] = cruise_info['ctd_filename']

        ctd_meta_names, ctd_param_names = get_meta_param_names(nc_dict['ctd'])

        ctd_names['meta'] = ctd_meta_names
        ctd_names['param'] = ctd_param_names

    # print(nc_dict['bot'])
    # print(nc_dict['ctd'])

    print('=====================')

    return (filenames, nc_dict, bot_names, ctd_names)


# def process_folder(nc_folder):

#     # folder_name = nc_folder.name

#     nc_files = os.scandir(nc_folder)

#     nc_dict = {}
#     nc_dict['bot'] = None
#     nc_dict['ctd'] = None

#     filenames = {}

#     for file in nc_files:

#         filename = file.name

#         if not filename.endswith('.nc'):
#             continue

#         print('-------------')
#         print(filename)
#         print('-------------')

#         filepath = os.path.join(nc_folder, filename)
#         nc = xr.load_dataset(filepath)

#         profile_type = nc['profile_type'].values[0]

#         if profile_type == 'B':
#             nc_dict['bot'] = nc
#             filenames['bot'] = filename
#         elif profile_type == 'C':
#             nc_dict['ctd'] = nc
#             filenames['ctd'] = filename
#         else:
#             print('No bottle or ctd files')
#             exit(1)

#     nc_files.close()

#     nc_bot = nc_dict['bot']
#     nc_ctd = nc_dict['ctd']

#     print(nc_bot)
#     print(nc_ctd)

#     print('=====================')

#     bot_names = {}
#     ctd_names = {}

#     if nc_bot:
#         bot_meta_names, bot_param_names = get_meta_param_names(nc_bot)
#         bot_names['meta'] = bot_meta_names
#         bot_names['param'] = bot_param_names

#     if nc_ctd:
#         ctd_meta_names, ctd_param_names = get_meta_param_names(nc_ctd)
#         ctd_names['meta'] = ctd_meta_names
#         ctd_names['param'] = ctd_param_names

#     return (filenames, nc_dict, bot_names, ctd_names)


def find_bot_ctd_file_info(s, file_ids):

    # Get file meta for each file id to search for cruise doc id and bottle id

    file_info = {}

    bot_found = False
    ctd_found = False

    file_info['bot_id'] = None
    file_info['bot_path'] = ''
    file_info['ctd_id'] = None
    file_info['ctd_path'] = ''

    for file_id in file_ids:

        # Find all bottle ids and doc ids for each cruise
        # Following api only lists active files

        query = f"{API_END_POINT}/file/{file_id}"
        response = s.get(query, headers=headers)

        if response.status_code != 200:
            print('api not reached in function get_cruise_information')
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
                file_info['bot_filename'] = file_path.split(
                    '/')[-1]
                bot_found = True

            if data_type == "ctd" and data_format == "cf_netcdf":
                file_info['ctd_id'] = file_id
                file_info['ctd_path'] = file_path
                file_info['ctd_filename'] = file_path.split(
                    '/')[-1]
                ctd_found = True

    if bot_found or ctd_found:
        return file_info, bot_found, ctd_found

    return {}, False, False


def get_all_file_ids(s):

    # Use api query to get all active file ids
    query = f"{API_END_POINT}/file"

    response = s.get(query, headers=headers)

    if response.status_code != 200:
        print('api not reached in get_all_files')
        exit(1)

    all_files = response.json()['files']

    all_file_ids = [file['id'] for file in all_files]

    return all_file_ids


def get_all_cruises(s):

    # Use api query to get all cruise id with their attached file ids

    query = f"{API_END_POINT}/cruise/all"

    response = s.get(query, headers=headers)

    if response.status_code != 200:
        print('api not reached in get_all_cruises')
        exit(1)

    all_cruises = response.json()

    return all_cruises


def get_cruise_information(s):

    # To get expocodes and cruise ids, Use get cruise/all to get all cruise metadata
    # and search cruises to get Go-Ship cruises expocodes and cruise ids,
    # from attached file ids, Search file metadata from doc file id, bottle file id

    # Get all cruises and active files
    all_cruises = get_all_cruises(s)
    all_file_ids = get_all_file_ids(s)

    all_cruises_info = []

    cruise_count = 0

    for cruise in all_cruises:

        programs = cruise['collections']['programs']
        programs = [x.lower() for x in programs]

        country = cruise['country']

        # Only want US GoShip
        if 'go-ship' in programs and country == 'US':

            print(f"Finding cruise information for {cruise['expocode']}")

            # Get files attached to the cruise
            # Could be deleted ones so check if exist in all_files
            file_ids = cruise['files']

            # Get only file_ids in all_file_ids (active files)
            active_file_ids = list(
                filter(lambda x: (x in all_file_ids), file_ids))

            # Get file meta for each file id to search for
            # cruise doc and bottle info
            file_info, bot_found, ctd_found = find_bot_ctd_file_info(
                s, active_file_ids)

            # Only want cruises with both dataset bot and doc files
            if not len(file_info):
                continue

            # bot_url = https://cchdo.ucsd.edu/data/<file id>/<filename>

            cruise_info = {}

            cruise_info['expocode'] = cruise['expocode']
            cruise_info['cruise_id'] = cruise['id']

            if bot_found:
                cruise_info['bot_found'] = True
                cruise_info['bot_path'] = file_info['bot_path']
                cruise_info['bot_filename'] = file_info['bot_filename']

            if ctd_found:
                cruise_info['ctd_found'] = True
                cruise_info['ctd_path'] = file_info['ctd_path']
                cruise_info['ctd_filename'] = file_info['ctd_filename']

            all_cruises_info.append(cruise_info)

            # DEVELOPEMENT
            cruise_count = cruise_count + 1

            if cruise_count == 2:
                return all_cruises_info

    return all_cruises_info


def main():

    # TODO: Need to add in bot and ctd files used in creating the json

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

    # TODO
    # For now, grabbed by hand a bot and ctd file of one expocode and put
    # in a folder. Later generalize to match up all files by looping
    # through them
    # input_netcdf_data_directory = './data/same_expocode_bot_ctd_netcdf'
    json_data_directory = './data/same_expocode_json'

    argo_name_mapping_file = 'argo_goship_mapping.csv'
    argo_units_mapping_file = 'argo_goship_units_mapping.csv'

    s = requests.Session()
    s.headers.update({"X-Authentication-Token": API_KEY})

    a = requests.adapters.HTTPAdapter(max_retries=3)
    b = requests.adapters.HTTPAdapter(max_retries=3)

    s.mount('http://', a)
    s.mount('https://', b)

    all_cruises_info = get_cruise_information(s)

    # nc_data_entry = os.scandir(input_netcdf_data_directory)

    # for nc_folder in nc_data_entry:

    #     if nc_folder.name != '32NH047_1':
    #         continue

    for cruise_info in all_cruises_info:

        expocode = cruise_info['expocode']

        print('=======')
        print(f"Processing {expocode}")

        # # Process folder to combine bot and ctd without
        # # caring about setting new coords and vals
        # if nc_folder.is_dir():

        # filenames, nc_dict, bot_names, ctd_names = process_folder(
        #     nc_folder)

        bot_profile_dicts = {}
        ctd_profile_dicts = {}

        bot_found = cruise_info['bot_found']
        ctd_found = cruise_info['ctd_found']

        filenames, nc_dict, bot_names, ctd_names = process_cruise(
            cruise_info, bot_found, ctd_found)

        if bot_found:

            print('---------------------------')
            print('Start processing bottle profiles')
            print('---------------------------')

            df_bot_mapping = get_argo_goship_mapping(nc_dict['bot'],
                                                     argo_name_mapping_file)

            df_bot_mapping = rename_mapping_names_units(
                df_bot_mapping, bot_names, argo_units_mapping_file)

            nc_dict['bot'], df_bot_mapping = convert_units_add_ref_scale(
                nc_dict['bot'], df_bot_mapping, bot_names)

            # Create ArgoVis json
            bot_profile_dicts, bot_json_entries = create_json_and_dict(
                nc_dict['bot'], bot_names, df_bot_mapping, filenames['bot'])

            print('---------------------------')
            print('Created bottle profiles')
            print('---------------------------')

        if ctd_found:

            print('---------------------------')
            print('Start processing ctd profiles')
            print('---------------------------')

            df_ctd_mapping = get_argo_goship_mapping(nc_dict['ctd'],
                                                     argo_name_mapping_file)

            df_ctd_mapping = rename_mapping_names_units(
                df_ctd_mapping, ctd_names, argo_units_mapping_file)

            nc_dict['ctd'], df_ctd_mapping = convert_units_add_ref_scale(
                nc_dict['ctd'], df_ctd_mapping, ctd_names)

            # Create ArgoVis json
            ctd_profile_dicts, ctd_json_entries = create_json_and_dict(
                nc_dict['ctd'], ctd_names, df_ctd_mapping, filenames['ctd'])

            print('---------------------------')
            print('Created ctd profiles')
            print('---------------------------')

        if bot_found or ctd_found:
            print('processing output')
            process_output(nc_dict, bot_profile_dicts,
                           ctd_profile_dicts, json_data_directory)

    # nc_data_entry.close()

    logging.info(datetime.now() - start_time)


if __name__ == '__main__':
    main()
