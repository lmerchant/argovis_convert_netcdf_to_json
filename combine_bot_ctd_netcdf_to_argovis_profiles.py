from pandas.core.algorithms import isin
import xarray as xr
import pandas as pd
import numpy as np
import os
from datetime import datetime
import json
import logging
from decimal import Decimal
# import requests
# from requests.adapters import HTTPAdapter
import fsspec
import re
from cchdo.auth.session import session as requests
import copy


API_END_POINT = "https://cchdo.ucsd.edu/api/v1"

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


def get_goship_salniity_reference_scale():

    return {
        'goship_ref_scale': 'PSS-78'
    }


def get_argovis_reference_scale_per_type():

    return {
        'temperature': 'ITS-90',
        'salinity': 'PSS-78'
    }


def get_goship_argovis_unit_mapping():

    return {
        'dbar': 'decibar',
        '1': 'psu',
        'degC': 'Celsiius',
        'umol/kg': 'micromole/kg',
        'meters': 'meters',
        'degree_north': 'degree north',
        'degree_east': 'degree_east'
    }


def get_goship_argovis_name_mapping():

    return {
        'pressure': 'pres',
        'ctd_salinity': 'psal',
        'ctd_temperature': 'temp',
        'ctd_oxygen': 'doxy',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def get_core_values_no_qc():

    # return ['pres', 'temp', 'psal', 'doxy']
    return ['pressure', 'ctd_temperature', 'ctd_salinity', 'ctd_oxygen']


# def get_core_values():
#     return ['pres', 'temp', 'psal',
#             'doxy', 'temp_qc', 'psal_qc', 'doxy_qc']

def get_core_values():
    return ['pressure', 'ctd_temperature', 'ctd_temperature_qc', 'ctd_salinity', 'ctd_oxygen', 'ctd_temperature_qc', 'ctd_salinity_qc', 'ctd_oxygen_qc']


def get_argovis_meta_mapping():
    return {'latitude': 'lat',
            'longitude': 'lon'}


def get_core_values_mapping_bot():
    return {'pressure': 'pres',
            'ctd_temperature': 'temp_btl',
            'ctd_salinity': 'psal_btl',
            'ctd_oxygen': 'doxy_btl',
            'bottle_salinity': 'psal_btl',
            'latitude': 'lat_btl',
            'longitude': 'lon_btl'}


def get_core_values_mapping_ctd():
    return {'pressure': 'pres',
            'ctd_temperature': 'temp_ctd',
            'ctd_salinity': 'psal_ctd',
            'ctd_oxygen': 'doxy_ctd',
            'latitude': 'lat',
            'longitude': 'lon'}


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


def create_renamed_list_of_objs(cur_list, type):

    # Common names are pres, temp, psal, and doxy will have suffix _ctd and _ctd_qc
    # All ctd vars will have suffix _ctd and _ctd_qc
    # All bot vars will have suffx _btl and _btl_qc
    # Special case is bottle_salinity to psal_btl

    new_list = []

    for obj in cur_list:

        new_obj = {}

        if type == 'bot':

            new_obj = rename_bot_key_not_meta(obj)

            new_list.append(new_obj)

        if type == 'ctd':

            new_obj = rename_ctd_key_not_meta(obj)

            new_list.append(new_obj)

    return new_list


def rename_bot_key_not_meta_list(dict_list):

    # core_values = get_core_values()

    new_list = []
    for obj in dict_list:

        new_obj = rename_bot_key_not_meta(obj)

        new_list.append(new_obj)

    return new_list


# def rename_ctd(name):

#     if '_qc' in name:
#         return name.replace('_qc', '_ctd_qc')
#     else:
#         return f"{name}_ctd"


# def rename_core_bot(name):
#     # Add suffix _ctd, and for _ctd_qc for all core
#     # except temp for botttle
#     if '_qc' in name and name == 'temp':
#         return 'temp_btl_qc'
#     elif name == 'temp':
#         return 'temp_btl'
#     elif '_qc' in name:
#         return name.replace('_qc', '_ctd_qc')
#     elif name != 'pres':
#         return f"{name}_ctd"
#     else:
#         return name


# def rename_meta_bot(meta_bot):

#     # Lon_btl,lat_btl,date_btl, geoLocation_btl
#     new_obj = {}
#     for key, val in meta_bot.items():
#         new_key = f"{key}_btl"
#         new_obj[new_key] = val

#     return new_obj


def rename_bot_value_not_meta(obj):

  # For mapping cases where goship is the key
  # and argovis is the value
    core_values_mapping = get_core_values_mapping_bot()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key in obj:

        if '_qc' in key:
            check_core = key.replace('_qc', '')
            if check_core in core_values:
                new_val = f"{core_values_mapping[check_core]}_qc"

        elif key in core_values:
            new_val = core_values_mapping[key]

        elif '_qc' in key:
            key_wo_qc = key.replace('_qc', '')
            new_val = f"{key_wo_qc}_btl_qc"
        else:
            new_val = f"{key}_btl"

        new_obj[key] = new_val

    return new_obj


def rename_argovis_meta(obj):

    core_values_mapping = get_argovis_meta_mapping()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key, val in obj.items():

        if key in core_values:
            new_key = core_values_mapping[key]

        else:
            new_key = key

        new_obj[new_key] = val

    return new_obj


def rename_bot_key_meta(obj):

    core_values_mapping = get_core_values_mapping_bot()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key, val in obj.items():

        if key in core_values:
            new_key = core_values_mapping[key]

        else:
            new_key = f"{key}_btl"

        new_obj[new_key] = val

    return new_obj


def rename_bot_key_not_meta(obj):

    core_values_mapping = get_core_values_mapping_bot()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key, val in obj.items():

        if '_qc' in key:
            check_core = key.replace('_qc', '')
            if check_core in core_values:
                new_key = f"{core_values_mapping[check_core]}_qc"

        elif key in core_values:
            new_key = core_values_mapping[key]

        elif '_qc' in key:
            key_wo_qc = key.replace('_qc', '')
            new_key = f"{key_wo_qc}_btl_qc"
        else:
            new_key = f"{key}_btl"

        new_obj[new_key] = val

    return new_obj


def rename_ctd_value_not_meta(obj):

    # For case when goship is key and mapping to argovis

    core_values_mapping = get_core_values_mapping_ctd()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key in obj:

        if '_qc' in key:
            check_core = key.replace('_qc', '')
            if check_core in core_values:
                new_val = f"{core_values_mapping[check_core]}_qc"

        elif key in core_values:
            new_val = core_values_mapping[key]

        elif '_qc' in key:
            key_wo_qc = key.replace('_qc', '')
            new_val = f"{key_wo_qc}_ctd_qc"
        else:
            new_val = f"{key}_ctd"

        new_obj[key] = new_val

    return new_obj


def rename_ctd_key_not_meta(obj):

    core_values_mapping = get_core_values_mapping_ctd()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key, val in obj.items():

        if '_qc' in key:
            check_core = key.replace('_qc', '')
            if check_core in core_values:
                new_key = f"{core_values_mapping[check_core]}_qc"

        elif key in core_values:
            new_key = core_values_mapping[key]

        elif '_qc' in key:
            key_wo_qc = key.replace('_qc', '')
            new_key = f"{key_wo_qc}_ctd_qc"
        else:
            new_key = f"{key}_ctd"

        new_obj[new_key] = val

    return new_obj


def rename_ctd_key_meta(obj):

    core_values_mapping = get_core_values_mapping_ctd()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key, val in obj.items():

        if key in core_values:
            new_key = core_values_mapping[key]

        else:
            new_key = f"{key}_ctd"

        new_obj[new_key] = val

    return new_obj


# TODO. Do i use this?
def rename_mapping_ctd_key_list(obj_list):

    core_values = get_core_values()

    new_list = []

    for obj in obj_list:
        new_mapping = {}
        for key, val in obj.items():
            if key in core_values:
                new_key = key
            elif '_qc' in key:
                new_key = key.replace('_qc', '_ctd_qc')
            else:
                new_key = f"{key}_ctd"

            new_mapping[new_key] = val
        new_list.append(new_mapping)

    return new_list


def process_output_per_profile(profile_dict, type):

    # Make a new object because will be using it next if combine profiles
    # but do rename lat and lon
    meta = profile_dict['meta']
    meta_copy = copy.deepcopy(meta)

    measurements_list = profile_dict['measurements']
    bgc_list = profile_dict['bgc_meas']
    names = profile_dict['goship_argovis_name_mappping']

    if type == 'bot':

        renamed_meta = rename_argovis_meta(meta)

        renamed_meta_copy = rename_argovis_meta(meta_copy)

        bgc_list = create_renamed_list_of_objs(bgc_list, type)

        measurements_list = create_renamed_list_of_objs(
            measurements_list, type)

        name_mapping = rename_bot_value_not_meta(names)

        argovis_ref_scale_mapping = rename_bot_key_not_meta(profile_dict[
            'argovis_ref_scale_mapping'])

    elif type == 'ctd':

        renamed_meta = rename_argovis_meta(meta)

        renamed_meta_copy = rename_argovis_meta(meta_copy)

        bgc_list = create_renamed_list_of_objs(bgc_list, type)

        measurements_list = create_renamed_list_of_objs(
            measurements_list, type)

        name_mapping = rename_ctd_value_not_meta(names)

        argovis_ref_scale_mapping = rename_ctd_key_not_meta(profile_dict[
            'argovis_ref_scale_mapping'])

    goship_ref_scale_mappping = profile_dict['goship_ref_scale_mapping']
    goship_argovis_unit_mapping = profile_dict['goship_argovis_unit_mappping']

    # don't rename meta yet incase not both bot and ctd combined
    data_dict = renamed_meta
    data_dict['measurements'] = measurements_list
    data_dict['bgcMeas'] = bgc_list
    data_dict['goshipArgovisNameMapping'] = name_mapping
    data_dict['goshipReferenceScale'] = goship_ref_scale_mappping
    data_dict['argovisReferenceScale'] = argovis_ref_scale_mapping
    data_dict['goshipArgovisUnitsMapping'] = goship_argovis_unit_mapping

    return data_dict, renamed_meta_copy


def process_output_per_profile_bot_ctd(bot_combined_dict, ctd_combined_dict, bot_meta, ctd_meta):

    bot_renamed_meta = rename_bot_key_meta(bot_meta)

    meta = {**ctd_meta, **bot_renamed_meta}

    a = ctd_meta.update(bot_renamed_meta)

    # ctd_bgc_meas_renamed = rename_mapping_ctd_key_list(
    #     ctd_combined_dict['bgcMeas'])

    # btl_bgc_meas_renamed = rename_bot_key_not_meta_list(
    #     bot_combined_dict['bgcMeas'])

    #bgc_meas = ctd_bgc_meas_renamed.extend(btl_bgc_meas_renamed)
    bot_bgc_meas = bot_combined_dict['bgcMeas']
    ctd_bgc_meas = ctd_combined_dict['bgcMeas']

    bgc_meas = [*ctd_bgc_meas, *bot_bgc_meas]

    # ctd_measurement = rename_mapping_ctd_key_list(
    #     ctd_combined_dict['measurements'])
    # btl_measurement = rename_bot_key_not_meta_list(
    #     bot_combined_dict['measurements'])
    # measurement = ctd_measurement.extend(btl_measurement)

    measurement = [*ctd_combined_dict['measurements'],
                   *bot_combined_dict['measurements']]

    # here

    exit(1)

    btl_goship_argovis_name_mapping = rename_bot_value_not_meta(
        bot_combined_dict['goshipArgovisNameMapping'])
    ctd_goship_argovis_name_mapping = rename_ctd_value(
        ctd_combined_dict['goshipArgovisNameMapping'])

    btl_ctd_goship_argovis_name_mapping = ctd_goship_argovis_name_mapping.update(
        btl_goship_argovis_name_mapping)

    goship_reference_scale = ctd_combined_dict['goshipReferenceScale'].update(
        bot_combined_dict['goshipReferenceScale'])

    ctd_argovis_reference_scale_renamed = rename_ctd_key(
        ctd_combined_dict['argovisReferenceScale'])
    bot_argovis_reference_scale_renamed = rename_bot_key_not_meta(
        bot_combined_dict['argovisReferenceScale'])
    argovis_reference_scale = ctd_argovis_reference_scale_renamed.update(
        bot_argovis_reference_scale_renamed)

    goship_argovis_units_mapping = ctd_combined_dict['goshipArgovisUnitsMapping'].update(
        bot_combined_dict['goshipArgovisUnitsMapping'])

    combined_bot_ctd_dict = {}
    combined_bot_ctd_dict = meta_data
    combined_bot_ctd_dict['measurements'] = measurement
    combined_bot_ctd_dict['bgcMeas'] = bgc_meas
    combined_bot_ctd_dict['goshipArgovisName_mapping'] = btl_ctd_goship_argovis_name_mapping
    combined_bot_ctd_dict['goshipReferenceScale'] = goship_reference_scale
    combined_bot_ctd_dict['argovis_ReferenceScale'] = argovis_reference_scale
    combined_bot_ctd_dict['goshipArgovisUnitsMapping'] = goship_argovis_units_mapping

    exit(1)


def process_output(bot_obj, ctd_obj, bot_profile_dicts, ctd_profile_dicts, json_data_directory):

    # TODO
    # add following flags
    # isGOSHIPctd = true
    # isGOSHIPbottle = true
    # core_info = 1  # is ctd
    # core_info = 2  # is bottle (no ctd)
    # core_info = 12  # is ctd and there is bottle too (edited)

    bot_num_profiles = 0
    ctd_num_profiles = 0

    bot_found = bot_obj['found']
    ctd_found = ctd_obj['found']

    #     ctd_found = False

    if bot_found:
        bot_num_profiles = len(bot_profile_dicts)

    if ctd_found:
        ctd_num_profiles = len(ctd_profile_dicts)

    num_profiles = max(bot_num_profiles, ctd_num_profiles)

    # TODO: what to do in the case when not equal when have both bot and ctd?
    #  Is there a case for this?
    if bot_found and ctd_found:
        if bot_num_profiles != ctd_num_profiles:
            print(
                f"bot profiles {bot_num_profiles} and ctd profiles {ctd_num_profiles} are different")

    for profile_number in range(num_profiles):

        if bot_found:

            bot_profile_dict = bot_profile_dicts[profile_number]

            bot_combined_dict, bot_meta = process_output_per_profile(
                bot_profile_dict, 'bot')

        if ctd_found:
            ctd_profile_dict = ctd_profile_dicts[profile_number]

            ctd_combined_dict, ctd_meta = process_output_per_profile(
                ctd_profile_dict, 'ctd')

        if bot_found and ctd_found:
            bot_ctd_combined_dict = process_output_per_profile_bot_ctd(
                bot_combined_dict, ctd_combined_dict, bot_meta, ctd_meta)

            print(bot_ctd_combined_dict)

            exit(1)

            # write_profile_json(json_data_directory, bot_ctd_combined_dict)

        elif bot_found:
            # write_profile_json(json_data_directory, bot_combined_dict)
            pass

        elif ctd_found:
            pass
            # write_profile_json(json_data_directory, ctd_combined_dict)

        else:
            print("Didn't process bottle or ctd")


# def create_goship_argovis_unit_mapping_json_str(df_mapping):

#     # Create goship to new units mapping json string

#     df_unit_mapping = df_mapping[['goship_unit', 'unit']].copy()
#     # Remove rows if any NaN
#     df_unit_mapping = df_unit_mapping.dropna(how='any')

#     unit_mapping_dict = dict(
#         zip(df_unit_mapping['goship_unit'], df_unit_mapping['unit']))

#     json_str = json.dumps(unit_mapping_dict)

#     return json_str


# def create_goship_ref_scale_mapping_json_str(df_mapping):

#     # Create goship to new names mapping json string
#     df_ref_scale_mapping = df_mapping[[
#         'goship_name', 'goship_reference_scale']].copy()

#     # Remove rows if any NaN
#     df_ref_scale_mapping = df_ref_scale_mapping.dropna(how='any')

#     ref_scale_mapping_dict = dict(
#         zip(df_ref_scale_mapping['goship_name'], df_ref_scale_mapping['goship_reference_scale']))

#     json_str = json.dumps(ref_scale_mapping_dict)

#     return json_str


def create_goship_ref_scale_mapping_dict(data_obj):

    nc = data_obj['nc']
    vars = nc.keys()
    mapping = {}

    for var in vars:
        try:
            ref_scale = nc[var].attrs['reference_scale']

            mapping[var] = ref_scale

        except KeyError:
            pass

    return mapping


def create_argovis_ref_scale_mapping_dict():

    return {
        'ctd_temperature': 'ITS-90',
        'ctd_salinity': 'PSS-78'
    }

# def create_argovis_ref_scale_mapping_json_str(df_mapping):

#     # Create new names to ref scale mapping json string
#     df_ref_scale_mapping = df_mapping[[
#         'name', 'reference_scale']].copy()

#     # Remove rows if any NaN
#     df_ref_scale_mapping = df_ref_scale_mapping.dropna(how='any')

#     ref_scale_mapping_dict = dict(
#         zip(df_ref_scale_mapping['name'], df_ref_scale_mapping['reference_scale']))

#     json_str = json.dumps(ref_scale_mapping_dict)

#     return json_str


# def create_name_mapping_json_str(df_mapping):

#     # Create goship to new names mapping json string
#     df_name_mapping = df_mapping[['goship_name', 'name']].copy()
#     # Remove rows if all NaN
#     df_name_mapping = df_name_mapping.dropna(how='all')

#     name_mapping_dict = dict(
#         zip(df_name_mapping['goship_name'], df_name_mapping['name']))

#     json_str = json.dumps(name_mapping_dict)

#     return json_str


def create_measurements_dict(df_bgc_meas):

    # TODO
    # QUESTION
    # what if there is no qc flag value, set as NaN

    df_meas = pd.DataFrame()

    core_values = get_core_values()

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
        if pd.notnull(val) and int(val) != 2:
            return val
        else:
            return np.nan

    for col in core_values:

        try:
            key = f"{col}_qc"
            df_meas[key] = df_meas[key].apply(mark_not_qc_2)
            # df_meas[key] = df_meas[key].apply(
            #     lambda row: mark_not_qc_2(df_meas[key]))

        except KeyError:
            pass

    # delete col if corresponding qc value is not 2
    column_names_not_qc = [col for col in df_meas.columns if '_qc' not in col]

    # TODO
    # DEVELOPMENT
    # UNCOMMENT THIS
    # for col in column_names_not_qc:
    #     drop_col = f"{col}_qc"
    #     try:
    #         indices = df_meas[(df_meas[drop_col] != 2)].index
    #         df_meas.drop(indices, inplace=True)
    #     except KeyError:
    #         pass

    # drop qc columns
    for col in df_meas.columns:
        if '_qc' in col:
            df_meas = df_meas.drop([col], axis=1)

    # Replace '' with nan to filter on nan
    df_meas = df_meas.replace(r'^\s*$', np.nan, regex=True)

    df_meas = df_meas.dropna(how='all')

    json_str = df_meas.to_json(orient='records')
    data_dict = json.loads(json_str)

    return data_dict


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

    # df = df.dropna(how='all')

    # df = df.reset_index(drop=True)

    return df


def create_bgc_meas_dict(df):

    json_str = df.to_json(orient='records')

    # _qc":2.0
    # If there is '.0' in qc value, remove it to get an int
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


def add_extra_coords(nc, filename):

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


def create_profile_json_and_dict(profile_group, data_obj):

    # Create json without translating to dataframe first

    # Consider this as way to get values out of the xarray
    # xr_strs = profile_group[name].astype('str')
    # np_arr = xr_strs.values
    filename = data_obj['filename']

    # profile_group = profile_group.rename(name_mapping_dict)

    profile_group = add_extra_coords(profile_group, filename)

    meta_names, param_names = get_meta_param_names(profile_group)

    meta_dict = create_meta_dict(profile_group, meta_names)

    param_json = create_json_profiles(profile_group, param_names)

    # TODO
    # PROBABLY DELETE CREATING JSON FIRST

    # When to rename
    # probably after create dict

    df_bgc = create_bgc_meas_df(param_json)

    bgc_meas_dict = create_bgc_meas_dict(df_bgc)

    measurements_dict = create_measurements_dict(
        df_bgc)

    goship_argovis_name_mapping_dict = get_goship_argovis_name_mapping()

    argovis_ref_scale_mapping_dict = create_argovis_ref_scale_mapping_dict()

    goship_argovis_unit_mapping_dict = get_goship_argovis_unit_mapping()

    goship_ref_scale_mapping_dict = create_goship_ref_scale_mapping_dict(
        data_obj)

    meta_copy = copy.deepcopy(meta_dict)

    # Save meta separate for renaming later
    profile_dict = {}
    profile_dict['meta'] = meta_copy
    profile_dict['bgc_meas'] = bgc_meas_dict
    profile_dict['measurements'] = measurements_dict
    profile_dict['goship_argovis_name_mappping'] = goship_argovis_name_mapping_dict
    profile_dict['goship_argovis_unit_mappping'] = goship_argovis_unit_mapping_dict
    profile_dict['argovis_ref_scale_mapping'] = argovis_ref_scale_mapping_dict
    profile_dict['goship_ref_scale_mapping'] = goship_ref_scale_mapping_dict

    profile = meta_dict
    profile['bgc_meas'] = bgc_meas_dict
    profile['measurements'] = measurements_dict
    profile['goship_argovis_name_mappping'] = goship_argovis_name_mapping_dict
    profile['goship_argovis_unit_mappping'] = goship_argovis_unit_mapping_dict
    profile['argovis_ref_scale_mapping'] = argovis_ref_scale_mapping_dict
    profile['goship_ref_scale_mapping'] = goship_ref_scale_mapping_dict

    profile_json = json.dumps(profile)

    return profile_json, profile_dict


# def create_profile_json_and_dict(profile_number, profile_group, df_mapping, data_obj, filename):

#     # Create json without translating to dataframe first

#     # Consider this as way to get values out of the xarray
#     # xr_strs = profile_group[name].astype('str')
#     # np_arr = xr_strs.values

#     name_mapping_dict = dict(
#         zip(df_mapping['goship_name'], df_mapping['name']))

#     profile_group = profile_group.rename(name_mapping_dict)
#     profile_group = add_extra_coords(profile_group, profile_number, filename)

#     meta_names, param_names = get_meta_param_names(profile_group)
#     meta_entries = create_json_profiles(profile_group, meta_names)
#     param_json_str = create_json_profiles(profile_group, param_names)

#     geolocation_json_str = create_geolocation_json_str(profile_group)

#     # Include geolocation dict into meta json string
#     meta_left_str = meta_entries.rstrip('}')
#     meta_geo_str = geolocation_json_str.lstrip('{')
#     meta_json_str = f"{meta_left_str}, {meta_geo_str}"

#     df_bgc = create_bgc_meas_df(param_json_str)
#     bgc_meas_json_str = create_bgc_meas_json_str(df_bgc)
#     measurements_json_str = create_measurements_json_str(df_bgc)

#     goship_argovis_name_mapping_json_str = create_name_mapping_json_str(
#         df_mapping)

#     # Map argovis name to reference scale
#     argovis_reference_scale_mapping_json_str = create_argovis_ref_scale_mapping_json_str(
#         df_mapping)

#     goship_reference_scale_mapping_json_str = create_goship_ref_scale_mapping_json_str(
#         df_mapping)

#     goship_argovis_unit_mapping_json_str = create_goship_argovis_unit_mapping_json_str(
#         df_mapping)

#     json_profile = {}
#     json_profile['meta'] = meta_json_str
#     json_profile['bgcMeas'] = bgc_meas_json_str
#     json_profile['measurements'] = measurements_json_str
#     json_profile['goshipArgovisNameMapping'] = goship_argovis_name_mapping_json_str
#     json_profile['goshipArgovisUnitMapping'] = goship_argovis_unit_mapping_json_str
#     json_profile['argovisRefScaleMapping'] = argovis_reference_scale_mapping_json_str
#     json_profile['goshipRefScaleMapping'] = goship_reference_scale_mapping_json_str

#     meta = json.loads(meta_json_str)
#     bgc_meas = json.loads(bgc_meas_json_str)
#     measurements = json.loads(measurements_json_str)
#     goship_argovis_name_mappping = json.loads(
#         goship_argovis_name_mapping_json_str)
#     goship_argovis_unit_mappping = json.loads(
#         goship_argovis_unit_mapping_json_str)
#     argovis_reference_scale_mapping = json.loads(
#         argovis_reference_scale_mapping_json_str)
#     goship_reference_scale_mapping = json.loads(
#         goship_reference_scale_mapping_json_str)

#     profile_dict = {}
#     profile_dict['meta'] = meta
#     profile_dict['bgc_meas'] = bgc_meas
#     profile_dict['measurements'] = measurements
#     profile_dict['goship_argovis_name_mappping'] = goship_argovis_name_mappping
#     profile_dict['goship_argovis_unit_mappping'] = goship_argovis_unit_mappping
#     profile_dict['argovis_reference_scale_mapping'] = argovis_reference_scale_mapping
#     profile_dict['goship_reference_scale_mapping'] = goship_reference_scale_mapping

#     return profile_dict, json_profile


def check_if_all_ctd_vars(data_obj):

    # Has all ctd vars if have both ctd temperature and pressure

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

    for var in vars:

        if var == 'ctd_temperature':
            has_ctd_temperature = True

        try:
            var_ref_scale = nc[var].attrs['reference_scale']
        except:
            var_ref_scale = None

        if var == 'ctd_temperature' and var_ref_scale:
            is_ctd_temp_w_refscale = True
        elif var == 'ctd_temperature' and not var_ref_scale:
            is_ctd_temp_w_refscale = False

    expocode = nc.coords['expocode'].data[0]

    if is_pres and is_ctd_temp_w_refscale:
        has_ctd_vars = True
    else:
        has_ctd_vars = False

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


# def check_if_all_ctd_vars(nc, df_mapping):

#     # Has all ctd vars if have both ctd temperature and pressure

#     logging_filename = 'logging.txt'

#     is_pres = False
#     is_ctd_temp_w_refscale = False
#     is_ctd_temp_w_no_refscale = False
#     has_ctd_vars = False

#     vars = df_mapping['name'].tolist()

#     vars = nc

#     for name in vars:

#         # From name mapping earlier, goship pressure name mapped to Argo equivalent
#         if name == 'pres':
#             is_pres = True

#         # From name mapping earlier, goship temperature name mapped to Argo equivalent
#         if name == 'temp':
#             is_ctd_temp_w_refscale = True

#     expocode = nc.coords['expocode'].data[0]

#     if not is_pres or not is_ctd_temp_w_refscale:
#         logging.info('===========')
#         logging.info('EXCEPTIONS FOUND')
#         logging.info(expocode)
#         logging.info(logging_filename)

#     if not is_pres:
#         logging.info('missing pres')
#         with open('files_no_pressure.csv', 'a') as f:
#             f.write(f"{expocode}, {logging_filename} \n")

#     # If possiblity have both a ctd temperature with and without a refscale
#     # Check what condition
#     if not is_ctd_temp_w_refscale and is_ctd_temp_w_no_refscale:
#         logging.info('CTD Temp with no ref scale')
#         # Write to file listing files without ctd variables
#         with open('files_ctd_temps_no_refscale.csv', 'a') as f:
#             f.write(f"{expocode}, {logging_filename} \n")

#     if not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale:
#         logging.info('NO CTD Temp')
#         # Write to file listing files without ctd variables
#         with open('files_no_ctd_temps.csv', 'a') as f:
#             f.write(f"{expocode}, {logging_filename} \n")

#     # TODO log these exceptions

#     # Skip making json if no expocode, no pressure or no ctd temp with ref scale
#     if not is_pres:
#         has_ctd_vars = False

#     if not is_ctd_temp_w_refscale:
#         has_ctd_vars = False

#     # elif not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale:
#     #     # files with no ctd temp
#     #     return

#     expocode = nc.coords['expocode'].data[0]

#     if expocode == 'None':
#         has_ctd_vars = False

#     # return is_pres, is_ctd_temp_w_refscale

#     return has_ctd_vars


# def check_if_all_ctd_vars(nc, df_mapping):

#     # Has all ctd vars if have both ctd temperature and pressure

#     logging_filename = 'logging.txt'

#     is_pres = False
#     is_ctd_temp_w_refscale = False
#     is_ctd_temp_w_no_refscale = False

#     has_ctd_vars = True

#     names = df_mapping['name'].tolist()

#     for name in names:

#         # From name mapping earlier, goship pressure name mapped to Argo equivalent
#         if name == 'pres':
#             is_pres = True

#         # From name mapping earlier, goship temperature name mapped to Argo equivalent
#         if name == 'temp':
#             is_ctd_temp_w_refscale = True

#     expocode = nc.coords['expocode'].data[0]

#     if not is_pres or not is_ctd_temp_w_refscale:
#         logging.info('===========')
#         logging.info('EXCEPTIONS FOUND')
#         logging.info(expocode)
#         logging.info(logging_filename)

#     if not is_pres:
#         logging.info('missing pres')
#         with open('files_no_pressure.csv', 'a') as f:
#             f.write(f"{expocode}, {logging_filename} \n")

#     # If possiblity have both a ctd temperature with and without a refscale
#     # Check what condition
#     if not is_ctd_temp_w_refscale and is_ctd_temp_w_no_refscale:
#         logging.info('CTD Temp with no ref scale')
#         # Write to file listing files without ctd variables
#         with open('files_ctd_temps_no_refscale.csv', 'a') as f:
#             f.write(f"{expocode}, {logging_filename} \n")

#     if not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale:
#         logging.info('NO CTD Temp')
#         # Write to file listing files without ctd variables
#         with open('files_no_ctd_temps.csv', 'a') as f:
#             f.write(f"{expocode}, {logging_filename} \n")

#     # TODO log these exceptions

#     # Skip making json if no expocode, no pressure or no ctd temp with ref scale
#     if not is_pres:
#         has_ctd_vars = False

#     if not is_ctd_temp_w_refscale:
#         has_ctd_vars = False

#     # elif not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale:
#     #     # files with no ctd temp
#     #     return

#     expocode = nc.coords['expocode'].data[0]

#     if expocode == 'None':
#         has_ctd_vars = False

#     # return is_pres, is_ctd_temp_w_refscale

#     return has_ctd_vars


def create_json_and_dict(data_obj):

    # Check if all ctd vars available: pressure and temperature
    nc = data_obj['nc']

    has_ctd_vars = check_if_all_ctd_vars(data_obj)

    if not has_ctd_vars:
        return {}, {}

    all_profile_dicts = []
    all_json_entries = []
    all_meta_data = []

    for nc_group in nc.groupby('N_PROF'):

        print(f"Processing profile {nc_group[0]}")

        profile_number = nc_group[0]
        profile_group = nc_group[1]

        json_profile, profile_dict = create_profile_json_and_dict(
            profile_group, data_obj)

        all_profile_dicts.append(profile_dict)
        all_json_entries.append(json_profile)

    return all_json_entries, all_profile_dicts


# def create_json_and_dict(nc, data_obj, df_mapping, filename):

#     # Consider this as way to get values out of the xarray
#     # xr_strs = nc[name].astype('str')
#     # np_arr = xr_strs.values

#     # Check if all ctd vars available: pressure and temperature
#     has_ctd_vars = check_if_all_ctd_vars(nc, df_mapping)

#     if not has_ctd_vars:
#         return

#     all_profile_dicts = []
#     all_json_entries = []

#     for nc_group in nc.groupby('N_PROF'):

#         print(f"Processing profile {nc_group[0]}")

#         profile_number = nc_group[0]
#         profile_group = nc_group[1]

#         profile_dict, json_profile = create_profile_json_and_dict(
#             profile_number, profile_group, df_mapping, data_obj, filename)

#         all_profile_dicts.append(profile_dict)
#         all_json_entries.append(json_profile)

#     return all_profile_dicts, all_json_entries


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

        # TODO: check if can set precision.
        # want the same as the orig temperature precision

        nc[var].data = new_temperature

        nc[var].attrs = 'IPT-90'

    return nc


# def convert_sea_water_temp(nc, var, goship_reference_scale):

#     # Check sea_water_temperature to be degree_Celsius and
#     # have goship_reference_scale be ITS-90

#     # So look for ref_scale = IPTS-68 or ITS-90

#     # loop through variables and look at reference scale,
#     # if it is IPTS-68 then convert

#     # Change this to work for all temperature names

#     reference_scale = np.nan

#     try:
#         temperature = nc[var].data

#         if goship_reference_scale == 'IPTS-68':

#             # Convert to ITS-90 scale
#             temperature90 = temperature/1.00024

#             # Set nc var of temp to this value

#             # TODO: check if can set precision.
#             # want the same as the orig temperature precision

#             nc[var].data = temperature90

#             reference_scale = 'IPT-90'

#         else:
#             print('temperature not IPTS-68')
#             reference_scale = np.nan

#         return nc, reference_scale

#     except KeyError:

#         return nc, reference_scale


def rename_units_to_argovis(data_obj):

    units_mapping = get_goship_argovis_unit_mapping()
    goship_salinity_ref_scale = get_goship_salniity_reference_scale()

    nc = data_obj['nc']

    coords = nc.coords
    vars = nc.keys()

    for var in coords:

        try:
            goship_unit = nc.coords[var].attrs['units']
        except KeyError:
            goship_unit = None

        # use try block because not all vars have a reference scale
        try:
            argovis_unit = units_mapping[goship_unit]
        except:
            argovis_unit = goship_unit

        if goship_unit:
            try:
                nc.coords[var].attrs['units'] = units_mapping[goship_unit]
            except:
                # No unit mapping
                pass

    for var in vars:

        try:
            goship_unit = nc[var].attrs['units']
        except KeyError:
            goship_unit = None

        # use try block because not all vars have a reference scale
        try:
            argovis_unit = units_mapping[goship_unit]
        except:
            argovis_unit = goship_unit

        # Check if salinity unit of 1
        if goship_unit == '1':
            try:
                var_goship_ref_scale = nc[var].attrs['reference_scale']

                if var_goship_ref_scale == goship_salinity_ref_scale:
                    argovis_unit = units_mapping[goship_unit]

                    nc[var].attrs['units'] = argovis_unit

            except KeyError:
                pass

        if goship_unit:
            try:
                nc[var].attrs['units'] = units_mapping[goship_unit]
            except:
                # No unit mapping
                pass

    data_obj['nc'] = nc

    return data_obj


def rename_converted_temperature(data_obj):

    nc = data_obj['nc']

    data_vars = nc.keys()

    def rename_var(var):
        new_name = var.replace('_68', '')
        return {var: new_name}

    new_name_mapping = [rename_var(var) for var in data_vars if '_68' in var]

    for name_map in new_name_mapping:
        nc.rename(name_map)

    return data_obj


def convert_goship_to_argovis_ref_scale(data_obj):

    nc = data_obj['nc']
    meta_vars = data_obj['meta']
    params = data_obj['param']

    # If argo ref scale not equal to goship ref scale, convert
    # So far, it's only the case for temperature

    argovis_ref_scale_per_type = get_argovis_reference_scale_per_type()

    for var in meta_vars:

        # Not all vars have a reference scale
        try:
            pass
        except:
            pass

    for var in params:

        # Not all vars have a reference scale
        try:
            # Get goship reference scale of var
            var_goship_ref_scale = nc[var].attrrs['reference_scale']

            if 'temperature' in var:
                argovis_ref_scale = argovis_ref_scale_per_type['temperature']
                is_same_scale = var_goship_ref_scale != argovis_ref_scale

                if argovis_ref_scale == 'IPT-90' and is_same_scale:
                    nc = convert_sea_water_temp(nc, var, var_goship_ref_scale)

        except:
            pass

    data_obj['nc'] = nc

    return data_obj


# def convert_goship_to_argovis_ref_scale_add_ref_scale(nc, df_mapping, data_obj):

#     # Create new column to hold new reference scales
#     df_mapping['reference_scale'] = df_mapping['goship_reference_scale']

#     # TODO
#     # Is it correct only looking at params and not meta?

#     for var in data_obj['param']:

#         row = df_mapping.loc[df_mapping['goship_name'] == var]

#         # new_ref_scale = row['goship_reference_scale']

#         # If argo ref scale not equal to goship ref scale, convert
#         # So far, it's only the case for temperature
#         # row = df_mapping.loc[print(
#         #     df_mapping['goship_reference_scale']) == var]

#         try:
#             goship_reference_scale = row['goship_reference_scale'].values[0]
#         except IndexError:
#             goship_reference_scale = np.nan

#         try:
#             argo_ref_scale = row['argo_reference_scale'].values[0]
#         except IndexError:
#             argo_ref_scale = np.nan

#         goship_is_nan = pd.isnull(goship_reference_scale)
#         argo_is_nan = pd.isnull(argo_ref_scale)

#         if goship_is_nan and argo_is_nan:
#             new_ref_scale = np.nan

#         elif goship_reference_scale == argo_ref_scale:
#             new_ref_scale = goship_reference_scale

#         elif (goship_reference_scale != argo_ref_scale) and not argo_is_nan and not goship_is_nan:
#             # Convert to argo ref scale
#             # then save this to add to new reference_scale column

#             # TODO: are there other ref scales to convert?

#             if argo_ref_scale == 'IPT-90' and goship_reference_scale == 'IPTS-68':
#                 # convert seawater temperature
#                 # TODO
#                 # Use C-format or precision of IPT-90 to get same precision
#                 nc, new_ref_scale = convert_sea_water_temp(nc,
#                                                            var, goship_reference_scale)

#         elif not goship_is_nan and argo_is_nan:
#             new_ref_scale = goship_reference_scale

#         df_mapping.loc[df_mapping['name'] == var,
#                        'reference_scale'] = new_ref_scale

#     return nc, df_mapping


# def rename_unit(var, df_mapping, unit_mapping):

#     # Unit mapping of names.

#     # New unit name is argo unit, but if no argo unit, use goship unit
#     # From name, get corresponding row to grab units information
#     row_index = df_mapping.index[df_mapping['name'] == var].tolist()[0]

#     argo_unit = df_mapping.loc[row_index, 'argo_unit']
#     goship_unit = df_mapping.loc[row_index, 'goship_unit']

#     try:
#         new_goship_unit = unit_mapping[goship_unit]
#     except:
#         new_goship_unit = goship_unit

#     if argo_unit == goship_unit:
#         new_unit = argo_unit
#     elif not pd.isnull(argo_unit) and not pd.isnull(new_goship_unit):
#         new_unit = argo_unit
#     elif pd.isnull(argo_unit) and not pd.isnull(new_goship_unit):
#         new_unit = new_goship_unit
#     elif not pd.isnull(argo_unit) and pd.isnull(new_goship_unit):
#         new_unit = argo_unit
#     else:
#         new_unit = argo_unit

#     return new_unit


# def create_new_units_mapping(df_mapping, argo_units_mapping_file):

#     # Mapping goship unit names to argo units
#     # Since unit of 1 for Salinity maps to psu,
#     # check reference scale to be PSS-78, otherwise
#     # leave unit as 1

#     # 3 columns
#     # goship_unit, argo_unit, reference_scale
#     df_mapping_unit_ref_scale = pd.read_csv(argo_units_mapping_file)

#     # Use df_mapping_unit_ref_scale to map goship units to argo units
#     # {'goship_unit': '1', 'argo_unit': 'psu', 'reference_scale': 'PSS-78'}
#     argo_mapping = df_mapping_unit_ref_scale.to_dict(orient='records')

#     unit_mapping = {argo_map['goship_unit']: argo_map['argo_unit']
#                     for argo_map in argo_mapping}

#     names = df_mapping['name'].tolist()

#     new_units = {}
#     for name in names:

#         new_unit = rename_unit(name, df_mapping, unit_mapping)

#         new_units[name] = new_unit

#     df_new_units = pd.DataFrame.from_dict(new_units.items())
#     df_new_units.columns = ['name', 'unit']

#     # Add name and corresponding unit mapping to df_mapping
#     df_mapping = df_mapping.merge(
#         df_new_units, how='left', left_on='name', right_on='name')

#     return df_mapping


def create_new_names_mapping(df, data_obj):

    # Look at var names to rename temperature and salinity
    # depending on goship name because there are multiple types
    params = data_obj['param']

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


# def rename_units_to_argovis(df_mapping, data_obj, argo_units_mapping_file):

#     # Add qc names since Argo names don't have any
#     # example: for argo name temp, add temp_qc if corresponding goship name has qc
#     df_mapping = add_qc_names_to_argo_names(df_mapping)

#     # Take mapping of goship to new Name and use Argo Names if exist
#     # Otherwise, use goship name
#     df_mapping = create_new_names_mapping(df_mapping, data_obj)

#     # Convert goship unit names into argo unit names
#     # Unit names rely on reference scale if unit = 1
#     df_mapping = create_new_units_mapping(df_mapping, argo_units_mapping_file)

#     # Later, compare ref scales and see if need to convert

#     return df_mapping


def get_goship_var_attributes(nc):

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

    var_attrs = {}

    var_attrs['goship_unit'] = name_to_units
    var_attrs['goship_reference_scale'] = name_to_ref_scale
    var_attrs['goship_c_format'] = name_to_c_format

    return var_attrs

# def get_goship_mapping_df(nc):

#     # Get units, reference scale and c_format from nc
#     coord_names = list(nc.coords)
#     var_names = list(nc.keys())

#     name_to_units = {}
#     name_to_ref_scale = {}
#     name_to_c_format = {}

#     for coord in coord_names:

#         try:
#             name_to_units[coord] = nc.coords[coord].attrs['units']
#         except KeyError:
#             name_to_units[coord] = np.nan

#         try:
#             name_to_ref_scale[coord] = nc.coords[coord].attrs['reference_scale']
#         except KeyError:
#             name_to_ref_scale[coord] = np.nan

#         try:
#             name_to_c_format[coord] = nc.coords[coord].attrs['C_format']
#         except KeyError:
#             name_to_c_format[coord] = np.nan

#     for var in var_names:

#         try:
#             name_to_units[var] = nc[var].attrs['units']
#         except KeyError:
#             name_to_units[var] = np.nan

#         try:
#             name_to_ref_scale[var] = nc[var].attrs['reference_scale']
#             # Replace 'unknown' with np.nan
#             if name_to_ref_scale[var] == 'unknown':
#                 name_to_ref_scale[var] = np.nan
#         except KeyError:
#             name_to_ref_scale[var] = np.nan

#         try:
#             name_to_c_format[var] = nc[var].attrs['C_format']
#         except KeyError:
#             name_to_c_format[var] = np.nan

#     df_dict = {}

#     df_dict['goship_unit'] = name_to_units
#     df_dict['goship_reference_scale'] = name_to_ref_scale
#     df_dict['goship_c_format'] = name_to_c_format

#     df = pd.DataFrame.from_dict(df_dict)
#     df.index.name = 'goship_name'
#     df = df.reset_index()

#     return df


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


def move_pressure_to_vars():
    pass


def read_file(data_obj):

    data_path = data_obj['path']
    data_url = f"https://cchdo.ucsd.edu{data_path}"

    with fsspec.open(data_url) as fobj:
        nc = xr.open_dataset(fobj)

    data_obj['nc'] = nc

    meta_names, param_names = get_meta_param_names(nc)

    data_obj['meta'] = meta_names
    data_obj['param'] = param_names

    return data_obj


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


def find_bot_ctd_file_info(file_ids):

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
        response = requests.get(query)

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
                file_info['bot_filename'] = file_path.split('/')[-1]
                file_info['bot_found'] = True

            if data_type == "ctd" and data_format == "cf_netcdf":
                file_info['ctd_id'] = file_id
                file_info['ctd_path'] = file_path
                file_info['ctd_filename'] = file_path.split('/')[-1]
                file_info['ctd_found'] = True

    return file_info


def get_all_file_ids():

    # Use api query to get all active file ids
    query = f"{API_END_POINT}/file"

    response = requests.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_files')
        exit(1)

    all_files = response.json()['files']

    all_file_ids = [file['id'] for file in all_files]

    return all_file_ids


def get_all_cruises():

    # Use api query to get all cruise id with their attached file ids

    query = f"{API_END_POINT}/cruise/all"

    response = requests.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_cruises')
        exit(1)

    all_cruises = response.json()

    return all_cruises


def get_cruise_information():

    # To get expocodes and cruise ids, Use get cruise/all to get all cruise metadata
    # and search cruises to get Go-Ship cruises expocodes and cruise ids,
    # from attached file ids, Search file metadata from doc file id, bottle file id

    # Get all cruises and active files
    all_cruises = get_all_cruises()
    all_file_ids = get_all_file_ids()

    all_cruises_info = []

    cruise_count = 0

    for cruise in all_cruises:

        bot_found = False
        ctd_found = False

        programs = cruise['collections']['programs']
        programs = [x.lower() for x in programs]

        country = cruise['country']
        expocode = cruise['expocode']

        # This cruise has both bot and ctd
        # if expocode == '318M20130321':
        #     continue

        cruise_info = {}
        cruise_info['bot'] = {}
        cruise_info['ctd'] = {}

        # Only want US GoShip
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
            file_info = find_bot_ctd_file_info(active_file_ids)

            bot_found = file_info['bot_found']
            ctd_found = file_info['ctd_found']

            # Only want cruises with both dataset bot and doc files
            if not len(file_info):
                continue

            # bot_url = https://cchdo.ucsd.edu/data/<file id>/<filename>

            cruise_info['expocode'] = cruise['expocode']
            cruise_info['cruise_id'] = cruise['id']

            if bot_found:

                bot_obj['found'] = True
                bot_obj['type'] = 'bot'
                bot_obj['path'] = file_info['bot_path']
                bot_obj['filename'] = file_info['bot_filename']

                cruise_info['bot'] = bot_obj

            else:
                bot_obj['found'] = False

            if ctd_found:
                ctd_obj['found'] = True
                ctd_obj['type'] = 'ctd'
                ctd_obj['path'] = file_info['ctd_path']
                ctd_obj['filename'] = file_info['ctd_filename']

                cruise_info['ctd'] = ctd_obj

            else:
                cruise_info['found'] = False

            cruise_count = cruise_count + 1

            # DEVELOPEMENT
            if cruise_count == 2:  # count 5 gives one bottle, count 2 gives both
                return all_cruises_info

        if cruise_info['bot'] or cruise_info['ctd']:
            cruise_info['expocode'] = expocode
            cruise_info['cruise_id'] = cruise['id']
            all_cruises_info.append(cruise_info)

        # if expocode == '33KI166_1':
        #     return all_cruises_info

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

    # s = requests.Session()
    # a = requests.adapters.HTTPAdapter(max_retries=3)
    # b = requests.adapters.HTTPAdapter(max_retries=3)

    # session.mount('http://', a)
    # session.mount('https://', b)

    all_cruises_info = get_cruise_information()

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

        if cruise_info['bot']:
            bot_found = cruise_info['bot']['found']
        else:
            bot_found = False

        if cruise_info['ctd']:
            ctd_found = cruise_info['ctd']['found']
        else:
            ctd_found = False

        nc_dict = {}
        bot_obj = {}
        ctd_obj = {}

        expocode = cruise_info['expocode']

        # filenames, nc_dict, bot_names, ctd_names = read_file(
        #     cruise_info, bot_found, ctd_found)

        if bot_found:

            print('---------------------------')
            print('Start processing bottle profiles')
            print('---------------------------')

            bot_obj = cruise_info['bot']

            bot_obj = read_file(bot_obj)

            bot_obj = convert_goship_to_argovis_ref_scale(bot_obj)

            bot_obj = rename_converted_temperature(bot_obj)

            bot_obj = rename_units_to_argovis(bot_obj)

            bot_json_entries, bot_profile_dicts = create_json_and_dict(
                bot_obj)

            print('---------------------------')
            print('Created bottle profiles')
            print('---------------------------')

        if ctd_found:

            print('---------------------------')
            print('Start processing ctd profiles')
            print('---------------------------')

            ctd_obj = cruise_info['ctd']

            ctd_obj = read_file(ctd_obj)

            ctd_obj = convert_goship_to_argovis_ref_scale(ctd_obj)

            ctd_obj = rename_converted_temperature(ctd_obj)

            ctd_obj = rename_units_to_argovis(ctd_obj)

            ctd_json_entries, ctd_profile_dicts = create_json_and_dict(
                ctd_obj)

            print('---------------------------')
            print('Created ctd profiles')
            print('---------------------------')

        if bot_found and ctd_found:
            print('processing profiles')
            combined_bot_ctd_dict = process_output(bot_obj, ctd_obj, bot_profile_dicts,
                                                   ctd_profile_dicts, json_data_directory)

            # write_profile_json(json_data_directory, combined_bot_ctd_dict)

        # elif bot_found:
        #     write_profile_json(json_data_directory, bot_profile_dict)

        # elif ctd_found:
        #     write_profile_json(json_data_directory, ctd_profile_dict)

    # nc_data_entry.close()

    logging.info(datetime.now() - start_time)


if __name__ == '__main__':
    main()
