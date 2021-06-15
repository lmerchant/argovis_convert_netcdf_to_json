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


def write_profile_json(json_dir, profile_dict):

    # Pop off meta key and use as start of data_dict
    meta_dict = profile_dict.pop('meta', None)

    # Now combine with left over profile_dict
    data_dict = {**meta_dict, **profile_dict}

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable

    id = data_dict['id']
    filename = f"{id}.json"
    expocode = data_dict['expocode']

    json_str = json.dumps(data_dict)

    # TODO
    # check did this earlier in program
    # _qc":2.0
    # If tgoship_argovis_name_mapping_bot is '.0' in qc value, remove it to get an int
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

    folder_name = expocode

    path = os.path.join(json_dir, folder_name)

    if not os.path.exists(path):
        os.makedirs(path)

    file = os.path.join(json_dir, folder_name, filename)

    # TODO Remove formatting when final
    with open(file, 'w') as f:
        json.dump(data_dict, f, indent=4, sort_keys=True, default=convert)


def write_profiles_json(json_dir, profile_dicts):

    # Write profile_dict to as json to a profile file

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable

    for profile_dict in profile_dicts:
        write_profile_json(json_dir, profile_dict)


def get_goship_salniity_reference_scale():

    return {
        'goship_ref_scale': 'PSS-78'
    }


def get_argovis_reference_scale_per_type():

    return {
        'temperature': 'ITS-90',
        'salinity': 'PSS-78'
    }


def get_argovis_reference_scale():

    return {
        'ctd_temperature': 'ITS-90',
        'ctd_salinity': 'PSS-78'
    }


def get_goship_argovis_unit_mapping():

    return {
        'dbar': 'decibar',
        '1': 'psu',
        'degC': 'Celsius',
        'umol/kg': 'micromole/kg',
        'meters': 'meters'
    }


# def get_goship_argovis_name_mapping_meta_bot():

#     return {
#         'latitude': 'lat',
#         'longitude': 'lon'
#     }


def get_goship_argovis_name_mapping_bot():

    return {
        'pressure': 'pres',
        'ctd_salinity': 'psal_btl',
        'ctd_temperature': 'temp_btl',
        'ctd_oxygen': 'doxy_btl',
        'bottle_salinity':  'salinity_btl',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def get_goship_argovis_name_mapping_ctd():

    return {
        'pressure': 'pres',
        'ctd_salinity': 'psal_ctd',
        'ctd_temperature': 'temp_ctd',
        'ctd_oxygen': 'doxy_ctd',
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
            'bottle_salinity':  'salinity_btl',
            'latitude': 'lat',
            'longitude': 'lon'
            }


def get_core_values_mapping_ctd():
    return {'pressure': 'pres',
            'ctd_temperature': 'temp_ctd',
            'ctd_salinity': 'psal_ctd',
            'ctd_oxygen': 'doxy_ctd',
            'latitude': 'lat',
            'longitude': 'lon'}


def get_goship_units(data_obj):

    nc = data_obj['nc']
    meta = data_obj['meta']
    params = data_obj['param']

    new_obj = {}

    for var in params:

        # Not all vars have units
        try:
            # Get goship units of var
            var_goship_unit = nc[var].attrrs['units']
            new_obj[var] = var_goship_unit
        except:
            pass

    for var in meta:

        # Not all vars have units
        try:
            # Get goship units of var
            var_goship_unit = nc.coords[var].attrrs['units']
            new_obj[var] = var_goship_unit
        except:
            pass

    return new_obj


def create_renamed_list_of_objs_argovis(cur_list, type):

    # Common names are pres, temp, psal, and doxy will have suffix _ctd and _ctd_qc
    # All ctd vars will have suffix _ctd and _ctd_qc
    # All bot vars will have suffx _btl and _btl_qc
    # Special case is bottle_salinity to psal_btl

    # Creating list, don't modify key since already renamed as element  of list

    # here

    if type == 'bot':

        new_list = []

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta_argovis(obj, type)

            new_list.append(new_obj)

    if type == 'ctd':

        new_list = []

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta_argovis(obj, type)

            new_list.append(new_obj)

    return new_list


def create_renamed_list_of_objs(cur_list, type):

    new_list = []

    if type == 'bot':

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta(obj, 'bot')

            new_list.append(new_obj)

    if type == 'ctd':

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta(obj, 'ctd')

            new_list.append(new_obj)

    return new_list


def rename_argovis_value_meta(obj):

  # For mapping cases goship_argovis_name_mapping_bot goship is the key
  # and argovis is the value
    core_values_mapping = get_argovis_meta_mapping()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key in obj:

        if key in core_values:
            new_val = core_values_mapping[key]

        else:
            new_val = f"{key}"

        new_obj[key] = new_val

    return new_obj


def rename_value_not_meta(obj, type):

  # For mapping cases wgoship_argovis_name_mapping_bot goship is the key
  # and argovis is the value

    if type == 'bot':

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

    if type == 'ctd':

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


def rename_argovis_not_meta(obj, type):

    if type == 'bot':

        core_values_mapping = get_core_values_mapping_bot()

        core_values = [core for core in core_values_mapping]

        new_obj = {}

        for key, val in obj.items():

            if key in core_values:
                new_key = core_values_mapping[key]

            else:
                new_key = key

            new_obj[new_key] = val

    if type == 'ctd':

        core_values_mapping = get_core_values_mapping_ctd()

        core_values = [core for core in core_values_mapping]

        new_obj = {}

        for key, val in obj.items():

            if key in core_values:
                new_key = core_values_mapping[key]

            else:
                new_key = key

            new_obj[new_key] = val

    return new_obj


def rename_bot_by_key_meta(obj):

    # Alread renamed lat  and lon
    # Want to add _btl extension to all

    new_obj = {}

    for key, val in obj.items():

        new_key = f"{key}_btl"

        new_obj[new_key] = val

    return new_obj


def rename_key_not_meta(obj, type):

    if type == 'bot':

        core_values_mapping = get_goship_argovis_name_mapping_bot()

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

    if type == 'ctd':

        core_values_mapping = get_goship_argovis_name_mapping_ctd()

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


def rename_key_not_meta_argovis(obj, type):

    if type == 'bot':

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

            else:
                new_key = val

            new_obj[new_key] = val

    if type == 'ctd':

        core_values_mapping = get_core_values_mapping_ctd()
        core_value = [core for core in core_values_mapping]

        new_obj = {}

        for key, val in obj.items():

            if '_qc' in key:
                check_core = key.replace('_qc', '')
                if check_core in core_value:
                    new_key = f"{core_values_mapping[check_core]}_qc"

            elif key in core_value:
                new_key = core_values_mapping[key]

            else:
                new_key = val

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
            new_key = f"{key}"

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


def rename_output_per_profile(profile_dict, type):

    if type == 'bot':

        # Make a new object because will be using it next if combine profiles
        # but do rename lat and lon
        meta = profile_dict['meta']
        renamed_meta = rename_argovis_meta(meta)

        # TODO. Do I still need a deep copy?
        meta_copy = copy.deepcopy(meta)

        measurements_list = profile_dict['measurements']
        bgc_list = profile_dict['bgc_meas']
        name_mapping = profile_dict['goship_argovis_name_mappping']

        renamed_bgc_list = create_renamed_list_of_objs(bgc_list, 'bot')

        renamed_measurements_list = {}

        renamed_measurements_list = create_renamed_list_of_objs_argovis(
            measurements_list, 'bot')

        # TODO
        # Should this meta renaming have btl suffix?

        # name_mapping_meta = rename_argovis_value_meta(names)
        # name_mapping_param = rename_value_not_meta(names, 'bot')
        # name_mapping = {**name_mapping_param, **name_mapping_meta}

        argovis_ref_scale_mapping = profile_dict['argovis_ref_scale_mapping']

    if type == 'ctd':

        # Make a new object because will be using it next if combine profiles
        # but do rename lat and lon
        meta = profile_dict['meta']
        meta_copy = copy.deepcopy(meta)

        renamed_meta = rename_argovis_meta(meta)

        measurements_list = profile_dict['measurements']
        bgc_list = profile_dict['bgc_meas']
        name_mapping = profile_dict['goship_argovis_name_mappping']

        renamed_bgc_list = create_renamed_list_of_objs(bgc_list, 'ctd')

        #renamed_measurements_list = []
        renamed_measurements_list = create_renamed_list_of_objs_argovis(
            measurements_list, 'ctd')

        # Ask if want extension to ctd variables like bot has
        # even when single

        # Don't need to do this since created a mapping dictionary
        # of goship to argovis names including meta and param names
        #name_mapping = rename_ctd_value_not_meta(names)

        # name_mapping_meta = rename_argovis_value_meta(names)
        # name_mapping_param = rename_value_not_meta(names, 'ctd')
        # name_mapping = {**name_mapping_param, **name_mapping_meta}

        argovis_ref_scale_mapping = profile_dict['argovis_ref_scale_mapping']

    goship_ref_scale_mappping = profile_dict['goship_ref_scale_mapping']
    goship_argovis_unit_mapping = profile_dict['goship_argovis_unit_mappping']

    # don't rename meta yet incase not both bot and ctd combined

    renamed_profile_dict = {}
    renamed_profile_dict['meta'] = renamed_meta
    renamed_profile_dict['measurements'] = renamed_measurements_list
    renamed_profile_dict['bgcMeas'] = renamed_bgc_list
    renamed_profile_dict['goshipArgovisNameMapping'] = name_mapping
    renamed_profile_dict['goshipReferenceScale'] = goship_ref_scale_mappping
    renamed_profile_dict['argovisReferenceScale'] = argovis_ref_scale_mapping
    renamed_profile_dict['goshipArgovisUnitsMapping'] = goship_argovis_unit_mapping

    return renamed_profile_dict


def combine_output_per_profile_bot_ctd(bot_renamed_dict, ctd_renamed_dict, bot_obj, ctd_obj):

    bot_meta = bot_renamed_dict['meta']
    ctd_meta = ctd_renamed_dict['meta']

    # Rename meta which has already had latitude and longitude changed
    bot_renamed_meta = rename_bot_by_key_meta(bot_meta)

    meta = {**ctd_meta, **bot_renamed_meta}

    # Remove expocode_btl
    meta.pop('expocode_btl', None)

    # Remove cruise_url_btl
    meta.pop('cruise_url_btl', None)

    bot_bgc_meas = bot_renamed_dict['bgcMeas']
    ctd_bgc_meas = ctd_renamed_dict['bgcMeas']

    bgc_meas = [*ctd_bgc_meas, *bot_bgc_meas]

    bot_measurements = bot_renamed_dict['measurements']
    ctd_measurements = ctd_renamed_dict['measurements']

    measurement = [*ctd_measurements, *bot_measurements]

    goship_argovis_name_mapping_bot = get_goship_argovis_name_mapping_bot()
    goship_argovis_name_mapping_ctd = get_goship_argovis_name_mapping_ctd()

    goship_ref_scale_mapping_bot = create_goship_ref_scale_mapping_dict(
        bot_obj)
    goship_ref_scale_mapping_ctd = create_goship_ref_scale_mapping_dict(
        ctd_obj)

    goship_ref_scale_mapping = {
        **goship_ref_scale_mapping_ctd, **goship_ref_scale_mapping_bot}

    argovis_ref_scale_ctd = ctd_renamed_dict['argovisReferenceScale']
    argovis_ref_scale_bot = bot_renamed_dict['argovisReferenceScale']

    argovis_reference_scale = {
        **argovis_ref_scale_ctd, **argovis_ref_scale_bot}

    goship_argovis_units_mapping = {
        **ctd_renamed_dict['goshipArgovisUnitsMapping'], **bot_renamed_dict['goshipArgovisUnitsMapping']}

    combined_bot_ctd_dict = {}
    combined_bot_ctd_dict['meta'] = meta
    combined_bot_ctd_dict['measurements'] = measurement
    combined_bot_ctd_dict['bgcMeas'] = bgc_meas
    combined_bot_ctd_dict['goshipArgovisNameMappingBtl'] = goship_argovis_name_mapping_bot
    combined_bot_ctd_dict['goshipArgovisNameMappingCtd'] = goship_argovis_name_mapping_ctd
    combined_bot_ctd_dict['goshipReferenceScale'] = goship_ref_scale_mapping
    combined_bot_ctd_dict['argovisReferenceScale'] = argovis_reference_scale
    combined_bot_ctd_dict['goshipArgovisUnitsMapping'] = goship_argovis_units_mapping

    return combined_bot_ctd_dict


def rename_profile_dicts_to_argovis(data_obj, profile_dicts):

    # TODO
    # add following flags
    # isGOSHIPctd = true
    # isGOSHIPbottle = true
    # core_info = 1  # is ctd
    # core_info = 2  # is bottle (no ctd)
    # core_info = 12  # is ctd and tgoship_argovis_name_mapping_bot is bottle too (edited)

    type = data_obj['type']

    num_profiles = len(profile_dicts)

    profile_dicts_list = []

    for profile_number in range(num_profiles):

        profile_dict = profile_dicts[profile_number]

        processed_profile_dict = rename_output_per_profile(profile_dict, type)

        profile_dicts_list.append(processed_profile_dict)

    return profile_dicts_list


def combine_profile_dicts_bot_ctd(bot_obj, ctd_obj, bot_profile_dicts, ctd_profile_dicts):

    # TODO
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
    #  Is tgoship_argovis_name_mapping_bot a case for this?
    # if bot_found and ctd_found:
    if bot_num_profiles != ctd_num_profiles:
        print(
            f"bot profiles {bot_num_profiles} and ctd profiles {ctd_num_profiles} are different")

    profile_dicts_list_bot_ctd = []

    for profile_number in range(num_profiles):

        bot_profile_dict = bot_profile_dicts[profile_number]
        ctd_profile_dict = ctd_profile_dicts[profile_number]

        # TODO Remove if bot and ctd since checked that befor running functiion
        # Check
        # Already renamed bot and ctd in preceding if statements
        # only need to rename  bot for meta

        combined_profile_dict_bot_ctd = combine_output_per_profile_bot_ctd(
            bot_profile_dict, ctd_profile_dict, bot_obj, ctd_obj)

        profile_dicts_list_bot_ctd.append(combined_profile_dict_bot_ctd)

        # combined_profile_dict_bot_ctd = combine_output_per_profile_bot_ctd(
        #     renamed_profile_dict_bot, renamed_profile_dict_ctd, bot_obj, ctd_obj)

        # profile_dicts_list_bot_ctd.append(combined_profile_dict_bot_ctd)

        # if bot_found:

        #     bot_profile_dict = bot_profile_dicts[profile_number]

        #     renamed_profile_dict_bot = rename_output_per_profile(
        #         bot_profile_dict, 'bot')

        #     profile_dicts_list_bot.append(renamed_profile_dict_bot)

        # if ctd_found:

        #     ctd_profile_dict = ctd_profile_dicts[profile_number]

        #     renamed_profile_dict_ctd = rename_output_per_profile(
        #         ctd_profile_dict, 'ctd')

        #     profile_dicts_list_ctd.append(renamed_profile_dict_ctd)

        # if bot_found and ctd_found:

        #     combined_profile_dict_bot_ctd = combine_output_per_profile_bot_ctd(
        #         renamed_profile_dict_bot, renamed_profile_dict_ctd, bot_obj, ctd_obj)

        #     profile_dicts_list_bot_ctd.append(combined_profile_dict_bot_ctd)

    return profile_dicts_list_bot_ctd


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


def get_argovis_ref_scale_mapping_dict():

    return {
        'temp_ctd': 'ITS-90',
        'psal_ctd': 'PSS-78'
    }


def get_argovis_ref_scale_mapping_dict_bot():

    return {
        'temp_btl': 'ITS-90',
        'psal_btl': 'PSS-78',
        'salinity_btl': 'PSS-78'
    }


def create_measurements_dict_list(df_bgc_meas):

    # TODO
    # QUESTION
    # what if goship_argovis_name_mapping_bot is no qc flag value, set as NaN

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

    # TODO
    # test with values not qc = 2

    # Keep qc = 2
    # Mark those without as np.nan  so can drop rows with all nan
    def mark_not_qc_2(val):
        if pd.notnull(val) and int(val) == 2:
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

    # drop qc columns
    for col in df_meas.columns:
        if '_qc' in col:
            df_meas = df_meas.drop([col], axis=1)

    # Replace '' with nan to filter on nan
    # TODO
    # Do I need this?
    # df_meas = df_meas.replace(r'^\s*$', np.nan, regex=True)

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


def create_bgc_meas_dict_list(df):

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

    # # Then remove time from nc
    # nc.drop('time')

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

    # Remove geometry_container from profile_group

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

    # don't rename yet

    # Create json without translating to dataframe first

    # Consider this as way to get values out of the xarray
    # xr_strs = profile_group[name].astype('str')
    # np_arr = xr_strs.values

    # profile_group = profile_group.rename(name_mapping_dict)

    profile_group = add_extra_coords(profile_group, data_obj)

    meta_names, param_names = get_meta_param_names(profile_group)

    meta_dict = create_meta_dict(profile_group, meta_names)

    param_json = create_json_profiles(profile_group, param_names)

    # TODO
    # PROBABLY DELETE CREATING JSON FIRST

    # When to rename
    # probably after create dict

    df_bgc = create_bgc_meas_df(param_json)

    # rename if bot  vs ctd or rename as a list?
    bgc_meas_dict_list = create_bgc_meas_dict_list(df_bgc)

    measurements_dict = create_measurements_dict_list(df_bgc)

    type = data_obj['type']

    if type == 'bot':

        goship_argovis_name_mapping_dict = get_goship_argovis_name_mapping_bot()
        argovis_ref_scale_mapping_dict = get_argovis_ref_scale_mapping_dict_bot()

    elif type == 'ctd':

        goship_argovis_name_mapping_dict = get_goship_argovis_name_mapping_ctd()
        argovis_ref_scale_mapping_dict = get_argovis_ref_scale_mapping_dict()

    goship_argovis_unit_mapping_dict = get_goship_argovis_unit_mapping()

    goship_ref_scale_mapping_dict = create_goship_ref_scale_mapping_dict(
        data_obj)

    # TODO
    # do  I need to do this now that not creating profile obj
    meta_copy = copy.deepcopy(meta_dict)

    # Remove  time from meta
    meta_copy.pop('time', None)

    # Save meta separate for renaming later
    profile_dict = {}
    profile_dict['meta'] = meta_copy
    profile_dict['bgc_meas'] = bgc_meas_dict_list
    profile_dict['measurements'] = measurements_dict

    # TODO
    # move mapping information later when do renaming
    profile_dict['goship_argovis_name_mappping'] = goship_argovis_name_mapping_dict
    profile_dict['goship_argovis_unit_mappping'] = goship_argovis_unit_mapping_dict
    profile_dict['argovis_ref_scale_mapping'] = argovis_ref_scale_mapping_dict
    profile_dict['goship_ref_scale_mapping'] = goship_ref_scale_mapping_dict

    return profile_dict


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


def create_profile_dicts(data_obj):

    # Check if all ctd vars available: pressure and temperature
    nc = data_obj['nc']

    type = data_obj['type']

    has_ctd_vars = check_if_all_ctd_vars(data_obj)

    if not has_ctd_vars:
        return {}, {}

    all_profile_dicts = []

    for nc_group in nc.groupby('N_PROF'):

        print(f"Processing {type} profile {nc_group[0] + 1}")

        profile_number = nc_group[0]
        profile_group = nc_group[1]

        profile_dict = create_profile_dict(profile_group, data_obj)

        all_profile_dicts.append(profile_dict)

    return all_profile_dicts


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


def create_new_names_mapping(df, data_obj):

    # Look at var names to rename temperature and salinity
    # depending on goship name because tgoship_argovis_name_mapping_bot are multiple types
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
    df['name'] = np.wgoship_argovis_name_mapping_bot(df['argovis_name'].isna(),
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


def get_argo_mapping_df(argo_name_mapping_file):

    df = pd.read_csv(argo_name_mapping_file)

    # Only want part of the csv file
    df = df[['argovis_name', 'argo_unit',
             'argo_reference_scale', 'goship_name']].copy()

    return df


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

    # Remove names
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

            cruise_count = cruise_count + 1

            # DEVELOPEMENT
            if cruise_count == 2:  # count 5 gives one bottle, count 2 gives both
                return all_cruises_info

        if cruise_info['bot'] or cruise_info['ctd']:
            cruise_info['expocode'] = expocode
            cruise_info['cruise_id'] = cruise['id']
            all_cruises_info.append(cruise_info)

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
    json_directory = './data/same_expocode_json'

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

        print("======================\n")
        print(f"Processing {expocode}")

        # # Process folder to combine bot and ctd without
        # # caring about setting new coords and vals
        # if nc_folder.is_dir():

        # filenames, nc_dict, bot_names, ctd_names = process_folder(
        #     nc_folder)

        bot_profile_dicts = []
        ctd_profile_dicts = []

        if cruise_info['bot']:
            bot_found = cruise_info['bot']['found']
        else:
            bot_found = False

        if cruise_info['ctd']:
            ctd_found = cruise_info['ctd']['found']
        else:
            ctd_found = False

        bot_obj = {}
        ctd_obj = {}
        bot_obj['found'] = False
        ctd_obj['found'] = False

        bot_profile_dicts = []
        ctd_profile_dicts = []

        expocode = cruise_info['expocode']

        # filenames, nc_dict, bot_names, ctd_names = read_file(
        #     cruise_info, bot_found, ctd_found)

        # TODO
        # remove
        # bot_found = False
        #ctd_found = False

        if bot_found:

            print('---------------------------')
            print('Start processing bottle profiles')
            print('---------------------------')

            bot_obj = cruise_info['bot']

            bot_obj = read_file(bot_obj)

            bot_obj = convert_goship_to_argovis_ref_scale(bot_obj)

            bot_obj = rename_converted_temperature(bot_obj)

            bot_obj = rename_units_to_argovis(bot_obj)

            bot_profile_dicts = create_profile_dicts(bot_obj)

            # Rename with _btl suffix unless it is an Argovis variable
            # But no _btl suffix to meta data
            renamed_bot_profile_dicts = rename_profile_dicts_to_argovis(
                bot_obj, bot_profile_dicts)

        if ctd_found:

            print('---------------------------')
            print('Start processing ctd profiles')
            print('---------------------------')

            ctd_obj = cruise_info['ctd']

            ctd_obj = read_file(ctd_obj)

            ctd_obj = convert_goship_to_argovis_ref_scale(ctd_obj)

            ctd_obj = rename_converted_temperature(ctd_obj)

            ctd_obj = rename_units_to_argovis(ctd_obj)

            ctd_profile_dicts = create_profile_dicts(ctd_obj)

            # Rename with _ctd suffix unless it is an Argovis variable
            # But no _ctd suffix to meta data
            renamed_ctd_profile_dicts = rename_profile_dicts_to_argovis(
                ctd_obj, ctd_profile_dicts)

        if bot_found and ctd_found:

            # Combine and add _btl suffix to meta variables
            combined_bot_ctd_dicts = combine_profile_dicts_bot_ctd(
                bot_obj, ctd_obj, renamed_bot_profile_dicts, renamed_ctd_profile_dicts)

            print('---------------------------')
            print('Processed bot and ctd combined profiles')
            print('---------------------------')

        if bot_found and ctd_found:
            write_profiles_json(json_directory, combined_bot_ctd_dicts)

        elif bot_found:
            write_profiles_json(json_directory, renamed_bot_profile_dicts)

        elif ctd_found:
            write_profiles_json(json_directory, renamed_ctd_profile_dicts)

        print('---------------------------')
        print(f"All profiles written to files for cruise {expocode}")
        print('---------------------------')

        print("*****************************\n")

        exit(1)

    logging.info(datetime.now() - start_time)


if __name__ == '__main__':
    main()
