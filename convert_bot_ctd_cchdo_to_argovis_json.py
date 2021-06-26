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
import logging

import get_variable_mappings as gvm
import rename_objects as rn
import check_if_has_ctd_vars as ckvar
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

Convert CCHDO CTD and bottle CF netCDF files to ArgoVis JSON format

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


def write_profile_goship_units(profile_dict, logging_dir):

    type = profile_dict['type']

    filename = 'files_goship_units.txt'
    filepath = os.path.join(logging_dir, filename)

    if type == 'btl':
        goship_units = profile_dict['goshipUnits']

    if type == 'ctd':
        goship_units = profile_dict['goshipUnits']

    if type == 'btl_ctd':
        try:
            goship_units_btl = profile_dict['goshipUnitsBtl']
            goship_units_ctd = profile_dict['goshipUnitsCtd']
            goship_units = {**goship_units_btl, **goship_units_ctd}
        except:
            goship_units = profile_dict['goshipUnits']

    with open(filepath, 'a') as f:
        json.dump(goship_units, f, indent=4,
                  sort_keys=True, default=convert)


def write_profile_json(cruise_expocode, json_dir, profile_dict):

    profile_dict.pop('stationCast', None)
    profile_dict.pop('type', None)

    # Remove  time from meta since it was just used to create date variable
    profile_dict['meta'].pop('time', None)

    # Pop off meta key and use as start of data_dict
    meta_dict = profile_dict.pop('meta', None)

    # Now combine with left over profile_dict
    data_dict = {**meta_dict, **profile_dict}

    # TODO
    # ask
    # probably use cruise expocode instead of that in file

    id = data_dict['id']

    # TODO
    # When create file id, ask if use cruise expocode instead
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

    # TODO
    # Sort keys or not?
    # with open(file, 'w') as f:
    #     json.dump(data_dict, f, indent=4, sort_keys=True, default=convert)
    # Sort keys or not?
    with open(file, 'w') as f:
        json.dump(data_dict, f, indent=4, sort_keys=False, default=convert)


def save_output(checked_ctd_variables, logging_dir, json_directory):

    for ctd_var_check in checked_ctd_variables:
        has_all_ctd_vars = ctd_var_check['has_all_ctd_vars']
        type = ctd_var_check['type']
        profile = ctd_var_check['profile_checked']
        profile_dict = profile['profile_dict']
        expocode = profile_dict['meta']['expocode']

        # Write one profile goship units to
        # keep a record of what units need to be converted
        write_goship_units = True
        if write_goship_units:
            write_goship_units = False
            write_profile_goship_units(profile_dict, logging_dir)

        if has_all_ctd_vars[type]:
            write_profile_json(
                expocode, json_directory, profile_dict)

        else:
            # Write to a file the cruise not converted
            logging.info(
                f"Cruise not converted {expocode}")
            filename = 'cruises_not_converted.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")


def save_output_btl_ctd(checked_ctd_variables, logging_dir, json_directory):

    for ctd_var_check in checked_ctd_variables:
        has_all_ctd_vars = ctd_var_check['has_all_ctd_vars']
        has_ctd_vars_no_qc = ctd_var_check['has_ctd_vars_no_qc']
        profile = ctd_var_check['profile_checked']
        profile_dict = profile['profile_dict']
        expocode = profile_dict['meta']['expocode']

        # Write one profile goship units to
        # keep a record of what units need to be converted
        write_goship_units = True
        if write_goship_units:
            write_goship_units = False
            write_profile_goship_units(profile_dict, logging_dir)

        if has_all_ctd_vars['btl'] and has_all_ctd_vars['ctd']:
            write_profile_json(
                expocode, json_directory, profile_dict)

        elif has_all_ctd_vars['btl'] and has_ctd_vars_no_qc['ctd']:
            write_profile_json(
                expocode, json_directory, profile_dict)
        elif has_ctd_vars_no_qc['btl'] and has_all_ctd_vars['ctd']:
            write_profile_json(
                expocode, json_directory, profile_dict)
        else:
            # Write to a file the cruise not converted
            logging.info(
                f"Cruise not converted {expocode}")
            filename = 'cruises_not_converted.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")


def combine_btl_ctd_measurements(btl_measurements, ctd_measurements):

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

    use_elems, flag = fp.find_measurements_hierarchy_btl_ctd(
        btl_measurements, ctd_measurements)

    combined_btl_ctd_measurements, measurements_source = fp.filter_btl_ctd_combined(
        btl_measurements, ctd_measurements, use_elems, flag)

    return combined_btl_ctd_measurements, measurements_source


def combine_output_per_profile_btl_ctd(btl_profile, ctd_profile):

    btl_meta = {}
    btl_bgc_meas = []
    btl_measurements = []
    goship_argovis_name_mapping_btl = {}
    goship_ref_scale_mapping_btl = {}
    argovis_ref_scale_btl = {}
    goship_units_btl = {}
    goship_argovis_units_btl = {}

    ctd_meta = {}
    ctd_bgc_meas = []
    ctd_measurements = []
    goship_argovis_name_mapping_ctd = {}
    goship_ref_scale_mapping_ctd = {}
    argovis_ref_scale_ctd = {}
    goship_units_ctd = {}
    goship_argovis_units_ctd = {}

    combined_btl_ctd_dict = {}

    # May have case where bot dict or ctd dict doesn't exist for same profile
    # But they have the same station_cast

    # All profiles have a profile and station_cast key but
    # profile_dict may be empty

    btl_dict = btl_profile['profile_dict']
    ctd_dict = ctd_profile['profile_dict']

    station_cast = btl_profile['station_cast']

    # For a combined dict, station_cast same for btl and ctd
    combined_btl_ctd_dict['type'] = 'btl_ctd'
    combined_btl_ctd_dict['stationCast'] = station_cast

    if btl_dict:

        btl_meta = btl_dict['meta']

        btl_bgc_meas = btl_dict['bgcMeas']
        btl_measurements = btl_dict['measurements']

        goship_argovis_name_mapping_btl = btl_dict['goshipArgovisNameMapping']

        goship_ref_scale_mapping_btl = btl_dict['goshipReferenceScale']
        argovis_ref_scale_btl = btl_dict['argovisReferenceScale']
        goship_units_btl = btl_dict['goshipUnits']
        goship_argovis_units_btl = btl_dict['goshipArgovisUnitNameMapping']

    if ctd_dict:

        ctd_meta = ctd_dict['meta']

        ctd_bgc_meas = ctd_dict['bgcMeas']
        ctd_measurements = ctd_dict['measurements']

        goship_argovis_name_mapping_ctd = ctd_dict['goshipArgovisNameMapping']

        goship_ref_scale_mapping_ctd = ctd_dict['goshipReferenceScale']
        argovis_ref_scale_ctd = ctd_dict['argovisReferenceScale']
        goship_units_ctd = ctd_dict['goshipUnits']
        goship_argovis_units_ctd = ctd_dict['goshipArgovisUnitNameMapping']

    if btl_dict and ctd_dict:

        # Put suffix of '_btl' in  bottle meta
        btl_meta = rn.rename_btl_by_key_meta(btl_meta)

        # Add extension of '_btl' to lat/lon and cast in name mapping
        new_obj = {}
        for key, val in goship_argovis_name_mapping_btl.items():
            if val == 'lat':
                new_obj[key] = 'lat_btl'
            elif val == 'lon':
                new_obj[key] = 'lon_btl'
            else:
                new_obj[key] = val

        goship_argovis_name_mapping_btl = new_obj

        # Remove _btl variables that are the same as CTD
        btl_meta.pop('expocode_btl', None)
        btl_meta.pop('cruise_url_btl', None)
        btl_meta.pop('DATA_CENTRE_btl', None)

    meta = {**ctd_meta, **btl_meta}

    bgc_meas = [*ctd_bgc_meas, *btl_bgc_meas]

    measurements, measurements_source = combine_btl_ctd_measurements(
        btl_measurements, ctd_measurements)

    goship_ref_scale_mapping = {
        **goship_ref_scale_mapping_ctd, **goship_ref_scale_mapping_btl}

    argovis_reference_scale = {
        **argovis_ref_scale_ctd, **argovis_ref_scale_btl}

    goship_argovis_units_mapping = {
        **goship_argovis_units_ctd, **goship_argovis_units_btl}

    combined_btl_ctd_dict['meta'] = meta
    combined_btl_ctd_dict['bgcMeas'] = bgc_meas
    combined_btl_ctd_dict['measurements'] = measurements
    combined_btl_ctd_dict['measurementsSource'] = measurements_source

    if goship_argovis_name_mapping_btl and goship_argovis_name_mapping_ctd:
        combined_btl_ctd_dict['goshipArgovisNameMappingBtl'] = goship_argovis_name_mapping_btl
        combined_btl_ctd_dict['goshipArgovisNameMappingCtd'] = goship_argovis_name_mapping_ctd

    elif goship_argovis_name_mapping_btl:
        combined_btl_ctd_dict['goshipArgovisNameMapping'] = goship_argovis_name_mapping_btl

    elif goship_argovis_name_mapping_ctd:
        combined_btl_ctd_dict['goshipArgovisNameMapping'] = goship_argovis_name_mapping_ctd

    combined_btl_ctd_dict['goshipReferenceScale'] = goship_ref_scale_mapping
    combined_btl_ctd_dict['argovisReferenceScale'] = argovis_reference_scale
    combined_btl_ctd_dict['goshipArgovisUnitNameMapping'] = goship_argovis_units_mapping

    if goship_units_btl and goship_units_ctd:
        combined_btl_ctd_dict['goshipUnitsBtl'] = goship_units_btl
        combined_btl_ctd_dict['goshipUnitsCtd'] = goship_units_ctd
    elif goship_units_btl:
        combined_btl_ctd_dict['goshipUnits'] = goship_units_btl
    elif goship_units_ctd:
        combined_btl_ctd_dict['goshipUnits'] = goship_units_ctd

    combined_profile = {}
    combined_profile['profile_dict'] = combined_btl_ctd_dict
    combined_profile['station_cast'] = station_cast

    return combined_profile


def get_same_station_cast_profile_btl_ctd(btl_profiles, ctd_profiles):

    station_casts_btl = [btl_profile['station_cast']
                         for btl_profile in btl_profiles]

    station_casts_ctd = [ctd_profile['station_cast']
                         for ctd_profile in ctd_profiles]

    different_pairs_in_btl = set(
        station_casts_btl).difference(station_casts_ctd)
    different_pairs_in_ctd = set(
        station_casts_ctd).difference(station_casts_btl)

    for pair in different_pairs_in_btl:
        # Create matching but empty profiles for ctd
        # Create new profile number and same key
        # increment on the  last profile #
        #ctd_profiles[pair] = index + 1
        new_profile = {}
        new_profile['profile_dict'] = {}
        new_profile['station_cast'] = pair
        ctd_profiles.append(new_profile)

    for pair in different_pairs_in_ctd:
        # Create matching but empty profiles for ctd
        # Create new profile number and same key
        # increment on the  last profile #
        #ctd_profiles[pair] = index + 1
        new_profile = {}
        new_profile['profile_dict'] = {}
        new_profile['station_cast'] = pair
        btl_profiles.append(new_profile)

    return btl_profiles, ctd_profiles


def get_station_cast_profile(profiles):

    num_profiles = range(len(profiles))

    station_casts = [profile['station_cast'] for profile in profiles]

    # Create a dictionary with tuple as key and profile num as value
    station_cast_profile = dict(zip(station_casts, num_profiles))

    return station_cast_profile


def combine_btl_ctd_profiles(btl_profiles, ctd_profiles):

    # Get profile dicts so have the same number of profiles
    # one may be blank while the other exists at a cast
    btl_profiles, ctd_profiles = get_same_station_cast_profile_btl_ctd(
        btl_profiles, ctd_profiles)

    #  bottle  and ctd have same keys, but  different values
    # which are the profile numbers

    profiles_list_btl_ctd = []

    # The number of station_casts are the same for btl and ctd
    station_casts = [btl_profile['station_cast']
                     for btl_profile in btl_profiles]

    for station_cast in station_casts:

        try:
            profile_dict_btl = [btl_profile['profile_dict']
                                for btl_profile in btl_profiles if btl_profile['station_cast'] == station_cast][0]

        except:
            profile_dict_btl = {}

        profile_btl = {}
        profile_btl['station_cast'] = station_cast
        profile_btl['profile_dict'] = profile_dict_btl

        try:
            profile_dict_ctd = [ctd_profile['profile_dict']
                                for ctd_profile in ctd_profiles if ctd_profile['station_cast'] == station_cast][0]

        except:
            profile_dict_ctd = {}

        profile_ctd = {}
        profile_ctd['station_cast'] = station_cast
        profile_ctd['profile_dict'] = profile_dict_ctd

        combined_profile_btl_ctd = combine_output_per_profile_btl_ctd(
            profile_btl, profile_ctd)

        profiles_list_btl_ctd.append(combined_profile_btl_ctd)

    return profiles_list_btl_ctd


def create_measurements_list(df_bgc_meas):

    df_meas = pd.DataFrame()

    # core values includes '_qc' vars
    core_values = gvm.get_goship_core_values()

    table_columns = list(df_bgc_meas.columns)

    core_cols = [col for col in table_columns if col in core_values]

    df_meas = df_bgc_meas[core_cols].copy()

    core_non_qc = [elem for elem in core_values if '_qc' not in elem]

    # If qc != 2, set corresponding value to np.nan
    for col in core_non_qc:
        qc_key = f"{col}_qc"

        if col == 'pressure' or col not in df_meas.columns:
            continue

        if qc_key not in table_columns:
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

    # See if using ctd_salinty or bottle_salinity
    # Is ctd_salinity NaN? If it is and bottle_salinity isn't NaN, use it
    # See if all ctd_salinity are NaN, if not, use it and
    # drop bottle salinity column
    is_ctd_sal_empty = True
    is_ctd_sal_column = 'ctd_salinity' in df_meas.columns
    if is_ctd_sal_column:
        is_ctd_sal_empty = df_meas['ctd_salinity'].isnull().all()

    is_bottle_sal_empty = True
    is_bottle_sal_column = 'bottle_salinity' in df_meas.columns
    if is_bottle_sal_column:
        # Check if not all NaN
        is_bottle_sal_empty = df_meas['bottle_salinity'].isnull().all()

    if is_ctd_sal_column and not is_ctd_sal_empty and is_bottle_sal_column:
        use_ctd_salinity = True
        use_bottle_salinity = False
        df_meas = df_meas.drop(['bottle_salinity'], axis=1)
    elif is_ctd_sal_column and not is_ctd_sal_empty and not is_bottle_sal_column:
        use_ctd_salinity = True
        use_bottle_salinity = False
    elif is_ctd_sal_column and is_ctd_sal_empty and is_bottle_sal_column and not is_bottle_sal_empty:
        use_ctd_salinity = False
        use_bottle_salinity = True
        df_meas = df_meas.drop(['ctd_salinity'], axis=1)
    elif not is_ctd_sal_column and is_bottle_sal_column and not is_bottle_sal_empty:
        use_ctd_salinity = False
        use_bottle_salinity = True
    else:
        use_ctd_salinity = False
        use_bottle_salinity = False

    ctd_temp_cols = ['ctd_temperature', 'ctd_temperature_68']
    is_ctd_temp_col = any(
        [True if col in df_meas.columns else False for col in ctd_temp_cols])

    is_ctd_temp_empty = True
    if is_ctd_temp_col:
        is_ctd_temp_empty = bool(
            next(df_meas[col].isnull().all() for col in df_meas.columns))

    if is_ctd_temp_empty:
        flag = None
    elif not is_ctd_temp_empty and use_ctd_salinity and not use_bottle_salinity:
        flag = 'CTD'
    elif not is_ctd_temp_empty and not use_ctd_salinity and use_bottle_salinity:
        flag = 'BTL'
    elif not is_ctd_temp_empty and not use_ctd_salinity and not use_bottle_salinity:
        flag = 'CTD'
    else:
        flag = None

    json_str = df_meas.to_json(orient='records')

    data_dict_list = json.loads(json_str)

    measurements_source = {}
    measurements_source['flag'] = flag
    measurements_source['qc'] = 2
    measurements_source['use_ctd_temp'] = not is_ctd_temp_empty
    measurements_source['use_ctd_salinity'] = use_ctd_salinity
    if use_bottle_salinity:
        measurements_source['use_bottle_salinity'] = use_bottle_salinity

    measurements_source = fp.convert_boolean(measurements_source)

    return data_dict_list,  measurements_source


def create_bgc_meas_df(param_dict):

    # Now split up param_json_str into multiple json dicts
    # And then only keep those that have a value not null for each key
    #param_json_dict = json.loads(param_json_str)

    # Read in as a list of dicts. If non null dict, put in a list
    # TODO
    # include everything but get rid of rows all NaN
    try:
        df = pd.DataFrame.from_dict(param_dict)
    except ValueError:
        df = pd.DataFrame.from_dict([param_dict])

    # objs = df.to_dict('records')

    # new_list = []

    # for obj in objs:

    #     obj_len = len(obj)
    #     null_count = 0

    #     for key, val in obj.items():
    #         if pd.isnull(val) or val == '' or val == 'NaT':
    #             null_count = null_count + 1

    #     if null_count != obj_len:
    #         new_list.append(obj)

    # df = pd.DataFrame.from_records(new_list)

    df = df.dropna(how='all')
    df = df.reset_index(drop=True)

    return df


def create_bgc_meas_list(df):

    json_str = df.to_json(orient='records')

    # _qc":2.0
    # If tgoship_argovis_name_mapping_btl is '.0' in qc value, remove it to get an int
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

    data_dict = json.loads(json_str)

    return data_dict


def apply_c_format(json_str, name, c_format):

    number_dict = json.loads(json_str)

    number_obj = number_dict[name]

    # Now get str in C_format. e.g. "%9.1f"
    # print(f'{val:.2f}') where val is a #
    f_format = c_format.lstrip('%')

    new_obj = {}

    if isinstance(number_obj, float):
        new_val = float(f"{number_obj:{f_format}}")
        new_obj[name] = new_val
        json_str = json.dumps(new_obj)
        return json_str
    elif isinstance(number_obj, list):
        new_val = [float(f"{item:{f_format}}") for item in number_obj]
        new_obj[name] = new_val
        json_str = json.dumps(new_obj)
        return json_str
    else:
        return json_str


def create_json_profiles(nc_profile_group, names, data_obj):

    # Do the  following to keep precision of numbers
    # If had used pandas dataframe, it would
    # have added more decimal places

    # If NaN in column, int qc becomes float
    # Will fix this later by doing a regex
    # replace to remove ".0" from qc

    coords_names = nc_profile_group.coords.keys()
    data_names = nc_profile_group.data_vars.keys()

    goship_c_format_mapping = data_obj['goship_c_format']

    profile_list = []

    for name in names:

        try:
            c_format = goship_c_format_mapping[name]
        except:
            c_format = None

        is_int = False
        is_float = False

        float_types = ['float64', 'float32']
        int_types = ['int8', 'int64']

        if name in coords_names:

            try:
                var = nc_profile_group.coords[name]

                if var.dtype in float_types:
                    vals = var.astype('str').values
                    is_float = True
                elif var.dtype in int_types:
                    vals = var.astype('str').values
                    is_int = True
                else:
                    vals = var.astype('str').values

            except Exception as e:
                print("inside create_json_profiles for coords names")
                print(e)

        if name in data_names:

            try:
                var = nc_profile_group.data_vars[name]

                if var.dtype in float_types:
                    vals = var.astype('str').values
                    is_float = True
                elif var.dtype in int_types:
                    vals = var.astype('str').values
                    is_int = True
                else:
                    vals = var.astype('str').values

            except Exception as e:
                print("inside create_json_profiles for data names")
                print(e)

        if vals.size == 1:

            val = vals.item(0)

            if is_float:
                result = Decimal(val)
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)

                if c_format and 'f' in c_format:
                    json_str = apply_c_format(json_str, name, c_format)

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

                if c_format and 'f' in c_format:
                    json_str = apply_c_format(json_str, name, c_format)

            elif is_int:
                result = [int(x) for x in vals]
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)
            else:
                result = vals.tolist()
                name_dict = {name: result}
                json_str = json.dumps(name_dict)

        #json_profile[name] = json_str
        profile_list.append(json.loads(json_str))

    profiles = {}
    for profile in profile_list:
        profiles.update(profile)

    return profiles

    # json_profiles = ''

    # for profile in json_profile.values():
    #     profile = profile.lstrip('{')
    #     bare_profile = profile.rstrip('}')

    #     json_profiles = json_profiles + ', ' + bare_profile

    # # Strip off starting ', ' of string
    # json_profiles = '{' + json_profiles.strip(', ') + '}'

    # return json_profiles


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


def create_meta_dict(profile_group, meta_names, data_obj):

    meta_dict = create_json_profiles(profile_group, meta_names, data_obj)

    geolocation_json_str = create_geolocation_json_str(
        profile_group)

    geolocation_dict = json.loads(geolocation_json_str)

    meta_dict.update(geolocation_dict)

    # # Include geolocation dict into meta json string
    # meta_left_str = meta_json_str.rstrip('}')
    # meta_geo_str = geolocation_json_str.lstrip('{')
    # meta_json_str = f"{meta_left_str}, {meta_geo_str}"

    #meta_dict = json.loads(meta_json_str)

    return meta_dict


def add_extra_coords(nc, data_obj):

    station = nc['station'].values
    cast = nc['cast'].values
    filename = data_obj['filename']
    data_path = data_obj['data_path']
    expocode = data_obj['cruise_expocode']

    # Use cruise expocode because file one could be different than cruise page
    # Drop existing expocode
    nc = nc.reset_coords(names=['expocode'], drop=True)

    if '/' in expocode:
        expocode = expocode.replace('/', '_')
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"
    elif expocode == 'None':
        logging.info(filename)
        logging.info('expocode is None')
        cruise_url = ''
    else:
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

    # TODO
    # for data path, need to add https://cchdo.ucsd.edu in front of it
    data_url = f"https://cchdo.ucsd.edu{data_path}"

    new_coords = {}

    padded_station = str(station).zfill(3)
    padded_cast = str(cast).zfill(3)

    _id = f"{expocode}_{padded_station}_{padded_cast}"

    new_coords['_id'] = _id
    new_coords['id'] = _id

    new_coords['POSITIONING_SYSTEM'] = 'GPS'
    new_coords['DATA_CENTRE'] = 'CCHDO'
    new_coords['cruise_url'] = cruise_url
    new_coords['netcdf_url'] = data_url

    new_coords['data_filename'] = filename

    new_coords['expocode'] = expocode

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


def create_profile(nc_profile_group, data_obj):

    # don't rename variables yet

    cast_number = str(nc_profile_group['cast'].values)

    # The station number is a string
    station = str(nc_profile_group['station'].values)

    station_cast = f"{station}_{cast_number}"

    nc_profile_group = add_extra_coords(nc_profile_group, data_obj)

    # Get meta names  again now that added extra coords
    meta_names, param_names = get_meta_param_names(nc_profile_group)

    meta_dict = create_meta_dict(nc_profile_group, meta_names, data_obj)

    goship_c_format_mapping_dict = data_obj['goship_c_format']

    param_dict = create_json_profiles(nc_profile_group, param_names, data_obj)

    df_bgc = create_bgc_meas_df(param_dict)

    # param_json = create_json_profiles(nc_profile_group, param_names, data_obj)
    # df_bgc = create_bgc_meas_df(param_json)

    bgc_meas_dict_list = create_bgc_meas_list(df_bgc)

    measurements_dict_list, measurements_source = create_measurements_list(
        df_bgc)

    goship_units_dict = data_obj['goship_units']

    goship_ref_scale_mapping_dict = data_obj['goship_ref_scale']

    goship_names_list = [*meta_names, *param_names]

    # Save meta separate for renaming later
    profile_dict = {}
    profile_dict['station_cast'] = station_cast
    profile_dict['meta'] = meta_dict
    profile_dict['bgc_meas'] = bgc_meas_dict_list
    profile_dict['measurements'] = measurements_dict_list
    profile_dict['measurements_source'] = measurements_source
    profile_dict['goship_ref_scale'] = goship_ref_scale_mapping_dict
    profile_dict['goship_units'] = goship_units_dict
    profile_dict['goship_c_format'] = goship_c_format_mapping_dict
    profile_dict['goship_names'] = goship_names_list

    output_profile = {}
    output_profile['profile_dict'] = profile_dict
    output_profile['station_cast'] = station_cast

    return output_profile


def create_profiles(data_obj):

    nc = data_obj['nc']

    type = data_obj['type']

    all_profiles_list = []

    for nc_group in nc.groupby('N_PROF'):

        logging.info(f"Processing {type} profile {nc_group[0] + 1}")

        profile_number = nc_group[0]
        nc_profile_group = nc_group[1]

        data_obj['profile_number'] = profile_number

        profile = create_profile(nc_profile_group, data_obj)

        all_profiles_list.append(profile)

    return all_profiles_list


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

    #  TODO
    # Why not find btm_depth? Not finding it for units mapping

    # It is  N_PROF dimension only. Maybe need to look
    # for a size  not N_LEVELS
    # Does it find cast and station for meta names?

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

    data_path = data_obj['data_path']

    nc = xr.open_dataset(data_path)

    data_obj['nc'] = nc

    file_expocode = nc.coords['expocode'].data[0]

    meta_names, param_names = get_meta_param_names(nc)

    data_obj['meta'] = meta_names
    data_obj['param'] = param_names

    data_obj['file_expocode'] = file_expocode

    return data_obj


def read_file(data_obj):

    flag = ''

    data_path = data_obj['data_path']

    data_url = f"https://cchdo.ucsd.edu{data_path}"

    try:
        with fsspec.open(data_url) as fobj:
            nc = xr.open_dataset(fobj)
    except:
        logging.warning(f"Error reading in file {data_url}")
        flag = 'error'
        return data_obj, flag

    data_obj['nc'] = nc

    meta_names, param_names = get_meta_param_names(nc)

    data_obj['meta'] = meta_names
    data_obj['param'] = param_names

    file_expocode = nc.coords['expocode'].data[0]

    data_obj['file_expocode'] = file_expocode

    return data_obj, flag


def process_ctd(ctd_obj):

    print('---------------------------')
    print('Start processing ctd profiles')
    print('---------------------------')

    ctd_obj = gvm.create_goship_unit_mapping(ctd_obj)
    ctd_obj = gvm.create_goship_ref_scale_mapping(ctd_obj)

    # get c-format (string representation of numbers)
    ctd_obj = gvm.create_goship_c_format_mapping(ctd_obj)

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

    ctd_profiles = create_profiles(ctd_obj)

    # Rename with _ctd suffix unless it is an Argovis variable
    # But no _ctd suffix to meta data
    renamed_ctd_profiles = rn.rename_profiles_to_argovis(ctd_profiles, 'ctd')

    print('---------------------------')
    print('Processed ctd profiles')
    print('---------------------------')

    return renamed_ctd_profiles


def process_bottle(btl_obj):

    print('---------------------------')
    print('Start processing bottle profiles')
    print('---------------------------')

    btl_obj = gvm.create_goship_unit_mapping(btl_obj)
    btl_obj = gvm.create_goship_ref_scale_mapping(btl_obj)

    # get c-format (string representation of numbers)
    btl_obj = gvm.create_goship_c_format_mapping(btl_obj)

    # Only converting temperature so far
    btl_obj = convert_goship_to_argovis_ref_scale(btl_obj)

    # Add convert units function

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    btl_profiles = create_profiles(btl_obj)

    # Rename with _btl suffix unless it is an Argovis variable
    # But no _btl suffix to meta data
    # Add _btl when combine files
    renamed_btl_profiles = rn.rename_profiles_to_argovis(btl_profiles, 'btl')

    print('---------------------------')
    print('Processed btl profiles')
    print('---------------------------')

    return renamed_btl_profiles


def setup_test_obj(dir, filename, type):

    if type == 'btl':
        btl_obj = {}
        btl_obj['found'] = True
        btl_obj['type'] = 'btl'
        btl_obj['data_path'] = os.path.join(dir, filename)
        btl_obj['filename'] = filename

        return btl_obj

    if type == 'ctd':
        ctd_obj = {}
        ctd_obj['found'] = True
        ctd_obj['type'] = 'ctd'
        ctd_obj['data_path'] = os.path.join(dir, filename)
        ctd_obj['filename'] = filename

        return ctd_obj


def setup_testing(btl_file, ctd_file, test_btl, test_ctd):

    # TODO Set  up start and end date for logging

    input_dir = './testing_data/modify_data_for_testing'
    output_dir = './testing_output'
    os.makedirs(output_dir, exist_ok=True)

    btl_obj = {}
    ctd_obj = {}
    btl_obj['found'] = False
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
    if test_btl:
        btl_obj = setup_test_obj(input_dir, btl_file, 'btl')

        logging.info("======================\n")
        logging.info(f"Processing btl test file {btl_file}")

    if test_ctd:
        ctd_obj = setup_test_obj(input_dir, ctd_file, 'ctd')

        logging.info("======================\n")
        logging.info(f"Processing ctd test file {ctd_file}")

    return output_dir, all_cruises_info, btl_obj, ctd_obj


def setup_logging(clear_old_logs):

    logging_dir = './logging'
    os.makedirs(logging_dir, exist_ok=True)

    if clear_old_logs:
        remove_file('output.log', logging_dir)
        remove_file('file_read_errors.txt', logging_dir)
        remove_file('found_goship_units.txt', logging_dir)
        remove_file('cruises_no_core_ctd_vars.txt', logging_dir)
        remove_file('cruises_w_ctd_temp_no_qc.txt', logging_dir)
        remove_file('cruises_no_ctd_temp_w_ref_scale.txt', logging_dir)
        remove_file('cruises_no_ctd_temp.txt', logging_dir)
        remove_file('cruises_no_expocode.txt', logging_dir)
        remove_file('cruises_no_pressure.txt', logging_dir)
        remove_file('found_cruises_with_coords_netcdf.txt', logging_dir)
        remove_file('diff_cruise_and_file_expocodes.txt', logging_dir)
        remove_file('cruises_no_ctd_temp_w_ref_scale.txt', logging_dir)
        remove_file('cruises_not_converted.txt', logging_dir)

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

    start_datetime = datetime(start_year, 1, 1)
    end_datetime = datetime(end_year, 12, 31)

    logging.info(f"Converting years {start_year} to {end_year}")

    # overwrite logs or append them for next run
    clear_old_logs = True

    return start_datetime, end_datetime, clear_old_logs


def main():

    # First create a list of the cruises found
    # with netCDF files

    start_datetime, end_datetime, clear_old_logs = get_user_input()

    program_start_time = datetime.now()

    logging_dir, logging = setup_logging(clear_old_logs)

    session = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount('https://', a)

    json_directory = './converted_data'
    os.makedirs(json_directory, exist_ok=True)

    # TESTING
    testing = False
    test_btl_obj = {}
    test_ctd_obj = {}

    if testing:

        # Change this to do bot and ctd separate or combined
        test_btl = False
        test_ctd = True

        btl_file = 'modified_318M20130321_bottle_no_psal.nc'
        ctd_file = 'modified_318M20130321_ctd_core_bad_flag.nc'

        json_directory, all_cruises_info, test_btl_obj, test_ctd_obj = setup_testing(
            btl_file, ctd_file, test_btl, test_ctd)

        btl_found = test_btl_obj['found']
        ctd_found = test_ctd_obj['found']

    else:
        # Loop through all cruises and grap NetCDF files
        all_cruises_info = gi.get_cruise_information(
            session, logging_dir, start_datetime, end_datetime)

        if not all_cruises_info:
            logging.info('No cruises within dates selected')
            exit(1)

    read_error_count = 0
    data_file_read_errors = []

    for cruise_info in all_cruises_info:

        if not testing:

            cruise_expocode = cruise_info['expocode']

            logging.info(f"Start converting Cruise: {cruise_expocode}")

            btl_found = cruise_info['btl']['found']
            ctd_found = cruise_info['ctd']['found']

            cruise_info['btl']['cruise_expocode'] = cruise_expocode
            cruise_info['ctd']['cruise_expocode'] = cruise_expocode

        if btl_found:

            if testing:
                btl_obj = read_file_test(test_btl_obj)

            else:
                btl_obj = cruise_info['btl']

                btl_obj, flag = read_file(btl_obj)
                if flag == 'error':
                    print("Error reading file")
                    read_error_count = read_error_count + 1
                    data_url = f"https://cchdo.ucsd.edu{btl_obj['data_path']}"
                    data_file_read_errors.append(data_url)
                    continue

            renamed_btl_profiles = process_bottle(btl_obj)

        if ctd_found:

            if testing:
                ctd_obj = read_file_test(test_ctd_obj)

            else:
                ctd_obj = cruise_info['ctd']

                ctd_obj, flag = read_file(ctd_obj)
                if flag == 'error':
                    print("Error reading file")
                    read_error_count = read_error_count + 1
                    data_url = f"https://cchdo.ucsd.edu{ctd_obj['data_path']}"
                    data_file_read_errors.append(data_url)
                    continue

            renamed_ctd_profiles = process_ctd(ctd_obj)

        if btl_found and ctd_found:

            # filter measurements when combine btl and ctd profiles
            combined_btl_ctd_profiles = combine_btl_ctd_profiles(
                renamed_btl_profiles, renamed_ctd_profiles)

            logging.info('---------------------------')
            logging.info('Processed btl and ctd combined profiles')
            logging.info('---------------------------')

            checked_ctd_variables = ckvar.check_of_ctd_variables(
                combined_btl_ctd_profiles, logging, logging_dir)

            save_output_btl_ctd(checked_ctd_variables,
                                logging_dir, json_directory)

        elif btl_found:
            renamed_btl_profiles = fp.filter_measurements(
                renamed_btl_profiles, 'btl')

            checked_ctd_variables = ckvar.check_of_ctd_variables(
                renamed_btl_profiles, logging, logging_dir)

            save_output(checked_ctd_variables, logging_dir, json_directory)

        elif ctd_found:
            renamed_ctd_profiles = fp.filter_measurements(
                renamed_ctd_profiles, 'ctd')

            checked_ctd_variables = ckvar.check_of_ctd_variables(
                renamed_ctd_profiles, logging, logging_dir)

            save_output(checked_ctd_variables, logging_dir, json_directory)

        if btl_found or ctd_found:

            if btl_found:
                file_expocode = btl_obj['file_expocode']
                cruise_expocode = btl_obj['cruise_expocode']

            if ctd_found:
                file_expocode = ctd_obj['file_expocode']
                cruise_expocode = ctd_obj['cruise_expocode']

            # Write the following to a file to keep track of when
            # the cruise expocode is different from the file expocode
            if cruise_expocode != file_expocode:
                filename = 'diff_cruise_and_file_expocodes.txt'
                filepath = os.path.join(logging_dir, filename)
                with open(filepath, 'a') as f:
                    f.write(
                        f"Cruise: {cruise_expocode} File: {file_expocode}\n")

            # TODO
            # could turn info a  function and also
            # check if file expocode exists and has
            # different ship, country, and expocode
            # Would need cruise id to find cruise json

            logging.info('---------------------------')
            logging.info(
                f"Finished for cruise {cruise_expocode}")
            logging.info(f"Expocode inside file: {file_expocode}")
            logging.info('---------------------------')

            logging.info("*****************************\n")

    if read_error_count:
        filename = 'file_read_errors.txt'
        logging.warning(f"Errors reading in {read_error_count} files")
        logging.warning(f"See {filename} for files")

        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'w') as f:
            for line in data_file_read_errors:
                f.write(f"{line}\n")

    logging.info("Time to run program")
    logging.info(datetime.now() - program_start_time)


if __name__ == '__main__':
    main()
