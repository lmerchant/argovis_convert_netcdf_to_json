# Create profiles for one type

from os import stat
import pandas as pd
import numpy as np
import json
from decimal import Decimal
import logging
import re
import dask
import dask.bag as db
import dask.dataframe as dd
from dask import delayed
from dask.diagnostics import ResourceProfiler
from datetime import datetime
# from dask.diagnostics import ProgressBar
import ctypes
from collections import defaultdict
from operator import itemgetter
import itertools


import get_variable_mappings as gvm
import filter_profiles as fp
import rename_objects as rn
import get_profile_mapping_and_conversions as pm


# pbar = ProgressBar()
# pbar.register()

rprof = ResourceProfiler(dt=1)


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


def get_station_cast(nc_profile_group):

    # Error for cruise
    # ctd file
    # file_id = 18420
    # expocode = '316N154_2'
    #     cast_number = str(nc_profile_group['cast'].values)
    # TypeError: list indices must be integers or slices, not str

    # cast_number is an integer
    cast_number = str(nc_profile_group['cast'].values)

    # The station number is a string
    station = str(nc_profile_group['station'].values)

    padded_station = str(station).zfill(3)
    padded_cast = str(cast_number).zfill(3)

    station_cast = f"{padded_station}_{padded_cast}"

    return station_cast


def combine_profiles(meta_profiles, data_profiles, mapping_profiles, type):

    #  https://stackoverflow.com/questions/5501810/join-two-lists-of-dictionaries-on-a-single-key

    profile_dict = defaultdict(dict)
    for elem in itertools.chain(meta_profiles, data_profiles, mapping_profiles):
        profile_dict[elem['station_cast']].update(elem)

    all_profiles = []
    for key, val in profile_dict.items():
        new_obj = {}
        new_obj['station_cast'] = key
        val['type'] = type
        new_obj['profile_dict'] = val
        all_profiles.append(new_obj)

    return all_profiles


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


def create_measurements_list_one(df_meas):

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
    #measurements_source['flag'] = flag
    measurements_source['qc'] = 2
    measurements_source['use_ctd_temp'] = not is_ctd_temp_empty
    measurements_source['use_ctd_salinity'] = use_ctd_salinity
    if use_bottle_salinity:
        measurements_source['use_bottle_salinity'] = use_bottle_salinity

    measurements_source = fp.convert_boolean(measurements_source)

    return data_dict_list,  measurements_source


def process_one_profile_group(nc_profile, df_bgc_station_cast_all,
                              measurements_list_df_all, type):

    station_cast = get_station_cast(nc_profile)

    bgc_meas_df = df_bgc_station_cast_all.loc[station_cast]

    bgc_meas_dict_list = create_bgc_meas_list(bgc_meas_df)
    renamed_bgc_list = rn.create_renamed_list_of_objs(
        bgc_meas_dict_list, type)

    measurements_df = measurements_list_df_all.loc[station_cast]
    measurements_dict_list, measurements_source = create_measurements_list_one(
        measurements_df)

    renamed_measurements_list = rn.create_renamed_list_of_objs_argovis_measurements(
        measurements_dict_list)

    data_dict = {}
    data_dict['bgcMeas'] = renamed_bgc_list
    data_dict['measurements'] = renamed_measurements_list
    data_dict['measurementsSource'] = measurements_source
    data_dict['station_cast'] = station_cast

    return data_dict


def create_mapping_profile(nc_profile_group, type):

    station_cast = get_station_cast(nc_profile_group)

    profile_mapping = pm.get_profile_mapping(nc_profile_group, station_cast)

    meta_names, param_names = pm.get_meta_param_names(nc_profile_group)
    goship_names_list = [*meta_names, *param_names]

    mapping_dict = {}

    mapping_dict['goshipNames'] = goship_names_list
    mapping_dict['goshipArgovisNameMapping'] = gvm.create_goship_argovis_core_values_mapping(
        goship_names_list, type)
    mapping_dict['argovisReferenceScale'] = gvm.get_argovis_ref_scale_mapping(
        goship_names_list, type)

    mapping_dict['goshipReferenceScale'] = profile_mapping['goship_ref_scale']
    mapping_dict['goshipUnits'] = profile_mapping['goship_units']
    mapping_dict['goshipCformat'] = profile_mapping['goship_c_format']
    mapping_dict['goshipArgovisUnitsMapping'] = gvm.get_goship_argovis_unit_mapping()
    mapping_dict['stationCast'] = station_cast
    mapping_dict['station_cast'] = station_cast

    return mapping_dict


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


def create_meta_profiles(nc_profile_group, df_metas, file_info):

    station_cast = get_station_cast(nc_profile_group)

    # nc_profile_group = add_extra_coords(nc_profile_group, file_info)

    df_meta_station_cast = df_metas.loc[station_cast]

    meta_dict = df_meta_station_cast.to_dict('records')[0]

    geolocation_json_str = create_geolocation_json_str(nc_profile_group)
    geolocation_dict = json.loads(geolocation_json_str)
    meta_dict['geolocation'] = geolocation_dict

    renamed_meta = rn.rename_argovis_meta(meta_dict)

    meta_profile = {}
    meta_profile['station_cast'] = station_cast
    meta_profile['meta'] = renamed_meta

    return meta_profile


def create_bgc_meas_list(df):

    json_str = df.to_json(orient='records')

    # _qc":2.0
    # If tgoship_argovis_name_mapping_btl is '.0' in qc value, remove it to get an int
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

    data_dict_list = json.loads(json_str)

    return data_dict_list


def create_measurements_df_all(df_bgc_meas_all):

    # core values includes '_qc' vars
    core_values = gvm.get_goship_core_values()

    table_columns = list(df_bgc_meas_all.columns)

    core_cols = [col for col in table_columns if col in core_values]

    df_meas_all = df_bgc_meas_all[core_cols].copy()

    core_non_qc = [elem for elem in core_values if '_qc' not in elem]

    # If qc != 2, set corresponding value to np.nan
    for col in core_non_qc:
        qc_key = f"{col}_qc"

        if col == 'pressure' or col not in df_meas_all.columns:
            continue

        if qc_key not in table_columns:
            df_meas_all[col] = np.nan
            continue

        try:
            df_meas_all[col] = df_meas_all.apply(lambda x: x[col] if pd.notnull(
                x[qc_key]) and int(x[qc_key]) == 2 else np.nan, axis=1)

        except KeyError:
            pass

    # drop qc columns now that have marked non_qc column values
    for col in df_meas_all.columns:
        if '_qc' in col:
            df_meas_all = df_meas_all.drop([col], axis=1)

    # If all core values have nan, drop row
    df_meas_all = df_meas_all.dropna(how='all')

    return df_meas_all


def clean_df_param_station_cast_all(df_param_station_cast_all):

    # Count # of elems in each row and save to new column
    def count_elems(row):
        result = [False if pd.isnull(
            cell) or cell == '' or cell == 'NaT' else True for cell in row]

        # Return number of True values
        return sum(result)

    new_df = df_param_station_cast_all.apply(count_elems, axis=1).copy()

    new_df.columns = ['num_elems']

    df = pd.concat([df_param_station_cast_all, new_df], axis=1)

    df.columns = [*df.columns[:-1], 'num_elems']

    # Then drop rows where num_elems is 0
    df_param_station_cast_all = df.drop(df[df['num_elems'] == 0].index)

    # And drop num_elems column
    df_param_station_cast_all = df_param_station_cast_all.drop(columns=[
        'num_elems'])

    return df_param_station_cast_all


def create_json_profiles(nc_profile_group, names, type):

    # Do the  following to keep precision of numbers
    # If had used pandas dataframe, it would
    # have added more decimal places

    # If NaN in column, int qc becomes float
    # Will fix this later by doing a regex
    # replace to remove ".0" from qc

    station_cast = get_station_cast(nc_profile_group)

    goship_c_format_mapping = gvm.create_goship_c_format_mapping(
        nc_profile_group)

    nc_profile_group = pm.get_profile_conversions(nc_profile_group)

    coords_names = nc_profile_group.coords.keys()
    data_names = nc_profile_group.data_vars.keys()

    df = pd.DataFrame()

    json_dict = {}

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

        json_dict.update(name_dict)

    json_str = json.dumps(json_dict, default=defaultencode)

    json_dict = json.loads(json_str)

    if type == 'meta':
        df = pd.DataFrame([json_dict])
    elif type == 'param':
        df = pd.DataFrame.from_dict(json_dict)

    profile = {}
    profile['df_nc'] = df
    profile['station_cast'] = station_cast

    return profile


def create_df_profiles_one_group(nc_group, file_info, type):

    if type == 'meta':
        # add extra coords to nc_group first
        nc_group = add_extra_coords(nc_group, file_info)

    meta_names, param_names = pm.get_meta_param_names(nc_group)

    if type == 'meta':
        names = meta_names

    if type == 'param':
        names = param_names

    nc_type_profile = create_json_profiles(nc_group, names, type)

    return nc_type_profile


# def create_meta_param_profiles_df(nc_groups):

#     # b1 = db.from_sequence(nc_groups)
#     # c1 = b1.map(create_df_profiles_one_group, 'meta')

#     # b2 = db.from_sequence(nc_groups)
#     # c2 = b2.map(create_df_profiles_one_group, 'param')

#     # all_df_meta, all_df_param = dask.compute(c1, c2)

#     frames_meta = [obj['df_nc'] for obj in all_df_meta]
#     keys_meta = [obj['station_cast'] for obj in all_df_meta]
#     df_meta = pd.concat(frames_meta, keys=keys_meta)

#     frames_param = [frame['df_nc'] for frame in all_df_param]
#     keys_param = [frame['station_cast'] for frame in all_df_param]
#     df_param = pd.concat(frames_param, keys=keys_param)

#     return df_meta, df_param

def create_meta_param_profiles_df(all_df_meta, all_df_param):

    frames_meta = [obj['df_nc'] for obj in all_df_meta]
    keys_meta = [obj['station_cast'] for obj in all_df_meta]
    df_metas = pd.concat(frames_meta, keys=keys_meta)

    frames_param = [frame['df_nc'] for frame in all_df_param]
    keys_param = [frame['station_cast'] for frame in all_df_param]
    df_params = pd.concat(frames_param, keys=keys_param)

    return df_metas, df_params


def add_extra_coords(nc, file_info):

    station_cast = get_station_cast(nc)

    filename = file_info['filename']
    data_path = file_info['data_path']
    expocode = file_info['cruise_expocode']

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

    # padded_station = str(station).zfill(3)
    # padded_cast = str(cast).zfill(3)

    # _id = f"{expocode}_{padded_station}_{padded_cast}"

    _id = f"{expocode}_{station_cast}"

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


def create_profiles_one_type(data_obj):

    # TODO
    # Can I appply all this to one xarray and then apply a group?
    # If did that, when add extra coords, would also need
    # to append to make an array of static variables
    # Is it better to put into a pandas or dask dataframe
    # and then work with meta and params
    # Meta seems harder to access
    # How to get attributes? They are the same for each variable
    # pull them out at the start
    # How to do groupby with pandas/dask?
    # Do I set N_levels and N_prof as multi-index after
    # converting to a dataframe?

    type = data_obj['type']

    file_info = {}
    file_info['type'] = type
    file_info['filename'] = data_obj['filename']
    file_info['data_path'] = data_obj['data_path']
    file_info['cruise_expocode'] = data_obj['cruise_expocode']

    logging.info('---------------------------')
    logging.info(f'Start processing {type} profiles')
    logging.info('---------------------------')

    nc = data_obj['nc']

    nc_groups = [obj[1] for obj in nc.groupby('N_PROF')]

    # TODO
    # Is it better to just put in one loop?

    # Get dataframes indexed by station_cast
    b1 = db.from_sequence(nc_groups)
    c1_meta = b1.map(create_df_profiles_one_group, file_info, 'meta')

    b2 = db.from_sequence(nc_groups)
    c2_param = b2.map(create_df_profiles_one_group, file_info, 'param')

    all_df_meta, all_df_param = dask.compute(c1_meta, c2_param)

    df_metas, df_params = create_meta_param_profiles_df(
        all_df_meta, all_df_param)

    df_bgc_station_cast_all = clean_df_param_station_cast_all(df_params)

    measurements_list_df_all = create_measurements_df_all(
        df_bgc_station_cast_all)

    b1 = db.from_sequence(nc_groups)
    c1 = b1.map(create_meta_profiles, df_metas, file_info)

    b2 = db.from_sequence(nc_groups)
    c2 = b2.map(create_mapping_profile, type)

    b3 = db.from_sequence(nc_groups)
    c3 = b3.map(process_one_profile_group,
                df_bgc_station_cast_all, measurements_list_df_all, type)

    meta_profiles, mappings, data_lists = dask.compute(c1, c2, c3)

    all_profiles = combine_profiles(meta_profiles, data_lists, mappings, type)

    # # Error tracing
    # try:
    #     c2.compute()
    # except Exception as e:
    #     import pdb
    #     pdb.set_trace()
    #     print(f"error {e}")

    logging.info('---------------------------')
    logging.info(f'End processing {type} profiles')
    logging.info('---------------------------')

    return all_profiles
