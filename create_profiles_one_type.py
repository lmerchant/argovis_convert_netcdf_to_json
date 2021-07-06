# Create profiles for one type

from os import stat
from dask.core import keys_in_tasks

from pandas.core.arrays import boolean
import xarray as xr
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
from collections import defaultdict
from operator import itemgetter
import itertools
from decimal import Decimal
import operator


import get_variable_mappings as gvm
import filter_profiles as fp
import rename_objects as rn
import get_profile_mapping_and_conversions as pm


# pbar = ProgressBar()
# pbar.register()

# https://stackoverflow.com/questions/40659212/futurewarning-elementwise-comparison-failed-returning-scalar-but-in-the-futur
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

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


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


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


def create_df_profiles_one_group(nc_group, type):

    meta_names, param_names = pm.get_meta_param_names(nc_group)

    if type == 'meta':
        names = meta_names

    if type == 'param':
        names = param_names

    nc_type_profile = create_json_profiles(nc_group, names, type)

    return nc_type_profile


# def add_extra_coords(nc, file_info):

#     station_cast = get_station_cast(nc)

#     filename = file_info['filename']
#     data_path = file_info['data_path']
#     expocode = file_info['cruise_expocode']

#     # Use cruise expocode because file one could be different than cruise page
#     # Drop existing expocode
#     nc = nc.reset_coords(names=['expocode'], drop=True)

#     if '/' in expocode:
#         expocode = expocode.replace('/', '_')
#         cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"
#     elif expocode == 'None':
#         logging.info(filename)
#         logging.info('expocode is None')
#         cruise_url = ''
#     else:
#         cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

#     # TODO
#     # for data path, need to add https://cchdo.ucsd.edu in front of it
#     data_url = f"https://cchdo.ucsd.edu{data_path}"

#     new_coords = {}

#     _id = f"{expocode}_{station_cast}"

#     new_coords['_id'] = _id
#     new_coords['id'] = _id
#     new_coords['expocode'] = expocode

#     new_coords['POSITIONING_SYSTEM'] = 'GPS'
#     new_coords['DATA_CENTRE'] = 'CCHDO'
#     new_coords['cruise_url'] = cruise_url
#     new_coords['netcdf_url'] = data_url
#     new_coords['data_filename'] = filename

#     datetime64 = nc['time'].values
#     date = pd.to_datetime(datetime64)

#     new_coords['date_formatted'] = date.strftime("%Y-%m-%d")

#     # Create date coordiinate and convert date to iso
#     new_coords['date'] = date.isoformat()

#     latitude = nc['latitude'].values
#     longitude = nc['longitude'].values

#     roundLat = np.round(latitude, 3)
#     roundLon = np.round(longitude, 3)

#     strLat = f"{roundLat} N"
#     strLon = f"{roundLon} E"

#     new_coords['strLat'] = strLat
#     new_coords['strLon'] = strLon

#     new_coords['roundLat'] = roundLat
#     new_coords['roundLon'] = roundLon

#     nc = nc.assign_coords(new_coords)

#     return nc


def remove_rows(nc):

    # Once remove variables like profile that exist
    # in all array values, can create array that
    # holds Boolean values of whether all array
    # values at a Level are empty
    # And then remove these in Pandas
    # Can I use np.where?

    # Want to compare arrays for null values
    # Since arrays could be of different
    # length, create one padded with False and length N_LEVELS

    nc_length = nc.dims['N_LEVELS']

    # padded_array = np.pad(array, (0, width), mode='constant', constant_values=False)
    # where array is the boolean array and width is nc_length - boolean_length

    vars = nc.keys()

    first_array = True

    print(nc.sizes)

    all_vars = []

    for var in vars:

        print(var)

        is_null = np.isnan(nc[var])

        is_empty_str = operator.eq(nc[var], '')

        is_nat = operator.eq(nc[var], 'NaT')

        is_empty_array = np.logical_or.reduce(
            (is_null, is_empty_str, is_nat))

        # padded_array = np.pad(array, (0, width), mode='constant', constant_values=False)
        # where array is the boolean array and width is nc_length - boolean_length
        padded_length = nc_length - np.size(is_empty_array)

        padded_array = np.pad(
            is_empty_array, (0, padded_length), mode='constant', constant_values=True)

        all_vars.append(padded_array)

    not_empty_arr = np.logical_not(np.logical_and.reduce(all_vars))

    nc['not_empty'] = (['N_LEVELS'], not_empty_arr)

    # Now trim each array where boolean True
    nc = nc.where(nc['not_empty'], drop=True)

    nc = nc.drop('not_empty')

    return nc


def combine_profiles(meta_profiles, bgc_profiles, meas_profiles, meas_source_profiles, mapping_dict, type):

    #  https://stackoverflow.com/questions/5501810/join-two-lists-of-dictionaries-on-a-single-key

    profile_dict = defaultdict(dict)
    for elem in itertools.chain(meta_profiles, bgc_profiles,  meas_profiles, meas_source_profiles):
        profile_dict[elem['station_cast']].update(elem)

    all_profiles = []
    for key, val in profile_dict.items():
        new_obj = {}
        new_obj['station_cast'] = key
        val['type'] = type
        # Insert a mapping dict
        val = {**val, **mapping_dict}
        # Add stationCast to the dict itself
        val['stationCast'] = key
        new_obj['profile_dict'] = val
        all_profiles.append(new_obj)

    return all_profiles

# second


def apply_c_format_to_num(name, num, dtype_mapping, c_format_mapping):

    # Now get str in C_format. e.g. "%9.1f"
    # dtype = mapping['dtype'][name]
    # c_format = mapping['c_format'][name]

    float_types = ['float64', 'float32']

    try:

        dtype = dtype_mapping[name]
        c_format = c_format_mapping[name]

        if dtype in float_types and c_format:
            f_format = c_format.lstrip('%')
            return float(f"{num:{f_format}}")

        else:
            return num

    except:
        return num


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


# def create_measurements_list_one(df_meas):

#     # See if using ctd_salinty or bottle_salinity
#     # Is ctd_salinity NaN? If it is and bottle_salinity isn't NaN, use it
#     # See if all ctd_salinity are NaN, if not, use it and
#     # drop bottle salinity column
#     is_ctd_sal_empty = True
#     is_ctd_sal_column = 'ctd_salinity' in df_meas.columns
#     if is_ctd_sal_column:
#         is_ctd_sal_empty = df_meas['ctd_salinity'].isnull().all()

#     is_bottle_sal_empty = True
#     is_bottle_sal_column = 'bottle_salinity' in df_meas.columns
#     if is_bottle_sal_column:
#         # Check if not all NaN
#         is_bottle_sal_empty = df_meas['bottle_salinity'].isnull().all()

#     if is_ctd_sal_column and not is_ctd_sal_empty and is_bottle_sal_column:
#         use_ctd_salinity = True
#         use_bottle_salinity = False
#         df_meas = df_meas.drop(['bottle_salinity'], axis=1)
#     elif is_ctd_sal_column and not is_ctd_sal_empty and not is_bottle_sal_column:
#         use_ctd_salinity = True
#         use_bottle_salinity = False
#     elif is_ctd_sal_column and is_ctd_sal_empty and is_bottle_sal_column and not is_bottle_sal_empty:
#         use_ctd_salinity = False
#         use_bottle_salinity = True
#         df_meas = df_meas.drop(['ctd_salinity'], axis=1)
#     elif not is_ctd_sal_column and is_bottle_sal_column and not is_bottle_sal_empty:
#         use_ctd_salinity = False
#         use_bottle_salinity = True
#     else:
#         use_ctd_salinity = False
#         use_bottle_salinity = False

#     ctd_temp_cols = ['ctd_temperature', 'ctd_temperature_68']
#     is_ctd_temp_col = any(
#         [True if col in df_meas.columns else False for col in ctd_temp_cols])

#     is_ctd_temp_empty = True
#     if is_ctd_temp_col:
#         is_ctd_temp_empty = bool(
#             next(df_meas[col].isnull().all() for col in df_meas.columns))

#     if is_ctd_temp_empty:
#         flag = None
#     elif not is_ctd_temp_empty and use_ctd_salinity and not use_bottle_salinity:
#         flag = 'CTD'
#     elif not is_ctd_temp_empty and not use_ctd_salinity and use_bottle_salinity:
#         flag = 'BTL'
#     elif not is_ctd_temp_empty and not use_ctd_salinity and not use_bottle_salinity:
#         flag = 'CTD'
#     else:
#         flag = None

#     json_str = df_meas.to_json(orient='records')

#     data_dict_list = json.loads(json_str)

#     measurements_source = {}
#     # measurements_source['flag'] = flag
#     measurements_source['qc'] = 2
#     measurements_source['use_ctd_temp'] = not is_ctd_temp_empty
#     measurements_source['use_ctd_salinity'] = use_ctd_salinity
#     if use_bottle_salinity:
#         measurements_source['use_bottle_salinity'] = use_bottle_salinity

#     measurements_source = fp.convert_boolean(measurements_source)

#     return data_dict_list,  measurements_source


# def process_one_profile_group(nc_profile, df_bgc_station_cast_all,
#                               measurements_list_df_all, type):

#     station_cast = get_station_cast(nc_profile)

#     bgc_meas_df = df_bgc_station_cast_all.loc[station_cast]

#     bgc_meas_dict_list = create_bgc_meas_list(bgc_meas_df)
#     renamed_bgc_list = rn.create_renamed_list_of_objs(
#         bgc_meas_dict_list, type)

#     measurements_df = measurements_list_df_all.loc[station_cast]
#     measurements_dict_list, measurements_source = create_measurements_list_one(
#         measurements_df)

#     renamed_measurements_list = rn.create_renamed_list_of_objs_argovis_measurements(
#         measurements_dict_list)

#     data_dict = {}
#     data_dict['bgcMeas'] = renamed_bgc_list
#     data_dict['measurements'] = renamed_measurements_list
#     data_dict['measurementsSource'] = measurements_source
#     data_dict['station_cast'] = station_cast

#     return data_dict


# def create_mapping_profile(nc_profile_group, type):

#     station_cast = get_station_cast(nc_profile_group)

#     profile_mapping = pm.get_profile_mapping(nc_profile_group, station_cast)

#     meta_names, param_names = pm.get_meta_param_names(nc_profile_group)
#     goship_names_list = [*meta_names, *param_names]

#     mapping_dict = {}

#     mapping_dict['goshipNames'] = goship_names_list
#     mapping_dict['goshipArgovisNameMapping'] = gvm.create_goship_argovis_core_values_mapping(
#         goship_names_list, type)
#     mapping_dict['argovisReferenceScale'] = gvm.get_argovis_ref_scale_mapping(
#         goship_names_list, type)

#     mapping_dict['goshipReferenceScale'] = profile_mapping['goship_ref_scale']
#     mapping_dict['goshipUnits'] = profile_mapping['goship_units']
#     mapping_dict['goshipCformat'] = profile_mapping['goship_c_format']
#     mapping_dict['goshipArgovisUnitsMapping'] = gvm.get_goship_argovis_unit_mapping()
#     mapping_dict['stationCast'] = station_cast
#     mapping_dict['station_cast'] = station_cast

#     return mapping_dict


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

    add_extra_coords(nc_profile_group, file_info)

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


def create_measurements_df_all2(df,  type):

    # core values includes '_qc' vars
    core_values = gvm.get_argovis_core_values_per_type(type)
    table_columns = list(df.columns)
    core_cols = [col for col in table_columns if col in core_values]

    core_non_qc = [elem for elem in core_cols if '_qc' not in elem]

    df_meas = df[core_cols].copy()

    # here

    def check_qc(row):
        if pd.notnull(row[1]) and int(row[1]) == 0:
            return row[0]
        elif pd.notnull(row[1]) and int(row[1]) == 2:
            return row[0]
        else:
            return np.nan

    # If qc != 2, set corresponding value to np.nan
    for col in core_non_qc:

        qc_key = f"{col}_qc"

        try:
            df_meas[col] = df_meas[[col, qc_key]].apply(
                check_qc, axis=1)
        except:
            pass

    # drop qc columns now that have marked non_qc column values
    for col in df_meas.columns:
        if '_qc' in col:
            df_meas = df_meas.drop([col], axis=1)

    # If all core values have nan, drop row
    # This won't work since
    df_meas = df_meas.dropna(how='all')

    df_meas = df_meas.sort_values(by=['pres'])

    # Remove type ('btl', 'ctd') from  variable names
    column_mapping = {}
    column_mapping[f"psal_{type}"] = 'psal'
    column_mapping[f"temp_{type}"] = 'temp'
    column_mapping[f"salinity_btl"] = 'salinity'

    df_meas = df_meas.rename(columns=column_mapping)

    return df_meas


def create_measurements_df_all(df):

    # core values includes '_qc' vars
    core_values = gvm.get_goship_core_values()
    table_columns = list(df.columns)
    core_cols = [col for col in table_columns if col in core_values]
    core_non_qc = [elem for elem in core_cols if '_qc' not in elem]

    df_meas = df[core_cols].copy()

    def check_qc(row):

        if pd.notnull(row[1]) and int(row[1]) == 0:
            return row[0]
        elif pd.notnull(row[1]) and int(row[1]) == 2:
            return row[0]
        else:
            return np.nan

    # If qc != 2, set corresponding value to np.nan
    for col in core_non_qc:

        qc_key = f"{col}_qc"

        meta_dask = df_meas[col].dtype
        try:
            df_meas[col] = df_meas[[col, qc_key]].apply(
                check_qc, axis=1, meta=meta_dask)
        except:
            pass

    # drop qc columns now that have marked non_qc column values
    for col in df_meas.columns:
        if '_qc' in col:
            df_meas = df_meas.drop([col], axis=1)

    # If all core values have nan, drop row
    # This won't work since
    df_meas = df_meas.dropna(how='all')

    return df_meas


# def clean_df_param_station_cast_all(df_param_station_cast_all):

#     # Count # of elems in each row and save to new column
#     def count_elems(row):
#         result = [False if pd.isnull(
#             cell) or cell == '' or cell == 'NaT' else True for cell in row]

#         # Return number of True values
#         return sum(result)

#     new_df = df_param_station_cast_all.apply(count_elems, axis=1).copy()

#     new_df.columns = ['num_elems']

#     df = pd.concat([df_param_station_cast_all, new_df], axis=1)

#     df.columns = [*df.columns[:-1], 'num_elems']

#     # Then drop rows where num_elems is 0
#     df_param_station_cast_all = df.drop(df[df['num_elems'] == 0].index)

#     # And drop num_elems column
#     df_param_station_cast_all = df_param_station_cast_all.drop(columns=[
#         'num_elems'])

#     return df_param_station_cast_all


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


def remove_empty_rows5(df):

    # TODO
    # use np.where to speed it up

    # n[89]: cond1 = df.Close > df.Close.shift(1)
    # # In[90]: cond2 = df.High < 12
    # # In[91]: df['New_Close'] = np.where(cond1, 'A', 'B')

    # df.loc[df['B'].isin(['one','three'])]

    # a = df.loc[df[cols].isin(['', np.nan, 'NaT'])]

    # df['test'] = pd.np.where(df[['col1', 'col2', 'col3']].eq('Y').any(1, skipna=True), 'Y',
    #                          pd.np.where(df[['col1', 'col2', 'col3']].isnull().all(1), None, 'N'))

    # a = pd.np.where(df[cols].isnull().all(1), None, False)

    # df['test'] = pd.np.where(df[cols].eq('').any(1), 'N', 'Y',
    #                          pd.np.where(df[cols].isnull().any(1), 'N', 'Y'),
    #                          pd.np.where(df[cols].eq('NaT').any(1), 'N', 'Y'))

    # df['empty'] = pd.np.where(df[cols].eq(
    #     '').any(1, skipna=True), 'Y', 'N')

    # df['empty'] = pd.np.where(df[cols].eq(
    #     'NaT').any(1, skipna=True), 'Y', df['empty'])

    # df['empty'] = pd.np.where(df[cols].isnull().any(
    #     1, skipna=True), 'Y', df['empty'])

   # Remove station cast for now since
    # it is filled
    # Need original order for dask compute  meta
    df = orig_cols = df.columns

    station_cast_col = df['station_cast']
    df = df.drop(['station_cast'], axis=1)

    cols = df.columns

    df['string'] = np.where(df[cols].eq(
        '').any(1), False, True)

    df['time_check'] = np.where(df[cols].eq(
        'NaT').any(1), False, True)

    df['null'] = np.where(df[cols].isnull().any(1), False, True)

    df["num_elems"] = df[['null', 'string', 'time_check']].sum(axis=1)

    df = df.join(station_cast_col)

    # Then drop rows where num_elems is 0
    df = df.drop(df[df['num_elems'] == 0].index)

    # # And drop num_elems column
    df = df.drop(columns=['null', 'string', 'time_check', 'num_elems'])

    df = df[orig_cols]

    return df

    # df = df.drop(df[df['empty'] == 'Y'].index)

    # # And drop num_elems column
    # df = df.drop(columns=['empty'])

    # print(df)
    # exit(1)

#     # # Count # of elems in each row and save to new column
#     # def count_elems(row):
#     #     result = [False if pd.isnull(
#     #         cell) or cell == '' or cell == 'NaT' else True for cell in row]

#     #     # Return number of True values
#     #     return sum(result)

#     # station_cast_col = df['station_cast']

#     # df = df.drop(['station_cast'], axis=1)

#     # new_df = df.apply(count_elems, axis=1).copy()

#     # new_df.columns = ['num_elems']

#     # df = pd.concat([df, new_df], axis=1)

#     # df.columns = [*df.columns[:-1], 'num_elems']

#     # df = df.join(station_cast_col)

#     # # Then drop rows where num_elems is 0
#     # df = df.drop(df[df['num_elems'] == 0].index)

#     # # And drop num_elems column
#     # df = df.drop(columns=[
#     #     'num_elems'])

#     return df


def remove_empty_rows(df):

    # Count # of non empty elems in each row and save to new column
    def count_elems(row):

        result = [0 if pd.isnull(
            cell) or cell == '' or cell == 'NaT' else 1 for cell in row]

        return sum(result)

    orig_cols = df.columns

    df_columns = list(df.columns)
    df_columns.remove('N_PROF')
    df_columns.remove('station_cast')
    df_columns.remove('index')
    df_subset = df[df_columns]

    new_df = df_subset.apply(count_elems, axis=1)

    new_df.columns = ['num_elems']

    df_end = pd.concat([df, new_df], axis=1)

    # name last column so know what to delete
    df_end.columns = [*df_end.columns[:-1], 'num_elems']

    # Then drop rows where num_elems is 0
    df_end = df_end.drop(df_end[df_end['num_elems'] == 0].index)

    df_end = df_end[orig_cols]

    return df_end


def remove_empty_rows4(df):
    # The  worst

    # Remove station cast for now since
    # it is filled
    station_cast_col = df['station_cast']
    df = df.drop(['station_cast'], axis=1)

    # Count # of elems in each row and save to new column
    def count_elems(row):
        result = [False if pd.isnull(
            cell) or cell == '' or cell == 'NaT' else True for cell in row]

        # Return number of True values
        # return result
        return sum(result)

    result = np.apply_along_axis(count_elems, axis=1, arr=df)

    df['not_empty'] = result.transpose()

    # df = pd.concat([df, new_df], axis=1)

    # df.columns = [*df.columns[:-1], 'num_elems']

    df = df.join(station_cast_col)

    # Then drop rows where num_elems is 0
    df = df.drop(df[df['not_empty'] == 0].index)

    # And drop num_elems column
    df = df.drop(columns=['not_empty'])

    return df


def remove_empty_rows3(df_orig):

    df = df_orig.copy()
    df2 = df_orig.copy()

    cols = df.columns

    def not_empty(x):
        if pd.isnull(x) or x == 'x' or x == 'NaT':
            return False
        else:
            return True

    func = np.vectorize(not_empty)

    for col in cols:
        result = func(df[col])
        df2[col] = result

    print(df2)

    # df2['sum'] = df2[cols].sum(axis=1)

    # df3 = df.join(df2)

    # print(df3)

    # df3 = df_new[cols].sum(axis=1)

    # print(df_new)

    return df2


def remove_empty_rows2(df):

    # Remove station cast for now since
    # it is filled
    station_cast_col = df['station_cast']
    df = df.drop(['station_cast'], axis=1)

    # Want to mark empty columns with False and then
    # when count, it it is 0, means all columns empty

    cols = df.columns
    conditions = [df[cols].isnull().any(1)]
    choices = [False]
    df["null"] = np.select(conditions, choices, default=True)

    conditions = [df[cols].eq('').any(1)]
    choices = [False]
    df["string"] = np.select(conditions, choices, default=True)

    conditions = [df[cols].eq('NaT').any(1)]
    choices = [False]
    df["time"] = np.select(conditions, choices, default=True)

    # df["num_elems"] = df[['null', 'string', 'time']].sum(axis=1)
    df["num_elems"] = df[['string', 'time']].sum(axis=1)

    df = df.join(station_cast_col)

    # Then drop rows where num_elems is 0
    df = df.drop(df[df['num_elems'] == 0].index)

    # # And drop num_elems column
    df = df.drop(columns=['null', 'string', 'time', 'num_elems'])

    return df


def apply_c_format_per_elem(val, c_format):

    # print(f'{val:.2f}') where val is a #
    f_format = c_format.lstrip('%')

    return f"{float(val):{f_format}}"


def apply_c_format_to_nc(nc_val, c_format):

    #  This work but not saved correctly in nc

    # Now get str in C_format. e.g. "%9.1f"
    # print(f'{val:.2f}') where val is a #
    f_format = c_format.lstrip('%')

    func_vec = np.vectorize(apply_c_format_per_elem)
    result = func_vec(nc_val, f_format)

    # print(new_val)

    # if isinstance(val, float):
    #     new_val = float(f"{val:{f_format}}")
    # elif isinstance(val, list):
    #     new_val = [float(f"{item:{f_format}}") for item in val]
    # else:
    #     new_val = val


def modify_floats(nc):

    # Apply C_format to floats and save as string
    # later can apply Decimal to it and save to json string
    # json_str = json.dumps(json_dict, default=defaultencode)
    float_types = ['float64', 'float32']

    for key in nc.keys():
        dtype = nc[key].dtype
        if dtype in float_types:
            try:
                nc[key] = nc[key].astype('str')
                c_format = nc[key].attrs['C_format']
                nc[key] = apply_c_format_to_nc(nc[key].values, c_format)
            except:
                pass

    for key in nc.coords:
        dtype = nc[key].dtype
        if dtype in float_types:
            try:
                nc[key] = nc[key].astype('str')
                c_format = nc[key].attrs['C_format']
                nc[key] = apply_c_format_to_nc(nc[key].values, c_format)
            except:
                pass

    return nc


def get_goship_mappings_per(nc):

    meta_mapping = {}

    meta_units = {}
    meta_ref_scale = {}
    meta_c_format = {}
    meta_dtype = {}

    param_mapping = {}

    param_units = {}
    param_ref_scale = {}
    param_c_format = {}
    param_dtype = {}

    # Meta: Save units, ref_scale, c_format, dtype
    for var in nc.coords:
        try:
            meta_units[var] = nc[var].attrs['units']
        except:
            meta_units[var] = None

        try:
            meta_ref_scale[var] = nc[var].attrs['reference_scale']
        except:
            meta_ref_scale[var] = None

        try:
            meta_c_format[var] = nc[var].attrs['C_format']
        except:
            meta_c_format[var] = None

        try:
            meta_dtype[var] = nc[var].dtype
        except KeyError:
            meta_dtype[var] = None

    # Param: Save units, ref_scale, and c_format, dtype
    for var in nc.keys():
        try:
            param_units[var] = nc[var].attrs['units']
        except:
            param_units[var] = None

        try:
            param_ref_scale[var] = nc[var].attrs['reference_scale']
        except:
            param_ref_scale[var] = None

        try:
            param_c_format[var] = nc[var].attrs['C_format']
        except:
            param_c_format[var] = None

        try:
            param_dtype[var] = nc[var].dtype
        except KeyError:
            param_dtype[var] = None

    meta_mapping['names'] = list(nc.coords)
    meta_mapping['units'] = meta_units
    meta_mapping['ref_scale'] = meta_ref_scale
    meta_mapping['c_format'] = meta_c_format
    meta_mapping['dtype'] = meta_dtype

    param_mapping['names'] = list(nc.keys())
    param_mapping['units'] = param_units
    param_mapping['ref_scale'] = param_ref_scale
    param_mapping['c_format'] = param_c_format
    param_mapping['dtype'] = param_dtype

    return meta_mapping, param_mapping


# def get_profile_mapping(nc, station_cast):

#     goship_units = gvm.create_goship_unit_mapping(nc)

#     goship_ref_scale = gvm.create_goship_ref_scale_mapping(nc)

#     # get c-format (string representation of numbers)
#     goship_c_format = gvm.create_goship_c_format_mapping(nc)

#     profile_mapping = {}
#     profile_mapping['station_cast'] = station_cast
#     profile_mapping['goship_c_format'] = goship_c_format
#     profile_mapping['goship_ref_scale'] = goship_ref_scale
#     profile_mapping['goship_units'] = goship_units

#     return profile_mapping


def apply_equations_and_ref_scale(nc):

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    # Converting to argovis ref scale if needed
    nc = pm.convert_goship_to_argovis_ref_scale(nc)

    # Apply equations to convert units
    nc = pm.convert_goship_to_argovis_units(nc)

    return nc


def keep_precision(nc):

    # Doesn't  work with xarray
    # Turns coordinates into  dimensions
    # add actually adds precision it looks like

    # s = [['123.123','23'],['2323.212','123123.21312']]
    # decimal_s = [[decimal.Decimal(x) for x in y] for y in s]
    # ss = numpy.array(decimal_s)

    # func_vec = np.vectorize(apply_c_format_per_elem)
    # result = func_vec(nc_val, f_format)

    func_vec = np.vectorize(lambda x: Decimal(x))

    float_types = ['float64', 'float32']

    for key in nc.keys():
        dtype = nc[key].dtype
        if dtype in float_types:
            try:
                nc[key] = func_vec(nc[key].values)
            except:
                pass

    for key in nc.coords:
        dtype = nc[key].dtype
        if dtype in float_types:
            try:
                nc[key] = func_vec(nc[key].values)
            except:
                pass

    return nc


def get_nc_dtypes(nc):

    meta_dtype = {}
    param_dtype = {}

    for key in nc.coords:
        dtype = nc[key].dtype
        meta_dtype[key] = dtype

    for key in nc.keys():
        dtype = nc[key].dtype
        param_dtype[key] = dtype

    return meta_dtype, param_dtype


# def string_to_float2(df, meta_mappings, param_mappings, meta_dtype, param_dtype):

#     def convert_to_decimal(x):
#         return Decimal(x)

#     # Now apply c_format to it
#     @ njit
#     def apply_c_format_per(val, c_format):
#         # print(f'{val:.2f}') where val is a #
#         f_format = c_format.lstrip('%')
#         formatted = f"{val:{f_format}}"
#         return float(formatted)

#     meta_c_formats = meta_mappings['c_format']
#     param_c_formats = param_mappings['c_format']

#     float_types = ['float64', 'float32']

#     for key, val in meta_dtype.items():
#         if val in float_types and meta_c_formats[key]:
#             # Apply Decimal to it
#             df[key] = convert_to_decimal(df[key].values)
#             c_format = meta_c_formats[key]
#             df[key] = df[key].apply(
#                 lambda x: apply_c_format_per(x, c_format))
#         elif val in float_types:
#             # Apply Decimal and then float
#             df[key] = df[key].apply(convert_to_decimal)
#             df[key] = df[key].apply(lambda x: float(x))

#     for key, val in param_dtype.items():
#         if val in float_types and param_c_formats[key]:
#             # Apply Decimal to it
#             df[key] = df[key].apply(convert_to_decimal)
#             c_format = param_c_formats[key]
#             df[key] = df[key].apply(
#                 lambda x: apply_c_format_per(x, c_format))
#         elif val in float_types:
#             # Apply Decimal and then float
#             df[key] = df[key].apply(convert_to_decimal)
#             df[key] = df[key].apply(lambda x: float(x))

#     return df


def string_to_float(df, meta_mappings, param_mappings, meta_dtype, param_dtype):

    def convert_to_decimal(x):
        return Decimal(x)

    # Now apply c_format to it
    def apply_c_format_per(val, c_format):
        # print(f'{val:.2f}') where val is a #
        f_format = c_format.lstrip('%')
        formatted = f"{val:{f_format}}"
        return float(formatted)

    meta_c_formats = meta_mappings['c_format']
    param_c_formats = param_mappings['c_format']

    float_types = ['float64', 'float32']
    int_types = ['int8', 'int64']

    # What to do about NaN and NaT that were turned to strings,
    # need change back. apply int type

    for key, val in meta_dtype.items():
        if val in float_types and meta_c_formats[key]:
            # Apply Decimal to it
            df[key] = df[key].apply(convert_to_decimal)
            c_format = meta_c_formats[key]
            df[key] = df[key].apply(
                lambda x: apply_c_format_per(x, c_format))
        elif val in float_types:
            # Apply Decimal and then float
            df[key] = df[key].apply(convert_to_decimal)
            df[key] = df[key].apply(lambda x: float(x))
        elif val in int_types:
            df[key] = df[key].apply(lambda x: int(x))

    for key, val in param_dtype.items():
        if val in float_types and param_c_formats[key]:
            # Apply Decimal to it
            df[key] = df[key].apply(convert_to_decimal)
            c_format = param_c_formats[key]
            df[key] = df[key].apply(
                lambda x: apply_c_format_per(x, c_format))
        elif val in float_types:
            # Apply Decimal and then float
            df[key] = df[key].apply(convert_to_decimal)
            df[key] = df[key].apply(lambda x: float(x))
        elif val in int_types:
            df[key] = df[key].apply(lambda x: int(x))

    return df


def float_to_string(nc):
    # Map to string to retain same precision as in xarray in
    # case piping into pandas changes anything

    # problem is casting NaN and NaT to strings
    # and  then they stay that way

    float_types = ['float64', 'float32']

    for key in nc.keys():
        dtype = nc[key].dtype
        if dtype in float_types:
            try:
                nc[key] = nc[key].astype('str')
            except:
                pass

    for key in nc.coords:
        dtype = nc[key].dtype
        if dtype in float_types:
            try:
                nc[key] = nc[key].astype('str')
            except:
                pass

    return nc


def create_mapping_profile(meta_mapping, param_mapping, type):

    # This function is for one station_cast
    meta_names = meta_mapping['names']
    meta_units = meta_mapping['units']
    meta_ref_scale = meta_mapping['ref_scale']
    meta_c_format = meta_mapping['c_format']

    param_names = param_mapping['names']
    param_units = param_mapping['units']
    param_ref_scale = param_mapping['ref_scale']
    param_c_format = param_mapping['c_format']

    goship_names = [*meta_names, *param_names]
    goship_units = {**meta_units, **param_units}
    goship_ref_scale = {**meta_ref_scale, **param_ref_scale}
    goship_c_format = {**meta_c_format, **param_c_format}

    mapping_dict = {}

    mapping_dict['goshipArgovisNameMapping'] = gvm.create_goship_argovis_core_values_mapping(
        type)
    mapping_dict['argovisReferenceScale'] = gvm.get_argovis_ref_scale_mapping(
        goship_names, type)

    mapping_dict['goshipNames'] = goship_names
    mapping_dict['goshipReferenceScale'] = goship_ref_scale
    mapping_dict['goshipUnits'] = goship_units
    mapping_dict['goshipCformat'] = goship_c_format

    mapping_dict['goshipArgovisUnitsMapping'] = gvm.get_goship_argovis_unit_mapping()

    return mapping_dict

# here


def get_measurements_source(df_meas, temp_qc, type):

    # See if using ctd_salinty or bottle_salinity
    # Is ctd_salinity NaN? If it is and bottle_salinity isn't NaN, use it
    # See if all ctd_salinity are NaN, if not, use it and
    # drop bottle salinity column
    is_ctd_sal_empty = True
    is_ctd_sal_column = 'psal' in df_meas.columns
    if is_ctd_sal_column:
        is_ctd_sal_empty = df_meas['psal'].isnull().all()

    is_bottle_sal_empty = True
    is_bottle_sal_column = 'salinity' in df_meas.columns
    if is_bottle_sal_column:
        # Check if not all NaN
        is_bottle_sal_empty = df_meas['salinity'].isnull().all()

    if is_ctd_sal_column and not is_ctd_sal_empty and is_bottle_sal_column:
        use_ctd_psal = True
        use_bottle_salinity = False
        df_meas = df_meas.drop(['salinity'], axis=1)
    elif is_ctd_sal_column and not is_ctd_sal_empty and not is_bottle_sal_column:
        use_ctd_psal = True
        use_bottle_salinity = False
    elif is_ctd_sal_column and is_ctd_sal_empty and is_bottle_sal_column and not is_bottle_sal_empty:
        use_ctd_psal = False
        use_bottle_salinity = True
        df_meas = df_meas.drop(['psal'], axis=1)
    elif not is_ctd_sal_column and is_bottle_sal_column and not is_bottle_sal_empty:
        use_ctd_psal = False
        use_bottle_salinity = True
    else:
        use_ctd_psal = False
        use_bottle_salinity = False

    # ctd_temp_cols = ['ctd_temperature', 'ctd_temperature_68']
    # is_ctd_temp_col = any(
    #     [True if col in df_meas.columns else False for col in ctd_temp_cols])
    #is_ctd_temp_col = 'temp' in df_meas.columns
    # Don't want to match variables with temp in name
    is_ctd_temp = next(
        (True for col in df_meas.columns if col == 'temp'), False)

    if is_ctd_temp:
        is_ctd_temp_empty = df_meas['temp'].isnull().all()
    else:
        is_ctd_temp_empty = True

    # if is_ctd_temp_col:
    #     is_ctd_temp_empty = next(
    #         (df_meas[col].isnull().all() for col in df_meas.columns), False)

    if is_ctd_temp_empty:
        flag = None
    elif not is_ctd_temp_empty and use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    elif not is_ctd_temp_empty and not use_ctd_psal and use_bottle_salinity:
        flag = 'BTL'
    elif type == 'ctd' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    elif type == 'btl' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
        flag = 'BTL'
    else:
        flag = None

    # json_str = df_meas.to_json(orient='records')

    # data_dict_list = json.loads(json_str)

    measurements_source = flag

    measurements_source_qc = {}
    measurements_source_qc['qc'] = temp_qc
    measurements_source_qc['use_ctd_temp'] = not is_ctd_temp_empty
    measurements_source_qc['use_ctd_psal'] = use_ctd_psal
    if use_bottle_salinity:
        measurements_source_qc['use_bottle_salinity'] = use_bottle_salinity

    # For json_str, convert True, False to 'true','false'
    measurements_source_qc = fp.convert_boolean(measurements_source_qc)

    return measurements_source, measurements_source_qc


# def get_measurements_source(df_meas):

#     # See if using ctd_salinty or bottle_salinity
#     # Is ctd_salinity NaN? If it is and bottle_salinity isn't NaN, use it
#     # See if all ctd_salinity are NaN, if not, use it and
#     # drop bottle salinity column
#     is_ctd_sal_empty = True
#     is_ctd_sal_column = 'ctd_salinity' in df_meas.columns
#     if is_ctd_sal_column:
#         is_ctd_sal_empty = df_meas['ctd_salinity'].isnull().all()

#     is_bottle_sal_empty = True
#     is_bottle_sal_column = 'bottle_salinity' in df_meas.columns
#     if is_bottle_sal_column:
#         # Check if not all NaN
#         is_bottle_sal_empty = df_meas['bottle_salinity'].isnull().all()

#     if is_ctd_sal_column and not is_ctd_sal_empty and is_bottle_sal_column:
#         use_ctd_salinity = True
#         use_bottle_salinity = False
#         df_meas = df_meas.drop(['bottle_salinity'], axis=1)
#     elif is_ctd_sal_column and not is_ctd_sal_empty and not is_bottle_sal_column:
#         use_ctd_salinity = True
#         use_bottle_salinity = False
#     elif is_ctd_sal_column and is_ctd_sal_empty and is_bottle_sal_column and not is_bottle_sal_empty:
#         use_ctd_salinity = False
#         use_bottle_salinity = True
#         df_meas = df_meas.drop(['ctd_salinity'], axis=1)
#     elif not is_ctd_sal_column and is_bottle_sal_column and not is_bottle_sal_empty:
#         use_ctd_salinity = False
#         use_bottle_salinity = True
#     else:
#         use_ctd_salinity = False
#         use_bottle_salinity = False

#     ctd_temp_cols = ['ctd_temperature', 'ctd_temperature_68']
#     is_ctd_temp_col = any(
#         [True if col in df_meas.columns else False for col in ctd_temp_cols])

#     is_ctd_temp_empty = True
#     if is_ctd_temp_col:
#         is_ctd_temp_empty = bool(
#             next(df_meas[col].isnull().all() for col in df_meas.columns))

#     if is_ctd_temp_empty:
#         flag = None
#     elif not is_ctd_temp_empty and use_ctd_salinity and not use_bottle_salinity:
#         flag = 'CTD'
#     elif not is_ctd_temp_empty and not use_ctd_salinity and use_bottle_salinity:
#         flag = 'BTL'
#     elif not is_ctd_temp_empty and not use_ctd_salinity and not use_bottle_salinity:
#         flag = 'CTD'
#     else:
#         flag = None

#     # json_str = df_meas.to_json(orient='records')

#     # data_dict_list = json.loads(json_str)

#     measurements_source = {}
#     # measurements_source['flag'] = flag
#     measurements_source['source'] = flag
#     measurements_source['qc'] = 2
#     measurements_source['use_ctd_temp'] = not is_ctd_temp_empty
#     measurements_source['use_ctd_salinity'] = use_ctd_salinity
#     if use_bottle_salinity:
#         measurements_source['use_bottle_salinity'] = use_bottle_salinity

#     measurements_source = fp.convert_boolean(measurements_source)

#     return measurements_source


def find_temp_qc_val(df, type):

    # Check if have temp_{type}_qc with qc = 0 or qc = 2 values
    has_ctd_temp_qc = f"temp_{type}_qc" in df.columns

    if has_ctd_temp_qc:
        temp_qc = df[f"temp_{type}_qc"].astype(int).values

        if 0 in temp_qc:
            qc = 0
        elif 2 in temp_qc:
            qc = 2
        else:
            qc = None

    else:
        qc = None

    return qc


def check_if_temp_qc(nc, type):

    # Now check so see if there is a 'temp_{type}'  column and a corresponding
    # qc col. 'temp_{type}_qc'. If not, add a 'temp' qc col. with values 0

    has_ctd_temp = f"temp_{type}" in nc.keys()
    has_ctd_temp_qc = f"temp_{type}_qc" in nc.keys()

    if has_ctd_temp and not has_ctd_temp_qc:
        temp_shape = np.shape(nc[f"temp_{type}"])
        shape = np.transpose(temp_shape)
        temp_qc = np.zeros(shape)

        nc[f"temp_{type}_qc"] = (['N_PROF', 'N_LEVELS'], temp_qc)

    return nc


def create_geolocation_dict(lat, lon):

    # "geoLocation": {
    #     "coordinates": [
    #         -158.2927,
    #         21.3693
    #     ],
    #     "type": "Point"
    # },

    coordinates = [lon, lat]

    geo_dict = {}
    geo_dict['coordinates'] = coordinates
    geo_dict['type'] = 'Point'

    # geolocation_dict = {}
    # geolocation_dict['geoLocation'] = geo_dict

    return geo_dict


def add_extra_general_coords(nc, file_info):

    # Use cruise expocode because file one could be different than cruise page
    # Drop existing expocode
    # nc = nc.reset_coords(names=['expocode'], drop=True)

    nc = nc.rename({'expocode':  'file_expocode'})

    filename = file_info['filename']
    data_path = file_info['data_path']
    expocode = file_info['cruise_expocode']

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

    coord_length = nc.dims['N_PROF']

    new_coord_list = ['GPS']*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(POSITIONING_SYSTEM=('N_PROF', new_coord_np))

    new_coord_list = ['CCHDO']*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(DATA_CENTRE=('N_PROF', new_coord_np))

    new_coord_list = [cruise_url]*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(cruise_url=('N_PROF', new_coord_np))

    new_coord_list = [data_url]*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(netcdf_url=('N_PROF', new_coord_np))

    new_coord_list = [filename]*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(data_filename=('N_PROF', new_coord_np))

    new_coord_list = [expocode]*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(expocode=('N_PROF', new_coord_np))

    # The station number is a string
    station_list = nc['station'].values

   # cast_number is an integer
    cast_list = nc['cast'].values

    def create_station_cast(x, y):
        station = str(x).zfill(3)
        cast = str(y).zfill(3)
        return f"{station}_{cast}"

    station_cast = list(
        map(create_station_cast, station_list, cast_list))

    nc = nc.assign_coords(station_cast=('N_PROF', station_cast))

    expocode_list = [expocode]*coord_length
    id = list(map(lambda e, s: f"{e}_{s}", expocode_list, station_cast))

    nc = nc.assign_coords(id=('N_PROF', id))
    nc = nc.assign_coords(_id=('N_PROF', id))

    # Convert times
    xr.apply_ufunc(lambda x: pd.to_datetime(x), nc['time'], dask='allowed')

    time = nc['time'].values
    new_date = list(
        map(lambda x: pd.to_datetime(x).strftime("%Y-%m-%d"), time))

    nc = nc.assign_coords(date_formatted=('N_PROF', new_date))

    time = nc['time'].values
    new_date = list(
        map(lambda x: pd.to_datetime(x).isoformat(), time))

    nc = nc.assign_coords(date=('N_PROF', new_date))

    # Drop time
    nc = nc.drop('time')

    latitude = nc['latitude'].values
    longitude = nc['longitude'].values

    round_lat = list(map(lambda x: np.round(x, 3), latitude))
    round_lon = list(map(lambda x: np.round(x, 3), longitude))

    nc = nc.assign_coords(roundLat=('N_PROF', round_lat))
    nc = nc.assign_coords(roundLon=('N_PROF', round_lon))

    str_lat = list(map(lambda x: f"{x} N", round_lat))
    str_lon = list(map(lambda x: f"{x} E", round_lon))

    nc = nc.assign_coords(strLat=('N_PROF', str_lat))
    nc = nc.assign_coords(strLon=('N_PROF', str_lon))

    return nc


def modify_nc(nc, file_info):

    nc = add_extra_general_coords(nc, file_info)

    logging.info('afterr add coords')

    # move pressure from coordinate to variable
    nc = nc.reset_coords(names=['pressure'], drop=False)

    logging.info('after reset')

    # move section_id  and btm_depth to coordinates
    try:
        nc = nc.set_coords(names=['btm_depth'])
    except:
        pass

    try:
        nc = nc.set_coords(names=['section_id'])
    except:
        pass

    logging.info('aftter set coords')

    # remove instrument_id profile_type  geometry_container from vars

    try:
        nc = nc.drop_vars(['profile_type'])
    except:
        pass

    try:
        nc = nc.drop_vars(['instrument_id'])
    except:
        pass

    try:
        nc = nc.drop_vars(['geometry_container'])
    except:
        pass

    # drop_vars = ['profile_type', 'instrument_id', 'geometry_container']

    # for var in drop_vars:
    #     try:
    #         nc = nc.drop_vars([var])
    #     except KeyError:
    #         pass

    # Remove N_LEVELS dim
    # Because dask array doesn't support  multi-index

    return nc


def to_int_qc(obj):
    # _qc":2.0
    # If float qc with '.0' in qc value, remove it to get an int
    json_str = json.dumps(obj,  default=dtjson)
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)
    obj = json.loads(json_str)
    return obj


def create_profiles_one_type(data_obj):

    start_time = datetime.now()

    # TODO
    # Can I appply all this to one xarray and then apply a group?
    # If did that, when add extra coords, would also need
    # to append to make an array of static variables
    # Is it better to put into a pandas or dask dataframe
    # and then work with meta and params

    # dask DataFrames do not support multi-indexes
    # Try using pandas first

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

    logging.info('Start modify_nc')
    nc = modify_nc(nc, file_info)

    #  TODO
    # If no ctd_temp qc, add one with qc=0

    logging.info('Start get_goship_mappings')
    # Get universal attributes independent of profile
    meta_mappings, param_mappings = get_goship_mappings_per(nc)

    # mapping
    meta_mapping_argovis = rn.rename_mapping_to_argovis(meta_mappings)

    param_mapping_argovis_btl = rn.rename_mapping_to_argovis_param(
        param_mappings, 'btl')
    param_mapping_argovis_ctd = rn.rename_mapping_to_argovis_param(
        param_mappings, 'ctd')

    logging.info('start create_mapping_profile')
    goship_mapping_dict = create_mapping_profile(
        meta_mappings, param_mappings, type)

    logging.info('start apply_equations_and_ref_scale')
    # TODO
    # Get formula for Oxygen unit conversion
    nc = apply_equations_and_ref_scale(nc)

    # Rename columns to argovis_names
    # Create mapping from goship_col names to argovis names
    argovis_col_mapping = rn.rename_cols_meta_no_type(list(nc.coords))
    nc = nc.rename_vars(argovis_col_mapping)

    argovis_col_mapping = rn.rename_cols_not_meta(list(nc.keys()), type)
    nc = nc.rename_vars(argovis_col_mapping)

    # TODO

    # Now check so see if there is a 'temp_{type}'  column and a corresponding
    # qc col. 'temp_{type}_qc'. If not, add a 'temp' qc col. with values 0
    nc = check_if_temp_qc(nc, type)

    # --------

    # nc = nc.groupby("N_PROF").map(remove_rows)
    # nc_groups = [obj[1] for obj in nc.groupby('N_PROF')]

    # Removing rows in xarray is too slow with groupby
    # since using a big loop. faster in pandas

    # for nc_group in nc_groups:
    #     #remove_rows(nc_group)
    #     nc_group = nc_group.apply_ufunc(remove_rows)

    # separate param variables to a pandas dataframe and
    # process like would with a smaller df
    meta_keys = list(nc.coords)
    param_keys = list(nc.keys())

    # logging.info('start get_nc_dtypes')
    # meta_dtype, param_dtype = get_nc_dtypes(nc)

    logging.info('start nc to dataframe')
    # nc.to_dask_dataframe causes strange df
    # with ... as entries
    # Maybe that's how dask represents things
    # ddf = nc.to_dataframe()

    # No longer has N_PROF or N_LEVELS to group over
    ddf = nc.to_dask_dataframe(dim_order=['N_PROF', 'N_LEVELS'])

    # # Rename cols to argovis_names
    # argovis_col_mapping = rn.rename_cols_to_argovis(ddf.columns)
    # ddf = ddf.rename(columns=argovis_col_mapping)

    # Reset index so not multi-index
    # add N_PROF AND N_LEVELS to both
    # and add station_cast to param
    # Add station_cast column so can
    # later combine on station_cast instead
    # of profile number

    # ------

    meta_keys.extend(['N_PROF', 'N_LEVELS'])
    param_keys.extend(['N_PROF', 'N_LEVELS', 'station_cast'])

    # param_keys.extend(['station_cast'])
    df_meta = ddf[meta_keys].copy()
    df_param = ddf[param_keys].copy()

    # Add station_cast column so can
    # later combine on station_cast instead
    # of profile number
    # station_cast_col = df_meta['station_cast']
    # df_param = df_param.join(station_cast_col)

    # logging.info('start meta and  param group by')
    # df_meta_grouped = df_meta.groupby(level="N_PROF")
    # df_param_grouped = df_param.groupby(level="N_PROF")

    logging.info('start apply removing empty rows')
    # Only need to apply to  param since meta is filled

    # if remove_empty_rows5
    # IndexError: only integers, slices (`:`), ellipsis (`...`), numpy.newaxis (`None`) and integer or boolean arrays are valid indices

    # df_param = df_param.reset_index()
    # df_param = df_param.set_index('station_cast')
    # df_param = df_param.drop('N_LEVELS', axis=1)

    df_param = df_param.reset_index()

    df_param = df_param.drop('N_LEVELS', axis=1)

    meta_dask = df_param.dtypes.to_dict()
    df_param = df_param.groupby("N_PROF").apply(
        remove_empty_rows, meta=meta_dask)

    # meta_dask = df_param.dtypes.to_dict()
    # df_param = df_param.groupby("N_PROF").apply(
    #     remove_empty_rows, meta=meta_dask)

    # turn N_PROF back into an index for meta
    # df_meta = df_meta.reset_index()

    # With meta columns, pandas exploded them
    # for all levels. Only keep one Level
    # since they repeat
    # TODO
    # Instead of dropping rows, just extract one
    logging.info('Get level = 0 meta rows')
    # df_meta.drop(df_meta[df_meta['N_LEVELS'] != 0].index, inplace=True)

    df_meta = df_meta[df_meta['N_LEVELS'] == 0]
    df_meta = df_meta.reset_index()
    df_meta = df_meta.drop('index', axis=1)
    df_meta = df_meta.drop('N_LEVELS', axis=1)
    df_meta = df_meta.compute()

    # Drop N_LEVELS column
    # logging.info('start dropping col')
    # df_meta.compute()
    # df_meta = df_meta.drop(['N_LEVELS'], axis=1)

    # keep station_cast column

    # following doesn't work
    # says ValueError: cannot insert N_PROF, already exists
    # even with reset index
    # df_bgc = df_bgc.reset_index()
    # df_bgc = df_bgc.set_index('N_PROF')
    # df_bgc = dd.from_pandas(df_bgc, npartitions=3)

    logging.info('start create_measurements_df_all')
    # Need  to  provide meta in calculation for dask
    # TODO
    # Update conditions to take ctd_temp with qc=0
    # apply group wise instead. drop N_LEVELS and
    # move station_cast as an index
    # df_meas = create_measurements_df_all(df_param)

    #df_param = df_param.drop('N_LEVELS', axis=1)
    df_param = df_param.set_index('station_cast')

    #  Determine here what meta returned will be
    # core values includes '_qc' vars
    # core_values = gvm.get_goship_core_values()
    # table_columns = list(df_param.columns)
    # core_cols = [col for col in table_columns if col in core_values]
    # core_non_qc = [elem for elem in core_cols if '_qc' not in elem]
    # meta_dask = df_param[core_non_qc].dtypes.to_dict()

    # temperatures = ['ctd_temperature', 'ctd_temperature_68']

    # for col in df_param.columns:
    #     if col in temperatures and f"{col}_qc" not in df_param.columns:
    #         df_param[f"{col}_qc"] = 0
    #         break

    # df_meas = create_measurements_df_all(df_param)
    # df_meas.compute()

    df_param = df_param.compute()

    # df_meas = create_measurements_df_all2(df_param)

    temp_qc = find_temp_qc_val(df_param, type)

    df_meas = df_param.groupby('N_PROF').apply(
        create_measurements_df_all2, type)

    # Put N_PROF and station_cast in cols
    # df_meas = df_meas.reset_index()
    # df_meas = df_meas.drop('index', axis=1)

    df_bgc = df_param
    df_bgc = df_bgc.reset_index()
    df_bgc = df_bgc.drop('index', axis=1)

    # Rename elements while dataframe
    # Just need  to rename the columns
    # renamed_meta = rn.rename_argovis_meta(meta_dict)

    # # Rename  meta
    # argovis_col_mapping = rn.rename_cols_to_argovis(df_meta.columns)
    # df_meta = df_meta.rename(columns=argovis_col_mapping)

    # # Rename  bgc
    # argovis_col_mapping = rn.rename_cols_to_argovis(df_bgc.columns)
    # df_bgc = df_bgc.rename(columns=argovis_col_mapping)

    # # Rename  meas
    # argovis_col_mapping = rn.rename_cols_to_argovis(df_meas.columns)
    # df_bgc = df_meas.rename(columns=argovis_col_mapping)

    # print(df_meas.head())

    # print(df_meta.head())

    # For each dataframe, separate into dictionaries
    logging.info('Start converting df to large dict')
    large_meta_dict = dict(tuple(df_meta.groupby('N_PROF')))
    large_bgc_dict = dict(tuple(df_bgc.groupby('N_PROF')))
    large_meas_dict = dict(tuple(df_meas.groupby('N_PROF')))

    # logging.info("Time to run function ")
    # logging.info(datetime.now() - start_time)

    # turn large dict into  lists of dicts with key of group

    logging.info('create all_meta list')
    all_meta = []
    all_meta_profiles = []
    for key, val in large_meta_dict.items():

        val = val.reset_index()
        station_cast = val['station_cast'].values[0]
        val = val.drop(['station_cast', 'N_PROF', 'index'],  axis=1)
        meta_dict = val.to_dict('records')[0]

        # Apply c_format for decimal places
        dtype_mapping = meta_mapping_argovis['dtype']
        c_format_mapping = meta_mapping_argovis['c_format']

        for name, value in meta_dict.items():

            try:
                new_val = apply_c_format_to_num(
                    name, value, dtype_mapping, c_format_mapping)
                meta_dict[name] = new_val
            except KeyError:
                pass

        lat = meta_dict['lat']
        lon = meta_dict['lon']

        geo_dict = create_geolocation_dict(lat, lon)
        meta_dict['geoLocation'] = geo_dict

        meta_obj = {}
        meta_obj['station_cast'] = station_cast
        meta_obj['dict'] = meta_dict

        all_meta.append(meta_obj)

    logging.info('create all_bgc list')

    # Apply c_format for decimal places
    if type == 'btl':
        param_mapping = param_mapping_argovis_btl
    elif type == 'ctd':
        param_mapping = param_mapping_argovis_ctd

    all_bgc = []
    for key, val in large_bgc_dict.items():
        val = val.reset_index()
        station_cast = val['station_cast'].values[0]
        val = val.drop(['station_cast', 'N_PROF', 'index'],  axis=1)
        bgc_dict_list = val.to_dict('records')

        formatted_list = []
        dtype_mapping = param_mapping['dtype']
        c_format_mapping = param_mapping['c_format']
        for obj in bgc_dict_list:

            new_obj = {name: apply_c_format_to_num(name, val, dtype_mapping, c_format_mapping)
                       for name, val in obj.items()}

            formatted_list.append(new_obj)

        bgc_obj = {}
        bgc_obj['station_cast'] = station_cast
        bgc_obj['list'] = formatted_list
        all_bgc.append(bgc_obj)

    logging.info('create all_meas list')
    all_meas = []
    for key, val in large_meas_dict.items():

        val = val.reset_index()
        station_cast = val['station_cast'].values[0]
        val = val.drop(['station_cast', 'N_PROF'],  axis=1)

        measurements_source, measurements_source_qc = get_measurements_source(
            val, temp_qc, type)

        meas_dict = val.to_dict('records')

        # For mapping, param_mapping has extensions and
        # measurements don't
        c_format_mapping = {name.replace(
            f"_{type}", ''): val for name, val in param_mapping['c_format'].items()}
        dtype_mapping = {name.replace(
            f"_{type}", ''): val for name, val in param_mapping['dtype'].items()}

        formatted_list = []
        for obj in meas_dict:

            new_obj = {name: apply_c_format_to_num(name, val, dtype_mapping, c_format_mapping)
                       for name, val in obj.items()}

            formatted_list.append(new_obj)

        meas_obj = {}
        meas_obj['station_cast'] = station_cast
        meas_obj['source'] = measurements_source
        meas_obj['qc'] = measurements_source_qc
        meas_obj['list'] = formatted_list

        all_meas.append(meas_obj)

    logging.info('start create all_meta_profiles')
    # TODO
    # do this earlier
    all_meta_profiles = []
    for obj in all_meta:

        meta_profile = {}
        meta_profile['station_cast'] = obj['station_cast']
        meta_profile['meta'] = obj['dict']

        all_meta_profiles.append(meta_profile)

    logging.info('start create all_bgc_profiles')
    all_bgc_profiles = []
    for obj in all_bgc:

        bgc_list = obj['list']

        bgc_profile = {}
        bgc_profile['station_cast'] = obj['station_cast']
        bgc_profile['bgcMeas'] = list(map(to_int_qc, bgc_list))

        all_bgc_profiles.append(bgc_profile)

    logging.info('start create all_meas_profiles')
    all_meas_profiles = []
    all_meas_source_profiles = []
    for obj in all_meas:

        meas_list = obj['list']

        meas_profile = {}
        meas_profile['station_cast'] = obj['station_cast']
        meas_profile['measurements'] = list(map(to_int_qc, meas_list))
        all_meas_profiles.append(meas_profile)

        # here
        meas_source_profile = {}
        meas_source_profile['station_cast'] = obj['station_cast']
        meas_source_profile['measurementsSource'] = obj['source']
        all_meas_source_profiles.append(meas_source_profile)

        meas_source_profile_qc = {}
        meas_source_profile_qc['station_cast'] = obj['station_cast']
        meas_source_profile_qc['measurementsSourceQC'] = obj['qc']
        all_meas_source_profiles.append(meas_source_profile_qc)

    # Combine
    logging.info('start combining profiles')
    all_profiles = combine_profiles(all_meta_profiles, all_bgc_profiles,
                                    all_meas_profiles, all_meas_source_profiles, goship_mapping_dict, type)

    logging.info("Time to run function create_profiles_one_type_ver2")
    logging.info(datetime.now() - start_time)

    # # Error tracing
    # try:
    #     c2.compute()
    # except Exception as e:
    #     import pdb
    #     pdb.set_trace()
    #     print(f"error {e}")

    logging.info('---------------------------')
    logging.info(f'End processing {type} profiles')
    logging.info(f"Shape of dims")
    logging.info(nc.dims)
    logging.info('---------------------------')

    # TODO
    # ddf = dd.from_pandas(df, npartitions=2)

    return all_profiles
