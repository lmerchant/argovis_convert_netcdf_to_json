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


import get_variable_mappings as gvm
import rename_objects as rn
import process_meta_data as pmd
import process_parameter_data as ppd


# # https://stackoverflow.com/questions/40659212/futurewarning-elementwise-comparison-failed-returning-scalar-but-in-the-futur
# import warnings
# warnings.simplefilter(action='ignore', category=FutureWarning)

#rprof = ResourceProfiler(dt=1)


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


def to_int_qc(obj):
    # _qc":2.0
    # If float qc with '.0' in qc value, remove it to get an int
    json_str = json.dumps(obj,  default=dtjson)
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)
    obj = json.loads(json_str)
    return obj


# def get_station_cast(nc_profile_group):

#     # Error for cruise
#     # ctd file
#     # file_id = 18420
#     # expocode = '316N154_2'
#     #     cast_number = str(nc_profile_group['cast'].values)
#     # TypeError: list indices must be integers or slices, not str

#     # cast_number is an integer
#     cast_number = str(nc_profile_group['cast'].values)

#     # The station number is a string
#     station = str(nc_profile_group['station'].values)

#     padded_station = str(station).zfill(3)
#     padded_cast = str(cast_number).zfill(3)

#     station_cast = f"{padded_station}_{padded_cast}"

#     return station_cast


def string_to_float(df, meta_mapping, param_mapping, meta_dtype, param_dtype):

    def convert_to_decimal(x):
        return Decimal(x)

    # Now apply c_format to it
    def apply_c_format_per(val, c_format):
        # print(f'{val:.2f}') where val is a #
        f_format = c_format.lstrip('%')
        formatted = f"{val:{f_format}}"
        return float(formatted)

    meta_c_formats = meta_mapping['c_format']
    param_c_formats = param_mapping['c_format']

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


# def create_bgc_meas_list(df):

#     json_str = df.to_json(orient='records')

#     # _qc":2.0
#     # If tgoship_argovis_name_mapping_btl is '.0' in qc value, remove it to get an int
#     json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

#     data_dict_list = json.loads(json_str)

#     return data_dict_list


# def get_measurements_source(df_meas, temp_qc, type):

#     # See if using ctd_salinty or bottle_salinity
#     # Is ctd_salinity NaN? If it is and bottle_salinity isn't NaN, use it
#     # See if all ctd_salinity are NaN, if not, use it and
#     # drop bottle salinity column
#     is_ctd_sal_empty = True
#     is_ctd_sal_column = 'psal' in df_meas.columns
#     if is_ctd_sal_column:
#         is_ctd_sal_empty = df_meas['psal'].isnull().all()

#     is_bottle_sal_empty = True
#     is_bottle_sal_column = 'salinity' in df_meas.columns
#     if is_bottle_sal_column:
#         # Check if not all NaN
#         is_bottle_sal_empty = df_meas['salinity'].isnull().all()

#     if is_ctd_sal_column and not is_ctd_sal_empty and is_bottle_sal_column:
#         use_ctd_psal = True
#         use_bottle_salinity = False
#         df_meas = df_meas.drop(['salinity'], axis=1)
#     elif is_ctd_sal_column and not is_ctd_sal_empty and not is_bottle_sal_column:
#         use_ctd_psal = True
#         use_bottle_salinity = False
#     elif is_ctd_sal_column and is_ctd_sal_empty and is_bottle_sal_column and not is_bottle_sal_empty:
#         use_ctd_psal = False
#         use_bottle_salinity = True
#         df_meas = df_meas.drop(['psal'], axis=1)
#     elif not is_ctd_sal_column and is_bottle_sal_column and not is_bottle_sal_empty:
#         use_ctd_psal = False
#         use_bottle_salinity = True
#     else:
#         use_ctd_psal = False
#         use_bottle_salinity = False

#     # ctd_temp_cols = ['ctd_temperature', 'ctd_temperature_68']
#     # is_ctd_temp_col = any(
#     #     [True if col in df_meas.columns else False for col in ctd_temp_cols])
#     #is_ctd_temp_col = 'temp' in df_meas.columns
#     # Don't want to match variables with temp in name
#     is_ctd_temp = next(
#         (True for col in df_meas.columns if col == 'temp'), False)

#     if is_ctd_temp:
#         is_ctd_temp_empty = df_meas['temp'].isnull().all()
#     else:
#         is_ctd_temp_empty = True

#     # if is_ctd_temp_col:
#     #     is_ctd_temp_empty = next(
#     #         (df_meas[col].isnull().all() for col in df_meas.columns), False)

#     if is_ctd_temp_empty:
#         flag = None
#     elif not is_ctd_temp_empty and use_ctd_psal and not use_bottle_salinity:
#         flag = 'CTD'
#     elif not is_ctd_temp_empty and not use_ctd_psal and use_bottle_salinity:
#         flag = 'BTL'
#     elif type == 'ctd' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
#         flag = 'CTD'
#     elif type == 'btl' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
#         flag = 'BTL'
#     else:
#         flag = None

#     # json_str = df_meas.to_json(orient='records')

#     # data_dict_list = json.loads(json_str)

#     measurements_source = flag

#     measurements_source_qc = {}
#     measurements_source_qc['qc'] = temp_qc
#     measurements_source_qc['use_ctd_temp'] = not is_ctd_temp_empty
#     measurements_source_qc['use_ctd_psal'] = use_ctd_psal
#     if use_bottle_salinity:
#         measurements_source_qc['use_bottle_salinity'] = use_bottle_salinity

#     # For json_str, convert True, False to 'true','false'
#     measurements_source_qc = fp.convert_boolean(measurements_source_qc)

#     return measurements_source, measurements_source_qc


# def create_measurements_df_all(df,  type):

#     # core values includes '_qc' vars
#     core_values = gvm.get_argovis_core_values_per_type(type)
#     table_columns = list(df.columns)
#     core_cols = [col for col in table_columns if col in core_values]

#     core_non_qc = [elem for elem in core_cols if '_qc' not in elem]

#     df_meas = df[core_cols].copy()

#     # here

#     def check_qc(row):
#         if pd.notnull(row[1]) and int(row[1]) == 0:
#             return row[0]
#         elif pd.notnull(row[1]) and int(row[1]) == 2:
#             return row[0]
#         else:
#             return np.nan

#     # If qc != 2, set corresponding value to np.nan
#     for col in core_non_qc:

#         qc_key = f"{col}_qc"

#         try:
#             df_meas[col] = df_meas[[col, qc_key]].apply(
#                 check_qc, axis=1)
#         except:
#             pass

#     # drop qc columns now that have marked non_qc column values
#     for col in df_meas.columns:
#         if '_qc' in col:
#             df_meas = df_meas.drop([col], axis=1)

#     # If all core values have nan, drop row
#     # This won't work since
#     df_meas = df_meas.dropna(how='all')

#     df_meas = df_meas.sort_values(by=['pres'])

#     # Remove type ('btl', 'ctd') from  variable names
#     column_mapping = {}
#     column_mapping[f"psal_{type}"] = 'psal'
#     column_mapping[f"temp_{type}"] = 'temp'
#     column_mapping[f"salinity_btl"] = 'salinity'

#     df_meas = df_meas.rename(columns=column_mapping)

#     return df_meas


# def create_meta_param_profiles_df(all_df_meta, all_df_param):

#     frames_meta = [obj['df_nc'] for obj in all_df_meta]
#     keys_meta = [obj['station_cast'] for obj in all_df_meta]
#     df_metas = pd.concat(frames_meta, keys=keys_meta)

#     frames_param = [frame['df_nc'] for frame in all_df_param]
#     keys_param = [frame['station_cast'] for frame in all_df_param]
#     df_params = pd.concat(frames_param, keys=keys_param)

#     return df_metas, df_params


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


# def apply_c_format_per_elem(val, c_format):

#     # print(f'{val:.2f}') where val is a #
#     f_format = c_format.lstrip('%')

#     return f"{float(val):{f_format}}"


# def apply_c_format_to_nc(nc_val, c_format):

#     #  This work but not saved correctly in nc

#     # Now get str in C_format. e.g. "%9.1f"
#     # print(f'{val:.2f}') where val is a #
#     f_format = c_format.lstrip('%')

#     func_vec = np.vectorize(apply_c_format_per_elem)
#     result = func_vec(nc_val, f_format)

#     # print(new_val)

#     # if isinstance(val, float):
#     #     new_val = float(f"{val:{f_format}}")
#     # elif isinstance(val, list):
#     #     new_val = [float(f"{item:{f_format}}") for item in val]
#     # else:
#     #     new_val = val


# def find_temp_qc_val(df, type):

#     # Check if have temp_{type}_qc with qc = 0 or qc = 2 values
#     has_ctd_temp_qc = f"temp_{type}_qc" in df.columns

#     if has_ctd_temp_qc:

#         temp_qc = df[f"temp_{type}_qc"]
#         temp_qc = list(temp_qc.array)

#         if 0 in temp_qc:
#             qc = 0
#         elif 2 in temp_qc:
#             qc = 2
#         else:
#             qc = None

#     else:
#         qc = None

#     return qc


# def check_if_temp_qc(nc, type):

#     # Now check so see if there is a 'temp_{type}'  column and a corresponding
#     # qc col. 'temp_{type}_qc'. If not, add a 'temp' qc col. with values 0

#     has_ctd_temp = f"temp_{type}" in nc.keys()
#     has_ctd_temp_qc = f"temp_{type}_qc" in nc.keys()

#     if has_ctd_temp and not has_ctd_temp_qc:
#         temp_shape = np.shape(nc[f"temp_{type}"])
#         shape = np.transpose(temp_shape)
#         temp_qc = np.zeros(shape)

#         nc[f"temp_{type}_qc"] = (['N_PROF', 'N_LEVELS'], temp_qc)

#     return nc


# def apply_equations_and_ref_scale(nc):

#     # Rename converted temperature later.
#     # Keep 68 in name and show it maps to temp_ctd
#     # and ref scale show what scale it was converted to

#     # Converting to argovis ref scale if needed
#     nc = pmc.convert_goship_to_argovis_ref_scale(nc)

#     # Apply equations to convert units
#     nc = pmc.convert_goship_to_argovis_units(nc)

#     return nc


# def create_geolocation_dict(lat, lon):

#     # "geoLocation": {
#     #     "coordinates": [
#     #         -158.2927,
#     #         21.3693
#     #     ],
#     #     "type": "Point"
#     # },

#     coordinates = [lon, lat]

#     geo_dict = {}
#     geo_dict['coordinates'] = coordinates
#     geo_dict['type'] = 'Point'

#     # geolocation_dict = {}
#     # geolocation_dict['geoLocation'] = geo_dict

#     return geo_dict


# def add_extra_general_coords(nc, file_info):

#     # Use cruise expocode because file one could be different than cruise page
#     # Drop existing expocode
#     # nc = nc.reset_coords(names=['expocode'], drop=True)

#     nc = nc.rename({'expocode':  'file_expocode'})

#     filename = file_info['filename']
#     data_path = file_info['data_path']
#     expocode = file_info['cruise_expocode']

#     if '/' in expocode:
#         expocode = expocode.replace('/', '_')
#         cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"
#     elif expocode == 'None':
#         logging.info(filename)
#         logging.info('expocode is None')
#         cruise_url = ''
#     else:
#         cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

#     data_url = f"https://cchdo.ucsd.edu{data_path}"

#     coord_length = nc.dims['N_PROF']

#     new_coord_list = ['GPS']*coord_length
#     new_coord_np = np.array(new_coord_list, dtype=object)
#     nc = nc.assign_coords(POSITIONING_SYSTEM=('N_PROF', new_coord_np))

#     new_coord_list = ['CCHDO']*coord_length
#     new_coord_np = np.array(new_coord_list, dtype=object)
#     nc = nc.assign_coords(DATA_CENTRE=('N_PROF', new_coord_np))

#     new_coord_list = [cruise_url]*coord_length
#     new_coord_np = np.array(new_coord_list, dtype=object)
#     nc = nc.assign_coords(cruise_url=('N_PROF', new_coord_np))

#     new_coord_list = [data_url]*coord_length
#     new_coord_np = np.array(new_coord_list, dtype=object)
#     nc = nc.assign_coords(netcdf_url=('N_PROF', new_coord_np))

#     new_coord_list = [filename]*coord_length
#     new_coord_np = np.array(new_coord_list, dtype=object)
#     nc = nc.assign_coords(data_filename=('N_PROF', new_coord_np))

#     new_coord_list = [expocode]*coord_length
#     new_coord_np = np.array(new_coord_list, dtype=object)
#     nc = nc.assign_coords(expocode=('N_PROF', new_coord_np))

#     # The station number is a string
#     station_list = nc['station'].values

#    # cast_number is an integer
#     cast_list = nc['cast'].values

#     def create_station_cast(x, y):
#         station = str(x).zfill(3)
#         cast = str(y).zfill(3)
#         return f"{station}_{cast}"

#     station_cast = list(
#         map(create_station_cast, station_list, cast_list))

#     nc = nc.assign_coords(station_cast=('N_PROF', station_cast))

#     #expocode_list = [expocode]*coord_length

#     def create_id(x, y):
#         station = str(x).zfill(3)
#         cast = str(y).zfill(3)
#         return f"expo_{expocode}_sta_{station}_cast_{cast}"

#     id = list(map(create_id, station_list, cast_list))

#     nc = nc.assign_coords(id=('N_PROF', id))
#     nc = nc.assign_coords(_id=('N_PROF', id))

#     # Convert times
#     xr.apply_ufunc(lambda x: pd.to_datetime(x), nc['time'], dask='allowed')

#     time = nc['time'].values
#     new_date = list(
#         map(lambda x: pd.to_datetime(x).strftime("%Y-%m-%d"), time))

#     nc = nc.assign_coords(date_formatted=('N_PROF', new_date))

#     time = nc['time'].values
#     new_date = list(
#         map(lambda x: pd.to_datetime(x).isoformat(), time))

#     nc = nc.assign_coords(date=('N_PROF', new_date))

#     # Drop time
#     nc = nc.drop('time')

#     latitude = nc['latitude'].values
#     longitude = nc['longitude'].values

#     round_lat = list(map(lambda x: np.round(x, 3), latitude))
#     round_lon = list(map(lambda x: np.round(x, 3), longitude))

#     nc = nc.assign_coords(roundLat=('N_PROF', round_lat))
#     nc = nc.assign_coords(roundLon=('N_PROF', round_lon))

#     str_lat = list(map(lambda x: f"{x} N", round_lat))
#     str_lon = list(map(lambda x: f"{x} E", round_lon))

#     nc = nc.assign_coords(strLat=('N_PROF', str_lat))
#     nc = nc.assign_coords(strLon=('N_PROF', str_lon))

#     return nc


def modify_nc(nc):

    # move pressure from coordinate to variable
    nc = nc.reset_coords(names=['pressure'], drop=False)

    # move section_id  and btm_depth to coordinates
    try:
        nc = nc.set_coords(names=['btm_depth'])
    except:
        pass

    try:
        nc = nc.set_coords(names=['section_id'])
    except:
        pass

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

    return nc


def create_profiles_one_type(data_obj):

    start_time = datetime.now()

    type = data_obj['type']

    file_info = {}
    file_info['type'] = type
    file_info['filename'] = data_obj['filename']
    file_info['data_path'] = data_obj['data_path']
    file_info['cruise_expocode'] = data_obj['cruise_expocode']
    file_info['cruise_id'] = data_obj['cruise_id']
    file_info['woce_lines'] = data_obj['woce_lines']

    logging.info('---------------------------')
    logging.info(f'Start processing {type} profiles')
    logging.info('---------------------------')

    nc = data_obj['nc']

    logging.info('Start modify_nc')
    nc = modify_nc(nc)

    # Add extra coordinates for ArgoVis
    nc = pmd.add_extra_general_coords(nc, file_info)

    logging.info('Start get_goship_mappings')
    meta_mapping, param_mapping = gvm.get_goship_mappings(nc)

    meta_mapping_argovis = rn.rename_mapping_to_argovis(meta_mapping)

    param_mapping_argovis_btl = rn.rename_mapping_to_argovis_param(
        param_mapping, 'btl')
    param_mapping_argovis_ctd = rn.rename_mapping_to_argovis_param(
        param_mapping, 'ctd')

    logging.info('start create_mapping_profile')

    # TODO

    # fix goshipArgovisNameMapping
    # It's  listing temp_68 wrong since there are both temp,
    # only  want the primarry one
    # It's listing all mappings and only want those  relevant to cruise
    #
    # Fix goship  names to  only  include orig names
    # before  modified it and added  extra coords

    # Fix goshipArgovisUnitsMapping, showing all mappings
    # and not just for cruise

    goship_mapping_dict = gvm.create_mapping_profile(
        meta_mapping, param_mapping, type)

    logging.info('start apply_equations_and_ref_scale')
    # TODO
    # Get formula for Oxygen unit conversion
    nc = ppd.apply_equations_and_ref_scale(nc)

    # Rename columns to argovis_names
    # Create mapping from goship_col names to argovis names
    argovis_col_mapping = rn.rename_cols_meta_no_type(list(nc.coords))
    nc = nc.rename_vars(argovis_col_mapping)

    argovis_col_mapping = rn.rename_cols_not_meta(list(nc.keys()), type)
    nc = nc.rename_vars(argovis_col_mapping)

    # Now check so see if there is a 'temp_{type}'  column and a corresponding
    # qc col. 'temp_{type}_qc'. If not, add a 'temp' qc col. with values 0
    nc = ppd.check_if_temp_qc(nc, type)

    # --------

    # Removing rows in xarray is too slow with groupby
    # since using a big loop. faster in pandas

    # for nc_group in nc_groups:
    #     nc_group = nc_group.apply_ufunc(remove_rows)

    # separate param variables to a pandas dataframe and
    # process like would with a smaller df
    meta_keys = list(nc.coords)
    param_keys = list(nc.keys())

    logging.info('start nc to dataframe')

    ddf = nc.to_dask_dataframe(dim_order=['N_PROF', 'N_LEVELS'])

    meta_keys.extend(['N_PROF', 'N_LEVELS'])
    param_keys.extend(['N_PROF', 'N_LEVELS', 'station_cast'])

    # param_keys.extend(['station_cast'])
    df_meta = ddf[meta_keys].copy()
    df_param = ddf[param_keys].copy()

    # TODO
    # Can I sort variable names?
    # because at the  moment, df gives temp  and temp_qc separated
    # in json output
    # df_param = df_param.reindex(sorted(df_param.columns), axis=1)

    # -----

    logging.info('start apply removing empty rows')
    # Only need to apply to  param since meta is filled

    # if remove_empty_rows5
    # IndexError: only integers, slices (`:`), ellipsis (`...`), numpy.newaxis (`None`) and integer or boolean arrays are valid indices

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

    logging.info('Get level = 0 meta rows')

    df_meta = df_meta[df_meta['N_LEVELS'] == 0]
    df_meta = df_meta.reset_index()
    df_meta = df_meta.drop('index', axis=1)
    df_meta = df_meta.drop('N_LEVELS', axis=1)
    df_meta = df_meta.compute()

    logging.info('start create_measurements_df_all')

    df_param = df_param.set_index('station_cast')

    # TODO
    # Should I use compute here or wait a bit
    df_param = df_param.compute()

    # df_meas = create_measurements_df_all(df_param)

    temp_qc = ppd.find_temp_qc_val(df_param, type)

    df_meas = df_param.groupby('N_PROF').apply(
        ppd.create_measurements_df_all, type)

    # Put N_PROF and station_cast in cols
    # df_meas = df_meas.reset_index()
    # df_meas = df_meas.drop('index', axis=1)

    df_bgc = df_param
    df_bgc = df_bgc.reset_index()
    df_bgc = df_bgc.drop('index', axis=1)

    # For each dataframe, separate into dictionaries
    logging.info('Start converting df to large dict')
    large_meta_dict = dict(tuple(df_meta.groupby('N_PROF')))

    large_bgc_dict = dict(tuple(df_bgc.groupby('N_PROF')))
    large_meas_dict = dict(tuple(df_meas.groupby('N_PROF')))

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

        geo_dict = pmd.create_geolocation_dict(lat, lon)
        meta_dict['geoLocation'] = geo_dict

        meta_obj = {}
        meta_obj['station_cast'] = station_cast
        meta_obj['dict'] = meta_dict

        all_meta.append(meta_obj)

    logging.info('start create all_meta_profiles')
    all_meta_profiles = []
    for obj in all_meta:

        meta_profile = {}
        meta_profile['station_cast'] = obj['station_cast']
        meta_profile['meta'] = obj['dict']

        all_meta_profiles.append(meta_profile)

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

    logging.info('start create all_bgc_profiles')
    all_bgc_profiles = []
    for obj in all_bgc:

        bgc_list = obj['list']

        bgc_profile = {}
        bgc_profile['station_cast'] = obj['station_cast']
        bgc_profile['bgcMeas'] = list(map(to_int_qc, bgc_list))

        all_bgc_profiles.append(bgc_profile)

    logging.info('create all_meas list')
    all_meas = []
    for key, val in large_meas_dict.items():

        val = val.reset_index()
        station_cast = val['station_cast'].values[0]
        val = val.drop(['station_cast', 'N_PROF'],  axis=1)

        measurements_source, measurements_source_qc = ppd.get_measurements_source(
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

    logging.info('start create all_meas_profiles')
    all_meas_profiles = []
    all_meas_source_profiles = []
    for obj in all_meas:

        meas_list = obj['list']

        meas_profile = {}
        meas_profile['station_cast'] = obj['station_cast']
        meas_profile['measurements'] = list(map(to_int_qc, meas_list))
        all_meas_profiles.append(meas_profile)

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

    logging.info("Time to run function create_profiles_one_type")
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

    return all_profiles
