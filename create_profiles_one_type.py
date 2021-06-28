# Create profiles for one type

import pandas as pd
import numpy as np
import json
from decimal import Decimal
import logging
import re
import dask.bag as db
from datetime import datetime
from dask.diagnostics import ProgressBar


import get_variable_mappings as gvm
import filter_profiles as fp
import rename_objects as rn
import get_profile_mapping_and_conversions as pm


# pbar = ProgressBar()
# pbar.register()


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


def create_measurements_list(df_bgc_meas):

    df_meas = pd.DataFrame()
    #df_meas = dd.DataFrame()

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

    # An example of what to delete in bgcMeas
    # check if values are all of type in  ['', NaN]
    # I didn't  want to replace all '' with NaN in case
    # it was important in a  previous measurement
    # {'pres': None, 'sample_ctd': '', 'temp_ctd': None, 'temp_ctd_qc': None, 'psal_ctd': None, 'psal_ctd_qc': None, 'doxy_ctd': None, 'doxy_ctd_qc': None}

    # Count # of elems in each row and save to new column
    def count_elems(row):
        result = [False if pd.isnull(
            cell) or cell == '' or cell == 'NaT' else True for cell in row]

        # Return number of True values
        return sum(result)

    df['num_elems'] = df.apply(count_elems, axis=1)

    # Then drop rows where num_elems is 0
    df.drop(df[df['num_elems'] == 0].index, inplace=True)

    # And drop num_elems column
    df = df.drop(columns=['num_elems'])

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


# def create_profile(nc_group, data_obj):

#     type = data_obj['type']

#     logging.info(f"Processing {type} profile {nc_group[0] + 1}")

#     profile_number = nc_group[0]
#     nc_profile_group = nc_group[1]

#     data_obj['profile_number'] = profile_number

#     # TODO
#     # Ask if for measurements, all values including pres are null,
#     # do I delete it? probably

#     # don't rename variables yet

#     cast_number = str(nc_profile_group['cast'].values)

#     # The station number is a string
#     station = str(nc_profile_group['station'].values)

#     station_cast = f"{station}_{cast_number}"

#     nc_profile_group = add_extra_coords(nc_profile_group, data_obj)

#     # Get meta names  again now that added extra coords
#     meta_names, param_names = pm.get_meta_param_names(nc_profile_group)

#     meta_dict = create_meta_dict(nc_profile_group, meta_names, data_obj)

#     goship_c_format_mapping_dict = data_obj['goship_c_format']

#     param_dict = create_json_profiles(nc_profile_group, param_names, data_obj)

#     df_bgc = create_bgc_meas_df(param_dict)

#     # param_json = create_json_profiles(nc_profile_group, param_names, data_obj)
#     # df_bgc = create_bgc_meas_df(param_json)

#     bgc_meas_dict_list = create_bgc_meas_list(df_bgc)

#     measurements_dict_list, measurements_source = create_measurements_list(
#         df_bgc)

#     goship_units_dict = data_obj['goship_units']

#     goship_ref_scale_mapping_dict = data_obj['goship_ref_scale']

#     goship_names_list = [*meta_names, *param_names]

#     # Save meta separate for renaming later
#     profile_dict = {}
#     profile_dict['station_cast'] = station_cast
#     profile_dict['type'] = type
#     profile_dict['meta'] = meta_dict
#     profile_dict['bgc_meas'] = bgc_meas_dict_list
#     profile_dict['measurements'] = measurements_dict_list
#     profile_dict['measurements_source'] = measurements_source
#     profile_dict['goship_ref_scale'] = goship_ref_scale_mapping_dict
#     profile_dict['goship_units'] = goship_units_dict
#     profile_dict['goship_c_format'] = goship_c_format_mapping_dict
#     profile_dict['goship_names'] = goship_names_list

#     output_profile = {}
#     output_profile['profile_dict'] = profile_dict
#     output_profile['station_cast'] = station_cast

#     # Rename with _type suffix unless it is an Argovis variable
#     # But no _btl suffix to meta data
#     # Add _btl when combine files
#     profile_one_type = rn.rename_profile_to_argovis(output_profile)

#     return profile_one_type


def create_profile(nc_profile_group, data_obj):

    type = data_obj['type']

    # TODO
    # Ask if for measurements, all values including pres are null,
    # do I delete it? probably

    # don't rename variables yet

    cast_number = str(nc_profile_group['cast'].values)

    # The station number is a string
    station = str(nc_profile_group['station'].values)

    station_cast = f"{station}_{cast_number}"

    #print(f"station cast {station_cast}")

    nc_profile_group = add_extra_coords(nc_profile_group, data_obj)

    # Get meta names  again now that added extra coords
    meta_names, param_names = pm.get_meta_param_names(nc_profile_group)

    meta_dict = create_meta_dict(nc_profile_group, meta_names, data_obj)

    goship_c_format_mapping_dict = data_obj['goship_c_format']

    param_dict = create_json_profiles(nc_profile_group, param_names, data_obj)

    # See if can speed up
    df_bgc = create_bgc_meas_df(param_dict)
    bgc_meas_dict_list = create_bgc_meas_list(df_bgc)
    measurements_dict_list, measurements_source = create_measurements_list(
        df_bgc)

    goship_units_dict = data_obj['goship_units']

    goship_ref_scale_mapping_dict = data_obj['goship_ref_scale']

    goship_names_list = [*meta_names, *param_names]

    # Save meta separate for renaming later
    profile_dict = {}
    profile_dict['station_cast'] = station_cast
    profile_dict['type'] = type
    profile_dict['meta'] = meta_dict
    profile_dict['bgc_meas'] = bgc_meas_dict_list
    profile_dict['measurements'] = measurements_dict_list
    profile_dict['measurements_source'] = measurements_source
    profile_dict['goship_ref_scale'] = goship_ref_scale_mapping_dict
    profile_dict['goship_units'] = goship_units_dict
    profile_dict['goship_c_format'] = goship_c_format_mapping_dict
    profile_dict['goship_names'] = goship_names_list

    profile = {}
    profile['profile_dict'] = profile_dict
    profile['station_cast'] = station_cast

    # Rename with _type suffix unless it is an Argovis variable
    # But no _btl suffix to meta data
    # Add _btl when combine files

    renamed_profile = rn.rename_profile_to_argovis(profile)

    return renamed_profile


def create_profiles_one_type(data_obj):

    type = data_obj['type']

    logging.info('---------------------------')
    logging.info(f'Start processing {type} profiles')
    logging.info('---------------------------')

    data_obj = pm.get_profile_mapping_and_conversions(data_obj)

    nc = data_obj['nc']

    all_profiles = []

    reduced_data_obj = {key: val for key,
                        val in data_obj.items() if key != 'nc'}

    nc_groups = [obj[1] for obj in nc.groupby('N_PROF')]

    logging.info(
        f"Total number of {type} profiles to process {len(nc_groups)}")

    b = db.from_sequence(nc_groups)

    c = b.map(create_profile, reduced_data_obj)

    # program_start_time = datetime.now()
    all_profiles = c.compute()
    # logging.info('Using dask bag')
    # logging.info(datetime.now() - program_start_time)

    # With a single thread
    # program_start_time = datetime.now()
    #all_profiles = c.compute(scheduler='single-threaded')
    # logging.info('Using single thread')
    # logging.info(datetime.now() - program_start_time)

    # for nc_group in nc.groupby('N_PROF'):

    #     profile = create_profile(nc_group, data_obj)

    #     all_profiles.append(profile)

    logging.info('---------------------------')
    logging.info(f'End processing {type} profiles')
    logging.info('---------------------------')

    return all_profiles

# def create_profiles_one_type(data_obj):

#     type = data_obj['type']

#     print('---------------------------')
#     print(f'Start processing {type} profiles')
#     print('---------------------------')

#     data_obj = pm.get_profile_mapping_and_conversions(data_obj)

#     nc = data_obj['nc']

#     all_profiles = []

#     for nc_group in nc.groupby('N_PROF'):

#         profile = create_profile(nc_group, data_obj)

#         all_profiles.append(profile)

#     print('---------------------------')
#     print(f'End processing {type} profiles')
#     print('---------------------------')

#     return all_profiles
