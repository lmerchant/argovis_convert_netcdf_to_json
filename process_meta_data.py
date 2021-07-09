# Process meta data

import numpy as np
import pandas as pd
import xarray as xr
import logging


# def remove_empty_rows5(df):

#     # TODO
#     # use np.where to speed it up

#     # n[89]: cond1 = df.Close > df.Close.shift(1)
#     # # In[90]: cond2 = df.High < 12
#     # # In[91]: df['New_Close'] = np.where(cond1, 'A', 'B')

#     # df.loc[df['B'].isin(['one','three'])]

#     # a = df.loc[df[cols].isin(['', np.nan, 'NaT'])]

#     # df['test'] = pd.np.where(df[['col1', 'col2', 'col3']].eq('Y').any(1, skipna=True), 'Y',
#     #                          pd.np.where(df[['col1', 'col2', 'col3']].isnull().all(1), None, 'N'))

#     # a = pd.np.where(df[cols].isnull().all(1), None, False)

#     # df['test'] = pd.np.where(df[cols].eq('').any(1), 'N', 'Y',
#     #                          pd.np.where(df[cols].isnull().any(1), 'N', 'Y'),
#     #                          pd.np.where(df[cols].eq('NaT').any(1), 'N', 'Y'))

#     # df['empty'] = pd.np.where(df[cols].eq(
#     #     '').any(1, skipna=True), 'Y', 'N')

#     # df['empty'] = pd.np.where(df[cols].eq(
#     #     'NaT').any(1, skipna=True), 'Y', df['empty'])

#     # df['empty'] = pd.np.where(df[cols].isnull().any(
#     #     1, skipna=True), 'Y', df['empty'])

#    # Remove station cast for now since
#     # it is filled
#     # Need original order for dask compute  meta
#     df = orig_cols = df.columns

#     station_cast_col = df['station_cast']
#     df = df.drop(['station_cast'], axis=1)

#     cols = df.columns

#     df['string'] = np.where(df[cols].eq(
#         '').any(1), False, True)

#     df['time_check'] = np.where(df[cols].eq(
#         'NaT').any(1), False, True)

#     df['null'] = np.where(df[cols].isnull().any(1), False, True)

#     df["num_elems"] = df[['null', 'string', 'time_check']].sum(axis=1)

#     df = df.join(station_cast_col)

#     # Then drop rows where num_elems is 0
#     df = df.drop(df[df['num_elems'] == 0].index)

#     # # And drop num_elems column
#     df = df.drop(columns=['null', 'string', 'time_check', 'num_elems'])

#     df = df[orig_cols]

#     return df

#     # df = df.drop(df[df['empty'] == 'Y'].index)

#     # # And drop num_elems column
#     # df = df.drop(columns=['empty'])


# def remove_rows(nc):

#     # Once remove variables like profile that exist
#     # in all array values, can create array that
#     # holds Boolean values of whether all array
#     # values at a Level are empty
#     # And then remove these in Pandas
#     # Can I use np.where?

#     # Want to compare arrays for null values
#     # Since arrays could be of different
#     # length, create one padded with False and length N_LEVELS

#     nc_length = nc.dims['N_LEVELS']

#     # padded_array = np.pad(array, (0, width), mode='constant', constant_values=False)
#     # where array is the boolean array and width is nc_length - boolean_length

#     vars = nc.keys()

#     print(nc.sizes)

#     all_vars = []

#     for var in vars:

#         print(var)

#         is_null = np.isnan(nc[var])

#         is_empty_str = operator.eq(nc[var], '')

#         is_nat = operator.eq(nc[var], 'NaT')

#         is_empty_array = np.logical_or.reduce(
#             (is_null, is_empty_str, is_nat))

#         # padded_array = np.pad(array, (0, width), mode='constant', constant_values=False)
#         # where array is the boolean array and width is nc_length - boolean_length
#         padded_length = nc_length - np.size(is_empty_array)

#         padded_array = np.pad(
#             is_empty_array, (0, padded_length), mode='constant', constant_values=True)

#         all_vars.append(padded_array)

#     not_empty_arr = np.logical_not(np.logical_and.reduce(all_vars))

#     nc['not_empty'] = (['N_LEVELS'], not_empty_arr)

#     # Now trim each array where boolean True
#     nc = nc.where(nc['not_empty'], drop=True)

#     nc = nc.drop('not_empty')

#     return nc


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

    #expocode_list = [expocode]*coord_length

    def create_id(x, y):
        station = str(x).zfill(3)
        cast = str(y).zfill(3)
        return f"expo_{expocode}_sta_{station}_cast_{cast}"

    id = list(map(create_id, station_list, cast_list))

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
