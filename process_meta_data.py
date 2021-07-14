# Process meta data

import numpy as np
import pandas as pd
import logging


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


def create_meta_profile(df_meta, meta_mapping_argovis):

    # With meta columns, pandas exploded them
    # for all levels. Only keep one Level
    # since they repeat
    logging.info('Get level = 0 meta rows')

    df_meta = df_meta[df_meta['N_LEVELS'] == 0]
    df_meta = df_meta.reset_index()
    df_meta = df_meta.drop('index', axis=1)
    df_meta = df_meta.drop('N_LEVELS', axis=1)
    df_meta = df_meta.compute()

    logging.info('create all_meta list')
    large_meta_dict = dict(tuple(df_meta.groupby('N_PROF')))

    all_meta = []
    all_meta_profiles = []
    for key, val_df in large_meta_dict.items():

        val_df = val_df.reset_index()
        station_cast = val_df['station_cast'].values[0]
        val_df = val_df.drop(['station_cast', 'N_PROF', 'index'],  axis=1)
        meta_dict = val_df.to_dict('records')[0]

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

    logging.info('start create all_meta_profiles')
    all_meta_profiles = []
    for obj in all_meta:

        meta_profile = {}
        meta_profile['station_cast'] = obj['station_cast']
        meta_profile['meta'] = obj['dict']

        all_meta_profiles.append(meta_profile)

    return all_meta_profiles


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


def add_extra_coords(nc, file_info):

    # Use cruise expocode because file one could be different than cruise page
    # Drop existing expocode
    # nc = nc.reset_coords(names=['expocode'], drop=True)

    nc = nc.rename({'expocode':  'file_expocode'})

    filename = file_info['filename']
    data_path = file_info['data_path']
    expocode = file_info['cruise_expocode']
    cruise_id = file_info['cruise_id']
    woce_lines = file_info['woce_lines']

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

    new_coord_list = ['cruise_id']*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(cruise_id=('N_PROF', new_coord_np))

    new_coord_list = ['woce_lines']*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(woce_lines=('N_PROF', new_coord_np))

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
