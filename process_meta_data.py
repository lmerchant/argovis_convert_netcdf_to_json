# Process meta data

import xarray as xr
import numpy as np
import pandas as pd
import logging
import time


import get_variable_mappings as gvm


def create_meta_profile(ddf_meta):

    # With meta columns, pandas exploded them
    # for all levels. Only keep one Level
    # since they repeat
    logging.info('Get level = 0 meta rows')

    # N_LEVELS is an index
    ddf_meta = ddf_meta[ddf_meta['N_LEVELS'] == 0]

    ddf_meta = ddf_meta.drop('N_LEVELS', axis=1)

    df_meta = ddf_meta.compute()

    logging.info('create all_meta list')
    large_meta_dict = dict(tuple(df_meta.groupby('N_PROF')))

    all_meta_profiles = []
    for key, val_df in large_meta_dict.items():

        station_cast = val_df['station_cast'].values[0]
        val_df = val_df.drop(['station_cast', 'N_PROF'],  axis=1)

        meta_dict = val_df.to_dict('records')[0]

        lat = meta_dict['lat']
        lon = meta_dict['lon']

        geo_dict = create_geolocation_dict(lat, lon)
        meta_dict['geoLocation'] = geo_dict

        meta_obj = {}
        meta_obj['station_cast'] = station_cast
        meta_obj['meta'] = meta_dict

        all_meta_profiles.append(meta_obj)

    # all_meta_profiles = []
    # for obj in all_meta:

    #     meta_profile = {}
    #     meta_profile['station_cast'] = obj['station_cast']
    #     meta_profile['meta'] = obj['dict']

    #     all_meta_profiles.append(meta_profile)

    # print('*add meta profiles *')
    # print(all_meta_profiles)

    return all_meta_profiles


def change_units_to_argovis(nc):

    # Rename units (no conversion)

    unit_name_mapping = gvm.get_goship_argovis_unit_name_mapping()
    goship_unit_names = unit_name_mapping.keys()

    # No saliniity in coordinates so don't have to
    # worry about units = 1 being salinity

    for var in nc.coords:

        # Change units if needed
        try:
            var_units = nc[var].attrs['units']
            if var_units in goship_unit_names and var_units != 1:
                nc[var].attrs['units'] = unit_name_mapping[var_units]
        except KeyError:
            pass

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

    return geo_dict


def drop_coords(nc):

    # Drop profile_type and instrument_id and geometry_container if exist

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


def add_extra_coords(nc, data_obj):

    filename = data_obj['filename']
    data_path = data_obj['data_path']
    expocode = data_obj['cruise_expocode']
    cruise_id = data_obj['cruise_id']
    woce_lines = data_obj['woce_lines']
    file_hash = data_obj['file_hash']

    # *************************************************
    # Use the cruise expocode rather than file expocode
    # because cruise expocode points to the cruise page
    # and the file expode can be different than the cruise value
    # *************************************************

    nc = nc.rename({'expocode':  'file_expocode'})

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

    # *********************************************
    # Add coords file_hash, cruise_id, woce_lines,
    # POSITIONING_SYSTEM, DATA_CENTRE, cruise_url
    # netcdf_url, data_filename, expocode
    # *********************************************

    coord_length = nc.dims['N_PROF']

    new_coord_list = [file_hash]*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(file_hash=('N_PROF', new_coord_np))

    new_coord_list = [cruise_id]*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords(cruise_id=('N_PROF', new_coord_np))

    new_coord_list = [woce_lines]*coord_length
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

    # **************************************************
    # Create station_cast identifier will use in program
    # to keep track of profile groups.
    # Also used to create unique profile id
    # **************************************************

    # The station number is a string
    station_list = nc.coords['station'].values

   # cast_number is an integer
    cast_list = nc.coords['cast'].values

    # The file expocode is a string
    file_expocode_list = nc.coords['file_expocode'].values

    # Add in station_cast var for later
    # processing of groups. But in
    # final JSON, it's dropped

    def create_station_cast(s, c):
        station = str(s).zfill(3)
        cast = str(c).zfill(3)
        return f"{station}_{cast}"

    # def create_file_expo_station_cast(e, s, c):
    #     station = str(s).zfill(3)
    #     cast = str(c).zfill(3)
    #     return f"{e}_{station}_{cast}"

    # expo_station_cast = list(
    #     map(create_file_expo_station_cast, file_expocode_list, station_list, cast_list))

    station_cast = list(
        map(create_station_cast, station_list, cast_list))

    nc = nc.assign_coords(station_cast=('N_PROF', station_cast))

    # Create unique id
    def create_id(x, y):
        station = str(x).zfill(3)
        cast = str(y).zfill(3)
        return f"expo_{expocode}_sta_{station}_cast_{cast}"

    id = list(map(create_id, station_list, cast_list))

    nc = nc.assign_coords(id=('N_PROF', id))
    nc = nc.assign_coords(_id=('N_PROF', id))

    # **********************************
    # Use dates instead of time variable
    # **********************************

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

    # Drop time for ArgoVis. Wiil be using date value instead
    nc = nc.drop('time')

    # **************************************
    # Create ArgoVis lat/lon extra meta data
    # **************************************

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


class FormatFloat(float):
    def __format__(self, format_spec):
        return 'nan' if pd.isnull(self) else float.__format__(self, format_spec)


def apply_c_format_meta(nc, meta_mapping):

    # apply c_format to float values to limit
    # number of decimal places in the final JSON format

    float_types = ['float64', 'float32']

    c_format_mapping = meta_mapping['c_format']
    dtype_mapping = meta_mapping['dtype']

    float_vars = [name for name,
                  dtype in dtype_mapping.items() if dtype in float_types]

    c_format_vars = [
        name for name in c_format_mapping.keys() if name in float_vars]

    def format_float(val, f_format):
        return float(f"{FormatFloat(val):{f_format}}")

    # TODO
    # Do I need to vectorize?
    def apply_c_format(var, f_format):
        vfunc = np.vectorize(format_float)
        return vfunc(var, f_format)

    def apply_c_format_xr(x, f_format, dtype):
        return xr.apply_ufunc(
            apply_c_format,
            x,
            f_format,
            input_core_dims=[[], []],
            output_core_dims=[[]],
            output_dtypes=[dtype],
            keep_attrs=True
        )

    for var in c_format_vars:
        c_format = c_format_mapping[var]
        f_format = c_format.lstrip('%')
        dtype = dtype_mapping[var]
        nc[var] = apply_c_format_xr(nc[var], f_format, dtype)

    return nc


def apply_c_format_meta_dask(nc,  chunk_size, meta_mapping):

    # apply c_format to float values to limit
    # number of decimal places in the final JSON format

    float_types = ['float64', 'float32']

    c_format_mapping = meta_mapping['c_format']
    dtype_mapping = meta_mapping['dtype']

    float_vars = [name for name,
                  dtype in dtype_mapping.items() if dtype in float_types]

    c_format_vars = [
        name for name in c_format_mapping.keys() if name in float_vars]

    def format_float(val, f_format):
        return float(f"{FormatFloat(val):{f_format}}")

    # TODO
    # Do I need to vectorize?
    def apply_c_format(var, f_format):
        vfunc = np.vectorize(format_float)
        return vfunc(var, f_format)

    def apply_c_format_xr(x, f_format, dtype):
        return xr.apply_ufunc(
            apply_c_format,
            x.chunk({'N_PROF': -1}),
            f_format,
            input_core_dims=[['N_PROF'], []],
            output_core_dims=[['N_PROF']],
            output_dtypes=[dtype],
            keep_attrs=True,
            dask="parallelized"
        )

    def apply_c_format_no_dims(x, f_format, dtype):
        return xr.apply_ufunc(
            apply_c_format,
            x,
            f_format,
            input_core_dims=[[], []],
            output_core_dims=[[]],
            output_dtypes=[dtype],
            keep_attrs=True
        )

    for var in c_format_vars:
        c_format = c_format_mapping[var]
        f_format = c_format.lstrip('%')
        dtype = dtype_mapping[var]

        try:
            nc[var] = apply_c_format_xr(nc[var], f_format, dtype)
            nc[var] = nc[var].chunk({'N_PROF': chunk_size})
        except ValueError:
            # May not have N_PROF dims since value constant
            nc[var] = apply_c_format_no_dims(nc[var], f_format, dtype)

    return nc
