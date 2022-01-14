# Process meta data

import xarray as xr
import numpy as np
import pandas as pd
import logging


from global_vars import GlobalVars


def get_cchdo_argovis_unit_name_mapping():

    return {
        'dbar': 'decibar',
        'degC': 'Celsius',
        'umol/kg': 'micromole/kg'
    }


def change_units_to_argovis(nc, cchdo_meta_mapping):

    # Rename units (no conversion)

    names = cchdo_meta_mapping['names']
    unit_name_mapping = get_cchdo_argovis_unit_name_mapping()
    cchdo_unit_names = list(unit_name_mapping.keys())

    # No saliniity in coordinates so don't have to
    # worry about units = 1 being salinity

    for var in names:

        # Change units if needed
        try:
            var_units = nc[var].attrs['units']
            if var_units in cchdo_unit_names and var_units != 1:
                nc[var].attrs['units'] = unit_name_mapping[var_units]
        except KeyError:
            pass

    return nc


def add_coord(nc, coord_length, coord_name, var):

    new_coord_list = [var]*coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords({coord_name: ('N_PROF', new_coord_np)})

    return nc


def add_cruise_meta(nc, cruise_meta):

    coord_length = nc.dims['N_PROF']

    expocode = cruise_meta['expocode']

    if '/' in expocode:
        expocode = expocode.replace('/', '_')
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"
    elif expocode == 'None':
        cruise_url = ''
    else:
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

    nc = add_coord(nc, coord_length, 'cruise_url', cruise_url)

    for key, value in cruise_meta.items():
        nc = add_coord(nc, coord_length, key, value)


def add_file_meta(nc, file_meta):

    # Add any file meta not in nc meta

    coord_length = nc.dims['N_PROF']

    # The expocode in a file can be different from the cruise page
    nc = nc.rename({'expocode':  'file_expocode'})


def add_station_cast(nc):

    # **************************************************
    # Create station_cast identifier will use in program
    # to keep track of profile groups.
    # Also used to create unique profile id
    # **************************************************

    # The station number is a string
    station_list = nc.coords['station'].values

   # cast_number is an integer
    cast_list = nc.coords['cast'].values

    # Add in station_cast var for later
    # processing of groups. But in
    # final JSON, it's dropped

    # lower case the station since BTL and CTD
    # could have the same station but won't compare
    # the same because case difference

    def create_station_cast(s, c):
        station = (str(s).zfill(3)).lower()
        cast = str(c).zfill(3)
        return f"{station}_{cast}"

    station_cast = list(
        map(create_station_cast, station_list, cast_list))

    nc = nc.assign_coords(station_cast=('N_PROF', station_cast))

    return nc


def add_extra_coords(nc, data_obj):

    filename = data_obj['filename']
    file_path = data_obj['file_path']
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

    new_coord_list = [file_path]*coord_length
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

    # Add in station_cast var for later
    # processing of groups. But in
    # final JSON, it's dropped

    # lower case the station since BTL and CTD
    # could have the same station but won't compare
    # the same because case difference

    def create_station_cast(s, c):
        station = (str(s).zfill(3)).lower()
        cast = str(c).zfill(3)
        return f"{station}_{cast}"

    station_cast = list(
        map(create_station_cast, station_list, cast_list))

    nc = nc.assign_coords(station_cast=('N_PROF', station_cast))

    # Create unique id
    def create_id(x, y):
        station = (str(x).zfill(3)).lower()
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
            nc[var] = nc[var].chunk({'N_PROF': GlobalVars.CHUNK_SIZE})
        except ValueError:
            # May not have N_PROF dims since value constant
            nc[var] = apply_c_format_no_dims(nc[var], f_format, dtype)

    return nc
