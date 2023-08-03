# Process meta data

import xarray as xr
import numpy as np
import pandas as pd
import logging


from global_vars import GlobalVars


def add_coord(nc, coord_length, coord_name, var):
    new_coord_list = [var] * coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords({coord_name: ("N_PROF", new_coord_np)})

    return nc


# def add_cruise_meta(nc, cruise_meta):
#     coord_length = nc.dims["N_PROF"]

#     expocode = cruise_meta["expocode"]

#     if "/" in expocode:
#         expocode = expocode.replace("/", "_")
#         cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"
#     elif expocode == "None":
#         cruise_url = ""
#     else:
#         cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

#     nc = add_coord(nc, coord_length, "cruise_url", cruise_url)

#     for key, value in cruise_meta.items():
#         nc = add_coord(nc, coord_length, key, value)


# def add_file_meta(nc, file_meta):
#     # Add any file meta not in nc meta

#     coord_length = nc.dims["N_PROF"]

#     # The expocode in a file can be different from the cruise page
#     nc = nc.rename({"expocode": "file_expocode"})


def add_station_cast(nc):
    # **************************************************
    # Create station_cast identifier will use in program
    # to keep track of profile groups.
    # Also used to create unique profile id
    # **************************************************

    # The station number is a string
    station_list = nc.coords["station"].values

    # cast_number is an integer
    cast_list = nc.coords["cast"].values

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

    station_cast = list(map(create_station_cast, station_list, cast_list))

    nc = nc.assign_coords(station_cast=("N_PROF", station_cast))

    return nc


class FormatFloat(float):
    def __format__(self, format_spec):
        return "nan" if pd.isnull(self) else float.__format__(self, format_spec)


def apply_c_format_meta(nc, meta_mapping):
    # apply c_format to float values to limit
    # number of decimal places in the final JSON format

    float_types = ["float64", "float32"]

    c_format_mapping = meta_mapping["c_format"]
    dtype_mapping = meta_mapping["dtype"]

    float_vars = [name for name, dtype in dtype_mapping.items() if dtype in float_types]

    c_format_vars = [name for name in c_format_mapping.keys() if name in float_vars]

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
            x.chunk({"N_PROF": -1}),
            f_format,
            input_core_dims=[["N_PROF"], []],
            output_core_dims=[["N_PROF"]],
            output_dtypes=[dtype],
            keep_attrs=True,
            dask="parallelized",
        )

    def apply_c_format_no_dims(x, f_format, dtype):
        return xr.apply_ufunc(
            apply_c_format,
            x,
            f_format,
            input_core_dims=[[], []],
            output_core_dims=[[]],
            output_dtypes=[dtype],
            keep_attrs=True,
        )

    for var in c_format_vars:
        c_format = c_format_mapping[var]
        f_format = c_format.lstrip("%")
        dtype = dtype_mapping[var]

        try:
            nc[var] = apply_c_format_xr(nc[var], f_format, dtype)
            nc[var] = nc[var].chunk({"N_PROF": GlobalVars.CHUNK_SIZE})
        except ValueError:
            # May not have N_PROF dims since value constant
            nc[var] = apply_c_format_no_dims(nc[var], f_format, dtype)

    return nc
