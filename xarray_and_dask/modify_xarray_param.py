import xarray as xr
import pandas as pd
import numpy as np

from global_vars import GlobalVars


def get_goship_argovis_unit_name_mapping():

    return {
        'dbar': 'decibar',
        'degC': 'Celsius',
        'umol/kg': 'micromole/kg'
    }


def change_units_to_argovis(nc):

    # Rename units (no conversion)

    unit_name_mapping = get_goship_argovis_unit_name_mapping()
    goship_unit_names = unit_name_mapping.keys()

    # Get reference scale to determine if salinity because there
    # can be a goship unit of '1' that is not salinity
    salinity_ref_scale = 'PSS-78'

    for var in nc.keys():

        # Change salinity  unit
        try:
            var_ref_scale = nc[var].attrs['reference_scale']
            var_units = nc[var].attrs['units']

            if var_ref_scale == salinity_ref_scale and var_units == '1':
                nc[var].attrs['units'] = 'psu'
        except KeyError:
            pass

        # Change other units
        try:
            var_units = nc[var].attrs['units']
            if var_units in goship_unit_names and var_units != 1:
                nc[var].attrs['units'] = unit_name_mapping[var_units]
        except KeyError:
            pass

    return nc


class FormatFloat(float):
    def __format__(self, format_spec):
        return 'nan' if pd.isnull(self) else float.__format__(self, format_spec)


# not used
# def apply_c_format_param(nc, param_mapping):

#     float_types = ['float64', 'float32']

#     c_format_mapping = param_mapping['c_format']
#     dtype_mapping = param_mapping['dtype']

#     float_vars = [name for name,
#                   dtype in dtype_mapping.items() if dtype in float_types]

#     c_format_vars = [
#         name for name in c_format_mapping.keys() if name in float_vars]

#     def format_float(num, f_format):
#         return float(f"{FormatFloat(num):{f_format}}")

#     def apply_c_format(var, f_format):
#         vfunc = np.vectorize(format_float)
#         return vfunc(var, f_format)

#     def apply_c_format_xr(x, f_format, dtype):
#         return xr.apply_ufunc(
#             apply_c_format,
#             x,
#             f_format,
#             input_core_dims=[['N_PROF', 'N_LEVELS'], []],
#             output_core_dims=[['N_PROF', 'N_LEVELS']],
#             output_dtypes=[dtype],
#             keep_attrs=True
#         )

#     for var in c_format_vars:
#         c_format = c_format_mapping[var]
#         f_format = c_format.lstrip('%')
#         dtype = dtype_mapping[var]
#         nc[var] = apply_c_format_xr(nc[var], f_format, dtype)

#     return nc


def apply_c_format_param(nc, param_mapping):

    float_types = ['float64', 'float32']

    c_format_mapping = param_mapping['c_format']
    dtype_mapping = param_mapping['dtype']

    float_vars = [name for name,
                  dtype in dtype_mapping.items() if dtype in float_types]

    c_format_vars = [
        name for name in c_format_mapping.keys() if name in float_vars]

    def format_float(num, f_format):
        return float(f"{FormatFloat(num):{f_format}}")

    def apply_c_format(var, f_format):
        vfunc = np.vectorize(format_float)
        return vfunc(var, f_format)

    def apply_c_format_xr(x, f_format, dtype):
        return xr.apply_ufunc(
            apply_c_format,
            x.chunk({'N_PROF': -1}),
            f_format,
            input_core_dims=[['N_PROF', 'N_LEVELS'], []],
            output_core_dims=[['N_PROF', 'N_LEVELS']],
            output_dtypes=[dtype],
            keep_attrs=True,
            dask="parallelized"
        )

    for var in c_format_vars:
        c_format = c_format_mapping[var]
        f_format = c_format.lstrip('%')
        dtype = dtype_mapping[var]
        nc[var] = apply_c_format_xr(nc[var], f_format, dtype)
        nc[var] = nc[var].chunk({'N_PROF': GlobalVars.CHUNK_SIZE})

    return nc


def add_qc_if_no_temp_qc(nc):

    # Now check so see if there is a ctd temperature  column and a corresponding
    # qc column. If not, add a ctd temperature qc column with values np.nan first
    # and later set = 0. Do np.nan first to make it easier to remove rows
    # with all nan values including np.nan
    is_ctd_temperature = any(
        [True if key == 'ctd_temperature' else False for key in nc.keys()])

    is_ctd_temperature_68 = any(
        [True if key == 'ctd_temperature_68' else False for key in nc.keys()])

    if is_ctd_temperature and is_ctd_temperature_68:
        temperature_var = 'ctd_temperature'
    elif is_ctd_temperature and not is_ctd_temperature_68:
        temperature_var = 'ctd_temperature'
    elif not is_ctd_temperature and is_ctd_temperature_68:
        temperature_var = 'ctd_temperature_68'
    else:
        temperature_var = ''

    qc_name = f"{temperature_var}_qc"

    has_ctd_temp_qc = qc_name in nc.keys()

    if temperature_var and not has_ctd_temp_qc and qc_name != '_qc':
        temp_shape = np.shape(nc[temperature_var])
        shape = np.transpose(temp_shape)
        temp_qc = np.empty(shape)
        temp_qc[:] = np.nan
        nc[qc_name] = (['N_PROF', 'N_LEVELS'], temp_qc)

    return nc