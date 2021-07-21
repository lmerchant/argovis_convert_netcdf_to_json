# Unit and Ref scale conversions

# To install gsw package
#
# Using conda with pyenv

# https://stackoverflow.com/questions/58044214/installing-anaconda-with-pyenv-unable-to-configure-virtual-environment

# Create empty conda environment and then add gsw
# conda create --name gsw
# conda activate gsw
# conda install -c conda-forge gsw

import logging

from pandas.core.dtypes.missing import isnull
import gsw
import xarray as xr
import numpy as np

import get_variable_mappings as gvm


# def convert_oxygen(nc, var, chunk_size):

#     # Convert ml/l to micromole/kg

#     # Need Absolute Salinity, g/kg
#     # SA = gsw.SA_from_SP(SP, p, lon, lat)
#     # SP = Practical Salinity (PSS-78), unitless

#     # then get the density
#     # rho = gsw.density.rho_t_exact(SA, t, p)

#     # where SA is Absolute Salinity, g/kg
#     # p is Sea pressure (absolute pressure minus 10.1325 dbar), dbar
#     # pressure is the "sea pressure", which is the absolute pressure minus the pressure of a standard atmosphere, which is 10.1325 dbars

#     # And finally,
#     # converted_oxygen = ((oxygen * 1000)/rho)/0.022403

#     # Matlab code from Sarah Purkey
#     # rho = sw_dens(salt(ok),temp(ok),0);
#     # o(ok) = ox(ok)*1e3./rho/.022403;
#     # Where ok are non nan values

#     oxy = nc[var]
#     oxy_dtype = nc[var].dtype

#     # Use ctd_salinity first (practical salinity)
#     # and if not exist, use bottle_salinity
#     if 'ctd_salinity' in nc.keys():
#         sal_pr = nc['ctd_salinity']
#         sal_ref_scale = sal_pr.attrs['reference_scale']
#     elif 'bottle_salinity' in nc.keys():
#         sal_pr = nc['bottle_salinity']
#         sal_ref_scale = sal_pr.attrs['reference_scale']
#     else:
#         logging.info("************************************")
#         logging.info("No salinity to convert oxygen units")
#         logging.info("************************************")
#         return nc

#     # Check if salinity ref scale is PSS-78
#     if sal_ref_scale != 'PSS-78':
#         return nc

#     # Use ctd_temperature first,  then ctd_temperature_68
#     # These have just been converted to the ITS-90 scale

#     if 'ctd_temperature' in nc.keys():
#         temp = nc['ctd_temperature']
#         temp_ref_scale = temp.attrs['reference_scale']
#     elif 'ctd_temperature_68' in nc.keys():
#         temp = nc['ctd_temperature_68']
#         temp_ref_scale = temp.attrs['reference_scale']
#     else:
#         logging.info("************************************")
#         logging.info("No ctd temp to convert oxygen units")
#         logging.info("************************************")
#         return nc

#     # Check if temperature ref scale is ITS-90
#     # Which it should be since did this conversion first
#     if temp_ref_scale != 'ITS-90':
#         return nc

#     pres = nc['pressure']
#     lat = nc['latitude']
#     lon = nc['longitude']

#     # If null argument, return NaN

#     def convert_units(oxy, temp, sal_pr, pres, lon, lat):
#         # args = [oxy, temp, sal_pr, pres, lon, lat]
#         # is_null = np.any(np.isnan(args))
#         # if is_null:
#         #     return np.nan

#         sal_abs = gsw.SA_from_SP(sal_pr, pres, lon, lat)
#         rho = gsw.density.rho_t_exact(sal_abs, temp, pres)
#         converted_oxygen = ((oxy * 1000)/rho)/0.022403
#         return converted_oxygen

#     def get_converted_oxy(oxy, temp, sal_pr, pres, lon, lat, oxy_dtype):
#         return xr.apply_ufunc(
#             convert_units,
#             oxy,
#             temp,
#             sal_pr,
#             pres,
#             lon,
#             lat,
#             input_core_dims=[['N_PROF', 'N_LEVELS'],
#                              ['N_PROF', 'N_LEVELS'], ['N_PROF', 'N_LEVELS'],
#                              ['N_PROF', 'N_LEVELS'], ['N_PROF'], ['N_PROF']],
#             output_core_dims=[['N_PROF', 'N_LEVELS']],
#             output_dtypes=[oxy_dtype],
#             keep_attrs=True,
#             dask="parallelized"
#         )

#     converted_oxygen = get_converted_oxy(oxy.chunk({"N_PROF": -1}),
#                                          temp.chunk({"N_PROF": -1}),
#                                          sal_pr.chunk({"N_PROF": -1}),
#                                          pres.chunk({"N_PROF": -1}),
#                                          lon.chunk({"N_PROF": -1}),
#                                          lat.chunk({"N_PROF": -1}),
#                                          oxy_dtype)

#     nc[var] = converted_oxygen.chunk(chunks={"N_PROF": chunk_size})
#     nc[var].attrs['units'] = 'micromole/kg'

#     return nc


def convert_oxygen(nc, var):

    # Convert ml/l to micromole/kg

    # Need Absolute Salinity, g/kg
    # SA = gsw.SA_from_SP(SP, p, lon, lat)
    # SP = Practical Salinity (PSS-78), unitless

    # then get the density
    # rho = gsw.density.rho_t_exact(SA, t, p)

    # where SA is Absolute Salinity, g/kg
    # p is Sea pressure (absolute pressure minus 10.1325 dbar), dbar
    # pressure is the "sea pressure", which is the absolute pressure minus the pressure of a standard atmosphere, which is 10.1325 dbars

    # And finally,
    # converted_oxygen = ((oxygen * 1000)/rho)/0.022403

    # Matlab code from Sarah Purkey
    # rho = sw_dens(salt(ok),temp(ok),0);
    # o(ok) = ox(ok)*1e3./rho/.022403;
    # Where ok are non nan values

    oxy = nc[var]
    oxy_dtype = nc[var].dtype

    # Use ctd_salinity first (practical salinity)
    # and if not exist, use bottle_salinity
    if 'ctd_salinity' in nc.keys():
        sal_pr = nc['ctd_salinity']
        sal_ref_scale = sal_pr.attrs['reference_scale']
    elif 'bottle_salinity' in nc.keys():
        sal_pr = nc['bottle_salinity']
        sal_ref_scale = sal_pr.attrs['reference_scale']
    else:
        logging.info("************************************")
        logging.info("No salinity to convert oxygen units")
        logging.info("************************************")
        return nc

    # Check if salinity ref scale is PSS-78
    if sal_ref_scale != 'PSS-78':
        return nc

    # Use ctd_temperature first,  then ctd_temperature_68
    # These have just been converted to the ITS-90 scale

    if 'ctd_temperature' in nc.keys():
        temp = nc['ctd_temperature']
        temp_ref_scale = temp.attrs['reference_scale']
    elif 'ctd_temperature_68' in nc.keys():
        temp = nc['ctd_temperature_68']
        temp_ref_scale = temp.attrs['reference_scale']
    else:
        logging.info("************************************")
        logging.info("No ctd temp to convert oxygen units")
        logging.info("************************************")
        return nc

    # Check if temperature ref scale is ITS-90
    # Which it should be since did this conversion first
    if temp_ref_scale != 'ITS-90':
        return nc

    pres = nc['pressure']
    lat = nc['latitude']
    lon = nc['longitude']

    def convert_units(oxy, temp, sal_pr, pres, lon, lat):

        sal_abs = gsw.SA_from_SP(sal_pr, pres, lon, lat)
        rho = gsw.density.rho_t_exact(sal_abs, temp, pres)
        converted_oxygen = ((oxy * 1000)/rho)/0.022403
        return converted_oxygen

    def get_converted_oxy(oxy, temp, sal_pr, pres, lon, lat, oxy_dtype):
        return xr.apply_ufunc(
            convert_units,
            oxy,
            temp,
            sal_pr,
            pres,
            lon,
            lat,
            input_core_dims=[['N_LEVELS'],
                             ['N_LEVELS'], ['N_LEVELS'],
                             ['N_LEVELS'], [], []],
            output_core_dims=[['N_LEVELS']],
            output_dtypes=[oxy_dtype],
            keep_attrs=True,
            # dask="parallelized"
        )

    converted_oxygen = get_converted_oxy(oxy,
                                         temp,
                                         sal_pr,
                                         pres,
                                         lon,
                                         lat,
                                         oxy_dtype)

    nc[var] = converted_oxygen
    nc[var].attrs['units'] = 'micromole/kg'

    return nc


def convert_goship_to_argovis_units(nc, chunk_size):

    params = nc.keys()

    # If goship units aren't the same as argovis units, convert
    # So far, just converting oxygen

    # required_units_per_elem = gvm.get_required_units()

    # # mapping of goship unit to argovis unit
    # unit_mapping = gvm.get_goship_argovis_unit_name_mapping()

    for var in params:

        try:
            var_goship_units = nc[var].attrs['units']
        except:
            continue

        if 'oxygen' in var and var_goship_units == 'ml/l':

            converted_groups = []

            for nc_group in nc.groupby('N_PROF'):
                nc_profile = nc_group[1]

                out = convert_oxygen(nc_profile, var)
                converted_groups.append(out)

            ds_grid = [converted_groups]

            nc = xr.combine_nested(
                ds_grid, concat_dim=["N_LEVELS", "N_PROF"], combine_attrs='identical')

    return nc


def convert_sea_water_temp(nc, var, var_goship_ref_scale, argovis_ref_scale):

    # Check sea_water_temperature to have goship_reference_scale be ITS-90

    if var_goship_ref_scale == 'IPTS-68' and argovis_ref_scale == 'ITS-90':

        # Convert to ITS-90 scal
        temperature = nc[var].data

        converted_temperature = temperature/1.00024

        # Set temperature value in nc because use it later to
        # create profile dict
        nc[var].data = converted_temperature
        nc[var].attrs['reference_scale'] = 'ITS-90'

    return nc


def convert_goship_to_argovis_ref_scale(nc):

    params = nc.keys()

    # If argo ref scale not equal to goship ref scale, convert

    # So far, it's only the case for temperature

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    argovis_ref_scale_per_type = gvm.get_argovis_reference_scale_per_type()

    for var in params:
        if 'temperature' in var:

            try:
                # Get goship reference scale of var
                var_goship_ref_scale = nc[var].attrs['reference_scale']

                argovis_ref_scale = argovis_ref_scale_per_type['temperature']
                is_same_scale = var_goship_ref_scale == argovis_ref_scale

                if not is_same_scale:
                    nc = convert_sea_water_temp(
                        nc, var, var_goship_ref_scale, argovis_ref_scale)
            except:
                pass

    return nc


def apply_conversions(nc, chunk_size):

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    # Converting to argovis ref scale if needed
    nc = convert_goship_to_argovis_ref_scale(nc)

    # Apply equations to convert units
    nc = convert_goship_to_argovis_units(nc, chunk_size)

    return nc
