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
import gsw
import xarray as xr
import numpy as np


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


def get_argovis_reference_scale_per_type():

    return {
        'temperature': 'ITS-90',
        'salinity': 'PSS-78'
    }


def get_qc_scale_for_oxygen_conversion(nc, var):

    # TODO

    # If the temp_qc is bad, use in calculation but set qc
    # of that oxygen value to qc of temperature. Can't
    # convert an oxygen with a bad temperature qc. Same
    # for the salinity.

    # Creat a new col where Oxygen qc = 2 if
    # temp qc = 0 or 2, the ctd_salinity qc = 2,
    # and the oxygen qc = 2

    # Create new column and set good qc to True
    # and bad qc to False. Then using the boolean of
    # these 3 columns, get another column. Set
    # True to qc = 2 and False to qc = 4 (bad meas). Then delete
    # the oxygen qc and rename this column as the
    # oxygen qc. Drop the 3 true/false columns

    # Can I do this within the xarray object or do
    # I need to convert to a dask dataframe and back.
    # This is already just one N_PROF group.
    # Could work with numpy arrays of the qc vars.

    if f"{var}_qc" in nc.keys():
        oxy_qc = nc[f"{var}_qc"]
    else:
        # TODO, check if this matches dims of
        # oxygen
        # Set ctd_oxygen_qc to array of length N_LEVELS
        # with qc=2 value
        length = nc.dims['N_LEVELS']
        oxy_qc = np.full((1, length), 2.0)

    if 'ctd_salinity_qc' in nc.keys():
        sal_qc = nc['ctd_salinity_qc']
    elif 'bottle_salinity_qc' in nc.keys():
        sal_qc = nc['bottle_salinity_qc']
    else:
        # TODO, check if this matches dims of
        # salinity
        # Set ctd_salinity_qc to array of length N_LEVELS
        # with qc=2 value
        N_LEVELS, N_PROF = nc.dims
        sal_qc = np.full((1, N_LEVELS), 2.0)

    if 'ctd_temperature_qc' in nc.keys():
        temp_qc = nc['ctd_temperature_qc']
    elif 'ctd_temperature_68_qc' in nc.keys():
        temp_qc = nc['ctd_temperature_68_qc']
    else:
        # TODO, check if this matches dims of
        # temperature
        # Set ctd_temperature_qc to array of length N_LEVELS
        # with qc=2 value
        N_LEVELS, N_PROF = nc.dims
        temp_qc = np.full((1, N_LEVELS), 2.0)

    # TODO
    # This doesn't give me horiz concat
    qc_comb_array = np.column_stack([oxy_qc, sal_qc, temp_qc])

    qc_comb_array = np.where(qc_comb_array != 2.0, 0, qc_comb_array)
    qc_comb_array = np.where(qc_comb_array == 2.0, 1, qc_comb_array)

    # Sum values row by row. If get 3, they are all true and so a good qc = 2
    qc_array = np.sum(qc_comb_array, axis=1)

    # Now set the sum values back  to qc values
    qc_array = np.where(qc_array != 3, 4, qc_array)
    qc_array = np.where(qc_array == 3, 2, qc_array)

    # Combine qc_array with oxy_qc array, If
    # oxy_qc = 2, and qc_array = 4, change oxy_qc to 4,
    # otherwise, leave oxy qc alone.
    c = np.column_stack([oxy_qc, qc_array])
    c[:, 0] = np.where((c[:, 0] == 2) & (c[:, 1] == 4), 4, c[:, 0])

    oxy_qc = c[:, 0].T

    # Xarray doesn't know what I mean to assign a new
    # variable when just working with one dimension here
    # Try returnin oxy_qc, building it up and then assiging
    # if 'ctd_oxygen_ml_l_qc' in nc.keys():
    #     nc['ctd_oxygen_ml_l_qc'] = oxy_qc
    # else:
    #     # nc.assign(ctd_oxygen_ml_l_qc=oxy_qc)
    #     pass

    # return nc

    return oxy_qc


def convert_units(oxy, temp, sal_pr, pres, lon, lat):

    # How about if temp, sal, not a qc =0 or 2, set
    # oxy value to NaN?

    # Would have to submit temp_qc and sal_pr_qc cols
    # Would NaN values mess up the calculation and
    # result in an error?

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


def get_temp_and_salinity(nc):

    # Use ctd_salinity first (practical salinity)
    # and if not exist, use bottle_salinity
    # TODO
    # make sure there are at least qc=2 in the ctd_salinity,
    # otherwise use bottle_salinity assuming it has some
    # qc = 2, otherwise can't convert. What
    # to do with naming the var doxy then? Would have to
    # rename if doxy_ml_l

    missing_var_flag = False

    if 'ctd_salinity' in nc.keys():
        sal_pr = nc['ctd_salinity']
        sal_ref_scale = sal_pr.attrs['reference_scale']

        # If sal_pr_qc is != 0 or != 2, set sal_pr to NaN
        # Have a temporary var sal_pr so don't overwrite ctd_salinity

        sal_pr_values = sal_pr.values
        nc.assign({'sal_pr': sal_pr_values})

        if 'ctd_salinity_qc' in nc.keys():

            # replace all values not equal to 0 or 2 with np.nan
            xr.where(nc['ctd_salinity_qc'] != 0 &
                     nc['ctd_salinity_qc'] != 2, np.nan, nc['sal_pr'])

            sal_pr = nc['sal_pr']

    elif 'bottle_salinity' in nc.keys():
        sal_pr = nc['bottle_salinity']
        sal_ref_scale = sal_pr.attrs['reference_scale']

        # If sal_pr_qc is != 0 or != 2, set sal_pr to NaN
        # Have a temporary var sal_pr so don't overwrite ctd_salinity

        sal_pr_values = sal_pr.values
        nc.assign({'sal_pr': sal_pr_values})

        if 'bottle_salinity_qc' in nc.keys():

            # replace all values not equal to 0 or 2 with np.nan
            xr.where(nc['bottle_salinity_qc'] != 0 &
                     nc['bottle_salinity_qc'] != 2, np.nan, nc['sal_pr'])

            sal_pr = nc['sal_pr']

    else:
        logging.info("************************************")
        logging.info("No salinity to convert oxygen units")
        logging.info("************************************")
        missing_var_flag = True

    # Check if salinity ref scale is PSS-78
    if sal_ref_scale != 'PSS-78':
        missing_var_flag = True

    # Use ctd_temperature first,  then ctd_temperature_68
    # These have just been converted to the ITS-90 scale

    if 'ctd_temperature' in nc.keys():
        temp = nc['ctd_temperature']
        temp_ref_scale = temp.attrs['reference_scale']

        # If temp_qc is != 0 or != 2, set temp to NaN
        # Have a temporary var temp so don't overwrite ctd_salinity

        temp_values = temp.values
        nc.assign({'temp': temp_values})

        if 'ctd_temperature_qc' in nc.keys():
            # replace all values not equal to 0 or 2 with np.nan
            xr.where(nc['ctd_temperature_qc'] != 0 &
                     nc['ctd_temperature_qc'] != 2, np.nan, nc['temp'])

            temp = nc['temp']

    elif 'ctd_temperature_68' in nc.keys():
        temp = nc['ctd_temperature_68']
        temp_ref_scale = temp.attrs['reference_scale']

        # If temp_qc is != 0 or != 2, set temp to NaN
        # Have a temporary var temp so don't overwrite ctd_salinity

        temp_values = temp.values
        nc.assign({'temp': temp_values})

        if 'ctd_temperature_68_qc' in nc.keys():
            # replace all values not equal to 0 or 2 with np.nan
            xr.where(nc['ctd_temperature_68_qc'] != 0 &
                     nc['ctd_temperature_68_qc'] != 2, np.nan, nc['temp'])

            temp = nc['temp']

    else:
        # This shouldn't happen since checked for required
        # CTD vars, ctd temp with ref scale, at the start
        # of the program, and excluded files if there
        # was no ctd temp with a ref scale.
        logging.info("************************************")
        logging.info("No ctd temp to convert oxygen units")
        logging.info("************************************")
        missing_var_flag = True

    # Check if temperature ref scale is ITS-90
    # Which it should be since did this conversion first
    if temp_ref_scale != 'ITS-90':
        missing_var_flag = True

    # Now drop these temporary vars from nc
    nc = nc.drop_vars(['temp'])
    nc = nc.drop_vars(['sal_pr'])

    return temp, sal_pr, missing_var_flag


def convert_oxygen(nc, var):

    # TODO
    # If the temp_qc is bad, use in calculation but set qc
    # of that oxygen value to qc of temperature. Can't
    # convert an oxygen with a bad temperature qc. Same
    # for the salinity.

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

    temp, sal_pr, missing_var_flag = get_temp_and_salinity(nc)

    if missing_var_flag:
        logging.info("Missing temp or sal needed to do Oxygen conversion")
        logging.info(f"Didn't convert {var}")
        return nc

    pres = nc['pressure']
    lat = nc['latitude']
    lon = nc['longitude']

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


def convert_oxygen_to_new_units(nc, var):

    converted_groups = []

    all_oxy_qc = []
    for nc_group in nc.groupby('N_PROF'):

        nc_profile = nc_group[1]

        out = convert_oxygen(nc_profile, var)

        converted_groups.append(out)

        oxy_qc = get_qc_scale_for_oxygen_conversion(out, var)

        # This stacks downward
        # Compare to oxy_qc before changes
        all_oxy_qc = np.vstack((all_oxy_qc, oxy_qc))

    ds_grid = [converted_groups]

    nc = xr.combine_nested(
        ds_grid, concat_dim=["N_LEVELS", "N_PROF"], combine_attrs='identical')

    # TODO
    # Check this
    # look at nc and extract out oxygen
    # see if nan values match up with qc
    # Or maybe don't even add a qc column?

    if f"{var}_qc" in nc.keys():
        nc[f"{var}_qc"] = all_oxy_qc
    else:
        # Do I need to provide coords and dims?
        nc = nc.assign({f"{var}_qc": all_oxy_qc})

    return nc


def convert_goship_to_argovis_units(nc):

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
            nc = convert_oxygen_to_new_units(nc, var)

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

    argovis_ref_scale_per_type = get_argovis_reference_scale_per_type()

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


def apply_conversions(nc):

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    # Converting to argovis ref scale if needed
    nc = convert_goship_to_argovis_ref_scale(nc)

    # Apply equations to convert units
    nc = convert_goship_to_argovis_units(nc)

    return nc
