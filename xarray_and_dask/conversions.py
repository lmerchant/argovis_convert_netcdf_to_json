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

    # If the temp_qc is bad, use in calculation but set qc
    # of that oxygen value to qc of temperature. Can't
    # convert an oxygen with a bad temperature qc. Same
    # for the salinity.

    # TODO
    # summarize possible combinations of good/bad vars
    # and ask Sarah about doing calulation with bad vals
    # and what flag to use. Do I do calc or skip it?

    if f"{var}_qc" in nc.keys():
        oxy_qc = nc[f"{var}_qc"]
    else:
        oxy_qc = []
        # shape = list(nc[var].shape)
        # oxy_qc = np.empty(shape)
        # oxy_qc[:] = 2.0
        # qc_var = {f"{var}_qc": ('N_LEVELS', oxy_qc)}
        # nc = nc.assign(qc_var)

    if 'ctd_salinity_qc' in nc.keys():
        sal_qc = nc['ctd_salinity_qc'].values

    elif 'bottle_salinity_qc' in nc.keys() and not 'ctd_salinity_qc' in nc.keys():
        sal_qc = nc['bottle_salinity_qc'].values
    else:
        sal_qc = []

    if 'ctd_temperature_qc' in nc.keys():
        temp_qc = nc['ctd_temperature_qc'].values
    elif 'ctd_temperature_68_qc' in nc.keys():
        temp_qc = nc['ctd_temperature_68_qc'].values
    else:
        temp_qc = []

    # Now vertically stack these rows together to
    # find what the combined qc is.
    # Then will take the combined temp qc and sal qc
    # and modify the oxy qc since the temp and sal go into
    # the calculation to convert oxy.

    if len(sal_qc) and len(temp_qc):
        qc_comb_array = np.column_stack([sal_qc, temp_qc])
    elif len(sal_qc) and not len(temp_qc):
        qc_comb_array = sal_qc
    elif not len(sal_qc) and len(temp_qc):
        qc_comb_array = temp_qc
    else:
        return []

    if len(sal_qc) and len(temp_qc):

        qc_comb_array = np.column_stack([sal_qc, temp_qc])

        # If any qc != 2 or !=0 and not nan, replace with -1
        # For any qc == 2 or == 0, replace with 1
        qc_comb_array = np.where(((qc_comb_array != 2.0) & (qc_comb_array != 0)) &
                                 (np.isfinite(qc_comb_array)), -1, qc_comb_array)

        qc_comb_array = np.where(
            ((qc_comb_array == 2.0) | (qc_comb_array == 0)), 1, qc_comb_array)

        # Sum values row by row.
        qc_array = np.sum(qc_comb_array, axis=1)

        # For stacked rows, if the sum = num_stacks,
        # they are all true and so a good qc = 2
        num_stacks = np.shape(qc_comb_array)[1]

        # Now set the sum values back to qc values where
        # 2.0 is good and 4.0 is bad
        qc_array = np.where((qc_array != num_stacks) & (
            np.isfinite(qc_array)), 4.0, qc_array)
        qc_array = np.where(qc_array == num_stacks, 2.0, qc_array)

    else:
        qc_array = qc_comb_array

    # Combine qc_array with oxy_qc array, If
    # oxy_qc = 2, and qc_array = 4, change oxy_qc to 4,
    # otherwise, leave oxy qc alone.
    c = np.column_stack([oxy_qc, qc_array])
    c[:, 0] = np.where((c[:, 0] == 2) & (c[:, 1] == 4), 4, c[:, 0])

    oxy_qc = c[:, 0].T

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

    # TODO
    # What is procedure for converting oxygen when all ctd_sal
    # is bad but some btl_sal are good. Do I keep the oxy uncoverted val
    # if sal is null? What flag to use if set oxygen to null?

    missing_var_flag = False
    sal_ref_scale = None
    temp_ref_scale = None

    found_good_ctd_sal = False
    found_good_btl_sal = False

    if 'ctd_salinity' in nc.keys():

        # Have a temporary var sal_pr so don't overwrite ctd_salinity

        shape = list(nc['ctd_salinity'].shape)
        nan_array = np.empty(shape)
        nan_array[:] = np.nan

        if 'ctd_salinity_qc' in nc.keys():

            # replace all values not equal to 0 or 2 with np.nan
            nc['sal_pr'] = xr.where((nc['ctd_salinity_qc'] != 0) &
                                    (nc['ctd_salinity_qc'] != 2.0), nan_array, nc['ctd_salinity'])

            # ctd salinity is all bad so can't do conversion,
            # try checking bottle salinity then

            sal_pr = nc['sal_pr']
            sal_ref_scale = nc['ctd_salinity'].attrs['reference_scale']

            # Check if sal_pr is all null. If it is, found no good values
            all_nan = np.isnan(sal_pr).all()

            if not all_nan:
                found_good_ctd_sal = True

                ctd_sal_pr = sal_pr

    if 'bottle_salinity' in nc.keys():

        # Have a temporary var sal_pr so don't overwrite ctd_salinity

        shape = list(nc['bottle_salinity'].shape)
        nan_array = np.empty(shape)
        nan_array[:] = np.nan

        if 'bottle_salinity_qc' in nc.keys():

            # replace all values not equal to 0 or 2 with np.nan
            nc['sal_pr'] = xr.where((nc['bottle_salinity_qc'] != 0) &
                                    (nc['bottle_salinity_qc'] != 2.0), nan_array, nc['bottle_salinity'])

            sal_pr = nc['sal_pr']
            sal_ref_scale = nc['bottle_salinity'].attrs['reference_scale']

            # Check if sal_pr is all null. If it is, found no good values
            all_nan = np.isnan(sal_pr).all()

            if not all_nan:
                found_good_btl_sal = True

            btl_sal_pr = sal_pr

    if found_good_ctd_sal:
        sal_pr = ctd_sal_pr

    elif not found_good_ctd_sal and found_good_btl_sal:
        # use btl salinity for conversion
        # TODO
        # ask if this is OK
        # save list of files where this is ever the case
        sal_pr = btl_sal_pr

    else:
        logging.info("************************************")
        logging.info("No salinity to convert oxygen units")
        logging.info("************************************")

        # TODO
        # So keep oxygen same units instead of set all to null

        # Save these to a file to look at

        sal_pr = np.nan
        missing_var_flag = True

    # Check if salinity ref scale is PSS-78
    if sal_ref_scale != 'PSS-78':
        missing_var_flag = True

    # ----------------
    # Get temperature
    # ----------------

    # Use ctd_temperature first,  then ctd_temperature_68
    # These have just been converted to the ITS-90 scale

    found_good_ctd_temp = False
    found_good_temp_68 = False

    if 'ctd_temperature' in nc.keys():

        # Have a temporary var temp so don't overwrite ctd_temperature

        shape = list(nc['ctd_temperature'].shape)
        nan_array = np.empty(shape)
        nan_array[:] = np.nan

        if 'ctd_temperature_qc' in nc.keys():

            # replace all values not equal to 0 or 2 with np.nan
            nc['temp'] = xr.where((nc['ctd_temperature_qc'] != 0) &
                                  (nc['ctd_temperature_qc'] != 2.0), nan_array, nc['ctd_temperature'])

            temp = nc['temp']
            temp_ref_scale = nc['ctd_temperature'].attrs['reference_scale']

            # Check if sal_pr is all null. If it is, found no good values
            all_nan = np.isnan(temp).all()

            if not all_nan:
                found_good_ctd_temp = True
                ctd_temp = temp

    if 'ctd_temperature_68' in nc.keys():

        # Have a temporary var temp so don't overwrite ctd_temperature

        shape = list(nc['ctd_temperature_68'].shape)
        nan_array = np.empty(shape)
        nan_array[:] = np.nan

        if 'ctd_temperature_68_qc' in nc.keys():

            # replace all values not equal to 0 or 2 with np.nan
            nc['temp'] = xr.where((nc['ctd_temperature_68_qc'] != 0) &
                                  (nc['ctd_temperature_68_qc'] != 2.0), nan_array, nc['ctd_temperature_68'])

            temp = nc['temp']
            temp_ref_scale = nc['ctd_temperature_68'].attrs['reference_scale']

            if not all_nan:
                found_good_temp_68 = True
                temp_68 = temp

    if found_good_ctd_temp:
        temp = ctd_temp

    elif not found_good_ctd_temp and found_good_temp_68:
        # TODO
        # ask if this is OK
        # But do I have a ctd_temperature and
        # ctd_temperature_68 at the same time?
        # save list of files where this is ever the case
        temp = temp_68
    else:
        # This shouldn't happen since checked for required
        # CTD vars, ctd temp with ref scale, at the start
        # of the program, and excluded files if there
        # was no ctd temp with a ref scale.
        logging.info("************************************")
        logging.info("No ctd temp to convert oxygen units")
        logging.info("************************************")

        # TODO
        # So keep oxygen same units instead of set all to null

        missing_var_flag = True
        temp = np.nan

    # Check if temperature ref scale is ITS-90
    # Which it should be since did this conversion first
    if temp_ref_scale != 'ITS-90':
        missing_var_flag = True

    # Now drop these temporary vars from nc
    if 'temp' in nc.keys():
        nc = nc.drop_vars(['temp'])

    if 'sal_pr' in nc.keys():
        nc = nc.drop_vars(['sal_pr'])

    return nc, temp, sal_pr, missing_var_flag


def convert_oxygen(nc, var):

    # TODO
    # Ask Sarah about all these possible combinations of
    # trying to find good values to do conversion with

    # If don't have good sal or temp, do I convert anyway
    # using whatever values I have and then set flag bad?
    # And what value for flag?

    #  TODO
    # sorted on pressure but made level go from large to small?

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

    nc, temp, sal_pr, missing_var_flag = get_temp_and_salinity(nc)

    # TODO
    # is this OK to leave as not converted. And keeping units same
    # as before. So user needs to be aware of oxygen units if case
    # where it couldn't be converted

    # TODO
    # ask Sarah if should convert anyway and use bad flag with it
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

    logging.info('***** o2 conversion ******')

    converted_groups = []

    first_group = True

    for nc_group in nc.groupby('N_PROF'):

        nc_profile = nc_group[1]

        out = convert_oxygen(nc_profile, var)

        converted_groups.append(out)

        if f"{var}_qc" in nc.keys():
            oxy_qc = get_qc_scale_for_oxygen_conversion(out, var)

            if first_group:
                all_oxy_qc = oxy_qc
                first_group = False
            else:
                # This stacks downward
                all_oxy_qc = np.vstack((all_oxy_qc, oxy_qc))

    ds_grid = [converted_groups]

    # problem with this where sometimes pressure didn't have 'N_PROF' dim
    # nc = xr.combine_nested(
    #     ds_grid, concat_dim=["N_LEVELS", "N_PROF"], combine_attrs='identical')

    nc = xr.combine_nested(
        ds_grid, concat_dim=["N_LEVELS", "N_PROF"], combine_attrs='identical', compat='broadcast_equals')

    # TODO
    # Check this
    # look at nc and extract out oxygen
    # see if nan values match up with qc

    # If no sal_qc or no temp_qc, all_oxy_qc = []
    # for all groups and so don't change oxy qc
    if f"{var}_qc" in nc.keys() and len(all_oxy_qc):
        qc_var = {f"{var}_qc": (('N_PROF', 'N_LEVELS'), all_oxy_qc)}

        nc = nc.assign(qc_var)

    return nc


def convert_goship_to_argovis_units(nc):

    params = nc.keys()

    # If goship units aren't the same as argovis units, convert
    # So far, just converting oxygen

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

        logging.info("*** Converting sea water temperature ref scale")

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

    # TODO
    # rename so more general than just argovis request
    # since Sarah wanted them anyway

    # Converting to argovis ref scale if needed
    nc = convert_goship_to_argovis_ref_scale(nc)

    # Apply equations to convert units
    nc = convert_goship_to_argovis_units(nc)

    return nc
