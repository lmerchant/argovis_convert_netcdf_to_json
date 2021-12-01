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


def get_qc_scale_for_oxygen_conversion(nc, var, temp_qc, sal_qc):

    profile_size = nc.sizes['N_LEVELS']

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
        # Set qc to nan same size as var
        oxy_qc = np.empty(profile_size)
        oxy_qc[:] = np.nan

        # shape = list(nc[var].shape)
        # oxy_qc = np.empty(shape)
        # oxy_qc[:] = 2.0
        # qc_var = {f"{var}_qc": ('N_LEVELS', oxy_qc)}
        # nc = nc.assign(qc_var)

    # if 'ctd_salinity_qc' in nc.keys():
    #     sal_qc = nc['ctd_salinity_qc'].values

    # elif 'bottle_salinity_qc' in nc.keys() and not 'ctd_salinity_qc' in nc.keys():
    #     sal_qc = nc['bottle_salinity_qc'].values
    # else:
    #     sal_qc = []

    # if 'ctd_temperature_qc' in nc.keys():
    #     temp_qc = nc['ctd_temperature_qc'].values
    # elif 'ctd_temperature_68_qc' in nc.keys():
    #     temp_qc = nc['ctd_temperature_68_qc'].values
    # else:
    #     temp_qc = []

    # Now vertically stack these rows together to
    # find what the combined qc is. If using
    # good vals 0 and 2, will be able to tell with
    # logic below

    # Then will take the combined temp qc and sal qc
    # and modify the oxy qc since the temp and sal go into
    # the calculation to convert oxy.

    # if len(sal_qc) and len(temp_qc):
    #     qc_comb_array = np.column_stack([sal_qc, temp_qc])
    # elif len(sal_qc) and not len(temp_qc):
    #     qc_comb_array = sal_qc
    # elif not len(sal_qc) and len(temp_qc):
    #     qc_comb_array = temp_qc
    # else:
    #     # If no sal or temp qc, use oxy qc
    #     qc_comb_array = oxy_qc
    #     # return []

    # Compare oxy_qc, sal_qc and temp_qc.
    # choose oxy_qc if oxy_qc is bad
    # choose oxy_qc if sal and temp good
    # choose temp_qc if oxy_qc good and temp_qc bad
    # choose sal_qc if oxy_qc good and sal_qc bad

    # Wherever qc is bad, put a -100 flag
    flagged_sal_qc = np.where(((sal_qc != 2.0) & (sal_qc != 0)) &
                              (np.isfinite(sal_qc)), -100, sal_qc)

    flagged_temp_qc = np.where(
        ((temp_qc != 2.0) & (temp_qc != 0)) &
        (np.isfinite(temp_qc)), -100, temp_qc)

    # If stack next to each other,
    # Where there is a -1 (bad qc) for temp or sal, set to oxy
    # otherwise use oxy value
    try:
        sal_temp_qc = np.column_stack([flagged_sal_qc, flagged_temp_qc])
    except:
        sal_qc = np.empty(profile_size)
        sal_qc[:] = np.nan

        temp_qc = np.empty(profile_size)
        temp_qc[:] = np.nan

        sal_temp_qc = np.column_stack([sal_qc, temp_qc])

    # Sum them together into one array
    # Sum values row by row, and then search for those with neg sum,
    # that will indicate a bad qc.
    sum_qc = np.sum(sal_temp_qc, axis=1)

    # new_qc = np.where(((sum_qc < 0) &
    #                    (np.isfinite(sum_qc)) & (np.isfinite(oxy_qc))), 4, oxy_qc)

    new_qc = np.where(((sum_qc < 0) |
                       (np.isnan(sum_qc)) & (np.isfinite(oxy_qc))), 4, oxy_qc)

    # For this qc, set back to nan if oxy starting was nan which
    # occurs because in xarray where it pads each profile with nan

    # if len(sal_qc) and len(temp_qc):

    #     qc_comb_array = np.column_stack([sal_qc, temp_qc])

    #     # If any qc != 2 or !=0 and not nan, replace with -1
    #     # For any qc == 2 or == 0, replace with 1
    #     qc_comb_array = np.where(((qc_comb_array != 2.0) & (qc_comb_array != 0)) &
    #                              (np.isfinite(qc_comb_array)), -1, qc_comb_array)

    #     qc_comb_array = np.where(
    #         ((qc_comb_array == 2.0) | (qc_comb_array == 0)), 1, qc_comb_array)

    #     # Sum values row by row.
    #     qc_array = np.sum(qc_comb_array, axis=1)

    #     # For stacked rows, if the sum = num_stacks,
    #     # they are all true and so a good qc = 2
    #     num_stacks = np.shape(qc_comb_array)[1]

    #     # Now set the sum values back to qc values where
    #     # 2.0 is good and 4.0 is bad
    #     # TODO
    #     # Don't change good qc values whatever defined as

    #     qc_array = np.where((qc_array != num_stacks) & (
    #         np.isfinite(qc_array)), 4.0, qc_array)

    #     qc_array = np.where(qc_array == num_stacks, 2.0, qc_array)

    # else:
    #     qc_array = qc_comb_array

    # Combine qc_array with oxy_qc array, If
    # oxy_qc = 2, and qc_array = 4, change oxy_qc to 4,
    # otherwise, leave oxy qc alone.
    # c = np.column_stack([oxy_qc, qc_array])
    # c[:, 0] = np.where((c[:, 0] == 2) & (c[:, 1] == 4), 4, c[:, 0])

    # oxy_qc = c[:, 0].T

    return new_qc


# def get_qc_scale_for_oxygen_conversion_orig(nc, var):

#     # If the temp_qc is bad, use in calculation but set qc
#     # of that oxygen value to qc of temperature. Can't
#     # convert an oxygen with a bad temperature qc. Same
#     # for the salinity.

#     # TODO
#     # summarize possible combinations of good/bad vars
#     # and ask Sarah about doing calulation with bad vals
#     # and what flag to use. Do I do calc or skip it?

#     if f"{var}_qc" in nc.keys():
#         oxy_qc = nc[f"{var}_qc"]
#     else:
#         oxy_qc = []
#         # shape = list(nc[var].shape)
#         # oxy_qc = np.empty(shape)
#         # oxy_qc[:] = 2.0
#         # qc_var = {f"{var}_qc": ('N_LEVELS', oxy_qc)}
#         # nc = nc.assign(qc_var)

#     if 'ctd_salinity_qc' in nc.keys():
#         sal_qc = nc['ctd_salinity_qc'].values

#     elif 'bottle_salinity_qc' in nc.keys() and not 'ctd_salinity_qc' in nc.keys():
#         sal_qc = nc['bottle_salinity_qc'].values
#     else:
#         sal_qc = []

#     if 'ctd_temperature_qc' in nc.keys():
#         temp_qc = nc['ctd_temperature_qc'].values
#     elif 'ctd_temperature_68_qc' in nc.keys():
#         temp_qc = nc['ctd_temperature_68_qc'].values
#     else:
#         temp_qc = []

#     # Now vertically stack these rows together to
#     # find what the combined qc is. If using
#     # good vals 0 and 2, will be able to tell with
#     # logic below

#     # Then will take the combined temp qc and sal qc
#     # and modify the oxy qc since the temp and sal go into
#     # the calculation to convert oxy.

#     if len(sal_qc) and len(temp_qc):
#         qc_comb_array = np.column_stack([sal_qc, temp_qc])
#     elif len(sal_qc) and not len(temp_qc):
#         qc_comb_array = sal_qc
#     elif not len(sal_qc) and len(temp_qc):
#         qc_comb_array = temp_qc
#     else:
#         # If no sal or temp qc, use oxy qc
#         qc_comb_array = []
#         #return []

#     if len(sal_qc) and len(temp_qc):

#         qc_comb_array = np.column_stack([sal_qc, temp_qc])

#         # If any qc != 2 or !=0 and not nan, replace with -1
#         # For any qc == 2 or == 0, replace with 1
#         qc_comb_array = np.where(((qc_comb_array != 2.0) & (qc_comb_array != 0)) &
#                                  (np.isfinite(qc_comb_array)), -1, qc_comb_array)

#         qc_comb_array = np.where(
#             ((qc_comb_array == 2.0) | (qc_comb_array == 0)), 1, qc_comb_array)

#         # Sum values row by row.
#         qc_array = np.sum(qc_comb_array, axis=1)

#         # For stacked rows, if the sum = num_stacks,
#         # they are all true and so a good qc = 2
#         num_stacks = np.shape(qc_comb_array)[1]

#         # Now set the sum values back to qc values where
#         # 2.0 is good and 4.0 is bad
#         # TODO
#         # Don't change good qc values whatever defined as

#         qc_array = np.where((qc_array != num_stacks) & (
#             np.isfinite(qc_array)), 4.0, qc_array)

#         qc_array = np.where(qc_array == num_stacks, 2.0, qc_array)

#     else:
#         qc_array = qc_comb_array

#     # Combine qc_array with oxy_qc array, If
#     # oxy_qc = 2, and qc_array = 4, change oxy_qc to 4,
#     # otherwise, leave oxy qc alone.
#     c = np.column_stack([oxy_qc, qc_array])
#     c[:, 0] = np.where((c[:, 0] == 2) & (c[:, 1] == 4), 4, c[:, 0])

#     oxy_qc = c[:, 0].T

#     return oxy_qc


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

    profile_size = nc.sizes['N_LEVELS']

    # Keep track of bad salinity or temperature and not converting
    # store station_cast. Later associate with var
    station_casts_bad_temp_salinity = []

    # TODO
    # Instead of setting bad qc to nan, just convert
    # and use flag of value with bad qc if exists
    # If bad temp and salinity, choose bad temp flag?

    # Use ctd_salinity first (practical salinity)
    # and if not exist, use bottle_salinity

    # TODO
    # make sure there are at least qc=2 in the ctd_salinity,
    # otherwise use bottle_salinity assuming it has some
    # qc = 2, otherwise can't convert. What
    # to do with naming the var doxy then? Would have to
    # rename if doxy_ml_l. Just convert anyway using bad values
    # and rely on the flag to designate the quality

    # TODO
    # What is procedure for converting oxygen when all ctd_sal
    # is bad but some btl_sal are good. Do I keep the oxy uncoverted val
    # if sal is null? What flag to use if set oxygen to null?

    missing_var_flag = False
    missing_good_sal_flag = False
    missing_good_temp_flag = False

    sal_ref_scale = None
    temp_ref_scale = None

    found_good_ctd_sal = False
    found_good_btl_sal = False

    ctd_sal_pr = np.empty(profile_size)
    btl_sal_pr = np.empty(profile_size)

    if 'ctd_salinity' in nc.keys():

        ctd_sal_pr = nc['ctd_salinity']

        all_nan = np.isnan(ctd_sal_pr.to_numpy()).all()

        if not all_nan:
            found_good_ctd_sal = True

            # Do I assign a qc of nan or maybe 0?

        else:
            found_good_ctd_sal = False
            # Do I assign a qc of nan or maybe 0?

        # Have a temporary var sal_pr so don't overwrite ctd_salinity

        if 'ctd_salinity_qc' in nc.keys():

            nan_array = np.empty(profile_size)
            nan_array[:] = np.nan

            # replace all values not equal to 0 or 2 with np.nan
            # nc['sal_pr'] = xr.where((nc['ctd_salinity_qc'] != 0) &
            #                         (nc['ctd_salinity_qc'] != 2.0), nan_array, nc['ctd_salinity'])

            search_good_vals = xr.where((nc['ctd_salinity_qc'] != 0) &
                                        (nc['ctd_salinity_qc'] != 2.0), nan_array, nc['ctd_salinity'])

            # ctd salinity is all bad so can't do conversion,
            # try checking bottle salinity then

            # sal_pr = nc['sal_pr']
            sal_pr = nc['ctd_salinity']
            sal_ref_scale = nc['ctd_salinity'].attrs['reference_scale']

            # Check if sal_pr has qc=0 or 2. If it does, found good values

            # all_nan = np.isnan(sal_pr).all()
            all_nan = np.isnan(search_good_vals).all()

            if not all_nan:
                found_good_ctd_sal = True

                ctd_sal_pr = sal_pr
                ctd_sal_pr_qc = nc['ctd_salinity_qc']

            else:
                found_good_ctd_sal = False
                ctd_sal_pr = sal_pr
                ctd_sal_pr_qc = nc['ctd_salinity_qc']

        # else:
        #     ctd_sal_pr = nc['ctd_salinity']

        #     print('ctd sal pr')
        #     print(ctd_sal_pr.to_numpy())

        #     all_nan = np.isnan(ctd_sal_pr.to_numpy()).all()

        #     print(f"all_nan {all_nan}")

        #     if not all_nan:
        #         found_good_ctd_sal = True

        #         # Do I assign a qc of nan or maybe 0?

        #     else:
        #         found_good_ctd_sal = False
        #         # Do I assign a qc of nan or maybe 0?

        #     # Set qc to nan same size as sal
        #     ctd_sal_pr_qc = np.empty(profile_size)
        #     ctd_sal_pr_qc[:] = np.nan

    if 'bottle_salinity' in nc.keys():

        btl_sal_pr = nc['bottle_salinity']

        all_nan = np.isnan(btl_sal_pr.to_numpy()).all()

        if not all_nan:
            found_good_btl_sal = True

            # Do I assign a qc of nan or maybe 0?

        else:
            found_good_btl_sal = False
            # Do I assign a qc of nan or maybe 0?

        nan_array = np.empty(profile_size)
        nan_array[:] = np.nan

        if 'bottle_salinity_qc' in nc.keys():

            # replace all values not equal to 0 or 2 with np.nan
            # nc['sal_pr'] = xr.where((nc['bottle_salinity_qc'] != 0) &
            #                         (nc['bottle_salinity_qc'] != 2.0), nan_array, nc['bottle_salinity'])

            search_good_vals = xr.where((nc['bottle_salinity_qc'] != 0) &
                                        (nc['bottle_salinity_qc'] != 2.0), nan_array, nc['bottle_salinity'])

            sal_pr = nc['bottle_salinity']
            sal_ref_scale = nc['bottle_salinity'].attrs['reference_scale']

            # Check if sal_pr is all null. If it is, found no good values
            # all_nan = np.isnan(sal_pr).all()
            all_nan = np.isnan(search_good_vals).all()

            if not all_nan:
                found_good_btl_sal = True

            btl_sal_pr = sal_pr
            btl_sal_pr_qc = nc['bottle_salinity_qc']

        # else:

        #     # TODO
        #     # don't need this section
        #     btl_sal_pr = nc['bottle_salinity']

        #     all_nan = np.isnan(btl_sal_pr.to_numpy()).all()

        #     if not all_nan:
        #         found_good_btl_sal = True

        #         # Do I assign a qc of nan or maybe 0?

        #     else:
        #         found_good_btl_sal = False
        #         # Do I assign a qc of nan or maybe 0?

        #     # Set qc to nan same size as sal
        #     # Why do this? Does it just let qc be
        #     # determined by temp and sal?
        #     btl_sal_pr_qc = np.empty(profile_size)
        #     btl_sal_pr_qc[:] = np.nan

    if found_good_ctd_sal:
        sal_pr = ctd_sal_pr
        sal_pr_qc = ctd_sal_pr_qc

    elif not found_good_ctd_sal and found_good_btl_sal:
        # use btl salinity for conversion
        # TODO
        # ask if this is OK
        # save list of files where this is ever the case
        sal_pr = btl_sal_pr
        sal_pr_qc = btl_sal_pr_qc

    else:
        logging.info("********************************************")
        logging.info("No non nan salinity to convert oxygen units")
        logging.info("*******************************************")

        missing_good_sal_flag = True

        station_cast = nc['station_cast'].values
        station_cast = station_cast.tolist()

        station_casts_bad_temp_salinity.append(station_cast)

        if np.size(ctd_sal_pr):
            sal_pr = ctd_sal_pr
            sal_pr_qc = ctd_sal_pr_qc

        elif np.size(btl_sal_pr):
            sal_pr = btl_sal_pr
            sal_pr_qc = btl_sal_pr_qc

        else:
            sal_pr = np.empty(profile_size)
            sal_pr_qc = np.empty(profile_size)

        # TODO
        # So keep oxygen same units instead of set all to null. No?

        # Save these to a file to look at
        # sal_pr = np.nan

    # Check if salinity ref scale is PSS-78
    if sal_ref_scale != 'PSS-78':
        missing_good_sal_flag = True

    # ----------------
    # Get temperature
    # ----------------

    # Use ctd_temperature first,  then ctd_temperature_68
    # These have just been converted to the ITS-90 scale

    found_good_ctd_temp = False
    found_good_temp_68 = False

    ctd_temp = []
    temp_68 = []

    if 'ctd_temperature' in nc.keys():

        # Have a temporary var temp so don't overwrite ctd_temperature

        ctd_temp = nc['ctd_temperature']

        all_nan = np.isnan(ctd_temp.to_numpy()).all()

        if not all_nan:
            found_good_ctd_temp = True

            # Do I assign a qc of nan or maybe 0?

        else:
            found_good_ctd_temp = False
            # Do I assign a qc of nan or maybe 0?

        # Have a temporary var temp so don't overwrite ctd_salinity

        nan_array = np.empty(profile_size)
        nan_array[:] = np.nan

        if 'ctd_temperature_qc' in nc.keys():

            # replace all values not equal to 0 or 2 with np.nan
            # nc['temp'] = xr.where((nc['ctd_temperature_qc'] != 0) &
            #                       (nc['ctd_temperature_qc'] != 2.0), nan_array, nc['ctd_temperature'])

            search_good_vals = xr.where((nc['ctd_temperature_qc'] != 0) &
                                        (nc['ctd_temperature_qc'] != 2.0), nan_array, nc['ctd_temperature'])

            # temp = nc['temp']
            temp = nc['ctd_temperature']
            temp_ref_scale = nc['ctd_temperature'].attrs['reference_scale']

            # Check if sal_pr is all null. If it is, found no good values
            # all_nan = np.isnan(temp).all()
            all_nan = np.isnan(search_good_vals).all()

            if not all_nan:
                found_good_ctd_temp = True
                ctd_temp = temp
                ctd_temp_qc = nc['ctd_temperature_qc']

        # else:
        #     ctd_temp = nc['ctd_temperature']

        #     all_nan = np.isnan(ctd_temp.to_numpy()).all()

        #     if not all_nan:
        #         found_good_ctd_temp = True

        #         # Do I assign a qc of nan or maybe 0?

        #     else:
        #         found_good_ctd_temp = False
        #         # Do I assign a qc of nan or maybe 0?

        #     # Set qc to nan same size as temp
        #     ctd_temp_qc = np.empty(profile_size)
        #     ctd_temp_qc[:] = np.nan

    if 'ctd_temperature_68' in nc.keys():

        # Have a temporary var temp so don't overwrite ctd_temperature

        temp_68 = nc['ctd_temperature_68']

        all_nan = np.isnan(temp_68.to_numpy()).all()

        if not all_nan:
            found_good_temp_68 = True

            # Do I assign a qc of nan or maybe 0?

        else:
            found_good_temp_68 = False
            # Do I assign a qc of nan or maybe 0?

        nan_array = np.empty(profile_size)
        nan_array[:] = np.nan

        if 'ctd_temperature_68_qc' in nc.keys():

            # replace all values not equal to 0 or 2 with np.nan
            # nc['temp'] = xr.where((nc['ctd_temperature_68_qc'] != 0) &
            #                       (nc['ctd_temperature_68_qc'] != 2.0), nan_array, nc['ctd_temperature_68'])

            search_good_vals = xr.where((nc['ctd_temperature_68_qc'] != 0) &
                                        (nc['ctd_temperature_68_qc'] != 2.0), nan_array, nc['ctd_temperature_68'])

            # temp = nc['temp']
            temp = nc['ctd_temperature_68']
            temp_ref_scale = nc['ctd_temperature_68'].attrs['reference_scale']

            # all_nan = np.isnan(temp).all()
            all_nan = np.isnan(search_good_vals).all()

            if not all_nan:
                found_good_temp_68 = True
                temp_68 = temp
                temp_68_qc = nc['ctd_temperature_68_qc']

        # else:
        #     temp_68 = nc['ctd_temperature_68']

        #     all_nan = np.isnan(temp_68.to_numpy()).all()

        #     if not all_nan:
        #         found_good_temp_68 = True

        #         # Do I assign a qc of nan or maybe 0?

        #     else:
        #         found_good_temp_68 = False
        #         # Do I assign a qc of nan or maybe 0?

        #     # Set qc to nan same size as temp
        #     temp_68_qc = np.empty(profile_size)
        #     temp_68_qc[:] = np.nan

    if found_good_ctd_temp:
        temp = ctd_temp
        temp_qc = ctd_temp_qc

    elif not found_good_ctd_temp and found_good_temp_68:
        # TODO
        # ask if this is OK
        # But do I have a ctd_temperature and
        # ctd_temperature_68 at the same time?
        # save list of files where this is ever the case
        temp = temp_68
        temp_qc = temp_68_qc
    else:
        logging.info("*******************************************")
        logging.info("No non nan ctd temp to convert oxygen units")
        logging.info("*******************************************")

        station_cast = nc['station_cast'].values
        station_cast = station_cast.tolist()

        station_casts_bad_temp_salinity.append(station_cast)

        # TODO
        # So keep oxygen same units instead of set all to null

        # What happens if I try to proceed with all nan values?

        # Get problem when saving since attributes not all the
        # same for

        # Can only have one attribute for the entire nc set

        # Can I keep track of the profiles and change the attribute later
        # when I create mappings?
        # What to do in mean time? change attribute?

        if np.size(ctd_temp):
            temp = ctd_temp
            temp_qc = ctd_temp_qc
        elif np.size(temp_68):
            temp = temp_68
            temp_qc = temp_68_qc
        else:
            temp = np.empty(profile_size)
            temp_qc = np.empty(profile_size)
            missing_good_temp_flag = True

    # Check if temperature ref scale is ITS-90
    # Which it should be since did this conversion first
    if temp_ref_scale != 'ITS-90':
        missing_good_temp_flag = True

    # TODO
    # Why did I add them to nc?
    # change this

    # Now drop these temporary vars from nc
    if 'temp' in nc.keys():
        nc = nc.drop_vars(['temp'])

    if 'sal_pr' in nc.keys():
        nc = nc.drop_vars(['sal_pr'])

    if missing_good_sal_flag or missing_good_temp_flag:
        missing_var_flag = True

    # remove duplicates
    station_casts_bad_temp_salinity = list(
        set(station_casts_bad_temp_salinity))

    return nc, temp, temp_qc, sal_pr, sal_pr_qc, missing_var_flag, station_casts_bad_temp_salinity


# def get_temp_and_salinity_orig(nc):

#     # TODO
#     # Instead of setting bad qc to nan, just convert
#     # and use flag of value with bad qc if exists
#     # If bad temp and salinity, choose bad temp flag?

#     # Use ctd_salinity first (practical salinity)
#     # and if not exist, use bottle_salinity
#     # TODO
#     # make sure there are at least qc=2 in the ctd_salinity,
#     # otherwise use bottle_salinity assuming it has some
#     # qc = 2, otherwise can't convert. What
#     # to do with naming the var doxy then? Would have to
#     # rename if doxy_ml_l

#     # TODO
#     # What is procedure for converting oxygen when all ctd_sal
#     # is bad but some btl_sal are good. Do I keep the oxy uncoverted val
#     # if sal is null? What flag to use if set oxygen to null?

#     missing_var_flag = False
#     sal_ref_scale = None
#     temp_ref_scale = None

#     found_good_ctd_sal = False
#     found_good_btl_sal = False

#     if 'ctd_salinity' in nc.keys():

#         # Have a temporary var sal_pr so don't overwrite ctd_salinity

#         shape = list(nc['ctd_salinity'].shape)
#         nan_array = np.empty(shape)
#         nan_array[:] = np.nan

#         if 'ctd_salinity_qc' in nc.keys():

#             # replace all values not equal to 0 or 2 with np.nan
#             nc['sal_pr'] = xr.where((nc['ctd_salinity_qc'] != 0) &
#                                     (nc['ctd_salinity_qc'] != 2.0), nan_array, nc['ctd_salinity'])

#             # ctd salinity is all bad so can't do conversion,
#             # try checking bottle salinity then

#             sal_pr = nc['sal_pr']
#             sal_ref_scale = nc['ctd_salinity'].attrs['reference_scale']

#             # Check if sal_pr is all null. If it is, found no good values
#             all_nan = np.isnan(sal_pr).all()

#             if not all_nan:
#                 found_good_ctd_sal = True

#                 ctd_sal_pr = sal_pr

#     if 'bottle_salinity' in nc.keys():

#         # Have a temporary var sal_pr so don't overwrite ctd_salinity

#         shape = list(nc['bottle_salinity'].shape)
#         nan_array = np.empty(shape)
#         nan_array[:] = np.nan

#         if 'bottle_salinity_qc' in nc.keys():

#             # replace all values not equal to 0 or 2 with np.nan
#             nc['sal_pr'] = xr.where((nc['bottle_salinity_qc'] != 0) &
#                                     (nc['bottle_salinity_qc'] != 2.0), nan_array, nc['bottle_salinity'])

#             sal_pr = nc['sal_pr']
#             sal_ref_scale = nc['bottle_salinity'].attrs['reference_scale']

#             # Check if sal_pr is all null. If it is, found no good values
#             all_nan = np.isnan(sal_pr).all()

#             if not all_nan:
#                 found_good_btl_sal = True

#             btl_sal_pr = sal_pr

#     if found_good_ctd_sal:
#         sal_pr = ctd_sal_pr

#     elif not found_good_ctd_sal and found_good_btl_sal:
#         # use btl salinity for conversion
#         # TODO
#         # ask if this is OK
#         # save list of files where this is ever the case
#         sal_pr = btl_sal_pr

#     else:
#         logging.info("************************************")
#         logging.info("No salinity to convert oxygen units")
#         logging.info("************************************")

#         # TODO
#         # So keep oxygen same units instead of set all to null

#         # Save these to a file to look at

#         sal_pr = np.nan
#         missing_var_flag = True

#     # Check if salinity ref scale is PSS-78
#     if sal_ref_scale != 'PSS-78':
#         missing_var_flag = True

#     # ----------------
#     # Get temperature
#     # ----------------

#     # Use ctd_temperature first,  then ctd_temperature_68
#     # These have just been converted to the ITS-90 scale

#     found_good_ctd_temp = False
#     found_good_temp_68 = False

#     if 'ctd_temperature' in nc.keys():

#         # Have a temporary var temp so don't overwrite ctd_temperature

#         shape = list(nc['ctd_temperature'].shape)
#         nan_array = np.empty(shape)
#         nan_array[:] = np.nan

#         if 'ctd_temperature_qc' in nc.keys():

#             # replace all values not equal to 0 or 2 with np.nan
#             nc['temp'] = xr.where((nc['ctd_temperature_qc'] != 0) &
#                                   (nc['ctd_temperature_qc'] != 2.0), nan_array, nc['ctd_temperature'])

#             temp = nc['temp']
#             temp_ref_scale = nc['ctd_temperature'].attrs['reference_scale']

#             # Check if sal_pr is all null. If it is, found no good values
#             all_nan = np.isnan(temp).all()

#             if not all_nan:
#                 found_good_ctd_temp = True
#                 ctd_temp = temp

#     if 'ctd_temperature_68' in nc.keys():

#         # Have a temporary var temp so don't overwrite ctd_temperature

#         shape = list(nc['ctd_temperature_68'].shape)
#         nan_array = np.empty(shape)
#         nan_array[:] = np.nan

#         if 'ctd_temperature_68_qc' in nc.keys():

#             # replace all values not equal to 0 or 2 with np.nan
#             nc['temp'] = xr.where((nc['ctd_temperature_68_qc'] != 0) &
#                                   (nc['ctd_temperature_68_qc'] != 2.0), nan_array, nc['ctd_temperature_68'])

#             temp = nc['temp']
#             temp_ref_scale = nc['ctd_temperature_68'].attrs['reference_scale']

#             if not all_nan:
#                 found_good_temp_68 = True
#                 temp_68 = temp

#     if found_good_ctd_temp:
#         temp = ctd_temp

#     elif not found_good_ctd_temp and found_good_temp_68:
#         # TODO
#         # ask if this is OK
#         # But do I have a ctd_temperature and
#         # ctd_temperature_68 at the same time?
#         # save list of files where this is ever the case
#         temp = temp_68
#     else:
#         # This shouldn't happen since checked for required
#         # CTD vars, ctd temp with ref scale, at the start
#         # of the program, and excluded files if there
#         # was no ctd temp with a ref scale.
#         logging.info("************************************")
#         logging.info("No ctd temp to convert oxygen units")
#         logging.info("************************************")

#         # TODO
#         # So keep oxygen same units instead of set all to null

#         missing_var_flag = True
#         temp = np.nan

#     # Check if temperature ref scale is ITS-90
#     # Which it should be since did this conversion first
#     if temp_ref_scale != 'ITS-90':
#         missing_var_flag = True

#     # Now drop these temporary vars from nc
#     if 'temp' in nc.keys():
#         nc = nc.drop_vars(['temp'])

#     if 'sal_pr' in nc.keys():
#         nc = nc.drop_vars(['sal_pr'])

#     return nc, temp, sal_pr, missing_var_flag


def convert_oxygen(nc, var, profiles_no_oxy_conversions):

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

    nc, temp, temp_qc, sal_pr, sal_pr_qc, missing_var_flag, station_casts_bad_temp_salinity = get_temp_and_salinity(
        nc)

    if station_casts_bad_temp_salinity:
        profiles_no_oxy_conversions[var] = station_casts_bad_temp_salinity

    # TODO
    # is this OK to leave as not converted. And keeping units same
    # as before. So user needs to be aware of oxygen units if case
    # where it couldn't be converted

    # TODO
    # ask Sarah if should convert anyway and use bad flag with it
    if missing_var_flag:
        logging.info(
            "Missing quality temp or sal needed to do Oxygen conversion")
        logging.info(f"Didn't convert {var} for profile")

        # # set attribute and keep track of whatt didn't convert
        # can do this after do oxygen conversion for var
        nc[var].attrs['units'] = 'micromole/kg'

        return nc, temp_qc, sal_pr_qc, profiles_no_oxy_conversions

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

    # If  station_casts_bad_temp_salinity, setting units as if
    # converted, but wasn't which is kept track of
    # by station_casts_bad_temp_salinity

    nc[var] = converted_oxygen
    nc[var].attrs['units'] = 'micromole/kg'

    return nc, temp_qc, sal_pr_qc, profiles_no_oxy_conversions


def convert_oxygen_to_new_units(nc, var, profiles_no_oxy_conversions):

    logging.info('***** o2 conversion ******')

    # Check if values exist or if empty
    # because variables in nc can exist but be all null
    # for some profiles or all

    converted_groups = []

    profile_size = nc.sizes['N_LEVELS']

    first_group = True

    for nc_group in nc.groupby('N_PROF'):

        nc_profile = nc_group[1]

        # TODO
        # Add check for all null values of oxy, sal or temp
        # if they are, can't convert and revert back to original
        # oxy with same units attribute.
        # Don't modify c[var].attrs['units']

        # oxy = nc_profile[var]
        # See what ctd temperature exists and check if all nan
        # Check both bottle and ctd salinity to see if one exists
        # or if both all nan

        oxy = nc_profile[var]

        oxy_all_nan = np.isnan(oxy).all()

        if 'ctd_temperature' in nc_profile.keys():
            temp = nc_profile['ctd_temperature']
            temp_all_nan = np.isnan(temp).all()
        else:
            temp_all_nan = True

        if 'ctd_temperature_68' in nc_profile.keys():
            temp = nc_profile['ctd_temperature_68']
            temp_68_all_nan = np.isnan(temp).all()
        else:
            temp_68_all_nan = True

        one_temp_exists = not temp_all_nan or not temp_68_all_nan

        if 'ctd_salinity' in nc_profile.keys():
            sal = nc_profile['ctd_salinity']
            sal_all_nan = np.isnan(sal).all()
        else:
            sal_all_nan = True

        if 'bottle_salinity' in nc_profile.keys():
            sal = nc_profile['bottle_salinity']
            sal_btl_all_nan = np.isnan(sal).all()
        else:
            sal_btl_all_nan = True

        one_sal_exists = not sal_all_nan or not sal_btl_all_nan

        if oxy_all_nan or not one_temp_exists or not one_sal_exists:
            logging.info(
                '*** No oxygen conversion because missing S,T or Ox ***')
            no_s_t_or_ox = True
            out = nc_profile

        else:
            out, temp_qc, sal_pr_qc, profiles_no_oxy_conversions = convert_oxygen(
                nc_profile, var, profiles_no_oxy_conversions)

            if profiles_no_oxy_conversions.keys():
                no_s_t_or_ox = True
            else:
                no_s_t_or_ox = False

        converted_groups.append(out)

        if oxy_all_nan or not one_temp_exists or not one_sal_exists:

            if f"{var}_qc" in nc.keys():
                oxy_qc = np.empty(profile_size)
                oxy_qc[:] = np.nan

                if first_group:
                    all_oxy_qc = oxy_qc
                    first_group = False
                else:
                    # This stacks downward
                    all_oxy_qc = np.vstack((all_oxy_qc, oxy_qc))

                continue

        if f"{var}_qc" in nc.keys():

            # If var qc is nan, skip this
            if no_s_t_or_ox:

                oxy_qc = np.empty(profile_size)
                # Set qc empty or nan?
                oxy_qc[:] = np.nan

            else:
                # TODO
                # I've been setting qc = nan if one doesn't exist
                # Is this right? Seems to be skip giving qc?
                # Or set as 9 or set as 0?
                oxy_qc = get_qc_scale_for_oxygen_conversion(
                    out, var, temp_qc, sal_pr_qc)

            if first_group:
                all_oxy_qc = oxy_qc
                first_group = False
            else:
                # This stacks downward
                all_oxy_qc = np.vstack((all_oxy_qc, oxy_qc))

    ds_grid = [converted_groups]

    # problem with this where sometimes pressure didn't have 'N_PROF' dim
    nc = xr.combine_nested(
        ds_grid, concat_dim=["N_LEVELS", "N_PROF"], combine_attrs='identical')

    # nc = xr.combine_nested(
    #     ds_grid, concat_dim=["N_LEVELS", "N_PROF"], combine_attrs='override', compat='broadcast_equals')

    # Try override instead of identical for combine_attrs
    # “override”: skip comparing and copy attrs from the first dataset to the result.

    # TODO
    # Even though been giving oxy nan qc, here it says has to be qc in nc
    if f"{var}_qc" in nc.keys() and len(all_oxy_qc):
        qc_var = {f"{var}_qc": (('N_PROF', 'N_LEVELS'), all_oxy_qc)}

        nc = nc.assign(qc_var)

    return nc, profiles_no_oxy_conversions


def convert_cchdo_to_argovis_units(nc, profiles_no_oxy_conversions):

    params = nc.keys()

    # If cchdo units aren't the same as argovis units, convert
    # So far, just converting oxygen

    for var in params:

        try:
            var_cchdo_units = nc[var].attrs['units']
        except:
            continue

        if 'oxygen' in var and var_cchdo_units == 'ml/l':
            nc, profiles_no_oxy_conversions = convert_oxygen_to_new_units(
                nc, var, profiles_no_oxy_conversions)

    return nc, profiles_no_oxy_conversions


def convert_sea_water_temp(nc, var, var_cchdo_ref_scale, argovis_ref_scale):

    # Check sea_water_temperature to have cchdo_reference_scale be ITS-90

    if var_cchdo_ref_scale == 'IPTS-68' and argovis_ref_scale == 'ITS-90':

        logging.info("*** Converting sea water temperature ref scale")

        # Convert to ITS-90 scal
        temperature = nc[var].data

        converted_temperature = temperature/1.00024

        # Set temperature value in nc because use it later to
        # create profile dict
        nc[var].data = converted_temperature
        nc[var].attrs['reference_scale'] = 'ITS-90'

    return nc


def convert_cchdo_to_argovis_ref_scale(nc):

    params = nc.keys()

    # If argo ref scale not equal to cchdo ref scale, convert

    # So far, it's only the case for temperature

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    argovis_ref_scale_per_type = get_argovis_reference_scale_per_type()

    for var in params:
        if 'temperature' in var:

            try:
                # Get cchdo reference scale of var
                var_cchdo_ref_scale = nc[var].attrs['reference_scale']

                argovis_ref_scale = argovis_ref_scale_per_type['temperature']
                is_same_scale = var_cchdo_ref_scale == argovis_ref_scale

                if not is_same_scale:
                    nc = convert_sea_water_temp(
                        nc, var, var_cchdo_ref_scale, argovis_ref_scale)
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
    nc = convert_cchdo_to_argovis_ref_scale(nc)

    # Apply equations to convert units
    # TODO
    # keep track of no oxy conversions for all vars
    profiles_no_oxy_conversions = {}

    nc, profiles_no_oxy_conversions = convert_cchdo_to_argovis_units(
        nc, profiles_no_oxy_conversions)

    return nc, profiles_no_oxy_conversions
