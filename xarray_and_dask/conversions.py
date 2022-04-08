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


def get_argovis_reference_scale_per_type():

    return {
        'temperature': 'ITS-90',
        'salinity': 'PSS-78'
    }


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

    converted_oxygen = xr.apply_ufunc(
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

    return converted_oxygen

 # TODO ***
 # break this up for checking separately


def convert_oxygen(nc_profile, var, profiles_no_oxy_conversions):

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

    oxy = nc_profile[var]
    oxy_dtype = nc_profile[var].dtype

    missing_var_flag = False

    use_sal, sal_qc, profiles_no_oxy_conversions = get_sal_to_use_and_qc(
        nc_profile, var, profiles_no_oxy_conversions)

    if use_sal is None:
        missing_var_flag = True
    else:
        sal_pr = nc_profile[use_sal]

    use_temp, temp_qc, profiles_no_oxy_conversions = get_temp_to_use_and_qc(
        nc_profile, var, profiles_no_oxy_conversions)

    if use_temp is None:
        missing_var_flag = True
    else:
        temp = nc_profile[use_temp]

    # nc_profile, temp, temp_qc, sal_pr, sal_pr_qc, missing_var_flag, station_casts_bad_temp_salinity = get_temp_and_salinity(
    #     nc_profile)

    # remove duplicates
    station_casts_bad = list(
        set(profiles_no_oxy_conversions[var]))

    if station_casts_bad:
        profiles_no_oxy_conversions[var] = station_casts_bad

    # TODO
    # is this OK to leave as not converted. And keeping units same
    # as before. So user needs to be aware of oxygen units if case
    # where it couldn't be converted

    # # TODO
    # # ask Sarah if should convert anyway and use bad flag with it
    # if missing_var_flag:
    #     logging.info(
    #         "Missing quality temp or sal needed to do Oxygen conversion")
    #     logging.info(f"Didn't convert {var} for profile")

    #     # # set attribute and keep track of whatt didn't convert
    #     # can do this after do oxygen conversion for var
    #     nc_profile[var].attrs['units'] = 'micromole/kg'

    #     return nc_profile, profiles_no_oxy_conversions

    pres = nc_profile['pressure']
    lat = nc_profile['latitude']
    lon = nc_profile['longitude']

    if not missing_var_flag:

        converted_oxygen = get_converted_oxy(oxy,
                                             temp,
                                             sal_pr,
                                             pres,
                                             lon,
                                             lat,
                                             oxy_dtype)

        try:
            # If the returned varible says name is temperature and not var,
            # rename it
            # Occured with oxygen all nan
            converted_var_name = list(converted_oxygen.keys())[0]
            rename_map = {converted_var_name: var}
            converted_oxygen = converted_oxygen.rename(rename_map)
        except:
            # It is a xarray.DataArray with var name
            pass

    else:
        converted_oxygen = oxy

    # Update with new values
    try:
        # If it's a dataset
        # Like if all oxy nan or unconverted
        nc_profile.update(converted_oxygen)
    except:
        # If it's a data array
        nc_profile[var] = converted_oxygen

    # If  station_casts_bad_temp_salinity, setting units as if
    # converted, but wasn't which is kept track of
    # by station_casts_bad_temp_salinity

    # *******************
    # TODO
    # Move this to end which I thoutht it was
    # Make a note if conversion went OK, and if so, change it below
    # Maybe because some profiles weren't converted, so need to
    # keep track of that too. so make a profiles_w_conversons. But
    # don't I reverse the unit change notation?

    # TODO ***
    # comment this out
    # nc_profile[var].attrs['units'] = 'micromole/kg'

    return nc_profile, profiles_no_oxy_conversions


def get_sal_to_use_and_qc(nc_profile, var, profiles_no_oxy_conversions):

    # TODO
    # Should I even be using bottle salinity if ctd_salinity?

    # Because if less bottle salinity, would mean less oxy converted
    # with it, then what to say about the rest of oxy?
    # Is there bottle oxygen?

    # TODO
    # Use bottle salinity, when there is no ctd_salinity. But will
    # there be enough points to do that?

    # Find salinity qc

    profile_size = nc_profile.sizes['N_LEVELS']

    nan_array = np.empty(profile_size)
    nan_array[:] = np.nan

    zero_array = np.zeros(profile_size)

    station_casts_bad_salinity = []

    is_finite_ctd_sal = False
    is_finite_ctd_sal_qc = False

    is_finite_btl_sal = False
    is_finite_btl_sal_qc = False

    is_ctd_sal = 'ctd_salinity' in nc_profile.keys()
    is_ctd_sal_qc = 'ctd_salinity_qc' in nc_profile.keys()

    if is_ctd_sal:
        is_finite_ctd_sal = not np.isnan(nc_profile['ctd_salinity']).all()

        logging.info(
            f"Finite values exist in ctd salinity: {is_finite_ctd_sal}")
        logging.info(
            f"Does ctd salinity have qc values: {is_ctd_sal_qc}")

    # Find if some good qc in ctd_salinity
    if is_ctd_sal and is_ctd_sal_qc:
        ctd_sal = nc_profile['ctd_salinity']
        ctd_sal_qc = nc_profile['ctd_salinity_qc']

        two_flags = np.isclose(ctd_sal_qc, 2, 0.1)
        zero_flags = np.isclose(ctd_sal_qc, 0, 0.1)

        search_good_vals = np.where(
            (zero_flags | two_flags), ctd_sal_qc, nan_array)

        has_good_ctd_sal_qc = not np.isnan(search_good_vals).all()

        logging.info(
            f"Does ctd salinity qc have good values: {has_good_ctd_sal_qc}")

    is_btl_sal = 'bottle_salinity' in nc_profile.keys()
    is_btl_sal_qc = 'bottle_salinity_qc' in nc_profile.keys()

    if is_btl_sal:
        is_finite_btl_sal = not np.isnan(nc_profile['bottle_salinity']).all()

        logging.info(
            f"Finite values exist in btl salinity: {is_finite_btl_sal}")
        logging.info(
            f"Does btl salinity have qc values: {is_btl_sal_qc}")

    # Find if some good qc in bottle_salinity
    if is_btl_sal and is_btl_sal_qc:
        btl_sal = nc_profile['bottle_salinity']
        btl_sal_qc = nc_profile['bottle_salinity_qc']

        two_flags = np.isclose(btl_sal_qc, 2, 0.1)
        zero_flags = np.isclose(btl_sal_qc, 0, 0.1)

        search_good_vals = np.where(
            (zero_flags | two_flags), btl_sal_qc, nan_array)

        has_good_btl_sal_qc = not np.isnan(search_good_vals).all()

        logging.info(
            f"Does bottle salinity qc have good values: {has_good_btl_sal_qc}")

    # Determine which salinity to use and create qc if it doesn't exist

    # Determine if using ctd_salinity
    if is_ctd_sal and is_ctd_sal_qc and is_finite_ctd_sal:
        sal_qc = nc_profile['ctd_salinity_qc']

        use_sal = 'ctd_salinity'

    elif is_ctd_sal and not is_ctd_sal_qc and is_finite_ctd_sal:
        # woce flag is NOFLAG = 0
        # Where var not nan, set flag = 0
        sal_qc = xr.where(
            nc_profile['ctd_salinity'].isnan, nan_array, zero_array)

        use_sal = 'ctd_salinity'

    # TODO
    # Should conversion still be done if no finite sal?
    elif is_ctd_sal and is_ctd_sal_qc and not is_finite_ctd_sal:
        sal_qc = nc_profile['ctd_salinity_qc']

        use_sal = 'ctd_salinity'

    # Determine if using bottle_salinity
    if not is_ctd_sal and is_btl_sal and is_btl_sal_qc and is_finite_btl_sal:
        sal_qc = nc_profile['bottle_salinity_qc']

        use_sal = 'bottle_salinity'

    elif not is_ctd_sal and is_btl_sal and not is_btl_sal_qc and is_finite_btl_sal:
        # woce flag is NOFLAG = 0
        # Where var not nan, set flag = 0
        sal_qc = xr.where(
            nc_profile['bottle_salinity'].isnan, nan_array, zero_array)

        use_sal = 'bottle_salinity'

    # TODO
    # Should conversion still be done if no finite sal?
    elif not is_ctd_sal and is_btl_sal and is_btl_sal_qc and not is_finite_btl_sal:
        sal_qc = nc_profile['bottle_salinity_qc']

        use_sal = 'bottle_salinity'

    # If neither salinity exists
    if not is_btl_sal and not is_ctd_sal:
        sal_qc = nan_array
        use_sal = None

        logging.info("********************************************")
        logging.info("No non nan salinity to convert oxygen units")
        logging.info("*******************************************")

        station_cast = nc_profile['station_cast'].values

        # To become string
        station_cast = station_cast.tolist()

        # join it
        station_cast = ''.join(station_cast)

        profiles_no_oxy_conversions[var].append(station_cast)

    if use_sal:
        sal_qc = sal_qc.values.tolist()

    return use_sal, sal_qc, profiles_no_oxy_conversions


def get_temp_to_use_and_qc(nc_profile, var, profiles_no_oxy_conversions):

    # Find temperature qc

    profile_size = nc_profile.sizes['N_LEVELS']

    nan_array = np.empty(profile_size)
    nan_array[:] = np.nan

    zero_array = np.zeros(profile_size)

    is_finite_ctd_temp = False
    is_finite_ctd_temp_qc = False

    is_finite_temp_68 = False
    is_finite_temp_68_qc = False

    is_ctd_temp = 'ctd_temperature' in nc_profile.keys()
    is_ctd_temp_qc = 'ctd_temperature_qc' in nc_profile.keys()

    if is_ctd_temp:
        is_finite_ctd_temp = not np.isnan(nc_profile['ctd_temperature']).all()

        logging.info(
            f"Finite values exist in ctd temperature: {is_finite_ctd_temp}")
        logging.info(
            f"Does ctd temperature have qc values: {is_ctd_temp_qc}")

    # Find if some good qc in ctd_temperature
    if is_ctd_temp and is_ctd_temp_qc:
        ctd_temp = nc_profile['ctd_temperature']
        ctd_temp_qc = nc_profile['ctd_temperature_qc']

        two_flags = np.isclose(ctd_temp_qc, 2, 0.1)
        zero_flags = np.isclose(ctd_temp_qc, 0, 0.1)

        search_good_vals = np.where(
            (zero_flags | two_flags), ctd_temp_qc, nan_array)

        has_good_ctd_temp_qc = not np.isnan(search_good_vals).all()

        is_good_ctd_temp = has_good_ctd_temp_qc and is_finite_ctd_temp

        logging.info(
            f"Does ctd temperature qc have good values: {has_good_ctd_temp_qc}")

    is_temp_68 = 'ctd_temperature_68' in nc_profile.keys()
    is_temp_68_qc = 'ctd_temperature_68_qc' in nc_profile.keys()

    if is_temp_68:
        is_finite_temp_68 = not np.isnan(
            nc_profile['ctd_temperature_68']).all()

        logging.info(
            f"Finite values exist in ctd temperature 68: {is_finite_temp_68}")
        logging.info(
            f"Does ctd temperature 68 have qc values: {is_temp_68_qc}")

    # Find if some good qc in ctd_temperature_68
    if is_temp_68 and is_temp_68_qc:
        temp_68 = nc_profile['ctd_temperature_68']
        temp_68_qc = nc_profile['ctd_temperature_68_qc']

        two_flags = np.isclose(temp_68_qc, 2, 0.1)
        zero_flags = np.isclose(temp_68_qc, 0, 0.1)

        search_good_vals = np.where(
            (zero_flags | two_flags), temp_68_qc, nan_array)

        has_good_temp_68_qc = not np.isnan(search_good_vals).all()
        is_good_temp_68 = has_good_temp_68_qc and is_finite_temp_68

        logging.info(
            f"Does temperature 68 qc have good vals: {has_good_temp_68_qc}")

    # Determine which temperature to use and create qc if it doesn't exist

    # Determine if using ctd_temperature or ctd_temperature_68
    if is_ctd_temp and is_ctd_temp_qc and is_finite_ctd_temp:
        temp_qc = nc_profile['ctd_temperature_qc']
        use_temp = 'ctd_temperature'

    elif is_temp_68 and is_temp_68_qc and is_finite_temp_68:
        temp_qc = nc_profile['ctd_temperature_68_qc']
        use_temp = 'ctd_temperature_68'

    elif is_ctd_temp and not is_ctd_temp_qc and is_finite_ctd_temp:
        # woce flag is NOFLAG = 0
        # Where var not nan, set flag = 0
        temp_qc = xr.where(
            nc_profile['ctd_temperature'].isnan, nan_array, zero_array)

        use_temp = 'ctd_temperature'

    elif is_temp_68 and not is_temp_68_qc and is_finite_temp_68:
        # woce flag is NOFLAG = 0
        # Where var not nan, set flag = 0
        temp_qc = xr.where(
            nc_profile['ctd_temperature_68'].isnan, nan_array, zero_array)

        use_temp = 'ctd_temperature_68'

    # TODO
    # Should conversion still be done if no finite temp?
    elif is_ctd_temp and is_ctd_temp_qc and not is_finite_ctd_temp and not is_temp_68:
        temp_qc = nc_profile['ctd_temperature_qc']

        use_temp = 'ctd_temperature'

    # TODO
    # Should conversion still be done if no finite temp?
    elif not is_ctd_temp and is_temp_68 and is_temp_68_qc and not is_finite_temp_68:
        temp_qc = nc_profile['ctd_temperature_68_qc']

        use_temp = 'ctd_temperature_68'

    else:

        temp_qc = nan_array
        use_temp = None

        logging.info("*******************************************")
        logging.info("No non nan ctd temp to convert oxygen units")
        logging.info("*******************************************")

        station_cast = nc_profile['station_cast'].values

        # To become string
        station_cast = station_cast.tolist()

        # join it
        station_cast = ''.join(station_cast)

        profiles_no_oxy_conversions[var].append(station_cast)

    if use_temp:
        temp_qc = temp_qc.values.tolist()

    return use_temp, temp_qc, profiles_no_oxy_conversions


def get_var_qc_if_sal_temp_and_oxy(nc_profile, var, profiles_no_oxy_conversions):

    # Depends on temperature and salinity flags

    # If the temp_qc is bad, use in calculation but set qc
    # of that oxygen value to qc of temperature. Can't
    # convert an oxygen with a bad temperature qc. Same
    # for the salinity.

    # already accounted for case of no_sal_temp

    profile_size = nc_profile.sizes['N_LEVELS']

    if not f"{var}_qc" in nc_profile.keys():

        # woce flag is NOFLAG = 0
        # Where var not nan, set flag = 0

        nan_array = np.empty(profile_size)
        nan_array[:] = np.nan

        zero_array = np.zeros(profile_size)

        var_vals = nc_profile[var].values.tolist()

        # var_qc = xr.where(nc_profile[var].isnan, nan_array, zero_array)
        var_qc = np.where(np.isnan(var_vals), nan_array, zero_array)

    else:

        # Already checked case of no salinity, temperature or var
        # So 'ctd_temperature' or 'ctd_temperature_68' exists
        # And 'ctd_salinity' or 'bottle_salinity' exists

        # Find qc depending on salinity and temperature qc

        # TODO
        # use same logic of choosing which salinity and temperature to use

        var_qc = nc_profile[f"{var}_qc"]

        var_qc = var_qc.values.tolist()

        nan_array = np.empty(profile_size)
        nan_array[:] = np.nan

        zero_array = np.zeros(profile_size)

        # Find salinity qc
        use_sal, sal_qc, profiles_no_oxy_conversions = get_sal_to_use_and_qc(
            nc_profile, var, profiles_no_oxy_conversions)

        # Find temperature qc
        use_temp, temp_qc, profiles_no_oxy_conversions = get_temp_to_use_and_qc(
            nc_profile, var, profiles_no_oxy_conversions)

        # Now find flag when all combined for calulation
        # Calculation will only have a good flag of qc=0 or 2 if all
        # var, salinity, and temperature have a good flag of qc=0 or 2

        # If all good, use qc of oxygen

        # TODO, ask if this is correct
        # If one is bad, use qc = 4 BAD

        # To combine, easist to mark non qc=0 or 2 flag with a negaive number (-100)
        # Then when look at sum of flags and if the result is negative, it's a bad flag,
        # and will use a qc=4 for conveerted var
        # If the sum is postive (from qc=0 or qc=2), will use the var qc

        # Since the xarray is a combination of all profiles, null values
        # are filled with nan. Keep any nan qc values

        neg_array = np.empty(profile_size)
        neg_array[:] = -100

        # Wherever qc is bad and finite, put a -100 flag
        two_flags = np.isclose(var_qc, 2, 0.1)
        zero_flags = np.isclose(var_qc, 0, 0.1)

        flagged_var_qc = np.where(
            (zero_flags | two_flags), var_qc, neg_array)

        two_flags = np.isclose(sal_qc, 2, 0.1)
        zero_flags = np.isclose(sal_qc, 0, 0.1)

        flagged_sal_qc = np.where(
            (zero_flags | two_flags), sal_qc, neg_array)

        two_flags = np.isclose(temp_qc, 2, 0.1)
        zero_flags = np.isclose(temp_qc, 0, 0.1)

        flagged_temp_qc = np.where(
            (zero_flags | two_flags), temp_qc, neg_array)

        # stack next to each other and sum row by row
        # TODO
        # Does this work stacking numpy after using xr.where?
        all_qc = np.column_stack(
            [flagged_var_qc, flagged_sal_qc, flagged_temp_qc])

        sum_qc = np.sum(all_qc, axis=1)

        # Where there is a neg # (bad qc) for temp or sal or var, set
        # to flag = 4 (bad). For positive, set to oxy qc

        bad_array = np.empty(profile_size)
        bad_array[:] = 4

        var_qc = np.where(((sum_qc < 0) | np.isnan(sum_qc)),
                          bad_array, var_qc)

    return var_qc


def check_if_all_nan(nc_profile, var):
    # TODO ***
    # Put this in it's own function checking if
    # have non nan values for all 3 variables oxy, sal, temp

    oxy = nc_profile[var].values.tolist()
    oxy_all_nan = np.isnan(oxy).all()

    if 'ctd_temperature' in nc_profile.keys():
        temp = nc_profile['ctd_temperature'].values.tolist()
        temp_all_nan = np.isnan(temp).all()
    else:
        temp_all_nan = True

    if 'ctd_temperature_68' in nc_profile.keys():
        temp = nc_profile['ctd_temperature_68'].values.tolist()
        temp_68_all_nan = np.isnan(temp).all()
    else:
        temp_68_all_nan = True

    one_temp_exists = not temp_all_nan or not temp_68_all_nan

    if 'ctd_salinity' in nc_profile.keys():
        sal = nc_profile['ctd_salinity'].values.tolist()
        sal_all_nan = np.isnan(sal).all()
    else:
        sal_all_nan = True

    if 'bottle_salinity' in nc_profile.keys():
        sal = nc_profile['bottle_salinity'].values.tolist()
        sal_btl_all_nan = np.isnan(sal).all()
    else:
        sal_btl_all_nan = True

    #one_sal_exists = not sal_all_nan or not sal_btl_all_nan

    if 'ctd_salinity' in nc_profile.keys():
        one_sal_exists = not sal_all_nan

    if 'ctd_salinity' not in nc_profile.keys() and 'bottle_salinity' in nc_profile.keys():
        one_sal_exists = not sal_btl_all_nan

    if 'ctd_salinity' not in nc_profile.keys() and 'bottle_salinity' not in nc_profile.keys():
        one_sal_exists = False

    # Change to not convert is no temp  or sal

    # if oxy all nan, result will be all nan

    # if oxy_all_nan or not one_temp_exists or not one_sal_exists:
    if not one_temp_exists or not one_sal_exists:
        if not one_temp_exists:
            logging.info(
                '*** No oxygen conversion because missing CTD Temperature')

        if not one_sal_exists:
            logging.info('*** No oxygen conversion because missing Salinity')

        no_sal_temp = True
    else:
        no_sal_temp = False

    return no_sal_temp


def convert_oxygen_to_new_units(nc, var, profiles_no_oxy_conversions):

    logging.info('***** o2 conversion ******')

    # Check if values exist or if empty
    # because variables in nc_profile can exist but be all null
    # for some profiles or all

    converted_groups = []

    profile_size = nc.sizes['N_LEVELS']

    first_group = True

    for nc_group in nc.groupby('N_PROF'):

        logging.info('-------------------')
        logging.info(f"Converting oxygen for profile {nc_group[0]}")

        nc_profile = nc_group[1]

        var_values = nc_profile[var].values.tolist()
        has_finite_values = not np.isnan(var_values).all()

        logging.info(f"Does {var} have finite values: {has_finite_values}")

        # TODO
        # Add check for all null values of oxy, sal or temp
        # if they are, can't convert and revert back to original
        # oxy with same units attribute.
        # Don't modify c[var].attrs['units']

        # oxy = nc_profile[var]
        # See what ctd temperature exists and check if all nan
        # Check both bottle and ctd salinity to see if one exists
        # or if both all nan

        # --------------------

        no_sal_temp = check_if_all_nan(nc_profile, var)

        # ----------------
        # Get var qc values
        # ----------------

        if no_sal_temp:

            # TODO
            # Don't convert var
            # Keep track of in profiles_no_oxy_conversions

            # profiles_no_oxy_conversions

            if f"{var}_qc" in nc_profile.keys():
                var_qc = nc_profile[f"{var}_qc"]

            else:
                # set to flag = 0 since no qc
                var_qc = np.zeros(profile_size)

            station_cast = nc_profile['station_cast'].values

            # To become string
            station_cast = station_cast.tolist()

            # join it
            station_cast = ''.join(station_cast)

            profiles_no_oxy_conversions[var].append(station_cast)

        else:

            var_qc = get_var_qc_if_sal_temp_and_oxy(
                nc_profile, var, profiles_no_oxy_conversions)

            two_flags = np.isclose(var_qc, 2, 0.1)
            zero_flags = np.isclose(var_qc, 0, 0.1)

            nan_array = np.empty(profile_size)
            nan_array[:] = np.nan

            search_good_vals = np.where(
                (zero_flags | two_flags), var_qc, nan_array)

            has_good_var_qc = not np.isnan(search_good_vals).all()

            logging.info(f"Does {var} have good qc values: {has_good_var_qc}")

        if first_group:
            all_var_qc = var_qc
            first_group = False
        else:
            # This stacks downward
            all_var_qc = np.vstack((all_var_qc, var_qc))

        # nc_profile[f"{var}_qc"] = var_qc

        # ------------------------
        # Calculate var conversion
        # ------------------------

        # Convert if any good vars, otherwise, don't convert

        if no_sal_temp:

            # TODO

            # add to profiles_no_oxy_conversions

            # TODO
            # ask Sarah if should convert anyway and use bad flag with it
            logging.info(
                "Missing quality temp or sal needed to do Oxygen conversion")
            logging.info(f"Didn't convert {var} for profile")

            # Leave as is
            converted_nc_profile = nc_profile

        else:

            converted_nc_profile, profiles_no_oxy_conversions = convert_oxygen(
                nc_profile, var, profiles_no_oxy_conversions)

        converted_groups.append(converted_nc_profile)

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
    # if f"{var}_qc" in nc.keys() and len(all_var_qc):
    if len(all_var_qc):
        qc_var = {f"{var}_qc": (('N_PROF', 'N_LEVELS'), all_var_qc)}

        nc = nc.assign(qc_var)

    return nc, profiles_no_oxy_conversions


def convert_cchdo_to_argovis_units(nc):

    params = nc.keys()

    # If cchdo units aren't the same as argovis units, convert
    # So far, just converting oxygen

    profiles_no_oxy_conversions = {}

    for var in params:

        try:
            var_cchdo_units = nc[var].attrs['units']
        except:
            continue

        if 'oxygen' in var and var_cchdo_units == 'ml/l':

            logging.info(f"Converting oxygen for var {var}")

            profiles_no_oxy_conversions[var] = []

            nc, profiles_no_oxy_conversions = convert_oxygen_to_new_units(
                nc, var, profiles_no_oxy_conversions)

            # TODO
            # Move this to end which I thoutht it was
            # Make a note if conversion went OK, and if so, change it below
            # Maybe because some profiles weren't converted, so need to
            # keep track of that too. so make a profiles_w_conversons. But
            # don't I reverse the unit change notation?

            # ***
            # uncomment this

            # set attribute and keep track of whatt didn't convert
            # can do this after do oxygen conversion for var
            nc[var].attrs['units'] = 'micromole/kg'

    return nc, profiles_no_oxy_conversions


def convert_sea_water_temp(nc_profile, var, var_cchdo_ref_scale, argovis_ref_scale):

    # Check sea_water_temperature to have cchdo_reference_scale be ITS-90

    if var_cchdo_ref_scale == 'IPTS-68' and argovis_ref_scale == 'ITS-90':

        logging.info("*** Converting sea water temperature ref scale")

        # Convert to ITS-90 scal
        temperature = nc_profile[var].data

        converted_temperature = temperature/1.00024

        # Set temperature value in nc_profile because use it later to
        # create profile dict
        nc_profile[var].data = converted_temperature
        nc_profile[var].attrs['reference_scale'] = 'ITS-90'

    return nc_profile


def convert_cchdo_to_argovis_ref_scale(nc_profile):

    params = nc_profile.keys()

    # If argo ref scale not equal to cchdo ref scale, convert

    # So far, it's only the case for temperature

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    argovis_ref_scale_per_type = get_argovis_reference_scale_per_type()

    for var in params:
        if 'temperature' in var:

            try:
                # Get cchdo reference scale of var
                var_cchdo_ref_scale = nc_profile[var].attrs['reference_scale']

                argovis_ref_scale = argovis_ref_scale_per_type['temperature']
                is_same_scale = var_cchdo_ref_scale == argovis_ref_scale

                if not is_same_scale:
                    nc_profile = convert_sea_water_temp(
                        nc_profile, var, var_cchdo_ref_scale, argovis_ref_scale)
            except:
                pass

    return nc_profile


def apply_conversions(nc):

    # TODO ***
    # Make copy of this file and start without all the
    # commented out code so it's clearer to work with

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

    nc, profiles_no_oxy_conversions = convert_cchdo_to_argovis_units(nc)

    return nc, profiles_no_oxy_conversions
