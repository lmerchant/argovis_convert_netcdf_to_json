
# Check if files have pressure and ctd_temperature to be a
# proper file for ArgoVis

import os
import logging


def check_missing_variables(profile, variables_check, logging_dir):

    # Now find which variables are missing

    profile_dict = profile['profile_dict']
    station_cast = profile['station_cast']
    expocode = profile_dict['meta']['expocode']
    type = profile_dict['type']

    has_pres = variables_check['has_pres']
    has_ctd_temp_btl = variables_check['has_ctd_temp_btl']
    has_ctd_temp_ctd = variables_check['has_ctd_temp_ctd']
    has_ctd_temp_qc_btl = variables_check['has_ctd_temp_qc_btl']
    has_ctd_temp_qc_ctd = variables_check['has_ctd_temp_qc_ctd']
    has_ctd_temp_w_ref_scale_btl = variables_check['has_ctd_temp_w_ref_scale_btl']
    has_ctd_temp_w_ref_scale_ctd = variables_check['has_ctd_temp_w_ref_scale_ctd']
    has_ctd_temp_unk_btl = variables_check['has_ctd_temp_unk_btl']
    has_ctd_temp_unk_ctd = variables_check['has_ctd_temp_unk_ctd']

    # No pressure
    if not has_pres:
        filename = 'cruises_no_pressure.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast}\n")

    # Has no ctd core variables with at least a ctd temperature
    # regardless of if it has a qc or not
    if not has_pres and not has_ctd_temp_btl and not has_ctd_temp_ctd:
        filename = 'cruises_no_core_ctd_vars.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast}\n")

    # Break up to look at btl
    if type == 'btl' or type == 'btl_ctd':

        # Not looking at pressure next but whether an indivdual type has ctd temperature
        # irregardless of qc
        if not has_ctd_temp_btl:
            filename = 'cruises_no_ctd_temp.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {type} and indiv type BTL\n")
                f.write(f"station cast {station_cast}\n")

        # # Doesn't have a ctd temp w ref scale
        # # Usually, if it doesn't have this, it has a ctd_temperature_unk
        # if not has_ctd_temp_w_ref_scale_btl:
        #     filename = 'cruises_no_ctd_temp_ref_scale.txt'
        #     filepath = os.path.join(logging_dir, filename)
        #     with open(filepath, 'a') as f:
        #         f.write('-----------\n')
        #         f.write(f"expocode {expocode}\n")
        #         f.write(f"collection type {type} and indiv type BTL\n")
        #         f.write(f"station cast {station_cast}\n")

        # Look for ctd_temperature_unk
        if has_ctd_temp_unk_btl:
            filename = 'cruises_w_ctd_temp_unk.txt'
            filepath = os.path.join(logging_dir, filename)
            cruise_date = profile_dict['meta']['date_formatted']
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"cruise date {cruise_date}\n")
                f.write(f"collection type {type} and indiv type BTL\n")
                f.write(f"station cast {station_cast}\n")

        # Has a ctd temperature but no qc
        if has_ctd_temp_btl and not has_ctd_temp_qc_btl:
            filename = 'cruises_w_ctd_temp_no_qc.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {type} and indiv type BTL\n")
                f.write(f"station cast {station_cast}\n")

        # Has a ctd temperature but no ref scale
        # Most likely doesn't exist
        # Because if this occurs, looks like ctd temp renamed with
        # suffix _unk
        if not has_ctd_temp_w_ref_scale_btl:
            filename = 'cruises_w_ctd_temp_no_ref_scale.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {type} and indiv type BTL\n")
                f.write(f"station cast {station_cast}\n")

    # Break up to look at ctd
    if type == 'ctd' or type == 'btl_ctd':

        # Not looking at pressure next but whether an indivdual type has ctd temperature
        # irregardless of qc

        if not has_ctd_temp_ctd:
            filename = 'cruises_no_ctd_temp.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {type} and indiv type CTD\n")
                f.write(f"station cast {station_cast}\n")

        # # Doesn't have a ctd temp w ref scale
        # # Usually, if it doesn't have this, it has a ctd_temperature_unk
        # if not has_ctd_temp_w_ref_scale_ctd:
        #     filename = 'cruises_no_ctd_temp_ref_scale.txt'
        #     filepath = os.path.join(logging_dir, filename)
        #     with open(filepath, 'a') as f:
        #         f.write('-----------\n')
        #         f.write(f"expocode {expocode}\n")
        #         f.write(f"collection type {type} and indiv type CTD\n")
        #         f.write(f"station cast {station_cast}\n")

        # Look for ctd_temperature_unk
        if has_ctd_temp_unk_ctd:
            cruise_date = profile_dict['meta']['date_formatted']
            filename = 'cruises_w_ctd_temp_unk.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"cruise date {cruise_date}\n")
                f.write(f"collection type {type} and indiv type CTD\n")
                f.write(f"station cast {station_cast}\n")

        # Has a ctd temperature but no qc
        if has_ctd_temp_ctd and not has_ctd_temp_qc_ctd:
            filename = 'cruises_w_ctd_temp_no_qc.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {type} and indiv type CTD\n")
                f.write(f"station cast {station_cast}\n")

        # Has a ctd temperature but no ref scale
        # Most likely doesn't exist
        # Because if this occurs, looks like ctd temp renamed with
        # suffix _unk

        if has_ctd_temp_ctd and not has_ctd_temp_w_ref_scale_ctd:
            filename = 'cruises_w_ctd_temp_no_ref_scale.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode} for CTD\n")
                f.write(f"collection type {type} and indiv type CTD\n")
                f.write(f"station cast {station_cast}\n")

    check_of_ctd_vars = {}
    check_of_ctd_vars['has_all_ctd_vars'] = False
    check_of_ctd_vars['collection_type'] = type
    check_of_ctd_vars['profile_checked'] = profile

    return check_of_ctd_vars


def check_ctd_vars_one_profile(profile, logging_dir):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    profile_dict = profile['profile_dict']
    station_cast = profile['station_cast']

    type = profile_dict['type']

    expocode = profile_dict['meta']['expocode']
    bgc_meas = profile_dict['bgcMeas']

    for obj in bgc_meas:
        if 'pres' in obj.keys():
            has_pres = True
        else:
            has_pres = False

    # try:
    #     goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMappingBtl']
    #     goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMappingCtd']
    # except:
    #     goship_argovis_name_mapping = profile_dict['goshipArgovisNameMapping']

    # Look for ctd_temperature
    try:
<<<<<<< HEAD
        has_ctd_temp_btl = 'temp_btl' in profile_dict['argovisParamNamesBtl']
        has_ctd_temp_ctd = 'temp_ctd' in profile_dict['argovisParamNamesCtd']
    except KeyError:
        has_ctd_temp_btl = 'temp_btl' in profile_dict['argovisParamNames']
        has_ctd_temp_ctd = 'temp_ctd' in profile_dict['argovisParamNames']

    # Look for ctd_temperature with qc
    try:
        has_ctd_temp_qc_btl = 'temp_btl_qc' in profile_dict['argovisParamNamesBtl']
        has_ctd_temp_qc_ctd = 'temp_ctd_qc' in profile_dict['argovisParamNamesCtd']
    except KeyError:
        has_ctd_temp_qc_btl = 'temp_btl_qc' in profile_dict['argovisParamNames']
        has_ctd_temp_qc_ctd = 'temp_ctd_qc' in profile_dict['argovisParamNames']
=======
        has_ctd_temp_btl = 'temp_btl' in profile_dict['argovisNamesBtl']
        has_ctd_temp_ctd = 'temp_ctd' in profile_dict['argovisNamesCtd']
    except:
        has_ctd_temp_btl = 'temp_btl' in profile_dict['argovisNames']
        has_ctd_temp_ctd = 'temp_ctd' in profile_dict['argovisNames']

    # Look for ctd_temperature with qc
    try:
        has_ctd_temp_qc_btl = 'temp_btl_qc' in profile_dict['argovisNamesBtl']
        has_ctd_temp_qc_ctd = 'temp_ctd_qc' in profile_dict['argovisNamesCtd']
    except:
        has_ctd_temp_qc_btl = 'temp_btl_qc' in profile_dict['argovisNames']
        has_ctd_temp_qc_ctd = 'temp_ctd_qc' in profile_dict['argovisNames']
>>>>>>> 689fb70962b7b22fd8af98fc653c839f744bbee8

    # Look at ctd_temperature ref scale
    argovis_ref_scale = profile_dict['argovisReferenceScale']

    has_ctd_temp_w_ref_scale_btl = 'temp_btl' in argovis_ref_scale.keys()
    has_ctd_temp_w_ref_scale_ctd = 'temp_ctd' in argovis_ref_scale.keys()

    # Look for ctd_temperature_unk (unknown ref scale)
    try:
<<<<<<< HEAD
        has_ctd_temp_unk_btl = 'ctd_temperature_unk_btl' in profile_dict['argovisParamNamesBtl']
    except KeyError:
        has_ctd_temp_unk_btl = 'ctd_temperature_unk_btl' in profile_dict['argovisParamNames']

    try:
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk_ctd' in profile_dict['argovisParamNamesCtd']
    except KeyError:
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk_ctd' in profile_dict['argovisParamNames']
=======
        has_ctd_temp_unk_btl = 'ctd_temperature_unk_btl' in profile_dict['argovisNamesBtl']
    except KeyError:
        has_ctd_temp_unk_btl = 'ctd_temperature_unk_btl' in profile_dict['argovisNames']

    try:
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk_ctd' in profile_dict['argovisNamesCtd']
    except KeyError:
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk_ctd' in profile_dict['argovisNames']
>>>>>>> 689fb70962b7b22fd8af98fc653c839f744bbee8

    # Summary of variables checked
    variables_check = {}
    variables_check['has_pres'] = has_pres
    variables_check['has_ctd_temp_btl'] = has_ctd_temp_btl
    variables_check['has_ctd_temp_ctd'] = has_ctd_temp_ctd
    variables_check['has_ctd_temp_qc_btl'] = has_ctd_temp_qc_btl
    variables_check['has_ctd_temp_qc_ctd'] = has_ctd_temp_qc_ctd
    variables_check['has_ctd_temp_w_ref_scale_btl'] = has_ctd_temp_w_ref_scale_btl
    variables_check['has_ctd_temp_w_ref_scale_ctd'] = has_ctd_temp_w_ref_scale_ctd
    variables_check['has_ctd_temp_unk_btl'] = has_ctd_temp_unk_btl
    variables_check['has_ctd_temp_unk_ctd'] = has_ctd_temp_unk_ctd

    # Summary of variables with CTD variables
    check_of_ctd_vars = {}
    check_of_ctd_vars['type'] = type
    check_of_ctd_vars['station_cast'] = station_cast
    check_of_ctd_vars['profile_checked'] = profile
    check_of_ctd_vars['has_all_ctd_vars'] = {}
    check_of_ctd_vars['has_ctd_vars_no_qc'] = {}
    check_of_ctd_vars['has_ctd_temp_unk'] = {}

    # Test if have all CTD vars including temp qc and ref scale
    check_of_ctd_vars['has_all_ctd_vars']['btl'] = False
    check_of_ctd_vars['has_all_ctd_vars']['ctd'] = False

    # partial CTD vars are pres, ctd_temp, ref_scale but no qc
    check_of_ctd_vars['has_ctd_vars_no_qc']['btl'] = False
    check_of_ctd_vars['has_ctd_vars_no_qc']['ctd'] = False

    # ctd temp but unkown ref scale
    check_of_ctd_vars['has_ctd_temp_unk']['btl'] = False
    check_of_ctd_vars['has_ctd_temp_unk']['ctd'] = False

    if has_pres and has_ctd_temp_btl and has_ctd_temp_qc_btl and has_ctd_temp_w_ref_scale_btl:
        check_of_ctd_vars['has_all_ctd_vars']['btl'] = True

    if has_pres and has_ctd_temp_ctd and has_ctd_temp_qc_ctd and has_ctd_temp_w_ref_scale_ctd:
        check_of_ctd_vars['has_all_ctd_vars']['ctd'] = True

    if has_pres and has_ctd_temp_btl and has_ctd_temp_qc_btl and has_ctd_temp_w_ref_scale_btl:
        check_of_ctd_vars['has_all_ctd_vars']['btl'] = True

    if has_ctd_temp_btl and not has_ctd_temp_qc_btl:
        check_of_ctd_vars['has_ctd_vars_no_qc']['btl'] = True

    if has_ctd_temp_ctd and not has_ctd_temp_qc_ctd:

        check_of_ctd_vars['has_ctd_vars_no_qc']['ctd'] = True

    if has_ctd_temp_unk_btl:
        check_of_ctd_vars['has_ctd_temp_unk']['btl'] = True

    if has_ctd_temp_unk_ctd:
        check_of_ctd_vars['has_ctd_temp_unk']['ctd'] = True

    # Now find which variables that are missing and write to files

    check_missing_variables(
        profile, variables_check, logging_dir)

    return check_of_ctd_vars


def check_of_ctd_variables(profiles, logging_dir):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    logging.info("---------------------------")
    logging.info("Check if have CTD variables")
    logging.info("---------------------------")

    all_profiles_checked_ctd_vars = []

    flags = []

    for profile in profiles:

        checked_ctd_vars_one_profile = check_ctd_vars_one_profile(
            profile, logging_dir)

        has_btl = checked_ctd_vars_one_profile['has_all_ctd_vars']['btl']
        has_ctd = checked_ctd_vars_one_profile['has_all_ctd_vars']['ctd']

        if has_btl or has_ctd:
            flags.append(True)

        all_profiles_checked_ctd_vars.append(checked_ctd_vars_one_profile)

    # sum(flags) gives number of True values, so if it is 0, no ctd vars

    logging.info("-------------------------------------------")
    logging.info("Finished checking for CTD variables")
    logging.info(f"Number of profiles  {len(profiles)}")
    logging.info(f"Found {sum(flags)} profiles with CTD vars")
    logging.info("-------------------------------------------")

    return all_profiles_checked_ctd_vars, sum(flags)
