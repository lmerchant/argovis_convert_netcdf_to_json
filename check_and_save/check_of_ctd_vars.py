
# Check if files have pressure and ctd_temperature to be a
# proper file for ArgoVis

import os
import logging

from global_vars import GlobalVars


def log_missing_variables(profile, variables_check):

    logging_dir = GlobalVars.LOGGING_DIR

    # Now find which variables are missing

    profile_dict = profile['profile_dict']
    station_cast = profile['station_cast']
    expocode = profile_dict['meta']['expocode']
    data_type = profile_dict['data_type']

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
            f.write(f"collection type {data_type}\n")
            f.write(f"station cast {station_cast}\n")

    # Has no ctd core variables with at least a ctd temperature
    # regardless of if it has a qc or not
    if not has_pres and not has_ctd_temp_btl and not has_ctd_temp_ctd:
        filename = 'cruises_no_core_ctd_vars.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {data_type}\n")
            f.write(f"station cast {station_cast}\n")

    # Break up to look at btl
    if data_type == 'btl' or data_type == 'btl_ctd':

        # Not looking at pressure next but whether an indivdual type has ctd temperature
        # irregardless of qc
        if not has_ctd_temp_btl:
            filename = 'cruises_no_ctd_temp.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {data_type} and indiv type BTL\n")
                f.write(f"station cast {station_cast}\n")

        # # Doesn't have a ctd temp w ref scale
        # # Usually, if it doesn't have this, it has a ctd_temperature_unk
        # if not has_ctd_temp_w_ref_scale_btl:
        #     filename = 'cruises_no_ctd_temp_ref_scale.txt'
        #     filepath = os.path.join(logging_dir, filename)
        #     with open(filepath, 'a') as f:
        #         f.write('-----------\n')
        #         f.write(f"expocode {expocode}\n")
        #         f.write(f"collection type {data_type} and indiv type BTL\n")
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
                f.write(f"collection type {data_type} and indiv type BTL\n")
                f.write(f"station cast {station_cast}\n")

        # Has a ctd temperature but no qc
        if has_ctd_temp_btl and not has_ctd_temp_qc_btl:
            filename = 'cruises_w_ctd_temp_no_qc.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {data_type} and indiv type BTL\n")
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
                f.write(f"collection type {data_type} and indiv type BTL\n")
                f.write(f"station cast {station_cast}\n")

    # Break up to look at ctd
    if data_type == 'ctd' or data_type == 'btl_ctd':

        # Not looking at pressure next but whether an indivdual type has ctd temperature

        if not has_ctd_temp_ctd:
            filename = 'cruises_no_ctd_temp.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {data_type} and indiv type CTD\n")
                f.write(f"station cast {station_cast}\n")

        # # Doesn't have a ctd temp w ref scale
        # # Usually, if it doesn't have this, it has a ctd_temperature_unk
        # if not has_ctd_temp_w_ref_scale_ctd:
        #     filename = 'cruises_no_ctd_temp_ref_scale.txt'
        #     filepath = os.path.join(logging_dir, filename)
        #     with open(filepath, 'a') as f:
        #         f.write('-----------\n')
        #         f.write(f"expocode {expocode}\n")
        #         f.write(f"collection type {data_type} and indiv type CTD\n")
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
                f.write(f"collection type {data_type} and indiv type CTD\n")
                f.write(f"station cast {station_cast}\n")

        # Has a ctd temperature but no qc
        if has_ctd_temp_ctd and not has_ctd_temp_qc_ctd:
            filename = 'cruises_w_ctd_temp_no_qc.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {data_type} and indiv type CTD\n")
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
                f.write(f"collection type {data_type} and indiv type CTD\n")
                f.write(f"station cast {station_cast}\n")


def check_ctd_vars_one_profile(profile):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    profile_dict = profile['profile_dict']
    station_cast = profile['station_cast']

    data_type = profile_dict['data_type']

    expocode = profile_dict['meta']['expocode']
    bgc_meas = profile_dict['bgcMeas']

    for obj in bgc_meas:
        if 'pres' in obj.keys():
            has_pres = True
        else:
            has_pres = False

    # try:
    #     cchdo_argovis_name_mapping_btl = profile_dict['cchdoArgovisNameMappingBtl']
    #     cchdo_argovis_name_mapping_ctd = profile_dict['cchdoArgovisNameMappingCtd']
    # except:
    #     cchdo_argovis_name_mapping = profile_dict['cchdoArgovisNameMapping']

    # Look for ctd_temperature
    try:
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

    # Look at ctd_temperature ref scale
    argovis_ref_scale = profile_dict['argovisReferenceScale']

    has_ctd_temp_w_ref_scale_btl = 'temp_btl' in argovis_ref_scale.keys()
    has_ctd_temp_w_ref_scale_ctd = 'temp_ctd' in argovis_ref_scale.keys()

    # Look for ctd_temperature_unk (unknown ref scale)
    try:
        has_ctd_temp_unk_btl = 'ctd_temperature_unk_btl' in profile_dict['argovisParamNamesBtl']
    except KeyError:
        has_ctd_temp_unk_btl = 'ctd_temperature_unk_btl' in profile_dict['argovisParamNames']

    try:
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk_ctd' in profile_dict['argovisParamNamesCtd']
    except KeyError:
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk_ctd' in profile_dict['argovisParamNames']

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
    checked_profile_info = {}
    checked_profile_info['data_type'] = data_type
    checked_profile_info['station_cast'] = station_cast
    checked_profile_info['profile_checked'] = profile
    checked_profile_info['has_all_ctd_vars'] = {}
    checked_profile_info['has_ctd_vars_no_qc'] = {}
    checked_profile_info['has_ctd_temp_unk'] = {}

    # Test if have all CTD vars including temp qc and ref scale
    checked_profile_info['has_all_ctd_vars']['btl'] = False
    checked_profile_info['has_all_ctd_vars']['ctd'] = False

    # partial CTD vars are pres, ctd_temp, ref_scale but no qc
    checked_profile_info['has_ctd_vars_no_qc']['btl'] = False
    checked_profile_info['has_ctd_vars_no_qc']['ctd'] = False

    # ctd temp but unkown ref scale
    checked_profile_info['has_ctd_temp_unk']['btl'] = False
    checked_profile_info['has_ctd_temp_unk']['ctd'] = False

    if has_pres and has_ctd_temp_btl and has_ctd_temp_qc_btl and has_ctd_temp_w_ref_scale_btl:
        checked_profile_info['has_all_ctd_vars']['btl'] = True

    if has_pres and has_ctd_temp_ctd and has_ctd_temp_qc_ctd and has_ctd_temp_w_ref_scale_ctd:
        checked_profile_info['has_all_ctd_vars']['ctd'] = True

    if has_pres and has_ctd_temp_btl and has_ctd_temp_qc_btl and has_ctd_temp_w_ref_scale_btl:
        checked_profile_info['has_all_ctd_vars']['btl'] = True

    if has_ctd_temp_btl and not has_ctd_temp_qc_btl:
        checked_profile_info['has_ctd_vars_no_qc']['btl'] = True

    if has_ctd_temp_ctd and not has_ctd_temp_qc_ctd:

        checked_profile_info['has_ctd_vars_no_qc']['ctd'] = True

    if has_ctd_temp_unk_btl:
        checked_profile_info['has_ctd_temp_unk']['btl'] = True

    if has_ctd_temp_unk_ctd:
        checked_profile_info['has_ctd_temp_unk']['ctd'] = True

    # Now find which variables that are missing and write to files

    log_missing_variables(profile, variables_check)

    return checked_profile_info


def check_of_ctd_vars(data_type_profiles):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    logging.info("---------------------------")
    logging.info("Check if have CTD variables")
    logging.info("---------------------------")

    checked_profiles_info = []

    flags = []

    for data_profile in data_type_profiles:

        checked_profile_info = check_ctd_vars_one_profile(data_profile)

        has_btl = checked_profile_info['has_all_ctd_vars']['btl']
        has_ctd = checked_profile_info['has_all_ctd_vars']['ctd']

        if has_btl or has_ctd:
            flags.append(True)

        checked_profiles_info.append(checked_profile_info)

    # sum(flags) gives number of True values, so if it is 0,
    # no ctd vars for entire cruise (all profiles)

    logging.info("-------------------------------------------")
    logging.info("Finished checking for CTD variables")
    logging.info(f"Number of profiles  {len(data_type_profiles)}")
    logging.info(f"Found {sum(flags)} profiles with CTD vars")
    logging.info("-------------------------------------------")

    return checked_profiles_info, sum(flags)
