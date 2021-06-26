
# Check if files have pressure and ctd_temperature to be a
# proper file for ArgoVis

import os


def check_missing_variables(profile, variables_check, logging, logging_dir):

    # Now find which variables are missing

    profile_dict = profile['profile_dict']
    station_cast = profile['station_cast']
    expocode = profile_dict['meta']['expocode']
    type = profile_dict['type']

    try:
        goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMappingBtl']
        goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMappingCtd']
    except:
        goship_argovis_name_mapping = profile_dict['goshipArgovisNameMapping']

    has_pres = variables_check['has_pres']
    has_ctd_temp_btl = variables_check['has_ctd_temp_btl']
    has_ctd_temp_ctd = variables_check['has_ctd_temp_ctd']
    has_ctd_temp_qc_btl = variables_check['has_ctd_temp_qc_btl']
    has_ctd_temp_qc_ctd = variables_check['has_ctd_temp_qc_ctd']
    has_ctd_temp_w_ref_scale_btl = variables_check['has_ctd_temp_w_ref_scale_btl']
    has_ctd_temp_w_ref_scale_ctd = variables_check['has_ctd_temp_w_ref_scale_ctd']

    if not has_pres:
        logging.info(f'No pressure for {expocode} {station_cast}')
        filename = 'cruises_no_pressure.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast}\n")

    if not has_pres and not has_ctd_temp_btl and not has_ctd_temp_ctd:
        logging.info(
            f'Missing pres and ctd temperature for {expocode} {station_cast}')
        filename = 'cruises_no_core_ctd_vars.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast}\n")

    if not has_ctd_temp_btl:
        logging.info(
            f'No ctd temperature for BTL {expocode} {station_cast}')
        filename = 'cruises_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type} and indiv type BTL\n")
            f.write(f"station cast {station_cast}\n")

    if not has_ctd_temp_ctd:
        logging.info(
            f'No ctd temperature for CTD {expocode} {station_cast}')
        filename = 'cruises_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type} and indiv type CTD\n")
            f.write(f"station cast {station_cast}\n")

    # Look for ctd_temperature_unk
    # Stands for ctd_temperature with and unknown reference scale
    try:
        has_ctd_temp_unk_btl = 'ctd_temperature_unk' in goship_argovis_name_mapping_btl.keys()
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk' in goship_argovis_name_mapping_ctd.keys()

    except:
        has_ctd_temp_unk_btl = 'ctd_temperature_unk' in goship_argovis_name_mapping.keys()
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk' in goship_argovis_name_mapping.keys()

    if not has_ctd_temp_btl and has_ctd_temp_unk_btl:
        logging.info(
            f'Has ctd_temperature_unk for BTL {expocode} {station_cast}')
        filename = 'cruises_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(f"Has ctd_temperature_unk for BTL\n")

    if not has_ctd_temp_ctd and has_ctd_temp_unk_ctd:
        # Look for ctd_temperature_unk
        # Stands for ctd_temperature with and unknown reference scale
        logging.info(
            f'Has ctd_temperature_unk for CTD {expocode} {station_cast}')
        filename = 'cruises_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(f"Has ctd_temperature_unk for CTD\n")

    if not has_ctd_temp_w_ref_scale_btl:
        logging.info(
            f'No ctd temperature w ref scale for BTL {expocode} {station_cast}')
        filename = 'cruises_no_ctd_temp_ref_scale.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type} and indiv type BTL\n")
            f.write(f"station cast {station_cast}\n")

    if not has_ctd_temp_w_ref_scale_ctd:
        logging.info(
            f'No ctd temperature w ref scale for CTD {expocode} {station_cast}')
        filename = 'cruises_no_ctd_temp_ref_scale.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type} and indiv type CTD\n")
            f.write(f"station cast {station_cast}\n")

    if has_ctd_temp_btl and not has_ctd_temp_qc_btl:
        logging.info(
            f'Has CTD temperature and no qc for BTL {expocode} {station_cast}')
        filename = 'cruises_w_ctd_temp_no_qc.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type} and indiv type BTL\n")
            f.write(f"station cast {station_cast}\n")

    if has_ctd_temp_ctd and not has_ctd_temp_qc_ctd:
        logging.info(
            f'Has CTD temperature and no qc for CTD {expocode} {station_cast}')
        filename = 'cruises_w_ctd_temp_no_qc.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type} and indiv type CTD\n")
            f.write(f"station cast {station_cast}\n")

    if has_ctd_temp_btl and not has_ctd_temp_w_ref_scale_btl:
        logging.info(
            f'CTD temperature with no ref scale for BTL {expocode} {station_cast}')
        filename = 'cruises_no_ctd_temp_w_ref_scale.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type} and indiv type BTL\n")
            f.write(f"station cast {station_cast}\n")

    if has_ctd_temp_ctd and not has_ctd_temp_w_ref_scale_ctd:
        logging.info(
            f'CTD temperature with no ref scale for CTD {expocode} {station_cast}')
        filename = 'cruises_no_ctd_temp_w_ref_scale.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode} for CTD\n")
            f.write(f"collection type {type} and indiv type CTD\n")
            f.write(f"station cast {station_cast}\n")

    # Skip any with expocode = None
    if expocode == 'None':
        logging.info(f'No expocode for {expocode} {station_cast}')
        filename = 'files_no_expocode.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast}\n")

    check_of_ctd_vars = {}
    check_of_ctd_vars['has_all_ctd_vars'] = False
    check_of_ctd_vars['collection_type'] = type
    check_of_ctd_vars['profile_checked'] = profile

    return check_of_ctd_vars


def check_ctd_vars_one_profile(profile, logging, logging_dir):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    profile_dict = profile['profile_dict']
    station_cast = profile['station_cast']
    type = profile_dict['type']

    expocode = profile_dict['meta']['expocode']
    bgc_meas = profile_dict['bgcMeas']

    logging.info('***********')
    logging.info(f"station cast {station_cast}")
    logging.info(f"collection type {type}")

    for obj in bgc_meas:
        if 'pres' in obj.keys():
            has_pres = True
        else:
            has_pres = False

    try:
        goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMappingBtl']
        goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMappingCtd']
    except:
        goship_argovis_name_mapping = profile_dict['goshipArgovisNameMapping']

    # Look for ctd_temperature
    try:
        has_ctd_temp_btl = 'temp_btl' in goship_argovis_name_mapping_btl.values()
        has_ctd_temp_ctd = 'temp_ctd' in goship_argovis_name_mapping_ctd.values()

    except:
        has_ctd_temp_btl = 'temp_btl' in goship_argovis_name_mapping.values()
        has_ctd_temp_ctd = 'temp_ctd' in goship_argovis_name_mapping.values()

    logging.info(f"has_ctd_temp_btl {has_ctd_temp_btl}")
    logging.info(f"has_ctd_temp_ctd {has_ctd_temp_ctd}")

    # Look for ctd_temperature qc
    try:
        has_ctd_temp_qc_btl = 'temp_btl_qc' in goship_argovis_name_mapping_btl.values()
        has_ctd_temp_qc_ctd = 'temp_ctd_qc' in goship_argovis_name_mapping_ctd.values()
    except:
        has_ctd_temp_qc_btl = 'temp_btl_qc' in goship_argovis_name_mapping.values()
        has_ctd_temp_qc_ctd = 'temp_ctd_qc' in goship_argovis_name_mapping.values()

    logging.info(f"has_ctd_temp_qc_btl {has_ctd_temp_qc_btl}")
    logging.info(f"has_ctd_temp_qc_ctd {has_ctd_temp_qc_ctd}")

    # Look at ctd_temperature ref scale
    argovis_ref_scale = profile_dict['argovisReferenceScale']

    has_ctd_temp_w_ref_scale_btl = 'temp_btl' in argovis_ref_scale.keys()
    has_ctd_temp_w_ref_scale_ctd = 'temp_ctd' in argovis_ref_scale.keys()

    logging.info(
        f"has_ctd_temp_w_ref_scale_btl {has_ctd_temp_w_ref_scale_btl}")
    logging.info(
        f"has_ctd_temp_w_ref_scale_ctd {has_ctd_temp_w_ref_scale_ctd}")

    # Summary of variables checked
    variables_check = {}
    variables_check['has_pres'] = has_pres
    variables_check['has_ctd_temp_btl'] = has_ctd_temp_btl
    variables_check['has_ctd_temp_ctd'] = has_ctd_temp_ctd
    variables_check['has_ctd_temp_qc_btl'] = has_ctd_temp_qc_btl
    variables_check['has_ctd_temp_qc_ctd'] = has_ctd_temp_qc_ctd
    variables_check['has_ctd_temp_w_ref_scale_btl'] = has_ctd_temp_w_ref_scale_btl
    variables_check['has_ctd_temp_w_ref_scale_ctd'] = has_ctd_temp_w_ref_scale_ctd

    # Summary of variables with CTD variables
    check_of_ctd_vars = {}
    check_of_ctd_vars['type'] = type
    check_of_ctd_vars['profile_checked'] = profile
    check_of_ctd_vars['has_all_ctd_vars'] = {}
    check_of_ctd_vars['has_ctd_vars_no_qc'] = {}

    # Test if have all CTD vars including temp qc and ref scale
    check_of_ctd_vars['has_all_ctd_vars']['btl'] = False
    check_of_ctd_vars['has_all_ctd_vars']['ctd'] = False

    # partial CTD vars are pres, ctd_temp, ref_scale but no qc
    check_of_ctd_vars['has_ctd_vars_no_qc']['btl'] = False
    check_of_ctd_vars['has_ctd_vars_no_qc']['ctd'] = False

    if has_pres and has_ctd_temp_btl and has_ctd_temp_qc_btl and has_ctd_temp_w_ref_scale_btl:
        logging.info(f"Found CTD variables for type BTL {station_cast}")
        check_of_ctd_vars['has_all_ctd_vars']['btl'] = True

    if has_pres and has_ctd_temp_ctd and has_ctd_temp_qc_ctd and has_ctd_temp_w_ref_scale_ctd:
        logging.info(f"Found CTD variables for type CTD {station_cast}")
        check_of_ctd_vars['has_all_ctd_vars']['ctd'] = True

    if has_pres and has_ctd_temp_btl and not has_ctd_temp_qc_btl and has_ctd_temp_w_ref_scale_btl:
        logging.info(f"Found CTD variables for type BTL {station_cast}")
        check_of_ctd_vars['has_ctd_vars_no_qc']['btl'] = True

    if has_pres and has_ctd_temp_ctd and not has_ctd_temp_qc_ctd and has_ctd_temp_w_ref_scale_ctd:
        logging.info(f"Found CTD variables for type CTD {station_cast}")
        check_of_ctd_vars['has_ctd_vars_no_qc']['ctd'] = True

    # Now find which variables that are missing and write to files
    check_missing_variables(
        profile, variables_check, logging, logging_dir)

    return check_of_ctd_vars


def check_of_ctd_variables(profiles, logging, logging_dir):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    all_profiles_checked_ctd_vars = []

    for profile in profiles:

        checked_ctd_vars_one_profile = check_ctd_vars_one_profile(
            profile, logging, logging_dir)

        all_profiles_checked_ctd_vars.append(checked_ctd_vars_one_profile)

    return all_profiles_checked_ctd_vars
