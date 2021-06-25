
# Check if files have pressure and ctd_temperature to be a
# proper file for ArgoVis

import os


def check_if_all_ctd_vars_per_profile(profile, logging, logging_dir):

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

    has_pres = False

    for obj in bgc_meas:
        has_pres = any(
            [True if key == 'pres' else False for key in obj.keys()])

        no_pres_type = f"{has_pres}"

    if type == 'btl_ctd':
        station_cast = profile_dict['stationCast']
        station_cast_str = f"btl & ctd {station_cast}"

    elif type == 'btl':
        station_cast = profile_dict['stationCast']
        station_cast_str = f"btl: {station_cast}"

    elif type == 'ctd':
        station_cast = profile_dict['stationCast']
        station_cast_str = f"ctd: {station_cast}"

    # See if have separated name mapping into btl and ctd
    # or if using common name

    try:
        goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMappingBtl']
        goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMappingCtd']
    except:
        goship_argovis_name_mapping = profile_dict['goshipArgovisNameMapping']

    # Look at ctd_temperature ref scale
    argovis_ref_scale = profile_dict['argovisReferenceScale']

    has_ctd_temp_w_ref_scale_btl = 'temp_btl' in argovis_ref_scale.keys()
    has_ctd_temp_w_ref_scale_ctd = 'temp_ctd' in argovis_ref_scale.keys()

    logging.info(
        f"has_ctd_temp_w_ref_scale_btl {has_ctd_temp_w_ref_scale_btl}")
    logging.info(
        f"has_ctd_temp_w_ref_scale_ctd {has_ctd_temp_w_ref_scale_ctd}")

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

    # Test if have required CTD vars
    if has_pres and has_ctd_temp_btl and has_ctd_temp_qc_btl and has_ctd_temp_w_ref_scale_btl:
        has_ctd_vars_output = {}
        has_ctd_vars_output['has_ctd_vars'] = True
        has_ctd_vars_output['profile_checked'] = profile
        logging.info(f"Found CTD variables for type BTL {station_cast}")
        return has_ctd_vars_output

    if has_pres and has_ctd_temp_ctd and has_ctd_temp_qc_ctd and has_ctd_temp_w_ref_scale_ctd:
        has_ctd_vars_output = {}
        has_ctd_vars_output['has_ctd_vars'] = True
        has_ctd_vars_output['profile_checked'] = profile
        logging.info(f"Found CTD variables for type CTD {station_cast}")

        return has_ctd_vars_output

    # Now find which variables are missing

    if not has_pres:
        logging.info(f'No pressure for {station_cast_str}')
        filename = 'cruises_no_pressure.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"type {no_pres_type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if not has_ctd_temp_w_ref_scale_btl and not has_ctd_temp_w_ref_scale_ctd:
        logging.info(f'No ctd temperature w ref scale for {station_cast_str}')
        filename = 'cruises_no_ctd_temp_ref_scale.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if not has_ctd_temp_btl and not has_ctd_temp_ctd:
        logging.info(f'No ctd temperature for BTL or CTD {station_cast_str}')
        filename = 'cruises_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if has_ctd_temp_btl and not has_ctd_temp_qc_btl:
        logging.info(
            f'Has CTD temperature and no qc for BTL {station_cast_str}')
        filename = 'cruises_w_ctd_temp_no_qc.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode} for BTL\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if has_ctd_temp_ctd and not has_ctd_temp_qc_ctd:
        logging.info(
            f'Has CTD temperature and no qc for CTD {station_cast_str}')
        filename = 'cruises_w_ctd_temp_no_qc.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode} for CTD\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if not has_pres and not has_ctd_temp_btl and not has_ctd_temp_ctd:
        logging.info(
            f'Missing pres and ctd temperature for {station_cast_str}')
        filename = 'cruises_no_core_ctd_vars.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if has_ctd_temp_btl and not has_ctd_temp_w_ref_scale_btl:
        logging.info(
            f'CTD temperature with no ref scale for BTL {station_cast_str}')
        filename = 'cruises_no_ctd_temp_w_ref_scale.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode} for BTL\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if has_ctd_temp_ctd and not has_ctd_temp_w_ref_scale_ctd:
        logging.info(
            f'CTD temperature with no ref scale for CTD {station_cast_str}')
        filename = 'cruises_no_ctd_temp_w_ref_scale.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode} for CTD\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    try:
        has_ctd_temp_unk_btl = 'ctd_temperature_unk' in goship_argovis_name_mapping_btl.keys()
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk' in goship_argovis_name_mapping_ctd.keys()

    except:
        has_ctd_temp_unk_btl = 'ctd_temperature_unk' in goship_argovis_name_mapping.keys()
        has_ctd_temp_unk_ctd = 'ctd_temperature_unk' in goship_argovis_name_mapping.keys()

    if not has_ctd_temp_btl and has_ctd_temp_unk_btl:
        # Look for ctd_temperature_unk
        # Stands for ctd_temperature with and unknown reference scale
        logging.info(f'Has ctd_temperature_unk for BTL {station_cast_str}')
        filename = 'cruises_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(f"Has ctd_temperature_unk for BTL\n")

    if not has_ctd_temp_ctd and has_ctd_temp_unk_ctd:
        # Look for ctd_temperature_unk
        # Stands for ctd_temperature with and unknown reference scale
        logging.info(f'Has ctd_temperature_unk for CTD {station_cast_str}')
        filename = 'cruises_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(f"Has ctd_temperature_unk for CTD\n")

    # Skip any with expocode = None
    if expocode == 'None':
        logging.info(f'No expocode for {station_cast_str}')
        filename = 'files_no_expocode.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    has_ctd_vars_output = {}
    has_ctd_vars_output['has_ctd_vars'] = False
    has_ctd_vars_output['profile_checked'] = profile

    return has_ctd_vars_output


def check_if_all_ctd_vars(profiles, logging, logging_dir):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    has_ctd_vars_all_profiles = []

    for profile in profiles:

        has_ctd_vars_one_profile = check_if_all_ctd_vars_per_profile(
            profile, logging, logging_dir)

        has_ctd_vars_all_profiles.append(has_ctd_vars_one_profile)

    return has_ctd_vars_all_profiles
