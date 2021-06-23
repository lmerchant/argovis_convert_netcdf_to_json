
# Check if files have pressure and ctd_temperature to be a
# proper file for ArgoVis

import os


def check_if_all_ctd_vars_per_profile(profile, type, logging, logging_dir):

    profile_dict = profile['profile_dict']
    station_cast = profile['station_cast']

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    has_pres = False
    has_ctd_temp_w_ref_scale = False
    has_ctd_temp_qc = False
    has_ctd_temp = False

    expocode = profile_dict['meta']['expocode']
    bgc_meas = profile_dict['bgcMeas']

    for obj in bgc_meas:
        has_pres = any(
            [True if key == 'pres' else False for key in obj.keys()])

        no_pres_type = f"{has_pres}"

    # Look at profile number
    if type == 'btl_ctd':
        station_cast = profile_dict['stationCast']
        station_cast_str = f"btl & ctd {station_cast}"

    elif type == 'btl':
        station_cast = profile_dict['stationCast']
        station_cast_str = f"btl: {station_cast}"

    elif type == 'ctd':
        station_cast = profile_dict['stationCast']
        station_cast_str = f"ctd: {station_cast}"

    # Look at name mapping
    if type == 'btl_ctd':
        goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMappingBtl']

        goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMappingCtd']

    elif type == 'btl':
        goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMapping']

    elif type == 'ctd':
        goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMapping']

    # Look at ctd_temperature ref scale
    has_ctd_temp_w_ref_scale_btl = False
    has_ctd_temp_w_ref_scale_ctd = False

    argovis_ref_scale = profile_dict['argovisReferenceScale']

    for key in argovis_ref_scale.keys():
        if key == 'temp_btl':
            has_ctd_temp_w_ref_scale_btl = True

        if key == 'temp_ctd':
            has_ctd_temp_w_ref_scale_ctd = True

    if type == 'btl_ctd':
        has_ctd_temp_w_ref_scale = any(
            [has_ctd_temp_w_ref_scale_btl, has_ctd_temp_w_ref_scale_ctd])

    elif type == 'btl':
        has_ctd_temp_w_ref_scale = has_ctd_temp_w_ref_scale_btl

    elif type == 'ctd':
        has_ctd_temp_w_ref_scale = has_ctd_temp_w_ref_scale_ctd

    # Look for ctd_temperature
    has_ctd_temp_btl = False
    has_ctd_temp_ctd = False
    has_ctd_temp = False

    if type == 'btl_ctd':
        has_ctd_temp_btl = any(
            [True if val == 'temp_btl' else False for key, val in goship_argovis_name_mapping_btl.items()])

        has_ctd_temp_ctd = any(
            [True if val == 'temp_ctd' else False for key, val in goship_argovis_name_mapping_ctd.items()])

        has_ctd_temp = any(
            [has_ctd_temp_btl, has_ctd_temp_ctd])

    elif type == 'btl':
        has_ctd_temp = any(
            [True if val == 'temp_btl' else False for key, val in goship_argovis_name_mapping_btl.items()])

    elif type == 'ctd':
        has_ctd_temp = any(
            [True if val == 'temp_ctd' else False for key, val in goship_argovis_name_mapping_ctd.items()])

    # Look for ctd_temperature qc
    has_ctd_temp_qc_btl = False
    has_ctd_temp_qc_ctd = False
    has_ctd_temp_qc = False

    if type == 'btl_ctd':

        has_ctd_temp_qc_btl = any(
            [True if val == 'temp_btl_qc' else False for key, val in goship_argovis_name_mapping_btl.items()])

        has_ctd_temp_qc_ctd = any(
            [True if val == 'temp_ctd_qc' else False for key, val in goship_argovis_name_mapping_ctd.items()])

        has_ctd_temp_qc = any([has_ctd_temp_qc_btl, has_ctd_temp_qc_ctd])

    elif type == 'btl':

        has_ctd_temp_qc = any(
            [True if val == 'temp_btl_qc' else False for key, val in goship_argovis_name_mapping_btl.items()])

    elif type == 'ctd':

        has_ctd_temp_qc = any(
            [True if val == 'temp_ctd_qc' else False for key, val in goship_argovis_name_mapping_ctd.items()])

     # Test if have required CTD vars
    if has_pres and has_ctd_temp and has_ctd_temp_qc and has_ctd_temp_w_ref_scale:
        has_ctd_vars_output = {}
        has_ctd_vars_output['has_ctd_vars'] = True
        has_ctd_vars_output['profile_checked'] = profile

        logging.info(f"Found CTD variables for {station_cast}")

        return has_ctd_vars_output

    #logging.info(f'No CTD variables found for {station_cast}')

    if not has_pres:
        logging.info(f'No pressure for {station_cast_str}')
        filename = 'files_no_pressure.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"type {no_pres_type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if not has_ctd_temp:
        logging.info(f'No ctd temperature for {station_cast_str}')
        filename = 'files_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if has_ctd_temp and not has_ctd_temp_qc:
        logging.info(f'CTD temperature and no qc for {station_cast_str}')
        filename = 'files_w_ctd_temp_no_qc.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if not has_pres and not has_ctd_temp:
        logging.info(
            f'Missing pres and ctd temperature for {station_cast_str}')
        filename = 'files_no_core_ctd_vars.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

    if has_ctd_temp and not has_ctd_temp_w_ref_scale:
        logging.info(
            f'CTD temperature with no ref scale for {station_cast_str}')
        filename = 'files_no_ctd_temp_w_ref_scale.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"station cast {station_cast_str}\n")

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

    if not has_ctd_temp:
        # Look for ctd_temperature_unk
        # Stands for ctd_temperature with and unknown reference scale
        logging.info(f'Has ctd_temperature_unk for {station_cast_str}')
        filename = 'files_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(f"Has ctd_temperature_unk\n")

    has_ctd_vars_output = {}
    has_ctd_vars_output['has_ctd_vars'] = False
    has_ctd_vars_output['profile_checked'] = profile

    return has_ctd_vars_output


def check_if_all_ctd_vars(profiles, logging, logging_dir):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    has_ctd_vars_all_profiles = []

    for profile in profiles:
        profile_dict = profile['profile_dict']

        # profile_dict may be empty for a combined profile
        try:
            type = profile_dict['type']
        except:
            continue

        has_ctd_vars_one_profile = check_if_all_ctd_vars_per_profile(
            profile, type, logging, logging_dir)

        has_ctd_vars_all_profiles.append(has_ctd_vars_one_profile)

    return has_ctd_vars_all_profiles
