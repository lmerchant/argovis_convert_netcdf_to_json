
# Check if files have pressure and ctd_temperature to be a CTD file

import os


def look_for_temperature_unk(profile_dict, type, profile_number, logging, logging_dir):

    expocode = profile_dict['meta']['expocode']
    contained_type = profile_dict['contains']

    # Look at name mapping

    if contained_type == 'btl_ctd':
        goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMappingBtl']

        goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMappingCtd']

        station_cast_btl = profile_dict['stationCastBtl']
        station_cast_ctd = profile_dict['stationCastCtd']

    elif contained_type == 'btl':
        goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMapping']
        station_cast = profile_dict['stationCast']

    elif contained_type == 'ctd':
        goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMapping']
        station_cast = profile_dict['stationCast']

    # Look for ctd_temperature_unk

    has_ctd_temp_w_btl = False
    has_ctd_temp_w_ctd = False

    no_ctd_temp_w_type_btl = False
    no_ctd_temp_w_type_ctd = False

    if contained_type == 'btl_ctd':
        has_ctd_temp_w_btl = any(
            [True if val == 'ctd_temperature_unk_btl' else False for key, val in goship_argovis_name_mapping_btl.items()])

        if not has_ctd_temp_w_btl:
            no_ctd_temp_w_type_btl = 'True'

        has_ctd_temp_w_ctd = any(
            [True if val == 'ctd_temperature_unk_ctd' else False for key, val in goship_argovis_name_mapping_ctd.items()])

        if not has_ctd_temp_w_ctd:
            no_ctd_temp_w_type_ctd = 'True'

        has_ctd_temp = any(
            [has_ctd_temp_w_btl, has_ctd_temp_w_ctd])

        no_ctd_temp_w_type = f"btl: {no_ctd_temp_w_type_btl} ctd:  {no_ctd_temp_w_type_ctd}"

    elif contained_type == 'btl':
        has_ctd_temp = any(
            [True if val == 'ctd_temperature_unk_btl' else False for key, val in goship_argovis_name_mapping_btl.items()])

        no_ctd_temp_w_type = f"btl: {has_ctd_temp}"

    elif contained_type == 'ctd':
        has_ctd_temp = any(
            [True if val == 'ctd_temperature_unk_ctd' else False for key, val in goship_argovis_name_mapping_ctd.items()])

        no_ctd_temp_w_type = f"ctd: {has_ctd_temp}"

    logging.info('Has ctd_temperature_unk')
    filename = 'files_no_ctd_temp.txt'
    filepath = os.path.join(logging_dir, filename)

    with open(filepath, 'a') as f:
        f.write(f"Has ctd_temperature_unk\n")

    # f.write('-----------\n')
    # f.write(f"expocode {expocode}\n")
    # f.write(f"collection type {type}\n")
    # f.write(f"in {no_ctd_temp_w_type}")
    # f.write(f"profile number {profile_number}\n")


def check_if_all_ctd_vars_per_profile(profile_dict, type, logging, logging_dir):

    # Really want to use the  (station #, cast #) tuple

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    has_pres = False
    has_ctd_temp_w_ref_scale = False
    has_ctd_temp_qc = False
    has_ctd_temp = False

    has_ctd_vars = False

    # Check for pres and temp

    # TODO
    # consolidate this into 2 checking functions for btl and ctd

    expocode = profile_dict['meta']['expocode']
    bgc_meas = profile_dict['bgcMeas']
    contained_type = profile_dict['contains']

    for obj in bgc_meas:
        has_pres = any(
            [True if key == 'pres' else False for key in obj.keys()])

        no_pres_type = f"{has_pres}"

    # Look at profile number
    if contained_type == 'btl_ctd':
        profile_number_btl = profile_dict['profileNumberBtl']
        profile_number_ctd = profile_dict['profileNumberCtd']

        station_cast_btl = profile_dict['stationCastBtl']
        station_cast_ctd = profile_dict['stationCastCtd']

        station_cast = f"btl: {station_cast_btl} and ctd: {station_cast_ctd}"

        # TODO
        # Assuming profile #'s equal here
        if profile_number_btl != profile_number_ctd:
            print('************')
            print('Check func check_if_all_ctd_vars_per_profile')
            print('Assume ctd and bot profile the same')
            print(f'bot # {station_cast_btl}')
            print(f'ctd # {station_cast_ctd}')

        profile_number = profile_number_btl

    elif contained_type == 'btl':
        profile_number = profile_dict['profileNumber']
        station_cast = profile_dict['stationCast']

    elif contained_type == 'ctd':
        profile_number = profile_dict['profileNumber']
        station_cast = profile_dict['stationCast']

    # Look at name mapping

    if contained_type == 'btl_ctd':
        goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMappingBtl']

        goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMappingCtd']

    elif contained_type == 'btl':
        goship_argovis_name_mapping_btl = profile_dict['goshipArgovisNameMapping']

    elif contained_type == 'ctd':
        goship_argovis_name_mapping_ctd = profile_dict['goshipArgovisNameMapping']

    # Look at ctd_temperature ref scale

    has_ctd_temp_w_ref_scale_btl = False
    has_ctd_temp_w_ref_scale_type_btl = False

    has_ctd_temp_w_ref_scale_ctd = False
    has_ctd_temp_w_ref_scale_type_ctd = False

    argovis_ref_scale = profile_dict['argovisReferenceScale']

    for key in argovis_ref_scale.keys():
        if key == 'temp_btl':
            has_ctd_temp_w_ref_scale_btl = True
            has_ctd_temp_w_ref_scale_type_btl = 'True'

        if key == 'temp_ctd':
            has_ctd_temp_w_ref_scale_ctd = True
            has_ctd_temp_w_ref_scale_type_ctd = 'True'

    if contained_type == 'btl_ctd':
        has_ctd_temp_w_ref_scale = any(
            [has_ctd_temp_w_ref_scale_btl, has_ctd_temp_w_ref_scale_ctd])

        has_ctd_temp_w_ref_scale_w_type = f"bot: {has_ctd_temp_w_ref_scale_type_btl} ctd: {has_ctd_temp_w_ref_scale_type_ctd}"

    elif contained_type == 'btl':
        has_ctd_temp_w_ref_scale = has_ctd_temp_w_ref_scale_btl

        has_ctd_temp_w_ref_scale_w_type = f"bot: {has_ctd_temp_w_ref_scale_type_btl}"

    elif contained_type == 'ctd':
        has_ctd_temp_w_ref_scale = has_ctd_temp_w_ref_scale_ctd

        has_ctd_temp_w_ref_scale_w_type = f"ctd: {has_ctd_temp_w_ref_scale_type_ctd}"

    # Look for ctd_temperature

    has_ctd_temp_w_btl = False
    has_ctd_temp_w_ctd = False

    no_ctd_temp_w_type_btl = False
    no_ctd_temp_w_type_ctd = False

    if contained_type == 'btl_ctd':
        has_ctd_temp_w_btl = any(
            [True if val == 'temp_btl' else False for key, val in goship_argovis_name_mapping_btl.items()])

        if not has_ctd_temp_w_btl:
            no_ctd_temp_w_type_btl = 'True'

        has_ctd_temp_w_ctd = any(
            [True if val == 'temp_ctd' else False for key, val in goship_argovis_name_mapping_ctd.items()])

        if not has_ctd_temp_w_ctd:
            no_ctd_temp_w_type_ctd = 'True'

        has_ctd_temp = any(
            [has_ctd_temp_w_btl, has_ctd_temp_w_ctd])

        no_ctd_temp_w_type = f"btl: {no_ctd_temp_w_type_btl} ctd:  {no_ctd_temp_w_type_ctd}"

    elif contained_type == 'btl':
        has_ctd_temp = any(
            [True if val == 'temp_btl' else False for key, val in goship_argovis_name_mapping_btl.items()])

        no_ctd_temp_w_type = f"btl: {has_ctd_temp}"

    elif contained_type == 'ctd':
        has_ctd_temp = any(
            [True if val == 'temp_ctd' else False for key, val in goship_argovis_name_mapping_ctd.items()])

        no_ctd_temp_w_type = f"ctd: {has_ctd_temp}"

    # Look for ctd_temperature qc

    has_ctd_temp_qc_btl = False
    has_ctd_temp_qc_ctd = False
    has_ctd_temp_qc = False

    no_ctd_temp_qc_w_type_btl = False
    no_ctd_temp_qc_w_type_ctd = False
    no_ctd_temp_qc = False

    if contained_type == 'btl_ctd':

        has_ctd_temp_qc_btl = any(
            [True if val == 'temp_qc' else False for key, val in goship_argovis_name_mapping_btl.items()])

        if not has_ctd_temp_qc_btl:
            no_ctd_temp_qc_w_type_btl = 'True'

        has_ctd_temp_qc = any(
            [True if val == 'temp_qc' else False for key, val in goship_argovis_name_mapping_ctd.items()])

        if not has_ctd_temp_qc_ctd:
            no_ctd_temp_qc_w_type_ctd = 'True'

        no_ctd_temp_qc = f"btl: {no_ctd_temp_qc_w_type_btl} ctd:  {no_ctd_temp_qc_w_type_ctd}"

    elif contained_type == 'btl':

        has_ctd_temp_qc = any(
            [True if val == 'temp_qc' else False for key, val in goship_argovis_name_mapping_btl.items()])

        if not has_ctd_temp_qc:
            no_ctd_temp_qc = f"btl: 'True'"

    elif contained_type == 'ctd':

        has_ctd_temp_qc = any(
            [True if val == 'temp_qc' else False for key, val in goship_argovis_name_mapping_ctd.items()])

        if not has_ctd_temp_qc:
            no_ctd_temp_qc = f"ctd: 'True'"

     # Test if have required CTD vars

    if has_pres and has_ctd_temp and has_ctd_temp_qc:
        has_ctd_vars = True
        return has_ctd_vars, profile_number

    # Logging

    print('**********************')
    print('No CTD variables found')
    print(f"expocode {expocode}")
    print('**********************')

    if not has_pres:
        has_ctd_vars = False

        logging.info('No pressure')
        logging.info(f"station cast {station_cast}")
        filename = 'files_no_pressure.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"type {no_pres_type}\n")
            f.write(f"station cast {station_cast}\n")

    if not has_ctd_temp:
        has_ctd_vars = False

        logging.info('No ctd temperature')
        logging.info(f"station cast {station_cast}")
        logging.info(f"collection type {type}")
        logging.info(f"in {no_ctd_temp_w_type}")
        filename = 'files_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"in {no_ctd_temp_w_type}\n")
            f.write(f"station cast {station_cast}\n")

    if has_ctd_temp and not has_ctd_temp_qc:
        has_ctd_vars = False

        logging.info('CTD temperature and no qc')
        logging.info(f"station cast {station_cast}")
        logging.info(f"collection type {type}")
        logging.info(f"in {no_ctd_temp_qc}")
        filename = 'files_w_ctd_temp_no_qc.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"in {no_ctd_temp_qc}\n")
            f.write(f"station cast {station_cast}\n")

    if not has_pres and not has_ctd_temp:
        has_ctd_vars = False

        logging.info('missing pres and ctd temperature')
        logging.info(f"station cast {station_cast}")
        logging.info(f"collection type {type}")
        filename = 'files_no_core_ctd_vars.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"type {type}\n")
            f.write(f"station cast {station_cast}\n")

    if has_ctd_temp and not has_ctd_temp_w_ref_scale:
        has_ctd_vars = False

        logging.info('CTD temperature with no ref scale')
        logging.info(f"station cast {station_cast}")
        logging.info(f"collection type {type}")
        logging.info(f"in {has_ctd_temp_w_ref_scale_w_type}")
        filename = 'files_no_ctd_temp_w_ref_scale.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {type}\n")
            f.write(f"in {has_ctd_temp_w_ref_scale_w_type}\n")
            f.write(f"station cast {station_cast}\n")

    # Skip any with expocode = None
    if expocode == 'None':
        has_ctd_vars = False
        logging.info('No expocode')
        filename = 'files_no_expocode.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"type {type}\n")
            f.write(f"station cast {station_cast}\n")

    if not has_ctd_temp:

        # Look for ctd_temperature_unk
        look_for_temperature_unk(
            profile_dict, type, profile_number, logging, logging_dir)

    return has_ctd_vars, profile_number


def check_if_all_ctd_vars(profile_dicts, station_cast_profile, logging, logging_dir, type):

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    has_ctd_vars_all_profiles = []

    # Incorporate station_cast_profile

    for profile_dict in profile_dicts:

        has_ctd_vars, profile_number = check_if_all_ctd_vars_per_profile(
            profile_dict, type, logging, logging_dir)

        has_ctd_vars_one_profile = {}

        has_ctd_vars_one_profile['profile_number'] = profile_number

        has_ctd_vars_one_profile['has_ctd_vars'] = has_ctd_vars

        has_ctd_vars_all_profiles.append(has_ctd_vars_one_profile)

    return has_ctd_vars_all_profiles
