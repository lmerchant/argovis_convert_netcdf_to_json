# Save output

import os
import json
import numpy as np
from pathlib import Path
import logging

from global_vars import GlobalVars
from check_and_save.check_of_ctd_vars import check_of_ctd_vars
from create_profiles.filter_measurements import filter_measurements
from check_and_save.save_as_zip import save_as_zip


def convert(o):

    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)

    if isinstance(o, np.int64):
        return int(o)


def write_profile_goship_units(checked_ctd_variables):

    # Write one profile goship units to
    # keep a record of what units need to be converted

    try:
        profile_dict = checked_ctd_variables[0]['profile_checked']['profile_dict']

        data_type = profile_dict['data_type']

        filename = 'found_goship_units.txt'
        filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)

        if data_type == 'btl':
            goship_units_profile = profile_dict['goshipUnits']

        if data_type == 'ctd':
            goship_units_profile = profile_dict['goshipUnits']

        if data_type == 'btl_ctd':
            try:
                goship_units_btl = profile_dict['goshipUnitsBtl']
                goship_units_ctd = profile_dict['goshipUnitsCtd']
                goship_units_profile = {**goship_units_btl, **goship_units_ctd}
            except:
                goship_units_profile = profile_dict['goshipUnits']

        with open(filepath, 'a') as f:
            json.dump(goship_units_profile, f, indent=4,
                      sort_keys=True, default=convert)

    except KeyError:
        # Skip writing file
        pass


def prepare_profile_json(profile_dict):

    # TODO
    # If want to remove goshipNames list, do it here
    # profile_dict.pop('goshipNames', None)

    # Remove station cast var used to group data
    profile_dict.pop('stationCast', None)
    profile_dict.pop('station_cast', None)

    profile_dict.pop('data_type', None)

    # Remove station_cast var used to group data
    # profile_dict['meta'].pop('station_cast', None)

    # Remove  time from meta since it was just used to create date variable
    profile_dict['meta'].pop('time', None)

    # Pop off meta key and use as start of data_dict
    meta_dict = profile_dict.pop('meta', None)

    # Now combine with left over profile_dict
    data_dict = {**meta_dict, **profile_dict}

    return data_dict


def write_profile_json(profile_dict):

    data_dict = prepare_profile_json(profile_dict)

    # TODO
    # ask
    # probably use cruise expocode instead of that in file

    id = data_dict['id']

    # TODO
    # When create file id, ask if use cruise expocode instead
    filename = f"{id}.json"

    expocode = data_dict['expocode']

    if '/' in filename:
        filename = filename.replace('/', '_')

    if '/' in expocode:
        folder = expocode.replace('/', '_')
    else:
        folder = expocode

    path = os.path.join(GlobalVars.JSON_DIR, folder)

    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    file = os.path.join(GlobalVars.JSON_DIR, folder, filename)

    # TESTING
    # TODO Remove formatting when final

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable

    # Sort keys or not?
    with open(file, 'w') as f:
        json.dump(data_dict, f, indent=4,
                  sort_keys=False, default=convert)


def save_one_btl_ctd_profile(ctd_var_check):

    has_all_ctd_vars = ctd_var_check['has_all_ctd_vars']

    profile = ctd_var_check['profile_checked']

    profile_dict = profile['profile_dict']

    if has_all_ctd_vars['btl'] or has_all_ctd_vars['ctd']:
        write_profile_json(profile_dict)


def save_all_btl_ctd_profiles(checked_ctd_variables):

    for checked_vars in checked_ctd_variables:
        save_one_btl_ctd_profile(checked_vars)


def save_included_excluded_goship_vars(included, excluded):
    """
        Save included vars
    """

    included_vars = [elem[0] for elem in included]
    unique_included_vars = list(set(included_vars))

    for var in unique_included_vars:

        included_str = [f"{elem[1]} {elem[2]}"
                        for elem in included if elem[0] == var]

        filename = f"{var}_included.txt"
        filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, filename)
        file = Path(filepath)
        file.touch(exist_ok=True)
        with file.open("a") as f:
            for id in included_str:
                f.write(f"{id}\n")

    """
        Save excluded vars
    """

    excluded_vars = [
        elem[0] for elem in excluded]
    unique_excluded_vars = list(set(excluded_vars))

    for var in unique_excluded_vars:

        excluded_str = [f"{elem[1]} {elem[2]}"
                        for elem in excluded if elem[0] == var]

        filename = f"{var}_excluded.txt"
        filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, filename)
        file = Path(filepath)
        file.touch(exist_ok=True)
        with file.open("a") as f:
            for id in excluded_str:
                f.write(f"{id}\n")


def save_profile_one_type(checked_vars):

    has_all_ctd_vars = checked_vars['has_all_ctd_vars']
    data_type = checked_vars['data_type']

    profile = checked_vars['profile_checked']
    profile_dict = profile['profile_dict']
    expocode = profile_dict['meta']['expocode']

    # add_vars_to_included_excluded_collections(
    #     profile_dict, cruises_collections)

    if has_all_ctd_vars[data_type]:
        write_profile_json(profile_dict)


def save_all_profiles_one_type(checked_ctd_variables):

    for checked_vars in checked_ctd_variables:
        save_profile_one_type(checked_vars)


def check_and_save_per_type(file_obj_profile):

    logging.info("inside check_and_save_per_type")

    # Now check if profiles have CTD vars and should be saved
    # And filter btl and ctd measurements separately

    data_type = file_obj_profile['data_type']
    file_profiles = file_obj_profile['profiles']

    # filter measurements for core using hierarchy
    file_profiles = filter_measurements(file_profiles, data_type)

    checked_ctd_variables, ctd_vars_flag = check_of_ctd_vars(file_profiles)

    if ctd_vars_flag:
        logging.info('----------------------')
        logging.info('Saving files')
        logging.info('----------------------')

        write_profile_goship_units(
            checked_ctd_variables)

        save_as_zip(checked_ctd_variables)

        # save_all_profiles_one_type(checked_ctd_variables)

    else:
        logging.info("*** Cruise not converted ***")

        profile = file_profiles[0]
        cruise_expocode = profile['profile_dict']['meta']['expocode']

        filename = 'cruises_not_converted.txt'
        filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)
        with open(filepath, 'a') as f:
            f.write(f"{cruise_expocode}\n")


def check_and_save_combined(profiles_btl_ctd):

    logging.info("inside check_and_save_combined")

    # Now check if profiles have CTD vars and should be saved
    # And filter btl and ctd measurements separately

    checked_ctd_variables, ctd_vars_flag = check_of_ctd_vars(
        profiles_btl_ctd)

    if ctd_vars_flag:
        logging.info('----------------------')
        logging.info('Saving files')
        logging.info('----------------------')
        write_profile_goship_units(checked_ctd_variables)

        save_as_zip(checked_ctd_variables)

        # save_all_btl_ctd_profiles(checked_ctd_variables)

    else:
        logging.info("*** Cruise not converted ***")

        profile = profiles_btl_ctd[0]
        cruise_expocode = profile['profile_dict']['meta']['expocode']

        filename = 'cruises_not_converted.txt'
        filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)
        with open(filepath, 'a') as f:
            f.write(f"{cruise_expocode}\n")
