# Save output

import os
import json
import numpy as np
from pathlib import Path
import logging

from global_vars import GlobalVars

from variable_mapping.meta_param_mapping import get_program_argovis_mapping
from check_and_save.save_as_zip import save_as_zip_data_type_profiles


def convert(o):

    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)

    if isinstance(o, np.int64):
        return int(o)


def get_unique_cchdo_units(data_type_profiles):

    # Keep my names and map to latest Argovis names
    # need this since already renamed individual files

    # cchdo names are the keys and argovis names are the value
    key_mapping = get_program_argovis_mapping()

    cchdo_units_key = 'cchdoUnits'
    renamed_cchdo_units_key = key_mapping[cchdo_units_key]

    cchdo_units_key_btl = 'cchdoUnitsBtl'
    renamed_cchdo_units_key_btl = key_mapping[cchdo_units_key_btl]

    cchdo_units_key_ctd = 'cchdoUnitsCtd'
    renamed_cchdo_units_key_ctd = key_mapping[cchdo_units_key_ctd]

    all_cchdo_units_mapping = {}

    for profile in data_type_profiles:

        # TODO
        # Did I keep or use 'data_type' as a key for profile
        #  when processing things?
        # Did I set data_type to 'btl_ctd'?
        #data_type = profile['data_type']
        profile_dict = profile['profile_dict']
        data_type = profile_dict['data_type']

        if data_type == 'btl' or data_type == 'ctd':
            cchdo_units_mapping = profile_dict[renamed_cchdo_units_key]

        if data_type == 'btl_ctd':

            try:
                cchdo_units_btl = profile_dict[renamed_cchdo_units_key_btl]
                cchdo_units_ctd = profile_dict[renamed_cchdo_units_key_ctd]
                cchdo_units_mapping = {**cchdo_units_btl, **cchdo_units_ctd}
            except:
                cchdo_units_mapping = profile_dict[renamed_cchdo_units_key]

        # TODO
        # overwrite key if it already exists
        # each variable has same units. Is this true
        # if it comes from either btl or ctd?
        # If same key but different units, add suffix to key
        # of profile_dict['meta']['expocode']
        expocode = profile_dict['meta']['expocode']
        for key, val in cchdo_units_mapping.items():
            if key in all_cchdo_units_mapping and val != all_cchdo_units_mapping[key]:
                new_key = f"{key}_{expocode}"
                all_cchdo_units_mapping[new_key] = val
            else:
                all_cchdo_units_mapping[key] = val

        return all_cchdo_units_mapping


def write_all_cchdo_units(all_cchdo_units_mapping):

    filename = 'found_cchdo_units.txt'
    filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)

    with open(filepath, 'a') as f:
        json.dump(all_cchdo_units_mapping, f, indent=4,
                  sort_keys=True, default=convert)


def write_profile_cchdo_units_one_profile(data_type, profile):

    profile_dict = profile['profile_dict']

    filename = 'found_cchdo_units.txt'
    filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)

    # Write one profile cchdo units to
    # keep a record of what units need to be converted

    # cchdo names are the keys and argovis names are the value
    key_mapping = get_program_argovis_mapping()

    cchdo_units_key = 'cchdoUnits'
    renamed_cchdo_units_key = key_mapping[cchdo_units_key]

    cchdo_units_key_btl = 'cchdoUnitsBtl'
    renamed_cchdo_units_key_btl = key_mapping[cchdo_units_key_btl]

    cchdo_units_key_ctd = 'cchdoUnitsCtd'
    renamed_cchdo_units_key_ctd = key_mapping[cchdo_units_key_ctd]

    if data_type == 'btl':
        cchdo_units_mapping = profile_dict[renamed_cchdo_units_key]

    if data_type == 'ctd':
        cchdo_units_mapping = profile_dict[renamed_cchdo_units_key]

    if data_type == 'btl_ctd':
        try:
            cchdo_units_btl = profile_dict[renamed_cchdo_units_key_btl]
            cchdo_units_ctd = profile_dict[renamed_cchdo_units_key_ctd]
            cchdo_units_mapping = {**cchdo_units_btl, **cchdo_units_ctd}
        except:
            cchdo_units_mapping = profile_dict[renamed_cchdo_units_key]

    with open(filepath, 'a') as f:
        json.dump(cchdo_units_mapping, f, indent=4,
                  sort_keys=True, default=convert)


def write_profile_cchdo_units(checked_profiles_info):

    # TODO
    # Keep my names and map to latest Argovis names
    # need this since already renamed individual files

    # cchdo names are the keys and argovis names are the value
    key_mapping = get_program_argovis_mapping()

    cchdo_units_key = 'cchdoUnits'
    renamed_cchdo_units_key = key_mapping[cchdo_units_key]

    cchdo_units_key_btl = 'cchdoUnitsBtl'
    renamed_cchdo_units_key_btl = key_mapping[cchdo_units_key_btl]

    cchdo_units_key_ctd = 'cchdoUnitsCtd'
    renamed_cchdo_units_key_ctd = key_mapping[cchdo_units_key_ctd]

    # Write one profile cchdo units to
    # keep a record of what units need to be converted

    try:
        profile_dict = checked_profiles_info[0]['profile_checked']['profile_dict']

        data_type = profile_dict['data_type']

        filename = 'found_cchdo_units.txt'
        filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)

        if data_type == 'btl':
            cchdo_units_profile = profile_dict[renamed_cchdo_units_key]

        if data_type == 'ctd':
            cchdo_units_profile = profile_dict[renamed_cchdo_units_key]

        if data_type == 'btl_ctd':
            try:
                cchdo_units_btl = profile_dict[renamed_cchdo_units_key_btl]
                cchdo_units_ctd = profile_dict[renamed_cchdo_units_key_ctd]
                cchdo_units_profile = {**cchdo_units_btl, **cchdo_units_ctd}
            except:
                cchdo_units_profile = profile_dict[renamed_cchdo_units_key]

        with open(filepath, 'a') as f:
            json.dump(cchdo_units_profile, f, indent=4,
                      sort_keys=True, default=convert)

    except KeyError:
        # Skip writing file
        pass


def prepare_profile_json(profile_dict):

    # TODO
    # If want to remove cchdoNames list, do it here
    # profile_dict.pop('cchdoNames', None)

    # Remove station cast var used to group data
    #profile_dict.pop('stationCast', None)

    profile_dict.pop('station_cast', None)

    profile_dict.pop('data_type', None)

    # Remove station_cast var used to group data
    # profile_dict['meta'].pop('station_cast', None)

    # TODO
    # already did this?

    # Remove  time from meta since it was just used to create date variable
    #profile_dict['meta'].pop('time', None)

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

    id = data_dict['_id']

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


def save_included_excluded_cchdo_vars(included, excluded):
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


def save_data_type_profiles(data_type_obj_profiles):

    logging.info('Saving files single type')

    data_type = data_type_obj_profiles['data_type']

    data_type_profiles = data_type_obj_profiles['data_type_profiles_list']

    # Look at one profile to get the cchdo units
    # to see what kinds there are
    all_cchdo_units_mapping = get_unique_cchdo_units(data_type_profiles)

    write_all_cchdo_units(all_cchdo_units_mapping)

    save_as_zip_data_type_profiles(data_type_profiles)


def save_data_type_profiles_per_type(single_data_type_cruises):

    # TODO test this part with expocode of one data type
    for cruise_obj in single_data_type_cruises:

        all_data_types_profile_objs = cruise_obj['all_data_types_profile_objs']

        for data_type_obj_profiles in all_data_types_profile_objs:
            # Remove any measurements with temp = NaN when saving
            save_data_type_profiles(data_type_obj_profiles)


def save_data_type_profiles_combined(combined_obj_profiles):

    logging.info('Saving files combined type')

    # don't know data type of profile, so this logic doesn't work
    # of just picking first profile.

    # Loop through all profiles, get all units and get unique
    all_cchdo_units_mapping = get_unique_cchdo_units(combined_obj_profiles)

    write_all_cchdo_units(all_cchdo_units_mapping)

    save_as_zip_data_type_profiles(combined_obj_profiles)
