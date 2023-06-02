# Save output

import os
import json
import numpy as np
from pathlib import Path
import logging

from global_vars import GlobalVars

from variable_naming.meta_param_mapping import get_program_argovis_source_info_mapping
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
    key_mapping = get_program_argovis_source_info_mapping()

    cchdo_units_key = "cchdo_units"
    renamed_cchdo_units_key = key_mapping[cchdo_units_key]

    all_cchdo_units_mapping = {}

    for profile in data_type_profiles:
        profile_dict = profile["profile_dict"]
        data_type = profile_dict["data_type"]

        if data_type == "btl" or data_type == "ctd":
            source_info = profile_dict["meta"]["source"][0]

            cchdo_units_mapping = source_info[renamed_cchdo_units_key]

        # TODO
        # overwrite key if it already exists
        # each variable has same units. Is this true
        # if it comes from either btl or ctd?
        # If same key but different units, add suffix to key
        # of profile_dict['meta']['expocode']
        expocode = profile_dict["meta"]["expocode"]
        for key, val in cchdo_units_mapping.items():
            if key in all_cchdo_units_mapping and val != all_cchdo_units_mapping[key]:
                new_key = f"{key}_{expocode}"
                all_cchdo_units_mapping[new_key] = val
            else:
                all_cchdo_units_mapping[key] = val

        return all_cchdo_units_mapping


def write_all_cchdo_units(all_cchdo_units_mapping):
    filename = "found_cchdo_units.txt"
    filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)

    with open(filepath, "a") as f:
        json.dump(all_cchdo_units_mapping, f, indent=4, sort_keys=True, default=convert)


def write_profile_cchdo_units_one_profile(data_type, profile):
    profile_dict = profile["profile_dict"]

    filename = "found_cchdo_units.txt"
    filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)

    # Write one profile cchdo units to
    # keep a record of what units need to be converted

    # cchdo names are the keys and argovis names are the value
    key_mapping = get_program_argovis_source_info_mapping()

    cchdo_units_key = "cchdo_units"
    renamed_cchdo_units_key = key_mapping[cchdo_units_key]

    cchdo_units_key_btl = "cchdo_units_btl"
    renamed_cchdo_units_key_btl = key_mapping[cchdo_units_key_btl]

    cchdo_units_key_ctd = "cchdo_units_ctd"
    renamed_cchdo_units_key_ctd = key_mapping[cchdo_units_key_ctd]

    if data_type == "btl":
        cchdo_units_mapping = profile_dict[renamed_cchdo_units_key]

    if data_type == "ctd":
        cchdo_units_mapping = profile_dict[renamed_cchdo_units_key]

    with open(filepath, "a") as f:
        json.dump(cchdo_units_mapping, f, indent=4, sort_keys=True, default=convert)


def write_profile_cchdo_units(checked_profiles_info):
    # TODO
    # Keep my names and map to latest Argovis names
    # need this since already renamed individual files

    # cchdo names are the keys and argovis names are the value
    key_mapping = get_program_argovis_source_info_mapping()

    cchdo_units_key = "cchdo_units"
    renamed_cchdo_units_key = key_mapping[cchdo_units_key]

    # Write one profile cchdo units to
    # keep a record of what units need to be converted

    try:
        profile_dict = checked_profiles_info[0]["profile_checked"]["profile_dict"]

        data_type = profile_dict["data_type"]

        filename = "found_cchdo_units.txt"
        filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)

        if data_type == "btl":
            cchdo_units_profile = profile_dict[renamed_cchdo_units_key]

        if data_type == "ctd":
            cchdo_units_profile = profile_dict[renamed_cchdo_units_key]

        with open(filepath, "a") as f:
            json.dump(cchdo_units_profile, f, indent=4, sort_keys=True, default=convert)

    except KeyError:
        # Skip writing file
        pass


def save_included_excluded_cchdo_vars(included, excluded):
    """
    Save included vars
    """

    included_vars = [elem[0] for elem in included]
    unique_included_vars = list(set(included_vars))

    for var in unique_included_vars:
        included_str = [f"{elem[1]} {elem[2]}" for elem in included if elem[0] == var]

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

    excluded_vars = [elem[0] for elem in excluded]
    unique_excluded_vars = list(set(excluded_vars))

    for var in unique_excluded_vars:
        excluded_str = [f"{elem[1]} {elem[2]}" for elem in excluded if elem[0] == var]

        filename = f"{var}_excluded.txt"
        filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, filename)
        file = Path(filepath)
        file.touch(exist_ok=True)
        with file.open("a") as f:
            for id in excluded_str:
                f.write(f"{id}\n")


# def save_data_type_profiles_new(all_profiles):

#     logging.info('Saving files')

#     # Loop through all profiles, get all units and get unique
#     all_cchdo_units_mapping = get_unique_cchdo_units(all_profiles)

#     write_all_cchdo_units(all_cchdo_units_mapping)

#     save_as_zip_data_type_profiles(all_profiles)


# def save_data_type_profiles2(all_profiles_objs):

#     logging.info('Saving files')

#     for profiles_obj in all_profiles_objs:

#         data_type = profiles_obj['data_type']
#         all_profiles = profiles_obj['data_type_profiles_list']

#         # Loop through all profiles, get all units and get unique
#         all_cchdo_units_mapping = get_unique_cchdo_units(all_profiles)

#         write_all_cchdo_units(all_cchdo_units_mapping)

#         save_as_zip_data_type_profiles(all_profiles)


def save_data_type_profiles(all_profiles):
    logging.info("Saving files")

    # Loop through all profiles, get all units and get unique
    all_cchdo_units_mapping = get_unique_cchdo_units(all_profiles)

    write_all_cchdo_units(all_cchdo_units_mapping)

    # Get pressure qc and expocodes with all pressure_qc = 1
    # Put in a pandas dataframe, check the data key and look in
    # each profile

    save_as_zip_data_type_profiles(all_profiles)
