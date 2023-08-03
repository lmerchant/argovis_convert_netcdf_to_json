# Save output

import os
import json
import numpy as np
from pathlib import Path
import logging

from global_vars import GlobalVars

from variable_naming.meta_param_mapping import get_program_argovis_data_info_mapping
from check_and_save.save_as_zip import save_as_zip_data_type_profiles


def convert(o):
    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)

    if isinstance(o, np.int64):
        return int(o)


def get_unique_cchdo_units(data_type_profiles):
    all_cchdo_units_mapping = {}

    for profile in data_type_profiles:
        profile_dict = profile["profile_dict"]
        data_type = profile_dict["data_type"]

        expocode = profile_dict["meta"]["expocode"]

        # Get source names and units

        # order of values in 3rd array position of data_info
        # ['units', 'reference_scale', 'data_keys_mapping', 'data_source_standard_names', 'data_source_units', 'data_source_reference_scale']

        # CCHDO name is in 3rd place and corresponding CCHDO units in the 5th place
        parameters = profile_dict["meta"]["data_info"][2]

        for parameter in parameters:
            cchdo_param_name = parameter[2]
            cchdo_unit = parameter[4]

            if (
                cchdo_param_name in all_cchdo_units_mapping
                and cchdo_unit != all_cchdo_units_mapping[cchdo_param_name]
            ):
                new_key = f"{cchdo_param_name}_{expocode}"
                all_cchdo_units_mapping[new_key] = cchdo_unit
            else:
                all_cchdo_units_mapping[cchdo_param_name] = cchdo_unit

        return all_cchdo_units_mapping


def write_all_cchdo_units(all_cchdo_units_mapping):
    filename = "found_cchdo_units.txt"
    filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)

    with open(filepath, "a") as f:
        json.dump(all_cchdo_units_mapping, f, indent=4, sort_keys=True, default=convert)


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


def save_data_type_profiles(all_profiles):
    logging.info("Saving files")

    # Loop through all profiles, get all units and get unique
    all_cchdo_units_mapping = get_unique_cchdo_units(all_profiles)

    write_all_cchdo_units(all_cchdo_units_mapping)

    # Get pressure qc and expocodes with all pressure_qc = 1
    # Put in a pandas dataframe, check the data key and look in
    # each profile

    save_as_zip_data_type_profiles(all_profiles)


def save_cruise_objs_by_type(cruise_objs_by_type):
    for cruise_obj in cruise_objs_by_type:
        expocode = cruise_obj["cruise_expocode"]

        # Save expocode processed to a file collecting all processed
        processed_cruises_file = (
            Path(GlobalVars.LOGGING_DIR) / "all_cruises_processed.txt"
        )
        with open(processed_cruises_file, "a") as f:
            f.write(f"{expocode}\n")

        all_data_types_profile_objs = cruise_obj["all_data_types_profile_objs"]

        for data_types_profile_obj in all_data_types_profile_objs:
            data_type_profile_list = data_types_profile_obj["data_type_profiles_list"]

            save_data_type_profiles(data_type_profile_list)
