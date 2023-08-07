# Save output

import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from zipfile import ZipFile
import zipfile

from global_vars import GlobalVars


def convert(o):
    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)

    if isinstance(o, np.int32):
        return int(o)

    if isinstance(o, np.int64):
        return int(o)


# def convert(o):
#     if isinstance(o, np.float32):
#         return np.float64(o)

#     if isinstance(o, np.int8):
#         return int(o)

#     if isinstance(o, np.int64):
#         return int(o)


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


def save_included_excluded_argovis_vars(kept_vars, deleted_vars):
    """
    Save included vars into individual files
    """

    for param, vals in kept_vars.items():
        filename = f"{param}_included.txt"
        filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, filename)
        file = Path(filepath)
        file.touch(exist_ok=True)
        with file.open("a") as f:
            for id in vals:
                f.write(f"{id}\n")

    """
    Save excluded vars into individual files
    """

    for param, vals in deleted_vars.items():
        filename = f"{param}_excluded.txt"
        filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, filename)
        file = Path(filepath)
        file.touch(exist_ok=True)
        with file.open("a") as f:
            for id in vals:
                f.write(f"{id}\n")

    """
    Save all included vars in one file
    """

    filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, "all_included_params.txt")
    file = Path(filepath)
    file.touch(exist_ok=True)

    with file.open("r") as f:
        all_params = f.read()

    all_saved_params = all_params.splitlines()

    cruises_kept_params = list(kept_vars.keys())

    params_to_save = list(set(cruises_kept_params) - set(all_saved_params))

    for param in params_to_save:
        with file.open("a") as f:
            f.write(f"{param}\n")


# def convert(o):
#     if isinstance(o, np.float32):
#         return np.float64(o)

#     if isinstance(o, np.int8):
#         return int(o)

#     if isinstance(o, np.int32):
#         return int(o)

#     if isinstance(o, np.int64):
#         return int(o)


def unzip_file(zip_folder, zip_file):
    zip_ref = zipfile.ZipFile(zip_file)  # create zipfile object
    zip_ref.extractall(zip_folder)  # extract file to dir
    zip_ref.close()  # close file
    os.remove(zip_file)  # delete zipped file


def find_float_qc_cols(json_dict):
    data = json_dict["data"]

    # Read into a pandas dataframe and check if there are null values
    # If there are, then do a loop through and coerce qc values to integer

    # Also do a coerce if there are qc values as a list (such as extra dims parameters)

    data_df = pd.DataFrame.from_dict(data)

    columns = list(data_df.columns)

    qc_columns = [col for col in columns if col.endswith("_woceqc")]

    col_dtypes = data_df[qc_columns].dtypes.apply(lambda x: x.name).to_dict()

    if "object" in col_dtypes.values():
        has_qc_object_columns = True
    else:
        has_qc_object_columns = False

    # Find which qc_columns have NaN values in them, and save to coerce to integer

    # Does this find columns with cells containing a list of with NaN values in it?
    qc_cols_w_nans = (
        data_df[qc_columns].columns[data_df[qc_columns].isna().any()].tolist()
    )

    return qc_cols_w_nans


def reformat_json_dict(json_dict):
    # Find qc columns that are float because of NaNs in values
    # and coerce to integer
    float_qc_columns = find_float_qc_cols(json_dict)

    # Read into pandas to save in different format of arrays
    df = pd.DataFrame.from_dict(json_dict["data"])

    output = df.to_dict()

    data_vars_order = []
    data = []
    for var, var_output in output.items():
        data_vars_order.append(var)

        if var in float_qc_columns:
            vals = var_output.values()

            coerced_vals = [val if np.isnan(val) else int(val) for val in vals]

            value_data = coerced_vals
        else:
            value_data = list(var_output.values())

        data.append(value_data)

    json_dict["data"] = data

    return json_dict


def get_data_dict(profile_dict, station_cast):
    data_type = profile_dict["data_type"]

    profile_dict.pop("data_type", None)

    # Remove  time from meta since it was just used to create date variable
    profile_dict["meta"].pop("time", None)

    # Pop off meta key and use as start of data_dict
    meta_dict = profile_dict.pop("meta", None)

    # Now combine with left over profile_dict
    data_dict = {**meta_dict, **profile_dict}

    return data_dict


def get_filename(profile_dict):
    # TODO
    # ask
    # probably use cruise expocode instead of that in file

    id = profile_dict["_id"]

    # TODO
    # When create file id, ask if use cruise expocode instead
    filename = f"{id}.json"

    # expocode = profile_dict['expocode']

    if "/" in filename:
        filename = filename.replace("/", "_")

    return filename


def write_profiles_stats(total_btl_profiles, total_ctd_profiles):
    # Save summary information to profiles_information.txt
    profiles_file = Path(GlobalVars.LOGGING_DIR) / "profiles_information.txt"

    if profiles_file.is_file():
        # read dataframe
        df_profiles_info = pd.read_csv(profiles_file)

        (
            file_total_cruises_processed,
            file_total_btl_profiles,
            file_total_ctd_profiles,
            file_total_profiles,
        ) = df_profiles_info.loc[0, :].values.tolist()

        total_cruises_processed = file_total_cruises_processed + 1

        total_btl_profiles = file_total_btl_profiles + total_btl_profiles
        total_ctd_profiles = file_total_ctd_profiles + total_ctd_profiles

        total_profiles = total_btl_profiles + total_ctd_profiles

        df_profiles_info.loc[0] = [
            total_cruises_processed,
            total_btl_profiles,
            total_ctd_profiles,
            total_profiles,
        ]

    else:
        # create dataframe

        columns = [
            ("total_cruises_processed", int),
            ("total_btl_profiles", int),
            ("total_ctd_profiles", int),
            ("total_profiles", int),
        ]

        df_profiles_info = pd.DataFrame(columns=columns)

        df_profiles_info.reset_index(drop=True, inplace=True)

        total_cruises_processed = 1
        total_profiles = total_btl_profiles + total_ctd_profiles

        df_profiles_info.loc[0] = [
            total_cruises_processed,
            total_btl_profiles,
            total_ctd_profiles,
            total_profiles,
        ]

        df_profiles_info.columns = [
            "total_cruises_processed",
            "total_btl_profiles",
            "total_ctd_profiles",
            "total_profiles",
        ]

    # Save profile information to first row
    logging.info(df_profiles_info)

    df_profiles_info.to_csv(profiles_file, index=False)


def get_json_dicts_and_stats(data_type_profiles):
    json_dicts = []

    total_btl_profiles = 0
    total_ctd_profiles = 0

    for data_type_profile in data_type_profiles:
        station_cast = data_type_profile["station_cast"]
        profile_dict = data_type_profile["profile_dict"]
        data_type = profile_dict["data_type"]

        json_dict = get_data_dict(profile_dict, station_cast)

        json_dict = reformat_json_dict(json_dict)

        json_dicts.append(json_dict)

        # Save summary information to df_profiles_info
        if data_type == "btl":
            total_btl_profiles = total_btl_profiles + 1
        elif data_type == "ctd":
            total_ctd_profiles = total_ctd_profiles + 1
        else:
            logging.info(
                "Profile type not found when saving to profile information file"
            )

    return json_dicts, total_btl_profiles, total_ctd_profiles


def save_as_zip_data_type_profiles(
    expocode, json_dicts, total_btl_profiles, total_ctd_profiles
):
    if "/" in expocode:
        folder = expocode.replace("/", "_")
    else:
        folder = expocode

    zip_folder = os.path.join(GlobalVars.JSON_DIR, folder)
    zip_file = f"{Path.cwd()}/{zip_folder}.zip"

    zf = zipfile.ZipFile(zip_file, mode="w", compression=zipfile.ZIP_DEFLATED)

    # TODO
    # how much space saved if remove the indent?
    with zf as f:
        for json_dict in json_dicts:
            filename = get_filename(json_dict)
            json_str = json.dumps(
                json_dict,
                ensure_ascii=False,
                indent=4,
                sort_keys=False,
                default=convert,
            )
            f.writestr(filename, json_str)

    # TODO
    # For development (comment out later)
    # unzip_file(zip_folder, zip_file)

    write_profiles_stats(total_btl_profiles, total_ctd_profiles)


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

        all_json_dicts = []

        all_btl_profiles = 0
        all_ctd_profiles = 0

        for data_types_profile_obj in all_data_types_profile_objs:
            data_type_profiles_list = data_types_profile_obj["data_type_profiles_list"]

            # Loop through all profiles, get all units and get unique
            all_cchdo_units_mapping = get_unique_cchdo_units(data_type_profiles_list)

            write_all_cchdo_units(all_cchdo_units_mapping)

            (
                json_dicts,
                total_btl_profiles,
                total_ctd_profiles,
            ) = get_json_dicts_and_stats(data_type_profiles_list)

            all_btl_profiles = all_btl_profiles + total_btl_profiles
            all_ctd_profiles = all_ctd_profiles + total_ctd_profiles

            all_json_dicts.extend(json_dicts)

        save_as_zip_data_type_profiles(
            expocode, all_json_dicts, all_btl_profiles, all_ctd_profiles
        )
