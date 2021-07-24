# Save output

import os
import json
import numpy as np

from global_vars import GlobalVars


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

    # add_vars_to_included_excluded_collections(
    #     profile_dict, cruises_collections)

    if has_all_ctd_vars['btl'] or has_all_ctd_vars['ctd']:
        write_profile_json(profile_dict)


def save_all_btl_ctd_profiles(checked_ctd_variables):

    for checked_vars in checked_ctd_variables:
        save_one_btl_ctd_profile(checked_vars)


# def save_included_excluded_goship_vars(cruises_collections):
#     # Sort each collection by variable name
#     # and save to separate files

#     included = cruises_collections['included']
#     excluded = cruises_collections['excluded']

#     sorted_included = sorted(included)
#     included_vars = [elem[0] for elem in sorted_included]
#     unique_included_vars = list(set(included_vars))

#     for var in unique_included_vars:

#         # Save included var
#         included_ids = [elem[1]
#                         for elem in sorted_included if elem[0] == var]

#         filename = f"{var}_included.txt"
#         filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, filename)
#         with open(filepath, 'w') as f:
#             for id in included_ids:
#                 f.write(f"{id}\n")

#     # Save excluded var
#     sorted_excluded = sorted(excluded)
#     excluded_vars = [
#         elem[0] for elem in sorted_excluded]
#     unique_excluded_vars = list(set(excluded_vars))

#     for var in unique_excluded_vars:

#         excluded_ids = [elem[1]
#                         for elem in sorted_excluded if elem[0] == var]

#         filename = f"{var}_excluded.txt"
#         filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, filename)
#         with open(filepath, 'w') as f:
#             for id in excluded_ids:
#                 f.write(f"{id}\n")


def save_included_excluded_goship_vars_dask(included, excluded):
    # Sort each collection by variable name
    # and save to separate files

    sorted_included = sorted(included)
    included_vars = [elem[0] for elem in sorted_included]
    unique_included_vars = list(set(included_vars))

    for var in unique_included_vars:

        # Save included var
        included_ids = [elem[1]
                        for elem in sorted_included if elem[0] == var]

        filename = f"{var}_included.txt"
        filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, filename)
        with open(filepath, 'w') as f:
            for id in included_ids:
                f.write(f"{id}\n")

    # Save excluded var
    sorted_excluded = sorted(excluded)
    excluded_vars = [
        elem[0] for elem in sorted_excluded]
    unique_excluded_vars = list(set(excluded_vars))

    for var in unique_excluded_vars:

        excluded_ids = [elem[1]
                        for elem in sorted_excluded if elem[0] == var]

        filename = f"{var}_excluded.txt"
        filepath = os.path.join(GlobalVars.INCLUDE_EXCLUDE_DIR, filename)
        with open(filepath, 'w') as f:
            for id in excluded_ids:
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
