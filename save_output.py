# Save output

import os
import json
import numpy as np


def convert(o):

    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)

    if isinstance(o, np.int64):
        return int(o)


def write_profile_goship_units(checked_ctd_variables, logging_dir):

    # Write one profile goship units to
    # keep a record of what units need to be converted

    try:
        profile_dict = checked_ctd_variables[0]['profile_checked']['profile_dict']

        type = profile_dict['type']

        filename = 'found_goship_units.txt'
        filepath = os.path.join(logging_dir, filename)

        if type == 'btl':
            goship_units_profile = profile_dict['goshipUnits']

        if type == 'ctd':
            goship_units_profile = profile_dict['goshipUnits']

        if type == 'btl_ctd':
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

    profile_dict.pop('type', None)

    # Remove station_cast var used to group data
    #profile_dict['meta'].pop('station_cast', None)

    # Remove  time from meta since it was just used to create date variable
    profile_dict['meta'].pop('time', None)

    # Pop off meta key and use as start of data_dict
    meta_dict = profile_dict.pop('meta', None)

    # Now combine with left over profile_dict
    data_dict = {**meta_dict, **profile_dict}

    return data_dict


def write_profile_json(json_dir, profile_dict):

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

    path = os.path.join(json_dir, folder)

    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    file = os.path.join(json_dir, folder, filename)

    # TESTING
    # TODO Remove formatting when final

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable

    # Sort keys or not?
    with open(file, 'w') as f:
        json.dump(data_dict, f, indent=4, sort_keys=False, default=convert)


def save_profile_one_type(ctd_var_check, goship_collection, excluded_collection, json_directory):

    has_all_ctd_vars = ctd_var_check['has_all_ctd_vars']
    type = ctd_var_check['type']

    profile = ctd_var_check['profile_checked']
    profile_dict = profile['profile_dict']
    expocode = profile_dict['meta']['expocode']

    add_vars_to_included_excluded_collections(
        profile_dict, goship_collection, excluded_collection)

    if has_all_ctd_vars[type]:
        write_profile_json(json_directory, profile_dict)


def save_all_profiles_one_type(checked_ctd_variables, goship_collection, excluded_collection, json_directory):

    for checked_vars in checked_ctd_variables:
        save_profile_one_type(checked_vars, goship_collection,
                              excluded_collection, json_directory)


def save_one_btl_ctd_profile(ctd_var_check, goship_collection, excluded_collection, json_directory):

    has_all_ctd_vars = ctd_var_check['has_all_ctd_vars']

    profile = ctd_var_check['profile_checked']

    profile_dict = profile['profile_dict']

    add_vars_to_included_excluded_collections(
        profile_dict, goship_collection, excluded_collection)

    if has_all_ctd_vars['btl'] or has_all_ctd_vars['ctd']:
        write_profile_json(json_directory, profile_dict)


def save_all_btl_ctd_profiles(checked_ctd_variables, goship_collection, excluded_collection, json_directory):

    for checked_vars in checked_ctd_variables:
        save_one_btl_ctd_profile(
            checked_vars, goship_collection, excluded_collection, json_directory)


def save_included_excluded_goship_vars(logging_dir, goship_collection, excluded_collection):
    # Sort each collection by variable name
    # and save to separate files

    # Sort the tuples by first element
    pass


def add_vars_to_included_excluded_collections(profile_dict, goship_collection, excluded_collection):

    profile_keys = profile_dict.keys()

    profile_id = profile_dict['meta']['id']

    included_goship_btl = []
    included_goship_ctd = []
    included_goship = []

    goship_names_btl = []
    goship_names_ctd = []
    goship_names = []

    excluded_goship_names_btl = []
    excluded_goship_names_ctd = []
    excluded_goship_names = []

    # *******************************
    # Get goship argovis name mapping
    # Only included non null goship names
    # *******************************

    try:
        name_mapping = profile_dict['goshipArgovisNameMapping']
        included_goship = name_mapping.keys()
    except KeyError:
        if 'goshipArgovisNameMappingBtl' in profile_keys:
            name_mapping_btl = profile_dict['goshipArgovisNameMappingBtl']
            included_goship_btl = name_mapping_btl.keys()
        if 'goshipArgovisNameMappingCtd' in profile_keys:
            name_mapping_ctd = profile_dict['goshipArgovisNameMappingCtd']
            included_goship_ctd = name_mapping_ctd.keys()

    # *********************
    # Get all  goship names
    # *********************

    try:
        goship_names = profile_dict['goshipNames']
    except KeyError:
        if 'goshipNamesBtl' in profile_keys:
            goship_names_btl = profile_dict['goshipNamesBtl']
        elif 'goshipNamesCtd' in profile_keys:
            goship_names_ctd = profile_dict['goshipNamesCtd']

    # ************************************
    # Find goship names not included
    # in mapping because they were all null
    # ************************************

    if included_goship_btl and goship_names_btl:
        included_goship_names_set = set(included_goship_btl)
        goship_names_set = set(goship_names_btl)
        excluded_goship_names_btl = goship_names_set.difference(
            included_goship_names_set)

    if included_goship_ctd and goship_names_ctd:
        included_goship_names_set = set(included_goship_ctd)
        goship_names_set = set(goship_names_ctd)
        excluded_goship_names_ctd = goship_names_set.difference(
            included_goship_names_set)

    if included_goship and goship_names:
        included_goship_names_set = set(included_goship)
        goship_names_set = set(goship_names)
        excluded_goship_names = goship_names_set.difference(
            included_goship_names_set)

    # **********************************
    # for included goship names
    # Add tuple (goship_name, profile_id)
    # to goship_collection
    # ***********************************

    for name in included_goship:
        goship_collection.append((name, profile_id))

    for name in included_goship_btl:
        goship_collection.append((name, profile_id))

    for name in included_goship_ctd:
        goship_collection.append((name, profile_id))

    # **********************************
    # for excluded goship names
    # Add tuple (excluded_goship_name, profile_id)
    # to excluded_collection
    # ***********************************

    for name in excluded_goship_names:
        excluded_collection.append((name, profile_id))

    for name in excluded_goship_names_btl:
        excluded_collection.append((name, profile_id))

    for name in excluded_goship_names_ctd:
        excluded_collection.append((name, profile_id))
