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


def save_one_btl_ctd_profile(ctd_var_check, collections):

    has_all_ctd_vars = ctd_var_check['has_all_ctd_vars']

    profile = ctd_var_check['profile_checked']

    profile_dict = profile['profile_dict']

    add_vars_to_included_excluded_collections(
        profile_dict, collections)

    if has_all_ctd_vars['btl'] or has_all_ctd_vars['ctd']:
        write_profile_json(profile_dict)


def save_all_btl_ctd_profiles(checked_ctd_variables, collections):

    for checked_vars in checked_ctd_variables:
        save_one_btl_ctd_profile(
            checked_vars, collections)


def save_included_excluded_goship_vars(collections):
    # Sort each collection by variable name
    # and save to separate files

    included = collections['included']
    excluded = collections['excluded']

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


def add_vars_to_included_excluded_collections(profile_dict, collections):

    # Don't save qc or meta vars

    profile_keys = profile_dict.keys()

    profile_id = profile_dict['meta']['id']

    # argovis_meta_keys = profile_dict['meta'].keys()

    # # Map back to goship names
    # goship_meta_keys = rn.convert_argovis_meta_to_goship_names(
    #     argovis_meta_keys)

    included_meta_goship_btl = []
    included_meta_goship_ctd = []
    included_meta_goship = []

    included_param_goship_btl = []
    included_param_goship_ctd = []
    included_param_goship = []

    goship_meta_names_btl = []
    goship_meta_names_ctd = []
    goship_meta_names = []

    goship_param_names_btl = []
    goship_param_names_ctd = []
    goship_param_names = []

    excluded_goship_meta_names_btl = []
    excluded_goship_meta_names_ctd = []
    excluded_goship_meta_names = []

    excluded_goship_param_names_btl = []
    excluded_goship_param_names_ctd = []
    excluded_goship_param_names = []

    # *******************************
    # Get goship argovis name mapping
    # Only included non null goship names
    # *******************************

    # For meta data
    try:
        name_mapping = profile_dict['goshipArgovisMetaMapping']
        included_meta_goship = [
            name for name in name_mapping.keys() if '_qc' not in name]
    except KeyError:
        if 'goshipArgovisMetaMappingBtl' in profile_keys:
            name_mapping_btl = profile_dict['goshipArgovisMetaMappingBtl']
            included_meta_goship_btl = [
                name for name in name_mapping_btl.keys() if '_qc' not in name]
        if 'goshipArgovisMetaMappingCtd' in profile_keys:
            name_mapping_ctd = profile_dict['goshipArgovisMetaMappingCtd']
            included_meta_goship_ctd = [
                name for name in name_mapping_ctd.keys() if '_qc' not in name]

    # For param data
    try:
        name_mapping = profile_dict['goshipArgovisParamMapping']
        included_param_goship = [
            name for name in name_mapping.keys() if '_qc' not in name]
    except KeyError:
        if 'goshipArgovisParamMappingBtl' in profile_keys:
            name_mapping_btl = profile_dict['goshipArgovisParamMappingBtl']
            included_param_goship_btl = [
                name for name in name_mapping_btl.keys() if '_qc' not in name]
        if 'goshipArgovisParamMappingCtd' in profile_keys:
            name_mapping_ctd = profile_dict['goshipArgovisParamMappingCtd']
            included_param_goship_ctd = [
                name for name in name_mapping_ctd.keys() if '_qc' not in name]

    # *********************
    # Get all  goship names
    # *********************

    # For meta data
    try:
        goship_meta_names = profile_dict['goshipMetaNames']
        goship_meta_names = [
            name for name in goship_meta_names if '_qc' not in name]
    except KeyError:
        if 'goshipMetaNamesBtl' in profile_keys:
            goship_meta_names_btl = profile_dict['goshipMetaNamesBtl']
            goship_meta_names_btl = [
                name for name in goship_meta_names_btl if '_qc' not in name]
        elif 'goshipMetaNamesCtd' in profile_keys:
            goship_meta_names_ctd = profile_dict['goshipMetaNamesCtd']
            goship_meta_names_ctd = [
                name for name in goship_meta_names_ctd if '_qc' not in name]

    # For param data
    try:
        goship_param_names = profile_dict['goshipParamNames']
        goship_param_names = [
            name for name in goship_param_names if '_qc' not in name]
    except KeyError:
        if 'goshipParamNamesBtl' in profile_keys:
            goship_param_names_btl = profile_dict['goshipParamNamesBtl']
            goship_param_names_btl = [
                name for name in goship_param_names_btl if '_qc' not in name]
        elif 'goshipParamNamesCtd' in profile_keys:
            goship_param_names_ctd = profile_dict['goshipParamNamesCtd']
            goship_param_names_ctd = [
                name for name in goship_param_names_ctd if '_qc' not in name]

    # ************************************
    # Find goship names not included
    # in mapping because they were all null
    # ************************************

    # For meta names
    if included_meta_goship_btl and goship_meta_names_btl:
        included_goship_meta_names_set = set(included_meta_goship_btl)
        goship_meta_names_set = set(goship_meta_names_btl)
        excluded_goship_meta_names_btl = goship_meta_names_set.difference(
            included_goship_meta_names_set)

    if included_meta_goship_ctd and goship_meta_names_ctd:
        included_goship_meta_names_set = set(included_meta_goship_ctd)
        goship_meta_names_set = set(goship_meta_names_ctd)
        excluded_goship_meta_names_ctd = goship_meta_names_set.difference(
            included_goship_meta_names_set)

    if included_meta_goship and goship_meta_names:
        included_goship_meta_names_set = set(included_meta_goship)
        goship_meta_names_set = set(goship_meta_names)
        excluded_goship_meta_names = goship_meta_names_set.difference(
            included_goship_meta_names_set)

    # For param names
    if included_param_goship_btl and goship_param_names_btl:
        included_goship_param_names_set = set(included_param_goship_btl)
        goship_param_names_set = set(goship_param_names_btl)
        excluded_goship_param_names_btl = goship_param_names_set.difference(
            included_goship_param_names_set)

    if included_param_goship_ctd and goship_param_names_ctd:
        included_goship_param_names_set = set(included_param_goship_ctd)
        goship_param_names_set = set(goship_param_names_ctd)
        excluded_goship_param_names_ctd = goship_param_names_set.difference(
            included_goship_param_names_set)

    if included_param_goship and goship_param_names:
        included_goship_param_names_set = set(included_param_goship)
        goship_param_names_set = set(goship_param_names)
        excluded_goship_param_names = goship_param_names_set.difference(
            included_goship_param_names_set)

    # *******************************
    # Save included and excluded vars
    # *******************************

    included = collections['included']
    excluded = collections['excluded']

    # **********************************
    # for included goship names
    # Add tuple (goship_name, profile_id)
    # to included
    # ***********************************

    for name in included_param_goship:
        included.append((name, profile_id))

    for name in included_param_goship_btl:
        included.append((name, profile_id))

    for name in included_param_goship_ctd:
        included.append((name, profile_id))

    # **********************************
    # for excluded goship names
    # Add tuple (excluded_goship_name, profile_id)
    # to excluded
    # ***********************************

    for name in excluded_goship_param_names:
        excluded.append((name, profile_id))

    for name in excluded_goship_param_names_btl:
        excluded.append((name, profile_id))

    for name in excluded_goship_param_names_ctd:
        excluded.append((name, profile_id))


def save_profile_one_type(ctd_var_check, collections):

    has_all_ctd_vars = ctd_var_check['has_all_ctd_vars']
    data_type = ctd_var_check['data_type']

    profile = ctd_var_check['profile_checked']
    profile_dict = profile['profile_dict']
    expocode = profile_dict['meta']['expocode']

    add_vars_to_included_excluded_collections(
        profile_dict, collections)

    if has_all_ctd_vars[data_type]:
        write_profile_json(profile_dict)


def save_all_profiles_one_type(checked_ctd_variables, collections):

    for checked_vars in checked_ctd_variables:
        save_profile_one_type(checked_vars, collections)
