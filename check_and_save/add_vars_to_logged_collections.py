import logging


def get_goship_meta_names(profile_dict):

    goship_meta_names_btl = []
    goship_meta_names_ctd = []
    goship_meta_names = []

    profile_keys = profile_dict.keys()

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

    return goship_meta_names, goship_meta_names_btl, goship_meta_names_ctd


def get_goship_param_names(profile_dict):

    goship_param_names_btl = []
    goship_param_names_ctd = []
    goship_param_names = []

    profile_keys = profile_dict.keys()

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

    return goship_param_names, goship_param_names_btl, goship_param_names_ctd


def find_meta_included(profile_dict):

    included_meta_goship_btl = []
    included_meta_goship_ctd = []
    included_meta_goship = []

    profile_keys = profile_dict.keys()

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

    return included_meta_goship, included_meta_goship_btl, included_meta_goship_ctd


def find_meta_excluded(profile_dict, included_meta_goship_btl, included_meta_goship_ctd, included_meta_goship):

    excluded_goship_meta_names_btl = []
    excluded_goship_meta_names_ctd = []
    excluded_goship_meta_names = []

    goship_meta_names, goship_meta_names_btl, goship_meta_names_ctd = get_goship_meta_names(
        profile_dict)

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

    return excluded_goship_meta_names_btl, excluded_goship_meta_names_ctd, excluded_goship_meta_names


def find_param_included(profile_dict):

    # TODO
    # Not  using this routiine with combined for batch, but called
    #  for after profiles are combined. But for regular
    # program, I think I am

    included_param_goship_btl = []
    included_param_goship_ctd = []
    included_param_goship = []

    profile_keys = profile_dict.keys()

    # TODO
    #  here
    # In batch mode, it's  only using the names of one cruise

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

    return included_param_goship, included_param_goship_btl, included_param_goship_ctd


def find_param_excluded(profile_dict, included_param_goship_btl, included_param_goship_ctd, included_param_goship):

    excluded_goship_param_names_btl = []
    excluded_goship_param_names_ctd = []
    excluded_goship_param_names = []

    goship_param_names, goship_param_names_btl, goship_param_names_ctd = get_goship_param_names(
        profile_dict)

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

    return excluded_goship_param_names_btl, excluded_goship_param_names_ctd, excluded_goship_param_names


def add_vars_one_profile(profile_dict):

    # Don't save qc or meta vars

    profile_id = profile_dict['meta']['id']

    # argovis_meta_keys = profile_dict['meta'].keys()

    # # Map back to goship names
    # goship_meta_keys = rn.convert_argovis_meta_to_goship_names(
    #     argovis_meta_keys)

    included = []
    excluded = []

    # Get Included Goship Meta and Param names after filtering empty cols

    name_mapping = profile_dict['goshipArgovisMetaMapping']
    included_meta_goship = [
        name for name in name_mapping.keys() if '_qc' not in name]

    name_mapping = profile_dict['goshipArgovisParamMapping']
    included_param_goship = [
        name for name in name_mapping.keys() if '_qc' not in name]

    # Get Goship Meta and Param names before filtering out empty cols

    goship_meta_names = profile_dict['goshipMetaNames']
    goship_meta_names = [
        name for name in goship_meta_names if '_qc' not in name]

    goship_param_names = profile_dict['goshipParamNames']
    goship_param_names = [
        name for name in goship_param_names if '_qc' not in name]

    # Get Excluded Goship Meta  and Param names

    included_goship_meta_names_set = set(included_meta_goship)
    goship_meta_names_set = set(goship_meta_names)
    excluded_goship_meta_names = goship_meta_names_set.difference(
        included_goship_meta_names_set)

    included_goship_param_names_set = set(included_param_goship)
    goship_param_names_set = set(goship_param_names)
    excluded_goship_param_names = goship_param_names_set.difference(
        included_goship_param_names_set)

    # *******************************
    # Save included and excluded vars
    # *******************************

    # **********************************
    # for included and excluded goship names
    # Add tuple (goship_name, profile_id, data_type)
    # ***********************************

    data_type = profile_dict['data_type']

    for name in included_param_goship:
        included.append((name, profile_id, data_type))

    for name in excluded_goship_param_names:
        excluded.append((name, profile_id, data_type))

    return included, excluded


def add_vars_one_cruise(data_type_profiles_objs):

    vars_included = []
    vars_excluded = []

    # Loop over the profiles for the collection of files with profiles
    for data_type_profiles_obj in data_type_profiles_objs:

        data_type_profiles = data_type_profiles_obj['data_type_profiles_list']

        for data_type_profile in data_type_profiles:

            profile_dict = data_type_profile['profile_dict']

            included, excluded = add_vars_one_profile(
                profile_dict)

            vars_included.extend(included)
            vars_excluded.extend(excluded)

    # Return values are lists of tuples (name, profile_id)
    return vars_included, vars_excluded


def gather_included_excluded_vars(batch_cruises_profiles_objs):

    cruises_all_included = []
    cruises_all_excluded = []

    for cruise_profiles_obj in batch_cruises_profiles_objs:

        expocode = cruise_profiles_obj['cruise_expocode']
        profiles_objs = cruise_profiles_obj['all_data_types_profile_objs']

        # Return values are lists of tuples (name, profile_id)
        included, excluded = add_vars_one_cruise(profiles_objs)

        logging.info(f"expocode: {expocode}")
        logging.info(f"# of included vars {len(included)}")
        logging.info(f"# of excluded vars {len(excluded)}")

        cruises_all_included.extend(included)
        cruises_all_excluded.extend(excluded)

    return cruises_all_included, cruises_all_excluded
