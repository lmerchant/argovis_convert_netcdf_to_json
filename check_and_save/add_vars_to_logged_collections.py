
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

    included_param_goship_btl = []
    included_param_goship_ctd = []
    included_param_goship = []

    profile_keys = profile_dict.keys()

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


def add_vars_to_logged_collection_one_profile(profile_dict):

    # Don't save qc or meta vars

    profile_id = profile_dict['meta']['id']

    # argovis_meta_keys = profile_dict['meta'].keys()

    # # Map back to goship names
    # goship_meta_keys = rn.convert_argovis_meta_to_goship_names(
    #     argovis_meta_keys)

    included = []
    excluded = []

    goship_meta_names, goship_meta_names_btl, goship_meta_names_ctd = get_goship_meta_names(
        profile_dict)

    goship_param_names, goship_param_names_btl, goship_param_names_ctd = get_goship_param_names(
        profile_dict)

    included_meta_goship, included_meta_goship_btl, included_meta_goship_ctd = find_meta_included(
        profile_dict)

    included_param_goship, included_param_goship_btl, included_param_goship_ctd = find_param_included(
        profile_dict)

    excluded_goship_meta_names_btl, excluded_goship_meta_names_ctd, excluded_goship_meta_names = find_meta_excluded(
        profile_dict, included_meta_goship_btl, included_meta_goship_ctd, included_meta_goship)

    excluded_goship_param_names_btl, excluded_goship_param_names_ctd, excluded_goship_param_names = find_param_excluded(
        profile_dict, included_param_goship_btl, included_param_goship_ctd, included_param_goship)

    # *******************************
    # Save included and excluded vars
    # *******************************

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

    return included, excluded


def add_vars_to_logged_collections_dask(profiles_objs):

    all_included = []
    all_excluded = []

    for profiles_obj in profiles_objs:

        profiles = profiles_obj['profiles']

        for profile in profiles:

            profile_dict = profile['profile_dict']

            included, excluded = add_vars_to_logged_collection_one_profile(
                profile_dict)

            all_included.extend(included)
            all_excluded.extend(excluded)

    # Return values are lists of tuples (name, profile_id)
    return all_included, all_excluded


def add_vars_to_logged_collections(profiles_objs):

    all_included = []
    all_excluded = []

    for profiles_obj in profiles_objs:

        profile_dict = profiles_obj['profile_dict']

        included, excluded = add_vars_to_logged_collection_one_profile(
            profile_dict)

        all_included.extend(included)
        all_excluded.extend(excluded)

    # Return values are lists of tuples (name, profile_id)
    return all_included, all_excluded
