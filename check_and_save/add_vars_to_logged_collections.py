from variable_mapping.meta_param_mapping import get_program_argovis_mapping
import logging


# def get_cchdo_meta_names(profile_dict):

#     cchdo_meta_names_btl = []
#     cchdo_meta_names_ctd = []
#     cchdo_meta_names = []

#     profile_keys = profile_dict.keys()

#     # For meta data
#     try:
#         cchdo_meta_names = profile_dict['cchdoMetaNames']
#         cchdo_meta_names = [
#             name for name in cchdo_meta_names if '_qc' not in name]
#     except KeyError:
#         if 'cchdoMetaNamesBtl' in profile_keys:
#             cchdo_meta_names_btl = profile_dict['cchdoMetaNamesBtl']
#             cchdo_meta_names_btl = [
#                 name for name in cchdo_meta_names_btl if '_qc' not in name]
#         elif 'cchdoMetaNamesCtd' in profile_keys:
#             cchdo_meta_names_ctd = profile_dict['cchdoMetaNamesCtd']
#             cchdo_meta_names_ctd = [
#                 name for name in cchdo_meta_names_ctd if '_qc' not in name]

#     return cchdo_meta_names, cchdo_meta_names_btl, cchdo_meta_names_ctd


# def get_argovis_meta_names(profile_dict):

#     argovis_meta_names_btl = []
#     argovis_meta_names_ctd = []
#     argovis_meta_names = []

#     profile_keys = profile_dict.keys()

#     # For meta data
#     try:
#         argovis_meta_names = profile_dict['argovisMetaNames']
#     except KeyError:
#         if 'argovisMetaNamesBtl' in profile_keys:
#             argovis_meta_names_btl = profile_dict['argovisMetaNamesBtl']

#         elif 'argovisMetaNamesCtd' in profile_keys:
#             argovis_meta_names_ctd = profile_dict['argovisMetaNamesCtd']

#     return argovis_meta_names, argovis_meta_names_btl, argovis_meta_names_ctd


# def get_cchdo_param_names(profile_dict):

#     cchdo_param_names_btl = []
#     cchdo_param_names_ctd = []
#     cchdo_param_names = []

#     profile_keys = profile_dict.keys()

#     # For param data
#     try:
#         cchdo_param_names = profile_dict['cchdoParamNames']
#         cchdo_param_names = [
#             name for name in cchdo_param_names if '_qc' not in name]
#     except KeyError:
#         if 'cchdoParamNamesBtl' in profile_keys:
#             cchdo_param_names_btl = profile_dict['cchdoParamNamesBtl']
#             cchdo_param_names_btl = [
#                 name for name in cchdo_param_names_btl if '_qc' not in name]
#         elif 'cchdoParamNamesCtd' in profile_keys:
#             cchdo_param_names_ctd = profile_dict['cchdoParamNamesCtd']
#             cchdo_param_names_ctd = [
#                 name for name in cchdo_param_names_ctd if '_qc' not in name]

#     return cchdo_param_names, cchdo_param_names_btl, cchdo_param_names_ctd


def get_argovis_param_names(profile_dict):

    argovis_param_names_btl = []
    argovis_param_names_ctd = []
    argovis_param_names = []

    profile_keys = profile_dict.keys()

    # Keep my names and map to latest Argovis names
    # need this since already renamed individual files

    # cchdo names are the keys and argovis names are the value
    key_mapping = get_program_argovis_mapping()

    param_names_key = 'argovisParamNames'
    renamed_param_names_key = key_mapping(param_names_key)

    param_names_key_btl = 'argovisParamNamesBtl'
    renamed_param_names_key_btl = key_mapping(param_names_key_btl)

    param_names_key_ctd = 'argovisParamNamesCtd'
    renamed_param_names_key_ctd = key_mapping(param_names_key_ctd)

    # For param data
    try:
        argovis_param_names = profile_dict[renamed_param_names_key]

    except KeyError:
        if renamed_param_names_key_btl in profile_keys:
            argovis_param_names_btl = profile_dict[renamed_param_names_key_btl]

        elif renamed_param_names_key_ctd in profile_keys:
            argovis_param_names_ctd = profile_dict[renamed_param_names_key_ctd]

    return argovis_param_names, argovis_param_names_btl, argovis_param_names_ctd


# def find_meta_included(profile_dict):

#     # Case for argovis names

#     included_meta_btl = []
#     included_meta_ctd = []
#     included_meta = []

#     profile_keys = profile_dict.keys()

#     # For meta data
#     try:
#         name_mapping = profile_dict['cchdoArgovisMetaMapping']
#         included_meta = name_mapping.values()

#     except KeyError:
#         if 'cchdoArgovisMetaMappingBtl' in profile_keys:
#             name_mapping_btl = profile_dict['cchdoArgovisMetaMappingBtl']
#             included_meta_btl = name_mapping_btl.values()

#         if 'cchdoArgovisMetaMappingCtd' in profile_keys:
#             name_mapping_ctd = profile_dict['cchdoArgovisMetaMappingCtd']
#             included_meta_ctd = name_mapping_ctd.values()

#     return included_meta, included_meta_btl, included_meta_ctd


# def find_meta_excluded(profile_dict, included_meta_btl, included_meta_ctd, included_meta):

#     # Case for argovis names

#     excluded_meta_names_btl = []
#     excluded_meta_names_ctd = []
#     excluded_meta_names = []

#     # cchdo_meta_names, cchdo_meta_names_btl, cchdo_meta_names_ctd = get_cchdo_meta_names(
#     #     profile_dict)

#     meta_names, meta_names_btl, meta_names_ctd = get_argovis_meta_names(
#         profile_dict)

#     # For meta names
#     if included_meta_btl and meta_names_btl:
#         included_meta_names_set = set(included_meta_btl)
#         meta_names_set = set(meta_names_btl)
#         excluded_meta_names_btl = meta_names_set.difference(
#             included_meta_names_set)

#     if included_meta_ctd and meta_names_ctd:
#         included_meta_names_set = set(included_meta_ctd)
#         meta_names_set = set(meta_names_ctd)
#         excluded_meta_names_ctd = meta_names_set.difference(
#             included_meta_names_set)

#     if included_meta and meta_names:
#         included_meta_names_set = set(included_meta)
#         meta_names_set = set(meta_names)
#         excluded_meta_names = meta_names_set.difference(
#             included_meta_names_set)

#     return excluded_meta_names_btl, excluded_meta_names_ctd, excluded_meta_names


# def find_param_included(profile_dict):

#     # Case for argovis names

#     # TODO
#     # Not  using this routiine with combined for batch, but called
#     #  for after profiles are combined. But for regular
#     # program

#     included_param_btl = []
#     included_param_ctd = []
#     included_param_names = []

#     profile_keys = profile_dict.keys()

#     try:
#         name_mapping = profile_dict['cchdoArgovisParamMapping']
#         included_param_names = list(name_mapping.values())

#     except KeyError:
#         if 'cchdoArgovisParamMappingBtl' in profile_keys:
#             name_mapping_btl = profile_dict['cchdoArgovisParamMappingBtl']
#             included_param_btl = list(name_mapping_btl.values())

#         if 'cchdoArgovisParamMappingCtd' in profile_keys:
#             name_mapping_ctd = profile_dict['cchdoArgovisParamMappingCtd']
#             included_param_ctd = list(name_mapping_ctd.values())

#     return included_param_names, included_param_btl, included_param_ctd


def find_param_excluded(profile_dict, included_param_btl, included_param_ctd, included_param_names):

    # Case for argovis names

    excluded_param_names_btl = []
    excluded_param_names_ctd = []
    excluded_param_names = []

    # param_names, param_names_btl, param_names_ctd = get_cchdo_param_names(
    #     profile_dict)
    param_names, param_names_btl, param_names_ctd = get_argovis_param_names(
        profile_dict)

    # For param names
    if included_param_btl and param_names_btl:
        included_param_names_set = set(included_param_btl)
        param_names_set = set(param_names_btl)
        excluded_param_names_btl = param_names_set.difference(
            included_param_names_set)

    if included_param_ctd and param_names_ctd:
        included_param_names_set = set(included_param_ctd)
        param_names_set = set(param_names_ctd)
        excluded_param_names_ctd = param_names_set.difference(
            included_param_names_set)

    if included_param_names and param_names:
        included_param_names_set = set(included_param_names)
        param_names_set = set(param_names)
        excluded_param_names = param_names_set.difference(
            included_param_names_set)

    return excluded_param_names_btl, excluded_param_names_ctd, excluded_param_names


# def add_cchdo_vars_one_profile(profile_dict):

#     # Don't save qc or meta vars

#     profile_id = profile_dict['meta']['id']

#     # argovis_meta_keys = profile_dict['meta'].keys()

#     # # Map back to cchdo names
#     # cchdo_meta_keys = rn.convert_argovis_meta_to_cchdo_names(
#     #     argovis_meta_keys)

#     included = []
#     excluded = []

#     # Get Included Goship Meta and Param names after filtering empty cols

#     # TODO
#     # Skipping this since not relevant
#     # name_mapping = profile_dict['cchdoArgovisMetaMapping']
#     # included_meta_cchdo = [
#     #     name for name in name_mapping.keys() if '_qc' not in name]

#     name_mapping = profile_dict['cchdoArgovisParamMapping']
#     included_param_cchdo_names = [
#         name for name in name_mapping.keys() if '_qc' not in name]

#     # Get Goship Meta and Param names before filtering out empty cols

#     cchdo_meta_names = profile_dict['cchdoMetaNames']
#     cchdo_meta_names = [
#         name for name in cchdo_meta_names if '_qc' not in name]

#     cchdo_param_names = profile_dict['cchdoParamNames']
#     cchdo_param_names = [
#         name for name in cchdo_param_names if '_qc' not in name]

#     # Get Excluded Goship Meta  and Param names

#     # TODO
#     # would this be necessary? Doesn't seem like there would be duplicates
#     # included_cchdo_meta_names_set = set(included_meta_cchdo)
#     # cchdo_meta_names_set = set(cchdo_meta_names)
#     # excluded_cchdo_meta_names = cchdo_meta_names_set.difference(
#     #     included_cchdo_meta_names_set)

#     included_cchdo_param_names_set = set(included_param_cchdo_names)
#     cchdo_param_names_set = set(cchdo_param_names)

#     excluded_cchdo_param_names = cchdo_param_names_set.difference(
#         included_cchdo_param_names_set)

#     # *******************************
#     # Save included and excluded vars
#     # *******************************

#     # **********************************
#     # for included and excluded cchdo names
#     # Add tuple (cchdo_name, profile_id, data_type)
#     # ***********************************

#     data_type = profile_dict['data_type']

#     for name in included_param_cchdo_names:
#         included.append((name, profile_id, data_type))

#     for name in excluded_cchdo_param_names:
#         excluded.append((name, profile_id, data_type))

#     return included, excluded, included_param_cchdo_names, excluded_cchdo_param_names


def add_argovis_vars_one_profile(profile_dict):

    # TODO
    # Use my names and map to latest argovis names
    # So since renamed single types already, need to use the argovis names

    profile_id = profile_dict['meta']['_id']

    # argovis_meta_keys = profile_dict['meta'].keys()

    # # Map back to cchdo names
    # cchdo_meta_keys = rn.convert_argovis_meta_to_cchdo_names(
    #     argovis_meta_keys)

    included = []
    excluded = []

    # Include qc columns

    # Get Included Goship Meta and Param names after filtering empty cols
    # because did mapping after empty columns excluded
    # name_mapping = profile_dict['cchdoArgovisMetaMapping']
    # included_meta_argovis = name_mapping.values()

    # cchdo names are the keys and argovis names are the value
    cchdo_key = 'cchdoArgovisParamMapping'
    key_mapping = get_program_argovis_mapping()
    renamed_key = key_mapping[cchdo_key]

    name_mapping = profile_dict[renamed_key]

    included_param_argovis_names = list(name_mapping.values())

    # Get Goship Meta and Param names before filtering out empty cols
    # These are the names in original cchdo file which includes
    # empty columns due to starting netcdf file containing all
    # variable names for all the profiles

    #argovis_meta_names = profile_dict['argovisMetaNames']

    key_mapping = get_program_argovis_mapping()
    cchdo_key = 'argovisParamNames'
    renamed_argovis_param_names_key = key_mapping[cchdo_key]

    argovis_param_names = profile_dict[renamed_argovis_param_names_key]

    # Get Excluded Goship Meta  and Param names

    # TODO
    # would this be necessary? Doesn't seem like there would be duplicates
    # included_argovis_meta_names_set = set(included_meta_argovis)
    # argovis_meta_names_set = set(argovis_meta_names)
    # excluded_argovis_meta_names = argovis_meta_names_set.difference(
    #     included_argovis_meta_names_set)

    included_argovis_param_names_set = set(included_param_argovis_names)
    argovis_param_names_set = set(argovis_param_names)

    excluded_argovis_param_names = argovis_param_names_set.difference(
        included_argovis_param_names_set)

    # *******************************
    # Save included and excluded vars
    # *******************************

    # **********************************
    # for included and excluded argovis names
    # Add tuple (argovis_name, profile_id, data_type)
    # ***********************************

    data_type = profile_dict['data_type']

    for name in included_param_argovis_names:
        included.append((name, profile_id, data_type))

    for name in excluded_argovis_param_names:
        excluded.append((name, profile_id, data_type))

    return included, excluded, included_param_argovis_names, excluded_argovis_param_names


def add_vars_one_cruise(data_type_profiles_objs):

    vars_included = []
    vars_excluded = []

    included_names = []
    excluded_names = []

    # Loop over the profiles for the collection of files that have profiles
    for data_type_profiles_obj in data_type_profiles_objs:

        data_type_profiles = data_type_profiles_obj['data_type_profiles_list']

        for data_type_profile in data_type_profiles:

            profile_dict = data_type_profile['profile_dict']

            # included and excluded are tuples with profile_id

            # included_param_cchdo_names and excluded_cchdo_param_names
            # are the names for one profile
            # included, excluded, included_param_cchdo_names, excluded_cchdo_param_names = add_cchdo_vars_one_profile(
            #     profile_dict)

            included, excluded, included_param_argovis_names, excluded_argovis_param_names = add_argovis_vars_one_profile(
                profile_dict)

            vars_included.extend(included)
            vars_excluded.extend(excluded)

            # included_names.extend(included_param_cchdo_names)
            # excluded_names.extend(excluded_cchdo_param_names)
            included_names.extend(included_param_argovis_names)
            excluded_names.extend(excluded_argovis_param_names)

    unique_included_names = list(set(included_names))
    unique_excluded_names = list(set(excluded_names))

    # Return values are lists of tuples (name, profile_id)
    return vars_included, vars_excluded, unique_included_names, unique_excluded_names


def gather_included_excluded_vars(batch_cruises_profiles_objs):

    cruises_all_included = []
    cruises_all_excluded = []

    for cruise_profiles_obj in batch_cruises_profiles_objs:

        expocode = cruise_profiles_obj['cruise_expocode']
        profiles_objs = cruise_profiles_obj['all_data_types_profile_objs']

        # included and excluded are lists of tuples (name, profile_id, data_type)

        # unique_included_names are unique included names for all cruise
        # unique_excluded_names are unique excluded names for all cruise
        included, excluded, unique_included_names, unique_excluded_names = add_vars_one_cruise(
            profiles_objs)

        logging.info(f"expocode: {expocode}")
        logging.info(f"Num of included vars {len(unique_included_names)}")
        logging.info(f"Num of excluded vars {len(unique_excluded_names)}")

        cruises_all_included.extend(included)
        cruises_all_excluded.extend(excluded)

    return cruises_all_included, cruises_all_excluded
