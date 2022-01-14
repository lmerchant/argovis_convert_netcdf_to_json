from variable_mapping.meta_param_mapping import get_mappings_keys_mapping


def create_combined_mappings(btl_mappings, ctd_mappings):

    # Add suffix if find base key name in mappings_keys_mapping
    # But only add suffix to some and for others combine the dicts

    # then combine this suffixed names together into one mappings var

    # keys are cchdo names and values are argovis names
    # for btl and ctd mapping, already using argovis names
    # just need to change into suffix version if in the mappings dict
    mappings_keys_mapping = get_mappings_keys_mapping()

    # Some mapping keys won't have a btl suffix like argovis names
    # but need to be combined into one dict

    # TODO
    # Create better search if change name and don't use suffix
    # Like could combine all into same name
    # I think it still works though

    suffixed_btl_mapping = {}
    same_btl_mapping = {}

    for key, value in btl_mappings.items():

        suffixed_key = f"{key}_btl"

        if suffixed_key in mappings_keys_mapping.values():
            suffixed_btl_mapping[suffixed_key] = value

        else:

            same_btl_mapping[key] = value

    suffixed_ctd_mapping = {}
    same_ctd_mapping = {}

    for key, value in ctd_mappings.items():

        suffixed_key = f"{key}_ctd"

        if suffixed_key in mappings_keys_mapping.values():
            suffixed_ctd_mapping[suffixed_key] = value
        else:
            same_ctd_mapping[key] = value

    combined_same_mapping = {**same_btl_mapping, **same_ctd_mapping}

    combined_suffixed_mapping = {
        **suffixed_btl_mapping, **suffixed_ctd_mapping}

    new_mappings = {**combined_same_mapping, **combined_suffixed_mapping}

    return new_mappings


# def create_combined_mappings_orig(btl_dict, ctd_dict):

#     print('inside get_combined_mappings')

#     mapping = {}

#     meta_param_mapping_keys = get_meta_mapping_keys()

#     mappings_keys_mapping = get_mappings_keys_mapping()

#     for key in meta_param_mapping_keys:

#         print(f"key {key}")

#         btl_key_name = f"{key}Btl"
#         mapping[btl_key_name] = btl_dict[key]

#         ctd_key_name = f"{key}Ctd"
#         mapping[ctd_key_name] = ctd_dict[key]

#     cchdo_var_attributes_mapping_keys = get_cchdo_var_attributes_keys()

#     for key in cchdo_var_attributes_mapping_keys:
#         btl_key_name = f"{key}Btl"
#         mapping[btl_key_name] = btl_dict[key]

#         ctd_key_name = f"{key}Ctd"
#         mapping[ctd_key_name] = ctd_dict[key]

#     argovis_var_attributes_keys = get_argovis_var_attributes_keys()

#     for key in argovis_var_attributes_keys:
#         mapping[key] = {**ctd_dict[key], **btl_dict[key]}

#     return mapping


# def get_data_type_mapping(data_dict):

#     mapping = {}

#     meta_param_mapping_keys = get_meta_mapping_keys()

#     cchdo_var_attributes_mapping_keys = get_cchdo_var_attributes_keys()
#     argovis_var_attributes_keys = get_argovis_var_attributes_keys()

#     for key in meta_param_mapping_keys:
#         mapping[key] = data_dict[key]

#     for key in cchdo_var_attributes_mapping_keys:
#         mapping[key] = data_dict[key]

#     for key in argovis_var_attributes_keys:
#         mapping[key] = data_dict[key]

#     return mapping


def get_combined_mappings(btl_dict, ctd_dict):

    # *******************************
    # Create combined mappings
    # Where btl and ctd suffix added
    # *******************************

    # May have case where bot dict or ctd dict doesn't exist for same profile
    # But they have the same station_cast

    # All profiles have a profile and station_cast key but
    # profile_dict may be empty

    btl_mappings = {}
    ctd_mappings = {}
    combined_mappings = {}

    # If decide to use mapping of meta names, use source_independent_meta_names
    # to take into account if have both btl and ctd, don't
    # include _btl suffix on meta not dependent on data source

    mapping_keys_mapping = get_mappings_keys_mapping()

    if btl_dict:

        # btl_dict already renamed
        # so use mapped key names from mapping_keys_mapping
        argovis_mapping_keys = list(mapping_keys_mapping.values())

        btl_mappings = {key: value for key,
                        value in btl_dict.items() if key in argovis_mapping_keys}

    if ctd_dict:

        # ctd_dict already renamed
        # so use mapped key names from mapping_keys_mapping
        argovis_mapping_keys = list(mapping_keys_mapping.values())

        ctd_mappings = {key: value for key,
                        value in ctd_dict.items() if key in argovis_mapping_keys}

    if btl_dict and not ctd_dict:
        combined_mappings = btl_mappings

    if not btl_dict and ctd_dict:
        combined_mappings = ctd_mappings

    if btl_dict and ctd_dict:

        combined_mappings = create_combined_mappings(
            btl_mappings, ctd_mappings)

    return combined_mappings
