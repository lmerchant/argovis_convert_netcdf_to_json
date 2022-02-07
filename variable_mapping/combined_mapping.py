import itertools
from collections import defaultdict


from variable_mapping.meta_param_mapping import get_program_argovis_mapping
from variable_mapping.meta_param_mapping import get_combined_mappings_keys


def get_updated_mappings(mappings, data_type):

    # Add suffix if find base key name in mappings_keys_mapping
    # But only add suffix to some and for others combine the dicts

    # then combine this suffixed names together into one mappings var

    # keys are cchdo names and values are argovis names
    # for btl and ctd mapping, already using argovis names
    # just need to change into suffix version if in the mappings dict
    all_mapping_keys = get_program_argovis_mapping()

    # Get the keys which will be combined independent of data type
    combined_mapping_keys = get_combined_mappings_keys()

    # Some mapping keys won't have a btl suffix such as argovis names key
    # but need to be combined into one dict

    # TODO
    # Create better search if change name and don't use suffix
    # Like could combine all into same name
    # I think it still works though

    data_type_mappings = {}
    combined_mappings = {}

    for key, value in mappings.items():

        suffixed_key = f"{key}_{data_type}"

        if (suffixed_key in all_mapping_keys.values()) and (key not in combined_mapping_keys):
            data_type_mappings[suffixed_key] = value
        else:
            combined_mappings[key] = value

    return combined_mappings, data_type_mappings


def create_combined_mappings(btl_mappings, ctd_mappings, data_type):

    # Add suffix if find base key name in mappings_keys_mapping
    # But only add suffix to some and for others combine the dicts

    # then combine this suffixed names together into one mappings var

    # keys are cchdo names and values are argovis names
    # for btl and ctd mapping, already using argovis names
    # just need to change into suffix version if in the mappings dict
    #mappings_keys_mapping = get_program_argovis_mapping()

    # Some mapping keys won't have a btl suffix such as argovis names key
    # but need to be combined into one dict

    if btl_mappings:
        combined_btl_mapping, suffixed_btl_mapping = get_updated_mappings(
            btl_mappings, 'btl')
    else:
        combined_btl_mapping = {}
        suffixed_btl_mapping = {}

    if ctd_mappings:

        combined_ctd_mapping, suffixed_ctd_mapping = get_updated_mappings(
            ctd_mappings, 'ctd')
    else:
        combined_ctd_mapping = {}
        suffixed_ctd_mapping = {}

    # print(f"\n\n")
    # print('btl mappings')
    # print(btl_mappings)

    # print(f"\n\n")
    # print('ctd mappings')
    # print(ctd_mappings)
    # print(f"\n\n")

    # print(f"\n\n")

    # print('combined btl mappings')
    # print(combined_btl_mapping)
    # print(f"\n\n")
    # print('suffixed btl mapping')
    # print(suffixed_btl_mapping)

    # print('combined ctd mappings')
    # print(combined_ctd_mapping)
    # print(f"\n\n")
    # print('suffixed ctd mapping')
    # print(suffixed_ctd_mapping)

    # print(f"\n\n")

    if combined_btl_mapping and combined_ctd_mapping:

        def merge_dict(dict1, dict2):
            dict3 = {**dict1, **dict2}
            for key, value in dict3.items():
                if key in dict1 and key in dict2:
                    if isinstance(value, list):
                        result = [*value, *dict1[key]]
                        dict3[key] = list(set(result))
                    elif isinstance(value, dict):
                        dict3[key] = {**value, **dict1[key]}
                    else:
                        dict3[key] = value

            return dict3

        combined_mapping = merge_dict(
            combined_btl_mapping, combined_ctd_mapping)

    elif combined_btl_mapping and not combined_ctd_mapping:
        combined_mapping = combined_btl_mapping
    elif combined_ctd_mapping and not combined_btl_mapping:
        combined_mapping = combined_ctd_mapping

    suffixed_mapping = {
        **suffixed_btl_mapping, **suffixed_ctd_mapping}

    new_mappings = {**combined_mapping, **suffixed_mapping}

    return new_mappings


def get_combined_mappings(btl_dict, ctd_dict):

    # *******************************
    # Create combined mappings
    # Where btl and ctd suffix added
    # *******************************

    # May have case where bot dict or ctd dict doesn't exist for same profile
    # But they have the same station_cast

    # All profiles have a profile and station_cast key but
    # profile_dict may be empty. If empty, then there are no mappings for it

    btl_mappings = {}
    ctd_mappings = {}
    combined_mappings = {}

    # If decide to use mapping of meta names, use source_combined_meta_names
    # to take into account if have both btl and ctd, don't
    # include _btl suffix on meta not dependent on data source

    mapping_keys_mapping = get_program_argovis_mapping()

    if btl_dict:

        # btl_dict already renamed
        # so use mapped key names from mapping_keys_mapping
        argovis_mapping_keys = list(mapping_keys_mapping.values())

        # print(btl_dict.items())

        btl_mappings = {key: value for key,
                        value in btl_dict.items() if key in argovis_mapping_keys}

    if ctd_dict:

        # ctd_dict already renamed
        # so use mapped key names from mapping_keys_mapping
        argovis_mapping_keys = list(mapping_keys_mapping.values())

        ctd_mappings = {key: value for key,
                        value in ctd_dict.items() if key in argovis_mapping_keys}

    # duplicate keys so always have set with and without suffix of data type

    if btl_dict and not ctd_dict:

        data_type = 'btl'

        #combined_mappings = btl_mappings

    if not btl_dict and ctd_dict:

        data_type = 'ctd'

        #combined_mappings = ctd_mappings

    if btl_dict and ctd_dict:

        data_type = 'btl_ctd'

    combined_mappings = create_combined_mappings(
        btl_mappings, ctd_mappings, data_type)

    return combined_mappings
