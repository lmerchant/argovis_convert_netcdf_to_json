from variable_mapping.meta_param_mapping import get_meta_param_mapping_keys
from variable_mapping.meta_param_mapping import get_goship_var_attributes_keys
from variable_mapping.meta_param_mapping import get_argovis_var_attributes_keys


def get_combined_variable_mapping(btl_dict, ctd_dict):

    mapping = {}

    meta_param_mapping_keys = get_meta_param_mapping_keys()

    for key in meta_param_mapping_keys:
        btl_key_name = f"{key}Btl"
        mapping[btl_key_name] = btl_dict[key]

        ctd_key_name = f"{key}Ctd"
        mapping[ctd_key_name] = ctd_dict[key]

    # # mapping['goshipMetaNamesBtl'] = btl_dict['goshipMetaNames']
    # # mapping['goshipMetaNamesCtd'] = ctd_dict['goshipMetaNames']

    # mapping['goshipParamNamesBtl'] = btl_dict['goshipParamNames']
    # mapping['goshipParamNamesCtd'] = ctd_dict['goshipParamNames']

    # # mapping['goshipArgovisMetaMappingBtl'] = goship_argovis_meta_mapping_btl
    # # mapping['goshipArgovisMetaMappingCtd'] = ctd_dict['goshipArgovisMetaMapping']

    # mapping['goshipArgovisParamMappingBtl'] = btl_dict['goshipArgovisParamMapping']
    # mapping['goshipArgovisParamMappingCtd'] = ctd_dict['goshipArgovisParamMapping']

    goship_var_attributes_mapping_keys = get_goship_var_attributes_keys()

    for key in goship_var_attributes_mapping_keys:
        btl_key_name = f"{key}Btl"
        mapping[btl_key_name] = btl_dict[key]

        ctd_key_name = f"{key}Ctd"
        mapping[ctd_key_name] = ctd_dict[key]

    # mapping['goshipReferenceScaleBtl'] = btl_dict['goshipReferenceScale']
    # mapping['goshipReferenceScaleCtd'] = ctd_dict['goshipReferenceScale']

    # mapping['goshipUnitsBtl'] = btl_dict['goshipUnits']
    # mapping['goshipUnitsCtd'] = ctd_dict['goshipUnits']

    # For Argovis meta names, didn't add suffix to them
    # But now that are combining, put suffix on the
    # btl dict names that  are filtered to remove
    # btl_exclude set
    # argovis_meta_names_btl = btl_dict['argovisMetaNames']
    # argovis_meta_names_btl = [
    #     f"{name}_btl" for name in argovis_meta_names_btl if name in argovis_meta_names_btl_subset]

    # argovis_meta_names = [*ctd_dict['argovisMetaNames'],
    #                       *argovis_meta_names_btl]

    # argovis_param_names = [*ctd_dict['argovisParamNames'],
    #                        *btl_dict['argovisParamNames']]

    # #mapping['argovisMetaNames'] = argovis_meta_names
    # mapping['argovisParamNames'] = argovis_param_names

    # argovis_ref_scale_mapping = {
    #     **ctd_dict['argovisReferenceScale'], **btl_dict['argovisReferenceScale']}

    # argovis_units_mapping = {
    #     **ctd_dict['argovisUnits'], **btl_dict['argovisUnits']}

    # mapping['argovisReferenceScale'] = argovis_ref_scale_mapping
    # mapping['argovisUnits'] = argovis_units_mapping

    argovis_var_attributes_keys = get_argovis_var_attributes_keys()

    for key in argovis_var_attributes_keys:
        mapping[key] = {**ctd_dict[key], **btl_dict[key]}

    # mapping['bgcMeasKeys'] = [
    #     *btl_dict['bgcMeasKeys'], *ctd_dict['bgcMeasKeys']]

    return mapping


def get_data_type_mapping(data_dict):

    mapping = {}

    meta_param_mapping_keys = get_meta_param_mapping_keys()
    goship_var_attributes_mapping_keys = get_goship_var_attributes_keys()
    argovis_var_attributes_keys = get_argovis_var_attributes_keys()

    for key in meta_param_mapping_keys:
        mapping[key] = data_dict[key]

    for key in goship_var_attributes_mapping_keys:
        mapping[key] = data_dict[key]

    for key in argovis_var_attributes_keys:
        mapping[key] = data_dict[key]

    return mapping


def get_all_variable_mappings(source_independent_meta_names, btl_dict, ctd_dict):

    # *******************************
    # Create combined mapping profile
    # *******************************

    # May have case where bot dict or ctd dict doesn't exist for same profile
    # But they have the same station_cast

    # All profiles have a profile and station_cast key but
    # profile_dict may be empty

    btl_mapping = {}
    ctd_mapping = {}
    combined_mapping = {}

    # If decide to use mapping of meta names, use source_independent_meta_names
    # to take into account if have both btl and ctd, don't
    # include _btl suffix on meta not dependent on data source

    if btl_dict and not ctd_dict:
        # #btl_mapping['argovisMetaNames'] = btl_dict['argovisMetaNames']
        # btl_mapping['argovisParamNames'] = btl_dict['argovisParamNames']
        # btl_mapping['argovisReferenceScale'] = btl_dict['argovisReferenceScale']
        # btl_mapping['argovisUnits'] = btl_dict['argovisUnits']

        # #btl_mapping['goshipMetaNames'] = btl_dict['goshipMetaNames']
        # btl_mapping['goshipParamNames'] = btl_dict['goshipParamNames']
        # # btl_mapping['goshipArgovisMetaMapping'] = btl_dict['goshipArgovisMetaMapping']
        # btl_mapping['goshipArgovisParamMapping'] = btl_dict['goshipArgovisParamMapping']
        # btl_mapping['goshipReferenceScale'] = btl_dict['goshipReferenceScale']
        # btl_mapping['goshipUnits'] = btl_dict['goshipUnits']
        # btl_mapping['bgcMeasKeys'] = btl_dict['bgcMeasKeys']

        btl_mapping = get_data_type_mapping(btl_dict)

    if ctd_dict and not btl_dict:
        # #ctd_mapping['argovisMetaNames'] = ctd_dict['argovisMetaNames']
        # ctd_mapping['argovisParamNames'] = ctd_dict['argovisParamNames']
        # ctd_mapping['argovisReferenceScale'] = ctd_dict['argovisReferenceScale']
        # ctd_mapping['argovisUnits'] = ctd_dict['argovisUnits']

        # #ctd_mapping['goshipMetaNames'] = ctd_dict['goshipMetaNames']
        # ctd_mapping['goshipParamNames'] = ctd_dict['goshipParamNames']
        # # ctd_mapping['goshipArgovisMetaMapping'] = ctd_dict['goshipArgovisMetaMapping']
        # ctd_mapping['goshipArgovisParamMapping'] = ctd_dict['goshipArgovisParamMapping']
        # ctd_mapping['goshipReferenceScale'] = ctd_dict['goshipReferenceScale']
        # ctd_mapping['goshipUnits'] = ctd_dict['goshipUnits']
        # ctd_mapping['bgcMeasKeys'] = ctd_dict['bgcMeasKeys']
        ctd_mapping = get_data_type_mapping(ctd_dict)

    if btl_dict and ctd_dict:

        # Check if have both because in combined profile,
        # can be case of only having one type for that station_cast

        combined_mapping = get_combined_variable_mapping(btl_dict, ctd_dict)

        # argovis_meta_names_btl_subset = list(
        #     set(btl_dict['argovisMetaNames']).difference(btl_exclude))

        # Put suffix of '_btl' in  bottle meta except
        # for unique meta names iin btl_exclude list
        # goship_argovis_meta_mapping_btl = btl_dict['goshipArgovisMetaMapping']
        # goship_argovis_meta_mapping_btl = {
        #     f"{key}_btl": val for key, val in goship_argovis_meta_mapping_btl.items() if val in argovis_meta_names_btl_subset}

        # # combined_btl_ctd_dict['goshipMetaNamesBtl'] = btl_dict['goshipMetaNames']
        # # combined_btl_ctd_dict['goshipMetaNamesCtd'] = ctd_dict['goshipMetaNames']

        # combined_btl_ctd_dict['goshipParamNamesBtl'] = btl_dict['goshipParamNames']
        # combined_btl_ctd_dict['goshipParamNamesCtd'] = ctd_dict['goshipParamNames']

        # # combined_btl_ctd_dict['goshipArgovisMetaMappingBtl'] = goship_argovis_meta_mapping_btl
        # # combined_btl_ctd_dict['goshipArgovisMetaMappingCtd'] = ctd_dict['goshipArgovisMetaMapping']

        # combined_btl_ctd_dict['goshipArgovisParamMappingBtl'] = btl_dict['goshipArgovisParamMapping']
        # combined_btl_ctd_dict['goshipArgovisParamMappingCtd'] = ctd_dict['goshipArgovisParamMapping']

        # combined_btl_ctd_dict['goshipReferenceScaleBtl'] = btl_dict['goshipReferenceScale']
        # combined_btl_ctd_dict['goshipReferenceScaleCtd'] = ctd_dict['goshipReferenceScale']

        # combined_btl_ctd_dict['goshipUnitsBtl'] = btl_dict['goshipUnits']
        # combined_btl_ctd_dict['goshipUnitsCtd'] = ctd_dict['goshipUnits']

        # # For Argovis meta names, didn't add suffix to them
        # # But now that are combining, put suffix on the
        # # btl dict names that  are filtered to remove
        # # btl_exclude set
        # # argovis_meta_names_btl = btl_dict['argovisMetaNames']
        # # argovis_meta_names_btl = [
        # #     f"{name}_btl" for name in argovis_meta_names_btl if name in argovis_meta_names_btl_subset]

        # # argovis_meta_names = [*ctd_dict['argovisMetaNames'],
        # #                       *argovis_meta_names_btl]

        # argovis_param_names = [*ctd_dict['argovisParamNames'],
        #                        *btl_dict['argovisParamNames']]

        # #combined_btl_ctd_dict['argovisMetaNames'] = argovis_meta_names
        # combined_btl_ctd_dict['argovisParamNames'] = argovis_param_names

        # argovis_ref_scale_mapping = {
        #     **ctd_dict['argovisReferenceScale'], **btl_dict['argovisReferenceScale']}

        # argovis_units_mapping = {
        #     **ctd_dict['argovisUnits'], **btl_dict['argovisUnits']}

        # combined_btl_ctd_dict['argovisReferenceScale'] = argovis_ref_scale_mapping
        # combined_btl_ctd_dict['argovisUnits'] = argovis_units_mapping

        # combined_btl_ctd_dict['bgcMeasKeys'] = [
        #     *btl_dict['bgcMeasKeys'], *ctd_dict['bgcMeasKeys']]

    mappings = {**btl_mapping, **ctd_mapping, **combined_mapping}

    return mappings
