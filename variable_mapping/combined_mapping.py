from variable_mapping.meta_param_mapping import get_meta_param_mapping_keys
from variable_mapping.meta_param_mapping import get_cchdo_var_attributes_keys
from variable_mapping.meta_param_mapping import get_argovis_var_attributes_keys


def get_combined_variable_mapping(btl_dict, ctd_dict):

    mapping = {}

    meta_param_mapping_keys = get_meta_param_mapping_keys()

    for key in meta_param_mapping_keys:
        btl_key_name = f"{key}Btl"
        mapping[btl_key_name] = btl_dict[key]

        ctd_key_name = f"{key}Ctd"
        mapping[ctd_key_name] = ctd_dict[key]

    # # mapping['cchdoMetaNamesBtl'] = btl_dict['cchdoMetaNames']
    # # mapping['cchdoMetaNamesCtd'] = ctd_dict['cchdoMetaNames']

    # mapping['cchdoParamNamesBtl'] = btl_dict['cchdoParamNames']
    # mapping['cchdoParamNamesCtd'] = ctd_dict['cchdoParamNames']

    # # mapping['cchdoArgovisMetaMappingBtl'] = cchdo_argovis_meta_mapping_btl
    # # mapping['cchdoArgovisMetaMappingCtd'] = ctd_dict['cchdoArgovisMetaMapping']

    # mapping['cchdoArgovisParamMappingBtl'] = btl_dict['cchdoArgovisParamMapping']
    # mapping['cchdoArgovisParamMappingCtd'] = ctd_dict['cchdoArgovisParamMapping']

    cchdo_var_attributes_mapping_keys = get_cchdo_var_attributes_keys()

    for key in cchdo_var_attributes_mapping_keys:
        btl_key_name = f"{key}Btl"
        mapping[btl_key_name] = btl_dict[key]

        ctd_key_name = f"{key}Ctd"
        mapping[ctd_key_name] = ctd_dict[key]

    # mapping['cchdoReferenceScaleBtl'] = btl_dict['cchdoReferenceScale']
    # mapping['cchdoReferenceScaleCtd'] = ctd_dict['cchdoReferenceScale']

    # mapping['cchdoUnitsBtl'] = btl_dict['cchdoUnits']
    # mapping['cchdoUnitsCtd'] = ctd_dict['cchdoUnits']

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
    cchdo_var_attributes_mapping_keys = get_cchdo_var_attributes_keys()
    argovis_var_attributes_keys = get_argovis_var_attributes_keys()

    for key in meta_param_mapping_keys:
        mapping[key] = data_dict[key]

    for key in cchdo_var_attributes_mapping_keys:
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

        # #btl_mapping['cchdoMetaNames'] = btl_dict['cchdoMetaNames']
        # btl_mapping['cchdoParamNames'] = btl_dict['cchdoParamNames']
        # # btl_mapping['cchdoArgovisMetaMapping'] = btl_dict['cchdoArgovisMetaMapping']
        # btl_mapping['cchdoArgovisParamMapping'] = btl_dict['cchdoArgovisParamMapping']
        # btl_mapping['cchdoReferenceScale'] = btl_dict['cchdoReferenceScale']
        # btl_mapping['cchdoUnits'] = btl_dict['cchdoUnits']
        # btl_mapping['bgcMeasKeys'] = btl_dict['bgcMeasKeys']

        btl_mapping = get_data_type_mapping(btl_dict)

    if ctd_dict and not btl_dict:
        # #ctd_mapping['argovisMetaNames'] = ctd_dict['argovisMetaNames']
        # ctd_mapping['argovisParamNames'] = ctd_dict['argovisParamNames']
        # ctd_mapping['argovisReferenceScale'] = ctd_dict['argovisReferenceScale']
        # ctd_mapping['argovisUnits'] = ctd_dict['argovisUnits']

        # #ctd_mapping['cchdoMetaNames'] = ctd_dict['cchdoMetaNames']
        # ctd_mapping['cchdoParamNames'] = ctd_dict['cchdoParamNames']
        # # ctd_mapping['cchdoArgovisMetaMapping'] = ctd_dict['cchdoArgovisMetaMapping']
        # ctd_mapping['cchdoArgovisParamMapping'] = ctd_dict['cchdoArgovisParamMapping']
        # ctd_mapping['cchdoReferenceScale'] = ctd_dict['cchdoReferenceScale']
        # ctd_mapping['cchdoUnits'] = ctd_dict['cchdoUnits']
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
        # cchdo_argovis_meta_mapping_btl = btl_dict['cchdoArgovisMetaMapping']
        # cchdo_argovis_meta_mapping_btl = {
        #     f"{key}_btl": val for key, val in cchdo_argovis_meta_mapping_btl.items() if val in argovis_meta_names_btl_subset}

        # # combined_btl_ctd_dict['cchdoMetaNamesBtl'] = btl_dict['cchdoMetaNames']
        # # combined_btl_ctd_dict['cchdoMetaNamesCtd'] = ctd_dict['cchdoMetaNames']

        # combined_btl_ctd_dict['cchdoParamNamesBtl'] = btl_dict['cchdoParamNames']
        # combined_btl_ctd_dict['cchdoParamNamesCtd'] = ctd_dict['cchdoParamNames']

        # # combined_btl_ctd_dict['cchdoArgovisMetaMappingBtl'] = cchdo_argovis_meta_mapping_btl
        # # combined_btl_ctd_dict['cchdoArgovisMetaMappingCtd'] = ctd_dict['cchdoArgovisMetaMapping']

        # combined_btl_ctd_dict['cchdoArgovisParamMappingBtl'] = btl_dict['cchdoArgovisParamMapping']
        # combined_btl_ctd_dict['cchdoArgovisParamMappingCtd'] = ctd_dict['cchdoArgovisParamMapping']

        # combined_btl_ctd_dict['cchdoReferenceScaleBtl'] = btl_dict['cchdoReferenceScale']
        # combined_btl_ctd_dict['cchdoReferenceScaleCtd'] = ctd_dict['cchdoReferenceScale']

        # combined_btl_ctd_dict['cchdoUnitsBtl'] = btl_dict['cchdoUnits']
        # combined_btl_ctd_dict['cchdoUnitsCtd'] = ctd_dict['cchdoUnits']

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
