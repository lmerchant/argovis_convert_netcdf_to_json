def get_source_independent_meta_names():

    names = ['expocode', 'cruise_url', 'section_id', 'station', 'cast', 'cruise_id',
             'woce_lines', 'data_center', 'positioning_system',  '_id']

    return names


def get_cchdo_var_attributes_keys():

    return ['cchdoReferenceScale', 'cchdoUnits']


def get_argovis_var_attributes_keys():
    return ['argovisReferenceScale', 'argovisUnits']


def get_meta_param_mapping_keys():
    return ['cchdoParamNames', 'argovisParamNames', 'cchdoArgovisParamMapping']


def get_meta_mapping():
    pass


def get_param_mapping():
    pass


def get_variable_properties_mapping():
    pass
