def get_source_independent_meta_names():

    names = ['expocode', 'cruise_url', 'section_id', 'station', 'cast', 'cruise_id',
             'woce_lines', 'DATA_CENTRE', 'POSITIONING_SYSTEM',  '_id']

    return names


def get_goship_var_attributes_keys():

    return ['goshipReferenceScale', 'goshipUnits']


def get_argovis_var_attributes_keys():
    return ['argovisReferenceScale', 'argovisUnits']


def get_meta_param_mapping_keys():
    return ['goshipParamNames', 'argovisParamNames', 'goshipArgovisParamMapping']


def get_meta_mapping():
    pass


def get_param_mapping():
    pass


def get_variable_properties_mapping():
    pass
