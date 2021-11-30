
# TODO
# Is this before or after rename?
# For combining, it would be after, so need to use argovis meta names

def get_source_independent_meta_names():

    names = ['expocode', 'station', 'cast', 'cchdo_cruise_id', 'woce_lines',
             'source', 'data_center', 'positioning_system',  '_id', 'country',
             'pi_name', 'cruise_url']

    return names


# def get_source_independent_meta_names():

#     names = ['expocode', 'cruise_url', 'section_id', 'station', 'cast', 'cruise_id',
#              'woce_lines', 'data_center', 'positioning_system',  '_id', 'country']

#     return names

# def get_cchdo_var_attributes_keys():
#     return ['cchdoReferenceScale', 'cchdoUnits']


# def get_argovis_var_attributes_keys():
#     return ['argovisReferenceScale', 'argovisUnits']


# TODO
# Why use this when could use get_mappings_keys_mapping

# def get_meta_mapping_keys():

#     # TODO
#     # don't want to map to new names yet
#     # where else do I call this

#     return ['cchdoParamNames',
#             'argovisParamNames', 'cchdoArgovisParamMapping']


# def get_meta_mapping_keys_orig():

#     # TODO
#     # don't want to map to new names yet
#     # where else do I call this

#     cchdo_mapping_keys = ['cchdoParamNames',
#                           'argovisParamNames', 'cchdoArgovisParamMapping']

#     key_mapping = get_mappings_keys_mapping()

#     new_mappings_keys = {}
#     for key in cchdo_mapping_keys:
#         new_key = key_mapping[key]
#         new_mappings_keys[new_key] = key_mapping[key]

#     return new_mappings_keys


def get_measurements_mapping():
    return {
        'measurements': 'measurements',
        'measurementsSource': 'measurements_source',
        'measurementsSources': 'measurements_sources'
    }

 # TODO
 # really just call get_meta_mappping()


def get_meta_mapping():

    # keys are argovis names
    # values are meta names
    return {
        'btm_depth': 'btm_depth',
        'latitude': 'latitude',
        'longitude': 'longitude',
        'expocode': 'expocode',
        'file_expocode': 'file_expocode',
        'station': 'station',
        'cast': 'cast',
        'file_hash': 'file_hash',
        'cruise_id': 'cchdo_cruise_id',
        'programs': 'source',
        'woce_lines': 'woce_lines',
        'chief_scientists': 'pi_name',
        'country': 'country',
        'positioning_system': 'positioning_system',
        'data_center': 'data_center',
        'cruise_url': 'cruise_url',
        'file_path': 'source_url',
        'file_name': 'file_name',
        '_id': '_id',
        'date_formatted': 'date_formatted',
        'date': 'timestamp',
        'roundLat': 'roundLat',
        'roundLon': 'roundLon',
        'strLat': 'strLat',
        'strLon': 'strLon',
        'geoLocation': 'geoLocation'
    }


def get_mappings_keys_mapping():

    # TODO
    # could have the case where always apply the suffix?

    return {
        # 'cchdoMetaNames': 'cchdo_meta_names',
        'cchdoParamNames': 'data_keys_source',
        'cchdoParamNamesBtl': 'data_keys_source_btl',
        'cchdoParamNamesCtd': 'data_keys_source_ctd',
        'argovisParamNames': 'data_keys',
        'argovisParamNamesBtl': 'data_keys_btl',
        'argovisParamNamesCtd': 'data_keys_ctd',
        'cchdoArgovisParamMapping': 'data_keys_mapping',
        'cchdoArgovisParamMappingBtl': 'data_keys_mapping_btl',
        'cchdoArgovisParamMappingCtd': 'data_keys_mapping_ctd',
        'cchdoReferenceScale': 'data_source_reference_scale',
        'cchdoReferenceScaleBtl': 'data_source_reference_scale_btl',
        'cchdoReferenceScaleCtd': 'data_source_reference_scale_ctd',
        'cchdoUnits': 'data_source_units',
        'cchdoUnitsBtl': 'data_source_units_btl',
        'cchdoUnitsCtd': 'data_source_units_ctd',
        'argovisReferenceScale': 'data_reference_scale',
        'argovisUnits': 'data_units'
    }


def get_core_profile_keys_mapping():

    return {'data': 'data'}


def rename_mappings_keys(mappings):

    # keys are CCHDO and values are Argovis
    key_mapping = get_mappings_keys_mapping()

    new_mappings = {}
    for key, value in mappings.items():
        if key in key_mapping:
            new_key = key_mapping[key]
            new_mappings[new_key] = value
        else:
            new_mappings[key] = value

    return new_mappings


def rename_core_profile_keys(profile):

    key_mapping = get_core_profile_keys_mapping()

    # keys are CCHDO and values are Argovis
    new_profile = {}
    for key, value in profile.items():
        if key in key_mapping:
            new_key = key_mapping[key]
            new_profile[new_key] = value
        else:
            new_profile[key] = value

    return new_profile


def rename_measurements_keys(profile):

    key_mapping = get_measurements_mapping()

    # keys are CCHDO and values are Argovis
    new_profile = {}
    for key, value in profile.items():
        if key in key_mapping:
            new_key = key_mapping[key]
            new_profile[new_key] = value
        else:
            new_profile[key] = value

    return new_profile
