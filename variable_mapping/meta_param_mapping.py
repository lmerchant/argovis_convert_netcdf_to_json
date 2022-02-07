

def get_source_independent_meta_names():

    # This is used when combining btl and ctd profiles which have already been renamed
    # So use the argovis names here

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
# Why use this when could use get_program_argovis_mapping

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

#     key_mapping = get_program_argovis_mapping()

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

    # keys are names used in this program
    # values are argovis names
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


def get_program_argovis_mapping():

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
        'argovisUnits': 'data_units',
        'cchdoStandardNames': 'data_source_standard_names',
        'cchdoStandardNamesBtl': 'data_source_standard_names_btl',
        'cchdoStandardNamesCtd': 'data_source_standard_names_ctd'
    }


def get_combined_mappings_keys():
    # Get mapping keys that can be combined
    # such as names. Even though the btl file and ctd file may have same names,
    # want to just have one set of names
    # But for the data units, keep separate since no guarantee the units will be the same
    # use argovis names

    return [
        'data_keys_source', 'data_keys', 'data_keys_mapping', 'data_reference_scale', 'data_units', 'data_source_standard_names'
    ]


def get_cchdo_argovis_name_mapping_per_type(data_type):

    return {
        'pressure': 'pres',
        'pressure_qc': 'pres_woceqc',
        'ctd_salinity': f'psal_{data_type}',
        'ctd_salinity_qc': f'psal_{data_type}_woceqc',
        'ctd_temperature': f'temp_{data_type}',
        'ctd_temperature_qc': f'temp_{data_type}_woceqc',
        'ctd_temperature_68': f'temp_{data_type}',
        'ctd_temperature_68_qc': f'temp_{data_type}_woceqc',
        'ctd_oxygen': f'doxy_{data_type}',
        'ctd_oxygen_qc': f'doxy_{data_type}_woceqc',
        'ctd_oxygen_ml_l': f'doxy_{data_type}',
        'ctd_oxygen_ml_l_qc': f'doxy_{data_type}_woceqc',
        'bottle_salinity': f'salinity_{data_type}',
        'bottle_salinity_qc': f'salinity_{data_type}_woceqc',
        'latitude': 'latitude',
        'longitude': 'longitude'
    }


def get_cchdo_argovis_name_mapping():

    return {
        'pressure': 'pres',
        'pressure_qc': 'pres_woceqc',
        'ctd_salinity': f'psal',
        'ctd_salinity_qc': f'psal_woceqc',
        'ctd_temperature': f'temp',
        'ctd_temperature_qc': f'temp_woceqc',
        'ctd_temperature_68': f'temp',
        'ctd_temperature_68_qc': f'temp_woceqc',
        'ctd_oxygen': f'doxy',
        'ctd_oxygen_qc': f'doxy_woceqc',
        'ctd_oxygen_ml_l': f'doxy',
        'ctd_oxygen_ml_l_qc': f'doxy_woceqc',
        'bottle_salinity': f'salinity',
        'bottle_salinity_qc': f'salinity_woceqc',
        'latitude': 'latitude',
        'longitude': 'longitude'
    }


def get_core_profile_keys_mapping():

    return {'data': 'data'}


def rename_mappings_keys(mappings):

    # keys are CCHDO and values are Argovis
    key_mapping = get_program_argovis_mapping()

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
