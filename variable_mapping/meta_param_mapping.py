

def get_source_independent_meta_names():

    # This is used when combining btl and ctd profiles which have already been renamed
    # So use the argovis names here

    names = ['expocode', 'station', 'cast', 'cchdo_cruise_id', 'woce_lines',
             'source', 'data_center', 'positioning_system',  '_id', 'country',
             'pi_name', 'cruise_url']

    return names


def get_measurements_mapping():
    return {
        'measurements': 'measurements',
        'measurementsSource': 'measurements_source',
        'measurementsSources': 'measurements_sources'
    }


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
        'cchdo_param_names': 'data_keys_source',
        'cchdo_param_names_btl': 'data_keys_source_btl',
        'cchdo_param_names_ctd': 'data_keys_source_ctd',
        'argovis_param_names': 'data_keys',
        'argovis_param_names_btl': 'data_keys_btl',
        'argovis_param_names_ctd': 'data_keys_ctd',
        'cchdo_argovis_param_mapping': 'data_keys_mapping',
        'cchdo_argovis_param_mapping_btl': 'data_keys_mapping_btl',
        'cchdo_argovis_param_mapping_ctd': 'data_keys_mapping_ctd',
        'cchdo_reference_scale': 'data_source_reference_scale',
        'cchdo_reference_scale_btl': 'data_source_reference_scale_btl',
        'cchdo_reference_scale_ctd': 'data_source_reference_scale_ctd',
        'cchdo_units': 'data_source_units',
        'cchdo_units_btl': 'data_source_units_btl',
        'cchdo_units_ctd': 'data_source_units_ctd',
        'argovis_reference_scale': 'data_reference_scale',
        'argovis_units': 'data_units',
        'cchdo_standard_names': 'data_source_standard_names',
        'cchdo_standard_names_btl': 'data_source_standard_names_btl',
        'cchdo_standard_names_ctd': 'data_source_standard_names_ctd'
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
        'ctd_temperature': f'temperature_{data_type}',
        'ctd_temperature_qc': f'temperature_{data_type}_woceqc',
        'ctd_temperature_68': f'temperature_{data_type}',
        'ctd_temperature_68_qc': f'temperature_{data_type}_woceqc',
        'ctd_oxygen': f'doxy_{data_type}',
        'ctd_oxygen_qc': f'doxy_{data_type}_woceqc',
        'ctd_oxygen_ml_l': f'doxy_{data_type}',
        'ctd_oxygen_ml_l_qc': f'doxy_{data_type}_woceqc',
        'bottle_salinity': f'salinity_{data_type}',
        'bottle_salinity_qc': f'salinity_{data_type}_woceqc'
    }


def get_cchdo_argovis_name_mapping():

    return {
        'pressure': 'pres',
        'pressure_qc': 'pres_woceqc',
        'ctd_salinity': f'psal',
        'ctd_salinity_qc': f'psal_woceqc',
        'ctd_temperature': f'temperature',
        'ctd_temperature_qc': f'temperature_woceqc',
        'ctd_temperature_68': f'temperature',
        'ctd_temperature_68_qc': f'temperature_woceqc',
        'ctd_oxygen': f'doxy',
        'ctd_oxygen_qc': f'doxy_woceqc',
        'ctd_oxygen_ml_l': f'doxy',
        'ctd_oxygen_ml_l_qc': f'doxy_woceqc',
        'bottle_salinity': f'salinity',
        'bottle_salinity_qc': f'salinity_woceqc'
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
