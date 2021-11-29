
def filter_argovis_mapping(nc_mappings, all_name_mapping):

    argovis_param_mapping = nc_mappings['argovis_param']

    # Take param mapping and filter it to only contain all_name_mapping
    # list of non-empty columnn for each station_cast
    # This is taking into account for dropped empty cols

    units = argovis_param_mapping['units']
    ref_scale = argovis_param_mapping['ref_scale']
    c_format = argovis_param_mapping['c_format']
    dtype = argovis_param_mapping['dtype']

    all_filtered_mappings = []

    for name_mapping in all_name_mapping:

        argovis_names = name_mapping['non_empty_cols']

        new_mapping = {}

        # ******************************
        # filter names to non empty cols
        # ******************************

        new_mapping['station_cast'] = name_mapping['station_cast']

        new_mapping['names'] = argovis_names

        new_mapping['units'] = {key: val for key,
                                val in units.items() if key in argovis_names}
        new_mapping['ref_scale'] = {
            key: val for key, val in ref_scale.items() if key in argovis_names}
        new_mapping['c_format'] = {
            key: val for key, val in c_format.items() if key in argovis_names}
        new_mapping['dtype'] = {key: val for key,
                                val in dtype.items() if key in argovis_names}

        all_filtered_mappings.append(new_mapping)

    return all_filtered_mappings


def get_goship_core_meas_var_names():
    return ['pressure', 'ctd_salinity', 'ctd_salinity_qc', 'bottle_salinity', 'bottle_salinity_qc', 'ctd_temperature', 'ctd_temperature_qc', 'ctd_temperature_68', 'ctd_temperature_68_qc']

    # return ['pressure', 'ctd_salinity', 'ctd_salinity_qc', 'bottle_salinity', 'bottle_salinity_qc', 'ctd_temperature', 'ctd_temperature_qc', 'ctd_temperature_68', 'ctd_temperature_68_qc', 'ctd_oxygen', 'ctd_oxygen_qc', 'ctd_oxygen_ml_l', 'ctd_oxygen_ml_l_qc']


def get_goship_core_meas_salinity_names():
    return ['ctd_salinity', 'ctd_salinity_qc', 'bottle_salinity', 'bottle_salinity_qc']


def get_goship_core_meas_temperature_names():
    return ['ctd_temperature', 'ctd_temperature_qc', 'ctd_temperature_68', 'ctd_temperature_68_qc']


def get_goship_core_meas_oxygen_names():
    return ['ctd_oxygen', 'ctd_oxygen_qc', 'ctd_oxygen_ml_l', 'ctd_oxygen_ml_l_qc']


def choose_core_temperature_from_hierarchy(col_names):

    # check which temperature to use if both exist
    is_ctd_temp = 'ctd_temperature' in col_names
    is_ctd_temp_68 = 'ctd_temperature_68' in col_names

    if (is_ctd_temp and is_ctd_temp_68) or is_ctd_temp:
        use_temperature = 'ctd_temperature'
    elif is_ctd_temp_68:
        use_temperature = 'ctd_temperature_68'
    else:
        use_temperature = ''

    return use_temperature


def choose_core_oxygen_from_hierarchy(col_names):

    # TODO
    # do i need this if oxygen not part of core meas values?

    # check which oxygen to use if both exist
    is_ctd_oxy = 'ctd_oxygen' in col_names
    is_ctd_oxy_ml_l = 'ctd_oxygen_ml_l' in col_names

    if (is_ctd_oxy and is_ctd_oxy_ml_l) or is_ctd_oxy:
        use_oxygen = 'ctd_oxygen'
    elif is_ctd_oxy_ml_l:
        use_oxygen = 'ctd_oxygen_ml_l'
    else:
        use_oxygen = ''

    return use_oxygen


def choose_core_salinity_from_hierarchy(col_names):

    # check which salinity to use if both exist
    is_ctd_sal = 'ctd_salinity' in col_names
    is_bottle_sal = 'bottle_salinity' in col_names

    if (is_ctd_sal and is_bottle_sal) or is_ctd_sal:
        use_salinity = 'ctd_salinity'
    elif is_bottle_sal:
        use_salinity = 'bottle_salinity'
    else:
        use_salinity = ''

    return use_salinity


def get_argovis_core_meas_values_per_type(data_type):

    # Add in bottle_salinity since will use this in
    # measurements to check if have ctd_salinity, and
    # if now, use bottle_salinity

    # Since didn't rename to argovis yet, use goship names
    # which means multiple temperature and oxygen names for ctd vars
    # standing for different ref scales

    return ['pres', f"temp_{data_type}", f"temp_{data_type}_qc", f"psal_{data_type}",  f"psal_{data_type}_qc", 'salinity_btl', 'salinity_btl_qc']


def get_goship_argovis_name_mapping_per_type(data_type):

    return {
        'pressure': 'pres',
        'pressure_qc': 'pres_qc',
        'ctd_salinity': f'psal_{data_type}',
        'ctd_salinity_qc': f'psal_{data_type}_qc',
        'ctd_temperature': f'temp_{data_type}',
        'ctd_temperature_qc': f'temp_{data_type}_qc',
        'ctd_temperature_68': f'temp_{data_type}',
        'ctd_temperature_68_qc': f'temp_{data_type}_qc',
        'ctd_oxygen': f'doxy_{data_type}',
        'ctd_oxygen_qc': f'doxy_{data_type}_qc',
        'ctd_oxygen_ml_l': f'doxy_{data_type}',
        'ctd_oxygen_ml_l_qc': f'doxy_{data_type}_qc',
        'bottle_salinity': f'salinity_{data_type}',
        'bottle_salinity_qc': f'salinity_{data_type}_qc',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def create_goship_argovis_mappings(
        nc_mappings, all_argovis_param_mapping_list, data_type):

    # nc_mappings gives properties of the xarray object of
    # all profiles at once

    # all_argovis_param_mapping_list gives the properties
    # of each profile

    goship_meta_mapping = nc_mappings['goship_meta']
    goship_param_mapping = nc_mappings['goship_param']

    argovis_meta_mapping = nc_mappings['argovis_meta']

    all_mapping_profiles = []

    # Loop over number of statin_cast mappings
    # Since meta and param have the same number,
    # loop over filtered  all_argovis_param_mapping_list

    # So for all profiles of xr object, it can include
    # parameters with empty cols, but in each
    # profile of  all_argovis_param_mapping_list, the
    # names are filtered to  remove empty cols of each profile

    for argovis_param_mapping in all_argovis_param_mapping_list:

        new_mapping = {}

        new_mapping['station_cast'] = argovis_param_mapping['station_cast']

        all_goship_meta_names = goship_meta_mapping['names']
        all_goship_param_names = goship_param_mapping['names']

        all_argovis_meta_names = argovis_meta_mapping['names']
        all_argovis_param_names = argovis_param_mapping['names']

        new_mapping['goshipMetaNames'] = all_goship_meta_names
        new_mapping['goshipParamNames'] = all_goship_param_names
        new_mapping['argovisMetaNames'] = all_argovis_meta_names
        new_mapping['argovisParamNames'] = all_argovis_param_names

        # TODO
        # List of non qc parameter names (or all keys?)
        # new_mapping['bgcMeasKeys'] = [
        #     name for name in all_argovis_param_names if '_qc' not in name]

        core_goship_argovis_name_mapping = get_goship_argovis_name_mapping_per_type(
            data_type)

        name_mapping = {}
        all_goship_names = [*all_goship_meta_names, *all_goship_param_names]
        all_argovis_names = [*all_argovis_meta_names, *all_argovis_param_names]

        for var in all_goship_names:
            try:
                argovis_name = core_goship_argovis_name_mapping[var]
                if argovis_name in all_argovis_names:
                    name_mapping[var] = argovis_name
                    continue
            except KeyError:
                pass

            if var.endswith('_qc'):
                argovis_name = f"{var}_{data_type}_qc"
                if argovis_name in all_argovis_names:
                    name_mapping[var] = argovis_name
            else:
                argovis_name = f"{var}_{data_type}"
                if argovis_name in all_argovis_names:
                    name_mapping[var] = argovis_name

        meta_name_mapping = {key: val for key,
                             val in name_mapping.items() if key in all_goship_meta_names}

        param_name_mapping = {key: val for key,
                              val in name_mapping.items() if key in all_goship_param_names}

        new_mapping['goshipArgovisMetaMapping'] = meta_name_mapping
        new_mapping['goshipArgovisParamMapping'] = param_name_mapping

        new_mapping['goshipReferenceScale'] = {
            **goship_meta_mapping['ref_scale'], **goship_param_mapping['ref_scale']}

        new_mapping['argovisReferenceScale'] = {
            **argovis_meta_mapping['ref_scale'], **argovis_param_mapping['ref_scale']}

        new_mapping['goshipUnits'] = {
            **goship_meta_mapping['units'], **goship_param_mapping['units']}

        new_mapping['argovisUnits'] = {
            **argovis_meta_mapping['units'], **argovis_param_mapping['units']}

        all_mapping_profiles.append(new_mapping)

    return all_mapping_profiles


def create_goship_mappings(nc_mappings, all_name_mapping):

    # nc_mappings gives properties of the xarray object of
    # all profiles at once

    # all_argovis_param_mapping_list gives the properties
    # of each profile

    goship_meta_mapping = nc_mappings['goship_meta']
    goship_param_mapping = nc_mappings['goship_param']

    all_goship_meta_names = goship_meta_mapping['names']
    all_goship_param_names = goship_param_mapping['names']

    params_units_changed = nc_mappings['goship_units_changed']
    params_ref_scale_changed = nc_mappings['goship_ref_scale_changed']

    all_mapping_profiles = []

    # For each profile, not all vars will have values
    for name_mapping in all_name_mapping:

        new_mapping = {}

        new_mapping['station_cast'] = name_mapping['station_cast']

        non_empty_cols = name_mapping['non_empty_cols']

        # So filter each goship mapping and keep only those in non_empty_cols
        # meta names don't change
        new_mapping['goshipMetaNames'] = all_goship_meta_names
        new_mapping['goshipParamNames'] = [
            col for col in all_goship_param_names if col in non_empty_cols]

        cur_param_ref_scale_mapping = goship_param_mapping['ref_scale']
        new_param_ref_scale_mapping = {
            key: val for key, val in cur_param_ref_scale_mapping.items() if key in non_empty_cols}

        new_mapping['goshipReferenceScale'] = {
            **goship_meta_mapping['ref_scale'], **new_param_ref_scale_mapping}

        cur_param_units_mapping = goship_param_mapping['units']
        new_param_units_mapping = {
            key: val for key, val in cur_param_units_mapping.items() if key in non_empty_cols}

        new_mapping['goshipUnits'] = {
            **goship_meta_mapping['units'], **new_param_units_mapping}

        new_param_ref_scale_mapping = {
            key: val for key, val in params_ref_scale_changed.items() if key in non_empty_cols}

        new_mapping['goshipConvertedReferenceScale'] = {
            **params_ref_scale_changed, **new_param_ref_scale_mapping}

        new_param_units_mapping = {
            key: val for key, val in params_units_changed.items() if key in non_empty_cols}

        new_mapping['goshipConvertedUnits'] = {
            **params_units_changed, **new_param_units_mapping}

        all_mapping_profiles.append(new_mapping)

    return all_mapping_profiles
