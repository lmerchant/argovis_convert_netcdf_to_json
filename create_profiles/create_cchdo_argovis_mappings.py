def create_cchdo_mappings(nc_mappings, all_name_mapping):

    # nc_mappings gives properties of the xarray object of
    # all profiles at once

    # all_argovis_param_mapping_list gives the properties
    # of each profile

    cchdo_meta_mapping = nc_mappings['cchdo_meta']
    cchdo_param_mapping = nc_mappings['cchdo_param']

    all_cchdo_meta_names = cchdo_meta_mapping['names']
    all_cchdo_param_names = cchdo_param_mapping['names']

    params_units_changed = nc_mappings['cchdo_units_changed']
    params_ref_scale_changed = nc_mappings['cchdo_ref_scale_changed']
    params_no_oxy_conversion = nc_mappings['cchdo_oxy_not_converted']

    all_mapping_profiles = []

    # For each profile, not all vars will have values
    for name_mapping in all_name_mapping:

        new_mapping = {}

        station_cast = name_mapping['station_cast']

        new_mapping['station_cast'] = name_mapping['station_cast']

        non_empty_cols = name_mapping['non_empty_cols']

        # So filter each cchdo mapping and keep only those in non_empty_cols
        # meta names don't change
        #new_mapping['cchdoMetaNames'] = all_cchdo_meta_names

        new_mapping['cchdo_param_names'] = [
            col for col in all_cchdo_param_names if col in non_empty_cols]

        cur_param_standard_names_mapping = cchdo_param_mapping['netcdf_name']
        new_param_standard_names_mapping = {
            key: val for key, val in cur_param_standard_names_mapping.items() if key in non_empty_cols}

        new_mapping['cchdo_standard_names'] = {
            **cchdo_meta_mapping['netcdf_name'], **new_param_standard_names_mapping}

        cur_param_ref_scale_mapping = cchdo_param_mapping['ref_scale']
        new_param_ref_scale_mapping = {
            key: val for key, val in cur_param_ref_scale_mapping.items() if key in non_empty_cols}

        new_mapping['cchdo_reference_scale'] = {
            **cchdo_meta_mapping['ref_scale'], **new_param_ref_scale_mapping}

        cur_param_units_mapping = cchdo_param_mapping['units']
        new_param_units_mapping = {
            key: val for key, val in cur_param_units_mapping.items() if key in non_empty_cols}

        new_mapping['cchdo_units'] = {
            **cchdo_meta_mapping['units'], **new_param_units_mapping}

        # Update station_casts with unconverted oxygen

        new_param_ref_scale_mapping = {
            key: val for key, val in params_ref_scale_changed.items() if key in non_empty_cols}

        new_mapping['cchdoConvertedReferenceScale'] = {
            **params_ref_scale_changed, **new_param_ref_scale_mapping}

        # params_no_oxy_conversion has key var and list of
        # station_cast without units attribute changing
        vars_no_oxy_conversion = list(params_no_oxy_conversion.keys())

        params_not_units_changed = []
        for var in vars_no_oxy_conversion:
            station_casts_no_oxy_conversion = params_no_oxy_conversion[var]
            if station_cast in station_casts_no_oxy_conversion:
                params_not_units_changed.append(var)

        # Remove params_not_units_changed from params_units_changed mapping
        for param in params_not_units_changed:
            params_units_changed.pop(param, None)

        new_param_units_mapping = {
            key: val for key, val in params_units_changed.items() if key in non_empty_cols}

        new_mapping['cchdoConvertedUnits'] = {
            **params_units_changed, **new_param_units_mapping}

        all_mapping_profiles.append(new_mapping)

    return all_mapping_profiles
