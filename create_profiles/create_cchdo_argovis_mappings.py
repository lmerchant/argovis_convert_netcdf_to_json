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


# def get_cchdo_argovis_name_mapping_per_type_for_meas(data_type):

#     # TODO
#     # Why am I doing this and using _qc and not _woceqc?

#     return {
#         # 'pressure': 'pres',
#         # 'pressure_qc': 'pres_qc',
#         'pressure': 'pressure',
#         # 'ctd_salinity': f'psal_{data_type}',
#         # 'ctd_salinity_qc': f'psal_{data_type}_qc',
#         # 'ctd_temperature': f'temp_{data_type}',
#         # 'ctd_temperature_qc': f'temp_{data_type}_qc',
#         # 'ctd_temperature_68': f'temp_{data_type}',
#         # 'ctd_temperature_68_qc': f'temp_{data_type}_qc',
#         # 'ctd_oxygen': f'doxy_{data_type}',
#         # 'ctd_oxygen_qc': f'doxy_{data_type}_qc',
#         # 'ctd_oxygen_ml_l': f'doxy_{data_type}',
#         # 'ctd_oxygen_ml_l_qc': f'doxy_{data_type}_qc',
#         # 'bottle_salinity': f'salinity_{data_type}',
#         # 'bottle_salinity_qc': f'salinity_{data_type}_qc',
#         # 'latitude': 'lat',
#         # 'longitude': 'lon'
#         'ctd_salinity': f'salinity_{data_type}',
#         'ctd_salinity_qc': f'salinity_{data_type}_qc',
#         'ctd_temperature': f'temperature_{data_type}',
#         'ctd_temperature_qc': f'temperature_{data_type}_qc',
#         'ctd_temperature_68': f'temperature_{data_type}',
#         'ctd_temperature_68_qc': f'temperature_{data_type}_qc',
#         'ctd_oxygen': f'oxygen_{data_type}',
#         'ctd_oxygen_qc': f'oxygen_{data_type}_qc',
#         'ctd_oxygen_ml_l': f'oxygen_{data_type}',
#         'ctd_oxygen_ml_l_qc': f'oxygen_{data_type}_qc',
#         'bottle_salinity': f'bottle_salinity_{data_type}',
#         'bottle_salinity_qc': f'bottle_salinity_{data_type}_qc',
#         'latitude': 'latitude',
#         'longitude': 'longitude'
#     }


# def create_cchdo_argovis_mappings(
#         nc_mappings, all_argovis_param_mapping_list, data_type):

#     # nc_mappings gives properties of the xarray object of
#     # all profiles at once

#     # all_argovis_param_mapping_list gives the properties
#     # of each profile

#     cchdo_meta_mapping = nc_mappings['cchdo_meta']
#     cchdo_param_mapping = nc_mappings['cchdo_param']

#     argovis_meta_mapping = nc_mappings['argovis_meta']

#     all_mapping_profiles = []

#     # Loop over number of station_cast mappings
#     # Since meta and param have the same number,
#     # loop over filtered  all_argovis_param_mapping_list

#     # So for all profiles of xr object, it can include
#     # parameters with empty cols, but in each
#     # profile of  all_argovis_param_mapping_list, the
#     # names are filtered to  remove empty cols of each profile

#     for argovis_param_mapping in all_argovis_param_mapping_list:

#         new_mapping = {}

#         new_mapping['station_cast'] = argovis_param_mapping['station_cast']

#         all_cchdo_meta_names = cchdo_meta_mapping['names']
#         all_cchdo_param_names = cchdo_param_mapping['names']

#         all_argovis_meta_names = argovis_meta_mapping['names']
#         all_argovis_param_names = argovis_param_mapping['names']

#         #new_mapping['cchdoMetaNames'] = all_cchdo_meta_names
#         new_mapping['cchdo_param_names'] = all_cchdo_param_names
#         #new_mapping['argovisMetaNames'] = all_argovis_meta_names
#         new_mapping['argovis_param_names'] = all_argovis_param_names

#         # TODO
#         # List of non qc parameter names (or all keys?)
#         # new_mapping['all_dataMeasKeys'] = [
#         #     name for name in all_argovis_param_names if '_qc' not in name]

#         core_cchdo_argovis_name_mapping = get_cchdo_argovis_name_mapping_per_type_for_meas(
#             data_type)

#         name_mapping = {}
#         all_cchdo_names = [*all_cchdo_meta_names, *all_cchdo_param_names]
#         all_argovis_names = [*all_argovis_meta_names, *all_argovis_param_names]

#         for var in all_cchdo_names:
#             try:
#                 argovis_name = core_cchdo_argovis_name_mapping[var]
#                 if argovis_name in all_argovis_names:
#                     name_mapping[var] = argovis_name
#                     continue
#             except KeyError:
#                 pass

#             if var.endswith('_qc'):
#                 argovis_name = f"{var}_{data_type}_qc"
#                 if argovis_name in all_argovis_names:
#                     name_mapping[var] = argovis_name
#             else:
#                 argovis_name = f"{var}_{data_type}"
#                 if argovis_name in all_argovis_names:
#                     name_mapping[var] = argovis_name

#         # meta_name_mapping = {key: val for key,
#         #                      val in name_mapping.items() if key in all_cchdo_meta_names}

#         param_name_mapping = {key: val for key,
#                               val in name_mapping.items() if key in all_cchdo_param_names}

#         #new_mapping['cchdoArgovisMetaMapping'] = meta_name_mapping
#         new_mapping['cchdo_argovis_param_mapping'] = param_name_mapping

#         new_mapping['cchdo_reference_scale'] = {
#             **cchdo_meta_mapping['ref_scale'], **cchdo_param_mapping['ref_scale']}

#         new_mapping['argovis_reference_scale'] = {
#             **argovis_meta_mapping['ref_scale'], **argovis_param_mapping['ref_scale']}

#         new_mapping['cchdo_units'] = {
#             **cchdo_meta_mapping['units'], **cchdo_param_mapping['units']}

#         new_mapping['argovis_units'] = {
#             **argovis_meta_mapping['units'], **argovis_param_mapping['units']}

#         all_mapping_profiles.append(new_mapping)

#     return all_mapping_profiles
