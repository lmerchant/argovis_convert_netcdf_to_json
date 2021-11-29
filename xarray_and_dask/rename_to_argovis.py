# TODO
# move this out of xarray and dask folder since not applying
# it till end of program


def has_both_temperatures(names):

    core_temperature_names = set(['ctd_temperature', 'ctd_temperature_68'])
    temperature_names = set(names).intersection(
        core_temperature_names)

    if len(temperature_names) == 2:
        return True
    else:
        return False


def has_both_oxygen(names):

    core_oxygen_names = set(['ctd_oxygen', 'ctd_oxygen_ml_l'])
    oxygen_names = set(names).intersection(
        core_oxygen_names)

    if len(oxygen_names) == 2:
        return True
    else:
        return False


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
        'latitude': 'latitude',
        'longitude': 'longitude'
    }


def rename_to_argovis(goship_names, data_type):

    print('inside rename_to_argovis')
    print('goship names')
    print(goship_names)

    goship_argovis_name_mapping = get_goship_argovis_name_mapping_per_type(
        data_type)

    print('goship_argovis_name_mapping')
    print(goship_argovis_name_mapping)

    core_goship_names = goship_argovis_name_mapping.keys()

    has_both_temp = has_both_temperatures(goship_names)
    has_both_oxy = has_both_oxygen(goship_names)

    if has_both_temp:

        goship_argovis_name_mapping.pop('ctd_temperature_68')

        if 'ctd_temperature_68_qc' in goship_argovis_name_mapping.keys():
            goship_argovis_name_mapping.pop('ctd_temperature_68_qc')

        core_goship_names = goship_argovis_name_mapping.keys()

    if has_both_oxy:

        goship_argovis_name_mapping.pop('ctd_oxygen_ml_l')

        if 'ctd_oxygen_ml_l_qc' in goship_argovis_name_mapping.keys():
            goship_argovis_name_mapping.pop('ctd_oxygen_ml_l_qc')

        core_goship_names = goship_argovis_name_mapping.keys()

    name_mapping = {}

    for var in goship_names:
        if var in core_goship_names:
            name_mapping[var] = goship_argovis_name_mapping[var]
        elif '_qc' in var:
            non_qc_name = var.replace('_qc', '')
            name_mapping[var] = f"{non_qc_name}_{data_type}_qc"
        else:
            name_mapping[var] = f"{var}_{data_type}"

    return name_mapping


# def rename_to_argovis_orig(nc, type):

#     goship_argovis_name_mapping = get_goship_argovis_name_mapping_per_type(
#         type)

#     core_goship_names = goship_argovis_name_mapping.keys()

#     goship_names = nc.keys()

#     has_both_temp = has_both_temperatures(goship_names)
#     has_both_oxy = has_both_oxygen(goship_names)

#     new_goship_argovis_name_mapping = goship_argovis_name_mapping

#     if has_both_temp:

#         new_goship_argovis_name_mapping.pop('ctd_temperature_68')

#         if 'ctd_temperature_68_qc' in new_goship_argovis_name_mapping.keys():
#             new_goship_argovis_name_mapping.pop('ctd_temperature_68_qc')

#         core_goship_names = new_goship_argovis_name_mapping.keys()

#     if has_both_oxy:

#         new_goship_argovis_name_mapping.pop('ctd_oxygen_ml_l')

#         if 'ctd_oxygen_ml_l_qc' in new_goship_argovis_name_mapping.keys():
#             new_goship_argovis_name_mapping.pop('ctd_oxygen_ml_l_qc')

#         core_goship_names = new_goship_argovis_name_mapping.keys()

#     name_mapping = {}

#     for var in nc.coords:
#         if var in core_goship_names:
#             name_mapping[var] = new_goship_argovis_name_mapping[var]

#     for var in nc.keys():
#         if var in core_goship_names:
#             name_mapping[var] = new_goship_argovis_name_mapping[var]
#         elif '_qc' in var:
#             non_qc_name = var.replace('_qc', '')
#             name_mapping[var] = f"{non_qc_name}_{type}_qc"
#         else:
#             name_mapping[var] = f"{var}_{type}"

#     nc = nc.rename(name_mapping)

#     return nc
