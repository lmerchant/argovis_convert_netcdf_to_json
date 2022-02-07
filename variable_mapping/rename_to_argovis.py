from variable_mapping.meta_param_mapping import get_cchdo_argovis_name_mapping_per_type
from variable_mapping.meta_param_mapping import get_cchdo_argovis_name_mapping


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


def rename_to_argovis(cchdo_names, data_type):

    # cchdo_argovis_name_mapping = get_cchdo_argovis_name_mapping_per_type(
    #     data_type)

    cchdo_argovis_name_mapping = get_cchdo_argovis_name_mapping()

    core_cchdo_names = list(cchdo_argovis_name_mapping.keys())

    # First find out which temperature and oxygen names are used

    # hierarchy if both ctd temperature names found
    # Choose ctd_temperature

    # hierarchy if both oxygen names found
    # Choose ctd_oxygen

    # print(cchdo_names)

    has_both_temp = has_both_temperatures(cchdo_names)
    has_both_oxy = has_both_oxygen(cchdo_names)

    if has_both_temp:

        cchdo_argovis_name_mapping.pop('ctd_temperature_68')

        if 'ctd_temperature_68_qc' in cchdo_argovis_name_mapping.keys():
            cchdo_argovis_name_mapping.pop('ctd_temperature_68_qc')

        core_cchdo_names = list(cchdo_argovis_name_mapping.keys())

    if has_both_oxy:

        print('has both oxy')

        cchdo_argovis_name_mapping.pop('ctd_oxygen_ml_l')

        if 'ctd_oxygen_ml_l_qc' in cchdo_argovis_name_mapping.keys():
            cchdo_argovis_name_mapping.pop('ctd_oxygen_ml_l_qc')

        core_cchdo_names = list(cchdo_argovis_name_mapping.keys())

    # Now rename variables

    name_mapping = {}

    for var in cchdo_names:
        if var in core_cchdo_names:
            name_mapping[var] = cchdo_argovis_name_mapping[var]
        elif '_qc' in var:
            non_qc_name = var.replace('_qc', '')
            #name_mapping[var] = f"{non_qc_name}_{data_type}_woceqc"
            name_mapping[var] = f"{non_qc_name}_woceqc"
        else:
            #name_mapping[var] = f"{var}_{data_type}"
            name_mapping[var] = var

    return name_mapping
