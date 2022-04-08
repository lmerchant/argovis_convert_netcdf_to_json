from variable_naming.meta_param_mapping import get_cchdo_argovis_name_mapping_per_type
from variable_naming.meta_param_mapping import get_cchdo_argovis_name_mapping
from variable_naming.meta_param_mapping import get_parameters_no_data_type


def rename_mapping_w_data_type(argovis_col_names_mapping_wo_data_type, data_type):
    # Add a suffix of the data_type

    # put suffix before _woceqc

    argovis_col_names_mapping = {}
    for key, val in argovis_col_names_mapping_wo_data_type.items():

        if '_woceqc' in val:
            non_qc = val.replace('_woceqc', '')
            new_name = f"{non_qc}_{data_type}_woceqc"
        else:
            new_name = f"{val}_{data_type}"

        argovis_col_names_mapping[key] = new_name

    return argovis_col_names_mapping


def rename_with_data_type(params, data_type):

    # Add a suffix of the data_type

    # put suffix before _woceqc

    new_params = []

    params_w_no_data_type = get_parameters_no_data_type()

    for param in params:

        if param in params_w_no_data_type:
            new_params.append(param)
            continue

        if '_woceqc' in param:
            non_qc = param.replace('_woceqc', '')
            new_name = f"{non_qc}_{data_type}_woceqc"
        else:
            new_name = f"{param}_{data_type}"

        new_params.append(new_name)

    return new_params


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


def rename_to_argovis_mapping(cchdo_names):

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

        # Only want to rename one

        print('has two ctd temperatures')

        exit(1)

        # TODO,
        # Find better way so not dependent on these name conversions

        cchdo_argovis_name_mapping['ctd_temperature_68'] = 'ctd_temperature_68'

        if 'ctd_temperature_68_qc' in cchdo_argovis_name_mapping.keys():
            cchdo_argovis_name_mapping['ctd_temperature_68_qc'] = 'ctd_temperature_68_qc'

        core_cchdo_names.pop('ctd_temperature_68')

    if has_both_oxy:

        print('has two ctd oxygens')

        exit(1)

        # Only want to rename one

        cchdo_argovis_name_mapping['ctd_oxygen_ml_l'] = 'ctd_oxygen_ml_l'

        if 'ctd_oxygen_ml_l_qc' in cchdo_argovis_name_mapping.keys():
            cchdo_argovis_name_mapping['ctd_oxygen_ml_l_qc'] = 'ctd_oxygen_ml_l_qc'

        core_cchdo_names.pop('ctd_oxygen_ml_l')

    # Now rename variables to ArgoVis names

    name_mapping = {}

    for var in cchdo_names:
        if var in core_cchdo_names:
            new_name = cchdo_argovis_name_mapping[var]
        elif '_qc' in var:
            new_name = var.replace('_qc', '_woceqc')
        else:
            new_name = var

        name_mapping[var] = new_name

    return name_mapping
