import logging

#from variable_naming.meta_param_mapping import get_cchdo_argovis_name_mapping_per_type
from variable_naming.meta_param_mapping import get_cchdo_argovis_name_mapping
from variable_naming.meta_param_mapping import get_parameters_no_data_type


def rename_mapping_w_data_type(argovis_col_names_mapping_wo_data_type, data_type):
    # Add a suffix of the data_type

    params_with_no_data_type = get_parameters_no_data_type()

    # put suffix before _woceqc

    argovis_col_names_mapping = {}
    for key, val in argovis_col_names_mapping_wo_data_type.items():

        if key in params_with_no_data_type:
            argovis_col_names_mapping[key] = val
            continue

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


def has_two_ctd_temperatures(names):

    has_ctd_temperature = any(
        [True for name in names if name == 'ctd_temperature'])
    has_ctd_temperature_68 = any(
        [True for name in names if name == 'ctd_temperature_68'])

    if has_ctd_temperature and has_ctd_temperature_68:
        return True
    else:
        return False


def has_two_ctd_oxygens(names):

    has_ctd_oxygen = any(
        [True for name in names if name == 'ctd_oxygen'])
    has_ctd_oxygen_ml_l = any(
        [True for name in names if name == 'ctd_oxygen_ml_l'])

    if has_ctd_oxygen and has_ctd_oxygen_ml_l:
        return True
    else:
        return False


def rename_to_argovis_mapping(cchdo_names):

    cchdo_argovis_name_mapping = get_cchdo_argovis_name_mapping()

    core_cchdo_names = list(cchdo_argovis_name_mapping.keys())

    # logging.info('core cchdo names')
    # logging.info(core_cchdo_names)

    # First find out which temperature and oxygen names are used

    # hierarchy if both ctd temperature names found
    # Choose ctd_temperature

    # hierarchy if both oxygen names found
    # Choose ctd_oxygen

    has_both_temp = has_two_ctd_temperatures(cchdo_names)
    has_both_oxy = has_two_ctd_oxygens(cchdo_names)

    if has_both_temp:

        # Only want to rename one
        logging.info('has two ctd temperatures in xarray, param names are')
        # logging.info(cchdo_names)

        if 'ctd_temperature_68' in cchdo_argovis_name_mapping.keys():
            cchdo_argovis_name_mapping['ctd_temperature_68'] = 'ctd_temperature_68'
            cchdo_argovis_name_mapping['ctd_temperature_68_qc'] = 'ctd_temperature_68_qc'

    if has_both_oxy:

        logging.info('has two ctd oxygens in xarray, param names are')
        # logging.info(cchdo_names)

        # Only want to rename one
        if 'ctd_oxygen_ml_l' in cchdo_argovis_name_mapping.keys():
            cchdo_argovis_name_mapping['ctd_oxygen_ml_l'] = 'ctd_oxygen_ml_l'
            cchdo_argovis_name_mapping['ctd_oxygen_ml_l_qc'] = 'ctd_oxygen_ml_l_qc'

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
