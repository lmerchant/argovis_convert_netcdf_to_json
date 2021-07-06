import copy
from os import rename


import get_variable_mappings as gvm

# Rename objects


def rename_mapping_to_argovis_map(mapping):

    # Get mapping including qc
    core_values_mapping = gvm.get_goship_argovis_name_mapping()

    goship_core_values = [core for core in core_values_mapping]

    new_mapping = {}

    for name, val in mapping.items():

        if name in goship_core_values:
            new_name = core_values_mapping[name]
            new_mapping[new_name] = val

        else:
            new_mapping[name] = val

    return new_mapping


def rename_mapping_to_argovis(mapping):

    goship_names_list = mapping['names']
    goship_units_mapping = mapping['units']
    goship_ref_scale_mapping = mapping['ref_scale']
    goship_c_format_mapping = mapping['c_format']
    goship_dtype_mapping = mapping['dtype']

    goship_argovis_name_mapping = gvm.get_goship_argovis_name_mapping()
    core_goship_names = goship_argovis_name_mapping.keys()

    renamed_names_list = [goship_argovis_name_mapping[name]
                          if name in core_goship_names else name for name in goship_names_list]

    renamed_units_mapping = rename_mapping_to_argovis_map(goship_units_mapping)

    renamed_ref_scale_mapping = rename_mapping_to_argovis_map(
        goship_ref_scale_mapping)

    renamed_c_format_mapping = rename_mapping_to_argovis_map(
        goship_c_format_mapping)

    renamed_dtype_mapping = rename_mapping_to_argovis_map(
        goship_dtype_mapping)

    argovis_mapping = {}
    argovis_mapping['names'] = renamed_names_list
    argovis_mapping['units'] = renamed_units_mapping
    argovis_mapping['ref_scale'] = renamed_ref_scale_mapping
    argovis_mapping['c_format'] = renamed_c_format_mapping
    argovis_mapping['dtype'] = renamed_dtype_mapping

    return argovis_mapping


# third
def rename_goship_name_list_param(goship_names_list, type):

    goship_argovis_core_mapping = gvm.create_goship_argovis_core_values_mapping(
        type)

    core_goship_names = goship_argovis_core_mapping.keys()

    new_names_list = []

    for name in goship_names_list:

        if name in core_goship_names:
            new_name = goship_argovis_core_mapping[name]

        elif name.endswith('_qc'):
            non_qc_name = name.replace('_qc', '')
            new_name = f"{non_qc_name}_{type}_qc"

        else:
            new_name = f"{name}_{type}"

        new_names_list.append(new_name)

    return new_names_list


# second
def rename_mapping_to_argovis_param(mapping, type):

    goship_names_list = mapping['names']
    goship_units_mapping = mapping['units']
    goship_ref_scale_mapping = mapping['ref_scale']
    goship_c_format_mapping = mapping['c_format']
    goship_dtype_mapping = mapping['dtype']

    renamed_names_list = rename_goship_name_list_param(goship_names_list, type)

    argovis_mapping = {}
    argovis_mapping['names'] = renamed_names_list

    renamed_units_mapping = rename_key_not_meta_argovis(
        goship_units_mapping, type)

    renamed_ref_scale_mapping = rename_key_not_meta_argovis(
        goship_ref_scale_mapping, type)

    renamed_c_format_mapping = rename_key_not_meta_argovis(
        goship_c_format_mapping, type)

    renamed_dtype_mapping = rename_key_not_meta_argovis(
        goship_dtype_mapping, type)

    argovis_mapping['units'] = renamed_units_mapping
    argovis_mapping['ref_scale'] = renamed_ref_scale_mapping
    argovis_mapping['c_format'] = renamed_c_format_mapping
    argovis_mapping['dtype'] = renamed_dtype_mapping

    return argovis_mapping


def rename_meta_to_argovis(cols):

    core_values_mapping = gvm.get_argovis_meta_mapping()

    core_values = [core for core in core_values_mapping]

    name_mapping = {}

    for name in cols:

        if name in core_values:
            new_name = core_values_mapping[name]

        else:
            new_name = name

        name_mapping[name] = new_name

    return name_mapping


def rename_argovis_meta(obj):

    core_values_mapping = gvm.get_argovis_meta_mapping()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key, val in obj.items():

        if key in core_values:
            new_key = core_values_mapping[key]

        else:
            new_key = key

        new_obj[new_key] = val

    return new_obj


def rename_goship_on_key_not_meta(obj, type):

    if type == 'btl':

        core_values_mapping = gvm.get_goship_argovis_core_values_mapping('btl')

        core_values = [core for core in core_values_mapping]

        new_obj = {}

        for key, val in obj.items():

            if key in core_values:
                new_key = core_values_mapping[key]

            else:
                new_key = key

            new_obj[new_key] = val

    if type == 'ctd':

        core_values_mapping = gvm.get_goship_argovis_core_values_mapping('ctd')

        core_values = [core for core in core_values_mapping]

        new_obj = {}

        for key, val in obj.items():

            if key in core_values:
                new_key = core_values_mapping[key]

            else:
                new_key = key

            new_obj[new_key] = val

    return new_obj


def rename_btl_by_key_meta(obj):

    new_obj = {}

    for key, val in obj.items():

        new_key = f"{key}_btl"

        new_obj[new_key] = val

    return new_obj


def rename_cols_meta_no_type(cols):

    core_values_mapping = gvm.get_goship_argovis_name_mapping()

    core_values = core_values_mapping.keys()

    col_mapping = {}

    for name in cols:

        if name in core_values:
            mapped_name = core_values_mapping[name]

        else:
            mapped_name = name

        col_mapping[name] = mapped_name

    return col_mapping


def rename_cols_not_meta(cols, type):

    # names without '_qc'
    core_values_mapping = gvm.get_goship_argovis_core_values_mapping(type)

    core_values = [core for core in core_values_mapping]

    col_mapping = {}

    for name in cols:

        if name.endswith('_qc'):
            non_qc_name = name.replace('_qc', '')
            if non_qc_name in core_values:
                new_name = f"{core_values_mapping[non_qc_name]}_qc"
            else:
                new_name = f"{non_qc_name}_{type}_qc"

        elif name in core_values:
            new_name = core_values_mapping[name]

        else:
            new_name = f"{name}_{type}"

        col_mapping[name] = new_name

    return col_mapping


def rename_key_not_meta(obj, type):

    if type == 'btl':

        core_values_mapping = gvm.get_goship_argovis_core_values_mapping('btl')

        core_values = [core for core in core_values_mapping]

        new_obj = {}

        for key, val in obj.items():

            if '_qc' in key:
                check_core = key.replace('_qc', '')
                if check_core in core_values:
                    new_key = f"{core_values_mapping[check_core]}_qc"

            elif key in core_values:
                new_key = core_values_mapping[key]

            elif '_qc' in key:
                key_wo_qc = key.replace('_qc', '')
                new_key = f"{key_wo_qc}_btl_qc"
            else:
                new_key = f"{key}_btl"

            new_obj[new_key] = val

    if type == 'ctd':

        core_values_mapping = gvm.get_goship_argovis_core_values_mapping('ctd')

        core_values = [core for core in core_values_mapping]

        new_obj = {}

        for key, val in obj.items():

            if '_qc' in key:
                check_core = key.replace('_qc', '')
                if check_core in core_values:
                    new_key = f"{core_values_mapping[check_core]}_qc"

            elif key in core_values:
                new_key = core_values_mapping[key]

            elif '_qc' in key:
                key_wo_qc = key.replace('_qc', '')
                new_key = f"{key_wo_qc}_ctd_qc"
            else:
                new_key = f"{key}_ctd"

            new_obj[new_key] = val

    return new_obj


def rename_key_not_meta_argovis(obj, type):

    core_values_mapping = gvm.get_goship_argovis_name_mapping_per_type(type)
    core_values = core_values_mapping.keys()

    new_obj = {}

    for name, val in obj.items():

        if name in core_values:
            new_name = core_values_mapping[name]

        elif '_qc' in name:
            no_qc = name.replace('_qc', '')
            new_name = f"{no_qc}_{type}_qc"

        else:
            new_name = f"{name}_{type}"

        new_obj[new_name] = val

    return new_obj


def rename_key_not_meta_argovis_measurements(obj):

    core_values_mapping = gvm.get_goship_argovis_measurements_mapping()

    # core_values_mapping = gvm.get_goship_argovis_core_values_mapping(type)
    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key, val in obj.items():

        if key in core_values:
            new_key = core_values_mapping[key]

        else:
            new_key = key

        new_obj[new_key] = val

    return new_obj


def rename_mapping_argovis_param(mapping, type):

    core_values_mapping = gvm.get_goship_argovis_core_values_mapping(type)
    core_values = [core for core in core_values_mapping]

    new_mapping = {}

    for key, val in mapping.items():
        if '_qc' in key:
            check_core = key.replace('_qc', '')
            if check_core in core_values:
                new_key = f"{core_values_mapping[check_core]}_qc"

        elif key in core_values:
            new_key = core_values_mapping[key]

        else:
            new_key = val

        new_mapping[new_key] = val

    return new_mapping


def create_renamed_list_of_objs_argovis_measurements(cur_list):

    # Rename without extension

    new_list = []

    new_obj = {}

    for obj in cur_list:

        new_obj = rename_key_not_meta_argovis_measurements(obj)
        # new_obj = rename_key_not_meta_argovis(obj, type)

        new_list.append(new_obj)

    return new_list


def create_renamed_list_of_objs_argovis(cur_list, type):

    # Common names are pres, temp, psal, and doxy will have suffix _ctd and _ctd_qc
    # All ctd vars will have suffix _ctd and _ctd_qc
    # All bot vars will have suffx _btl and _btl_qc
    # Special case is bottle_salinity to psal_btl

    # Creating list, don't modify key since already renamed as element  of list

    if type == 'btl':

        new_list = []

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta_argovis(obj, type)

            new_list.append(new_obj)

    if type == 'ctd':

        new_list = []

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta_argovis(obj, type)

            new_list.append(new_obj)

    return new_list


def create_renamed_list_of_objs(cur_list, type):

    new_list = []

    if type == 'btl':

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta(obj, 'btl')

            new_list.append(new_obj)

    if type == 'ctd':

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta(obj, 'ctd')

            new_list.append(new_obj)

    return new_list


def rename_profile_to_argovis(profile):

    station_cast = profile['station_cast']
    profile_dict = profile['profile_dict']

    type = profile_dict['type']

    # TODO
    # consolidate this

    if type == 'btl':

        # station_cast = profile['station_cast']
        # profile_dict = profile['profile_dict']

        meta = profile_dict['meta']
        renamed_meta = rename_argovis_meta(meta)

        bgc_list = profile_dict['bgc_meas']

        renamed_bgc_list = create_renamed_list_of_objs(bgc_list, 'btl')

        measurements_list = profile_dict['measurements']

        renamed_measurements_list = create_renamed_list_of_objs_argovis_measurements(
            measurements_list)

        measurements_source = profile_dict['measurements_source']
        goship_names = profile_dict['goship_names']
        goship_ref_scale = profile_dict['goship_ref_scale']
        goship_units = profile_dict['goship_units']

        goship_argovis_name = gvm.create_goship_argovis_core_values_mapping(
            goship_names, type)

        argovis_ref_scale = gvm.get_argovis_ref_scale_mapping(
            goship_names, type)

        goship_argovis_unit = gvm.get_goship_argovis_unit_mapping()

    if type == 'ctd':

        # station_cast = profile['station_cast']
        # profile_dict = profile['profile_dict']

        meta = profile_dict['meta']

        renamed_meta = rename_argovis_meta(meta)

        bgc_list = profile_dict['bgc_meas']
        renamed_bgc_list = create_renamed_list_of_objs(bgc_list, type)

        measurements_list = profile_dict['measurements']

        renamed_measurements_list = create_renamed_list_of_objs_argovis_measurements(
            measurements_list)

        measurements_source = profile_dict['measurements_source']

        goship_names = profile_dict['goship_names']
        goship_ref_scale = profile_dict['goship_ref_scale']
        goship_units = profile_dict['goship_units']

        goship_argovis_name = gvm.create_goship_argovis_core_values_mapping(
            goship_names, type)

        argovis_ref_scale = gvm.get_argovis_ref_scale_mapping(
            goship_names, type)

        goship_argovis_unit = gvm.get_goship_argovis_unit_mapping()

    renamed_profile_dict = {}
    renamed_profile_dict['type'] = type
    renamed_profile_dict['stationCast'] = station_cast
    renamed_profile_dict['meta'] = renamed_meta
    renamed_profile_dict['measurements'] = renamed_measurements_list
    renamed_profile_dict['measurementsSource'] = measurements_source
    renamed_profile_dict['bgcMeas'] = renamed_bgc_list
    renamed_profile_dict['goshipUnits'] = goship_units
    renamed_profile_dict['goshipArgovisNameMapping'] = goship_argovis_name
    renamed_profile_dict['goshipReferenceScale'] = goship_ref_scale
    renamed_profile_dict['argovisReferenceScale'] = argovis_ref_scale
    renamed_profile_dict['goshipArgovisUnitsMapping'] = goship_argovis_unit

    output_profile = {}
    output_profile['profile_dict'] = renamed_profile_dict
    output_profile['station_cast'] = station_cast

    return output_profile
