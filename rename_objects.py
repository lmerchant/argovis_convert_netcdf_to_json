import get_variable_mappings as gvm

# Rename objects


def rename_keys_to_argovis(obj, core_values_mapping):

    # Get mapping including qc
    goship_core_values = [core for core in core_values_mapping]

    new_obj = {}

    for name, val in obj.items():

        if name in goship_core_values:
            new_name = core_values_mapping[name]
            new_obj[new_name] = val

        else:
            new_obj[name] = val

    return new_obj


def convert_argovis_meta_to_goship_names(argovis_meta_keys):

    mapping = gvm.get_argovis_goship_name_mapping_meta()

    goship_meta_keys = []

    for key in argovis_meta_keys:
        if key in mapping.keys():
            new_key = mapping[key]
        else:
            new_key = key

        goship_meta_keys.append(new_key)

    return goship_meta_keys


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


def rename_to_argovis(nc, type):

    goship_argovis_name_mapping = gvm.get_goship_argovis_name_mapping_per_type(
        type)

    core_goship_names = goship_argovis_name_mapping.keys()

    goship_names = nc.keys()

    has_both_temp = has_both_temperatures(goship_names)
    has_both_oxy = has_both_oxygen(goship_names)

    new_goship_argovis_name_mapping = goship_argovis_name_mapping

    if has_both_temp:

        new_goship_argovis_name_mapping.pop('ctd_temperature_68')

        if 'ctd_temperature_68_qc' in new_goship_argovis_name_mapping.keys():
            new_goship_argovis_name_mapping.pop('ctd_temperature_68_qc')

        core_goship_names = new_goship_argovis_name_mapping.keys()

    if has_both_oxy:

        new_goship_argovis_name_mapping.pop('ctd_oxygen_ml_l')

        if 'ctd_oxygen_ml_l_qc' in new_goship_argovis_name_mapping.keys():
            new_goship_argovis_name_mapping.pop('ctd_oxygen_ml_l_qc')

        core_goship_names = new_goship_argovis_name_mapping.keys()

    name_mapping = {}

    for var in nc.coords:
        if var in core_goship_names:
            name_mapping[var] = new_goship_argovis_name_mapping[var]

    for var in nc.keys():
        if var in core_goship_names:
            name_mapping[var] = new_goship_argovis_name_mapping[var]
        elif '_qc' in var:
            non_qc_name = var.replace('_qc', '')
            name_mapping[var] = f"{non_qc_name}_{type}_qc"
        else:
            name_mapping[var] = f"{var}_{type}"

    nc = nc.rename(name_mapping)

    return nc


def rename_goship_name_list_by_type(goship_names_list, type):

    goship_argovis_core_mapping = gvm.get_goship_argovis_core_values_mapping(
        type)

    goship_names_to_map = [key for key in goship_argovis_core_mapping.keys()
                           if key in goship_names_list]

    new_names_list = []

    for name in goship_names_list:

        if name in goship_names_to_map:
            new_name = goship_argovis_core_mapping[name]

        elif name.endswith('_qc'):
            non_qc_name = name.replace('_qc', '')
            new_name = f"{non_qc_name}_{type}_qc"

        else:
            new_name = f"{name}_{type}"

        new_names_list.append(new_name)

    return new_names_list


def rename_keys_to_argovis_by_type(obj, core_values_mapping, type):

    # Get mapping including qc
    core_values_mapping = gvm.get_goship_argovis_name_mapping_per_type(type)

    core_values = [key for key in core_values_mapping.keys()
                   if key in obj.keys()]

    new_obj = {}

    for name, val in obj.items():

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

        new_obj[new_name] = val

    return new_obj


def rename_goship_on_key_not_meta(obj, type):

    core_values_mapping = gvm.get_goship_argovis_core_values_mapping(type)

    core_values = [
        core for core in core_values_mapping if core in obj.keys()]

    new_obj = {}

    for key, val in obj.items():

        if key in core_values:
            new_key = core_values_mapping[key]

        else:
            new_key = key

        new_obj[new_key] = val

    # if type == 'ctd':

    #     core_values_mapping = gvm.get_goship_argovis_core_values_mapping('ctd')

    #     core_values = [
    #         core for core in core_values_mapping if core in obj.keys()]

    #     new_obj = {}

    #     for key, val in obj.items():

    #         if key in core_values:
    #             new_key = core_values_mapping[key]

    #         else:
    #             new_key = key

    #         new_obj[new_key] = val

    return new_obj


def rename_btl_by_key_meta(obj):

    new_obj = {}

    for key, val in obj.items():

        new_key = f"{key}_btl"

        new_obj[new_key] = val

    return new_obj


def rename_key_not_meta_argovis_measurements(obj):

    core_values_mapping = gvm.get_goship_argovis_measurements_mapping()

    core_values = [core for core in core_values_mapping if core in obj.keys()]

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


# def rename_vars_to_argovis(mapping):

#     goship_names_list = mapping['names']
#     goship_units_mapping = mapping['units']
#     goship_ref_scale_mapping = mapping['ref_scale']
#     goship_c_format_mapping = mapping['c_format']
#     goship_dtype_mapping = mapping['dtype']

#     goship_argovis_name_mapping = gvm.get_goship_argovis_name_mapping()

#     # What if both ctd temps or oxygens in mapping argument,
#     # Use primary first

#     has_both_temp = has_both_temperatures(goship_names_list)
#     has_both_oxygen = has_both_oxygens(goship_names_list)

#     new_goship_argovis_name_mapping = goship_argovis_name_mapping

#     if has_both_temp:
#         new_goship_argovis_name_mapping = {key: val for key, val in goship_argovis_name_mapping.items(
#         ) if key != 'ctd_temperature_68' or key != 'ctd_temperature_68_qc'}

#     if has_both_oxygen:
#         new_goship_argovis_name_mapping = {key: val for key, val in new_goship_argovis_name_mapping.items(
#         ) if key != 'ctd_oxygen_ml_l' or key != 'ctd_oxygen_ml_l_qc'}

#     goship_names_to_map = [
#         key for key in new_goship_argovis_name_mapping.keys() if key in goship_names_list]

#     renamed_names_list = [new_goship_argovis_name_mapping[name]
#                           if name in goship_names_to_map else name for name in goship_names_list]

#     renamed_units_mapping = rename_keys_to_argovis(
#         goship_units_mapping, new_goship_argovis_name_mapping)

#     renamed_ref_scale_mapping = rename_keys_to_argovis(
#         goship_ref_scale_mapping, new_goship_argovis_name_mapping)

#     renamed_c_format_mapping = rename_keys_to_argovis(
#         goship_c_format_mapping, new_goship_argovis_name_mapping)

#     renamed_dtype_mapping = rename_keys_to_argovis(
#         goship_dtype_mapping, new_goship_argovis_name_mapping)

#     argovis_mapping = {}
#     argovis_mapping['names'] = renamed_names_list
#     argovis_mapping['units'] = renamed_units_mapping
#     argovis_mapping['ref_scale'] = renamed_ref_scale_mapping
#     argovis_mapping['c_format'] = renamed_c_format_mapping
#     argovis_mapping['dtype'] = renamed_dtype_mapping

#     return argovis_mapping


# def rename_argovis_mapping_by_type(mapping, type):

#     new_mapping = {}

#     for name, val in mapping.items():

#         if name.endswith('_qc'):
#             non_qc_name = name.replace('_qc', '')
#             new_name = f"{non_qc_name}_{type}_qc"
#         else:
#             new_name = f"{name}_{type}"

#         new_mapping[new_name] = val

#     return new_mapping


# def rename_argovis_name_list_by_type(argovis_names_list, type):

#     new_names_list = []

#     for name in argovis_names_list:

#         if name.endswith('_qc'):
#             non_qc_name = name.replace('_qc', '')
#             new_name = f"{non_qc_name}_{type}_qc"

#         else:
#             new_name = f"{name}_{type}"

#         new_names_list.append(new_name)

#     return new_names_list


# def rename_vars_to_argovis_by_type(mapping, type):

#     goship_names_list = mapping['names']
#     goship_units_mapping = mapping['units']
#     goship_ref_scale_mapping = mapping['ref_scale']
#     goship_c_format_mapping = mapping['c_format']
#     goship_dtype_mapping = mapping['dtype']

#     goship_argovis_name_mapping = gvm.get_goship_argovis_name_mapping_per_type(
#         type)

#     # What if both ctd temps or oxygens in mapping argument,
#     # Use primary first

#     has_both_temp = has_both_temperatures(goship_names_list)
#     has_both_oxygen = has_both_oxygens(goship_names_list)

#     new_goship_argovis_name_mapping = goship_argovis_name_mapping

#     if has_both_temp:
#         new_goship_argovis_name_mapping = {key: val for key, val in goship_argovis_name_mapping.items(
#         ) if key != 'ctd_temperature_68' or key != 'ctd_temperature_68_qc'}

#     if has_both_oxygen:
#         new_goship_argovis_name_mapping = {key: val for key, val in new_goship_argovis_name_mapping.items(
#         ) if key != 'ctd_oxygen_ml_l' or key != 'ctd_oxygen_ml_l_qc'}

#     renamed_names_list = rename_goship_name_list_by_type(
#         goship_names_list, type)

#     argovis_mapping = {}
#     argovis_mapping['names'] = renamed_names_list

#     renamed_units_mapping = rename_keys_to_argovis_by_type(
#         goship_units_mapping, new_goship_argovis_name_mapping, type)

#     renamed_ref_scale_mapping = rename_keys_to_argovis_by_type(
#         goship_ref_scale_mapping, new_goship_argovis_name_mapping, type)

#     renamed_c_format_mapping = rename_keys_to_argovis_by_type(
#         goship_c_format_mapping, new_goship_argovis_name_mapping, type)

#     renamed_dtype_mapping = rename_keys_to_argovis_by_type(
#         goship_dtype_mapping, new_goship_argovis_name_mapping, type)

#     argovis_mapping['units'] = renamed_units_mapping
#     argovis_mapping['ref_scale'] = renamed_ref_scale_mapping
#     argovis_mapping['c_format'] = renamed_c_format_mapping
#     argovis_mapping['dtype'] = renamed_dtype_mapping

#     return argovis_mapping


# def rename_meta_to_argovis(cols):

#     core_values_mapping = gvm.get_argovis_meta_mapping()

#     core_values = [core for core in core_values_mapping]

#     name_mapping = {}

#     for name in cols:

#         if name in core_values:
#             new_name = core_values_mapping[name]

#         else:
#             new_name = name

#         name_mapping[name] = new_name

#     return name_mapping


# def rename_argovis_meta(obj):

#     core_values_mapping = gvm.get_argovis_meta_mapping()

#     core_values = [core for core in core_values_mapping]

#     new_obj = {}

#     for key, val in obj.items():

#         if key in core_values:
#             new_key = core_values_mapping[key]

#         else:
#             new_key = key

#         new_obj[new_key] = val

#     return new_obj


# def rename_key_not_meta(obj, type):

#     core_values_mapping = gvm.get_goship_argovis_core_values_mapping(type)

#     core_values = [core for core in core_values_mapping]

#     new_obj = {}

#     for key, val in obj.items():

#         if '_qc' in key:
#             check_core = key.replace('_qc', '')
#             if check_core in core_values:
#                 new_key = f"{core_values_mapping[check_core]}_qc"

#         elif key in core_values:
#             new_key = core_values_mapping[key]

#         elif '_qc' in key:
#             key_wo_qc = key.replace('_qc', '')
#             new_key = f"{key_wo_qc}_{type}_qc"
#         else:
#             new_key = f"{key}_{type}"

#         new_obj[new_key] = val

#     # if type == 'ctd':

#     #     core_values_mapping = gvm.get_goship_argovis_core_values_mapping('ctd')

#     #     core_values = [core for core in core_values_mapping]

#     #     new_obj = {}

#     #     for key, val in obj.items():

#     #         if '_qc' in key:
#     #             check_core = key.replace('_qc', '')
#     #             if check_core in core_values:
#     #                 new_key = f"{core_values_mapping[check_core]}_qc"

#     #         elif key in core_values:
#     #             new_key = core_values_mapping[key]

#     #         elif '_qc' in key:
#     #             key_wo_qc = key.replace('_qc', '')
#     #             new_key = f"{key_wo_qc}_ctd_qc"
#     #         else:
#     #             new_key = f"{key}_ctd"

#     #         new_obj[new_key] = val

#     return new_obj


# def rename_btl_list_meta(names):

#     new_names = []

#     for name in names:
#         new_name = f"{name}_btl"
#         new_names.append(new_name)

#     return new_names


# def create_meta_col_name_mapping(cols):

#     core_values_mapping = gvm.get_goship_argovis_name_mapping()

#     #core_values = core_values_mapping.keys()
#     core_values = [key for key in core_values_mapping.keys() if key in cols]

#     col_mapping = {}

#     for name in cols:

#         if name in core_values:
#             mapped_name = core_values_mapping[name]

#         else:
#             mapped_name = name

#         col_mapping[name] = mapped_name

#     return col_mapping


# def create_param_col_name_mapping_w_type(cols, type):

#     # names without '_qc'
#     core_values_mapping = gvm.get_goship_argovis_core_values_mapping(type)

#     core_values = [core for core in core_values_mapping.keys() if core in cols]

#     # check if both ctd_temperature  and ctd_temperature_68 are
#     # in the file, if it is, only rename ctd_temperature as core.
#     # Same for oxygen, check for ctd_oxygen and ctd_oxygen_ml_l

#     has_both_temp = 'ctd_temperature' in cols and 'ctd_temperature_68' in cols
#     has_both_oxygen = 'ctd_oxygen' in cols and 'ctd_oxygen_ml_l' in cols

#     primary_core = ['ctd_temperature', 'ctd_oxygen']
#     secondary_core = ['ctd_temperature_68', 'ctd_oxygen_ml_l']

#     col_mapping = {}

#     for name in cols:

#         if name.endswith('_qc'):
#             non_qc_name = name.replace('_qc', '')

#             if has_both_temp and non_qc_name in primary_core:
#                 new_name = f"{core_values_mapping[non_qc_name]}_qc"

#             elif has_both_oxygen and non_qc_name in primary_core:
#                 new_name = f"{core_values_mapping[non_qc_name]}_qc"

#             elif non_qc_name in core_values and non_qc_name not in secondary_core:
#                 new_name = f"{core_values_mapping[non_qc_name]}_qc"
#             else:
#                 new_name = f"{non_qc_name}_{type}_qc"

#         elif has_both_temp and name in primary_core:
#             new_name = core_values_mapping[name]
#         elif has_both_oxygen and name in primary_core:
#             new_name = core_values_mapping[name]

#         elif name in core_values and name not in secondary_core:
#             new_name = core_values_mapping[name]
#         else:
#             new_name = f"{name}_{type}"

#         col_mapping[name] = new_name

#     return col_mapping


# def create_renamed_list_of_objs_argovis_measurements(cur_list):

#     # Rename without extension

#     new_list = []

#     new_obj = {}

#     for obj in cur_list:

#         new_obj = rename_key_not_meta_argovis_measurements(obj)
#         # new_obj = rename_keys_to_argovis_by_type(obj, type)

#         new_list.append(new_obj)

#     return new_list


# def create_renamed_list_of_objs_argovis(cur_list, type):

#     # Common names are pres, temp, psal, and doxy will have suffix _ctd and _ctd_qc
#     # All ctd vars will have suffix _ctd and _ctd_qc
#     # All bot vars will have suffx _btl and _btl_qc
#     # Special case is bottle_salinity to psal_btl

#     # Creating list, don't modify key since already renamed as element  of list

#     new_list = []

#     new_obj = {}

#     for obj in cur_list:

#         new_obj = rename_keys_to_argovis_by_type(obj, type)

#         new_list.append(new_obj)

#     # if type == 'ctd':

#     #     new_list = []

#     #     new_obj = {}

#     #     for obj in cur_list:

#     #         new_obj = rename_keys_to_argovis_by_type(obj, type)

#     #         new_list.append(new_obj)

#     return new_list


# def create_renamed_list_of_objs(cur_list, type):

#     new_list = []

#     if type == 'btl':

#         new_obj = {}

#         for obj in cur_list:

#             new_obj = rename_key_not_meta(obj, 'btl')

#             new_list.append(new_obj)

#     if type == 'ctd':

#         new_obj = {}

#         for obj in cur_list:

#             new_obj = rename_key_not_meta(obj, 'ctd')

#             new_list.append(new_obj)

#     return new_list


# def rename_profile_to_argovis(profile):

#     station_cast = profile['station_cast']
#     profile_dict = profile['profile_dict']

#     type = profile_dict['type']

#     # TODO
#     # consolidate this

#     if type == 'btl':

#         # station_cast = profile['station_cast']
#         # profile_dict = profile['profile_dict']

#         meta = profile_dict['meta']
#         renamed_meta = rename_argovis_meta(meta)

#         bgc_list = profile_dict['bgc_meas']

#         renamed_bgc_list = create_renamed_list_of_objs(bgc_list, 'btl')

#         measurements_list = profile_dict['measurements']

#         renamed_measurements_list = create_renamed_list_of_objs_argovis_measurements(
#             measurements_list)

#         measurements_source = profile_dict['measurements_source']
#         goship_names = profile_dict['goship_names']
#         goship_ref_scale = profile_dict['goship_ref_scale']
#         goship_units = profile_dict['goship_units']

#         goship_argovis_name = gvm.get_goship_argovis_core_values_mapping(
#             goship_names, type)

#         argovis_ref_scale = gvm.get_argovis_ref_scale_mapping(
#             goship_names, type)

#         goship_argovis_unit = gvm.get_goship_argovis_unit_name_mapping()

#     if type == 'ctd':

#         # station_cast = profile['station_cast']
#         # profile_dict = profile['profile_dict']

#         meta = profile_dict['meta']

#         renamed_meta = rename_argovis_meta(meta)

#         bgc_list = profile_dict['bgc_meas']
#         renamed_bgc_list = create_renamed_list_of_objs(bgc_list, type)

#         measurements_list = profile_dict['measurements']

#         renamed_measurements_list = create_renamed_list_of_objs_argovis_measurements(
#             measurements_list)

#         measurements_source = profile_dict['measurements_source']

#         goship_names = profile_dict['goship_names']
#         goship_ref_scale = profile_dict['goship_ref_scale']
#         goship_units = profile_dict['goship_units']

#         # TODO fix this
#         # If have multiple ctd temps, only map primary to argovis name
#         goship_argovis_name = gvm.get_goship_argovis_core_values_mapping(
#             goship_names, type)

#         argovis_ref_scale = gvm.get_argovis_ref_scale_mapping(
#             goship_names, type)

#         goship_argovis_unit = gvm.get_goship_argovis_unit_name_mapping()

#     renamed_profile_dict = {}
#     renamed_profile_dict['type'] = type
#     renamed_profile_dict['stationCast'] = station_cast
#     renamed_profile_dict['meta'] = renamed_meta
#     renamed_profile_dict['measurements'] = renamed_measurements_list
#     renamed_profile_dict['measurementsSource'] = measurements_source
#     renamed_profile_dict['bgcMeas'] = renamed_bgc_list
#     renamed_profile_dict['goshipUnits'] = goship_units
#     renamed_profile_dict['goshipArgovisNameMapping'] = goship_argovis_name
#     renamed_profile_dict['goshipReferenceScale'] = goship_ref_scale
#     renamed_profile_dict['argovisReferenceScale'] = argovis_ref_scale
#     renamed_profile_dict['goshipArgovisUnitsMapping'] = goship_argovis_unit

#     output_profile = {}
#     output_profile['profile_dict'] = renamed_profile_dict
#     output_profile['station_cast'] = station_cast

#     return output_profile
