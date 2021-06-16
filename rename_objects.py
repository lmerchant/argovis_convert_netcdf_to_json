import copy


import get_variable_mappings as gvm


# Rename objects


def rename_argovis_value_meta(obj):

    # hi

  # For mapping cases goship_argovis_name_mapping_bot goship is the key
  # and argovis is the value
    core_values_mapping = gvm.get_argovis_meta_mapping()

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key in obj:

        if key in core_values:
            new_val = core_values_mapping[key]

        else:
            new_val = f"{key}"

        new_obj[key] = new_val

    return new_obj


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

    if type == 'bot':

        core_values_mapping = gvm.get_goship_argovis_core_values_mapping('bot')

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


def rename_bot_by_key_meta(obj):

    # Alread renamed lat  and lon
    # Want to add _btl extension to all

    new_obj = {}

    for key, val in obj.items():

        new_key = f"{key}_btl"

        new_obj[new_key] = val

    return new_obj


def rename_key_not_meta(obj, type):

    if type == 'bot':

        core_values_mapping = gvm.get_goship_argovis_core_values_mapping('bot')

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

    if type == 'bot':

        core_values_mapping = gvm.get_goship_argovis_core_values_mapping('bot')
        core_values = [core for core in core_values_mapping]

        new_obj = {}

        for key, val in obj.items():

            if '_qc' in key:
                check_core = key.replace('_qc', '')
                if check_core in core_values:
                    new_key = f"{core_values_mapping[check_core]}_qc"

            elif key in core_values:
                new_key = core_values_mapping[key]

            else:
                new_key = val

            new_obj[new_key] = val

    if type == 'ctd':

        core_values_mapping = gvm.get_goship_argovis_core_values_mapping('ctd')
        core_value = [core for core in core_values_mapping]

        new_obj = {}

        for key, val in obj.items():

            if '_qc' in key:
                check_core = key.replace('_qc', '')
                if check_core in core_value:
                    new_key = f"{core_values_mapping[check_core]}_qc"

            elif key in core_value:
                new_key = core_values_mapping[key]

            else:
                new_key = val

            new_obj[new_key] = val

    return new_obj


def rename_ctd_value_not_meta(obj):

    # For case when goship is key and mapping to argovis

    core_values_mapping = gvm.get_goship_argovis_core_values_mapping('ctd')

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key in obj:

        if '_qc' in key:
            check_core = key.replace('_qc', '')
            if check_core in core_values:
                new_val = f"{core_values_mapping[check_core]}_qc"

        elif key in core_values:
            new_val = core_values_mapping[key]

        elif '_qc' in key:
            key_wo_qc = key.replace('_qc', '')
            new_val = f"{key_wo_qc}_ctd_qc"
        else:
            new_val = f"{key}_ctd"

        new_obj[key] = new_val

    return new_obj


def rename_ctd_key_not_meta(obj):

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


def rename_ctd_key_meta(obj):

    # fix
    core_values_mapping = gvm.get_goship_argovis_core_values_mapping('ctd')

    core_values = [core for core in core_values_mapping]

    new_obj = {}

    for key, val in obj.items():

        if key in core_values:
            new_key = core_values_mapping[key]

        else:
            new_key = f"{key}"

        new_obj[new_key] = val

    return new_obj


# TODO. Do i use this?
def rename_mapping_ctd_key_list(obj_list):

    core_values = gvm.get_goship_core_values()

    new_list = []

    for obj in obj_list:
        new_mapping = {}
        for key, val in obj.items():
            if key in core_values:
                new_key = key
            elif '_qc' in key:
                new_key = key.replace('_qc', '_ctd_qc')
            else:
                new_key = f"{key}_ctd"

            new_mapping[new_key] = val
        new_list.append(new_mapping)

    return new_list


def rename_output_per_profile(profile_dict, type):

    if type == 'bot':

        # Make a new object because will be using it next if combine profiles
        # but do rename lat and lon
        meta = profile_dict['meta']
        renamed_meta = rename_argovis_meta(meta)

        # TODO. Do I still need a deep copy?
        meta_copy = copy.deepcopy(meta)

        bgc_list = profile_dict['bgc_meas']
        renamed_bgc_list = create_renamed_list_of_objs(bgc_list, 'bot')

        measurements_list = profile_dict['measurements']
        renamed_measurements_list = []
        renamed_measurements_list = create_renamed_list_of_objs_argovis(
            measurements_list, 'bot')

        goship_names = profile_dict['goship_names']

        goship_ref_scale = profile_dict['goship_ref_scale']

        goship_units = profile_dict['goship_units']

        goship_argovis_name = gvm.create_goship_argovis_core_values_mapping(
            goship_names, type)

        argovis_ref_scale = gvm.get_argovis_ref_scale_mapping(
            goship_names, type)

        goship_argovis_unit = gvm.get_goship_argovis_unit_mapping()

    if type == 'ctd':

        # Make a new object because will be using it next if combine profiles
        # but do rename lat and lon
        meta = profile_dict['meta']
        meta_copy = copy.deepcopy(meta)

        renamed_meta = rename_argovis_meta(meta)

        bgc_list = profile_dict['bgc_meas']
        renamed_bgc_list = create_renamed_list_of_objs(bgc_list, type)

        measurements_list = profile_dict['measurements']
        #renamed_measurements_list = []
        renamed_measurements_list = create_renamed_list_of_objs_argovis(
            measurements_list, type)

        goship_names = profile_dict['goship_names']

        goship_ref_scale = profile_dict['goship_ref_scale']

        goship_units = profile_dict['goship_units']

        goship_argovis_name = gvm.create_goship_argovis_core_values_mapping(
            goship_names, type)

        argovis_ref_scale = gvm.get_argovis_ref_scale_mapping(
            goship_names, type)

        goship_argovis_unit = gvm.get_goship_argovis_unit_mapping()

    # don't rename meta yet in case not both bot and ctd combined

    renamed_profile_dict = {}
    renamed_profile_dict['meta'] = renamed_meta
    renamed_profile_dict['measurements'] = renamed_measurements_list
    renamed_profile_dict['bgcMeas'] = renamed_bgc_list
    renamed_profile_dict['goshipUnits'] = goship_units
    renamed_profile_dict['goshipArgovisNameMapping'] = goship_argovis_name
    renamed_profile_dict['goshipReferenceScale'] = goship_ref_scale
    renamed_profile_dict['argovisReferenceScale'] = argovis_ref_scale
    renamed_profile_dict['goshipArgovisUnitNameMapping'] = goship_argovis_unit

    return renamed_profile_dict


# def rename_units_to_argovis(data_obj):

#     units_mapping = gvm.get_goship_argovis_unit_mapping()
#     goship_salinity_ref_scale = gvm.get_goship_salniity_reference_scale()

#     nc = data_obj['nc']

#     coords = nc.coords
#     vars = nc.keys()

#     for var in coords:

#         # use try block because not all vars have a unit
#         try:
#             goship_unit = nc.coords[var].attrs['units']
#         except KeyError:
#             goship_unit = None

#         try:
#             argovis_unit = units_mapping[goship_unit]
#         except:
#             argovis_unit = goship_unit

#         if goship_unit:
#             try:
#                 nc.coords[var].attrs['units'] = units_mapping[goship_unit]
#             except:
#                 # No unit mapping
#                 pass

#     for var in vars:

#         try:
#             goship_unit = nc[var].attrs['units']
#         except KeyError:
#             goship_unit = None

#         # use try block because not all vars have a reference scale
#         try:
#             argovis_unit = units_mapping[goship_unit]
#         except:
#             argovis_unit = goship_unit

#         # Check if salinity unit of 1
#         if goship_unit == '1':
#             try:
#                 var_goship_ref_scale = nc[var].attrs['reference_scale']

#                 if var_goship_ref_scale == goship_salinity_ref_scale:
#                     argovis_unit = units_mapping[goship_unit]

#                     nc[var].attrs['units'] = argovis_unit

#             except KeyError:
#                 pass

#         if goship_unit:
#             try:
#                 nc[var].attrs['units'] = units_mapping[goship_unit]
#             except:
#                 # No unit mapping
#                 pass

#     data_obj['nc'] = nc

#     return data_obj


# def rename_converted_temperature(data_obj):

#     # Only renaming ctd temperatures on

#     nc = data_obj['nc']

#     data_vars = nc.keys()

#     def rename_var(var):
#         new_name = var.replace('_68', '')
#         return {var: new_name}

#     new_name_mapping = [rename_var(var) for var in data_vars if '_68' in var]

#     for name_map in new_name_mapping:
#         nc.rename(name_map)

#     return data_obj


def rename_profile_dicts_to_argovis(profile_dicts, type):

    # TODO
    # add following flags
    # isGOSHIPctd = true
    # isGOSHIPbottle = true
    # core_info = 1  # is ctd
    # core_info = 2  # is bottle (no ctd)
    # core_info = 12  # is ctd and tgoship_argovis_name_mapping_bot is bottle too (edited)

    #type = data_obj['type']

    num_profiles = len(profile_dicts)

    profile_dicts_list = []

    for profile_number in range(num_profiles):

        profile_dict = profile_dicts[profile_number]

        processed_profile_dict = rename_output_per_profile(profile_dict, type)

        profile_dicts_list.append(processed_profile_dict)

    return profile_dicts_list


def create_renamed_list_of_objs_argovis(cur_list, type):

    # Common names are pres, temp, psal, and doxy will have suffix _ctd and _ctd_qc
    # All ctd vars will have suffix _ctd and _ctd_qc
    # All bot vars will have suffx _btl and _btl_qc
    # Special case is bottle_salinity to psal_btl

    # Creating list, don't modify key since already renamed as element  of list

    if type == 'bot':

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

    if type == 'bot':

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta(obj, 'bot')

            new_list.append(new_obj)

    if type == 'ctd':

        new_obj = {}

        for obj in cur_list:

            new_obj = rename_key_not_meta(obj, 'ctd')

            new_list.append(new_obj)

    return new_list
