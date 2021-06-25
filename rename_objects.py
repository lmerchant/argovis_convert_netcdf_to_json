import copy
from os import rename


import get_variable_mappings as gvm

# Rename objects


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


def rename_key_not_meta_argovis(obj, type):

    # here

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


def rename_output_per_profile(profile, type):

    if type == 'btl':

        station_cast = profile['station_cast']
        profile_dict = profile['profile_dict']

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

        station_cast = profile['station_cast']
        profile_dict = profile['profile_dict']

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
    renamed_profile_dict['goshipArgovisUnitNameMapping'] = goship_argovis_unit

    output_profile = {}
    output_profile['profile_dict'] = renamed_profile_dict
    output_profile['station_cast'] = station_cast

    return output_profile


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


def rename_profiles_to_argovis(profiles, type):

    profiles_list = []

    for profile in profiles:

        processed_profile = rename_output_per_profile(profile, type)

        profiles_list.append(processed_profile)

    return profiles_list


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
