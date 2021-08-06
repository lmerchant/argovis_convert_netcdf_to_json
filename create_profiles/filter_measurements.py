# Filter  measurements object by element hierarchy
import pandas as pd
import logging


def convert_boolean(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    if isinstance(obj, (list, tuple)):
        return [convert_boolean(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_boolean(key): convert_boolean(value) for key, value in obj.items()}
    return obj


# def filter_measurements_one_profile_orig(measurements, use_elems):

#     # If a ctd file, filter on whether have salinity,
#     # if not, set it to  None

#     # If a bottle file, keep temp_btl and then choose psal_btl
#     # first but if doesn't exist and salinity_btl does, use that

#     use_temp = use_elems['use_temp']
#     use_psal = use_elems['use_psal']
#     use_salinity = use_elems['use_salinity']

#     new_measurements = []
#     for obj in measurements:
#         new_obj = obj.copy()
#         for key, val in obj.items():
#             if key == 'temp' and not use_temp:
#                 new_obj['temp'] = None
#             if key == 'psal' and not use_psal:
#                 new_obj['psal'] = None
#             if key == 'salinity' and not use_salinity:
#                 del new_obj[key]
#             if key == 'salinity' and not use_psal:
#                 new_obj['psal'] = val

#         new_measurements.append(new_obj)

#         keys = new_obj.keys()
#         if 'salinity' in keys:
#             del new_obj['salinity']

#     return new_measurements


# def filter_measurements_one_profile(measurements, use_elems):

#     # If a ctd file, filter on whether have salinity,
#     # if not, set it to  None

#     # If a bottle file, keep temp_btl and then choose psal_btl
#     # first but if doesn't exist and salinity_btl does, use that

#     use_temp = use_elems['use_temp']
#     use_psal = use_elems['use_psal']
#     use_salinity = use_elems['use_salinity']

#     new_measurements = []
#     for obj in measurements:
#         new_obj = obj.copy()
#         for key, val in obj.items():
#             if key == 'temp' and not use_temp:
#                 new_obj['temp'] = None
#             if key == 'psal' and not use_psal:
#                 new_obj['psal'] = None

#         new_measurements.append(new_obj)

#     return new_measurements


# def find_measurements_hierarchy(measurements):

#     has_psal = False
#     has_salinity = False
#     has_temp = False

#     try:
#         has_temp = any([True if pd.notnull(obj['temp'])
#                        else False for obj in measurements])

#     except:
#         has_temp = False

#     try:
#         has_psal = any([True if pd.notnull(obj['psal'])
#                        else False for obj in measurements])
#     except:
#         has_psal = False

#     try:
#         has_salinity = any([True if pd.notnull(obj['salinity'])
#                             else False for obj in measurements])
#     except:
#         has_salinity = False

#     use_temp = False
#     use_psal = False
#     use_salinity = False

#     if has_temp:
#         use_temp = True

#     if has_psal:
#         use_psal = True
#     if not has_psal and has_salinity:
#         use_salinity = True

#     use_elems = {
#         'use_temp': use_temp,
#         'use_psal': use_psal,
#         'use_salinity': use_salinity
#     }

#     return use_elems


# def get_filtered_measurements_for_profile_dict(measurements, type):

#     use_elems = find_measurements_hierarchy(measurements)

#     measurements = filter_measurements_one_profile(measurements, use_elems)

#     return measurements


def filter_measurements(data_type_profiles):

    # For measurements, already filtered whether to
    # keep psal or salinity, but didn't rename the
    # salinity col.

    output_profiles_list = []

    for profile in data_type_profiles:

        profile_dict = profile['profile_dict']
        station_cast = profile['station_cast']

        measurements = profile_dict['measurements']

        # Check if all elems null in measurements besides pressure

        all_vals = []
        for obj in measurements:
            vals = [val for key, val in obj.items() if pd.notnull(val)
                    and key != 'pres']
            all_vals.extend(vals)

        if not len(all_vals):
            measurements = []

        profile_dict['measurements'] = measurements

        output_profile = {}
        output_profile['profile_dict'] = profile_dict
        output_profile['station_cast'] = station_cast

        output_profiles_list.append(output_profile)

    return output_profiles_list


# def filter_measurements_orig(data_type_profiles, type):

#     # For measurements, already filtered whether to
#     # keep psal or salinity, but didn't rename the
#     # salinity col.

#     output_profiles_list = []

#     for profile in data_type_profiles:

#         profile_dict = profile['profile_dict']
#         station_cast = profile['station_cast']

#         measurements = profile_dict['measurements']

#         measurements = get_filtered_measurements_for_profile_dict(
#             measurements, type)

#         profile_dict['measurements'] = measurements

#         output_profile = {}
#         output_profile['profile_dict'] = profile_dict
#         output_profile['station_cast'] = station_cast

#         output_profiles_list.append(output_profile)

#     return output_profiles_list
