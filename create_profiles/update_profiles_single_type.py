import logging
import copy

from variable_mapping.meta_param_mapping import get_source_independent_meta_names


def add_meta_w_data_type(meta, data_type):

    # Also add meta with suffix of the data_type
    # Will have duplicate information in file
    # This is because if combining files, will be using ctd meta without
    # suffix but still have ctd meta with suffix. To have a consistent
    # naming scheme across all files

    # But don't need a suffix for common meta for btl and ctd
    source_independent_meta_names = get_source_independent_meta_names()

    meta_w_data_type = {}
    meta_wo_data_type = {}

    for key, val in meta.items():

        if key not in source_independent_meta_names:
            new_key = f"{key}_{data_type}"
            meta_w_data_type[new_key] = val
        else:
            meta_wo_data_type[key] = val

    return meta_w_data_type, meta_wo_data_type


def process_profiles(profiles):

    new_profiles = []

    for profile in profiles:

        station_cast = profile['station_cast']
        profile_dict = profile['profile_dict']

        data_type = profile_dict['data_type']

        ship_data_type = f"ship_{data_type}"

        new_profile = {}
        new_profile['station_cast'] = station_cast

        new_profile_dict = copy.deepcopy(profile_dict)

        meta = profile_dict['meta']

        meta_w_data_type, meta_wo_data_type = add_meta_w_data_type(
            meta, data_type)

        if data_type == 'btl':
            new_meta = {**meta, **meta_w_data_type}

        elif data_type == 'ctd':
            new_meta = {**meta, **meta_w_data_type}

        new_meta['instrument'] = ship_data_type

        new_profile_dict['meta'] = new_meta

        # TODO
        # remove any measurements objs with temp = nan
        # I do this when I save to zip. Can I do that here instead?

        new_profile['profile_dict'] = new_profile_dict

        new_profiles.append(new_profile)

    return new_profiles


def update_profiles_single_type(profiles_objs, data_type):

    for profiles_obj in profiles_objs:

        profiles_obj_data_type = profiles_obj['data_type']
        profiles = profiles_obj['data_type_profiles_list']

        if profiles_obj_data_type != data_type:
            continue

        # Add meta with data type suffix
        new_profiles = process_profiles(profiles)

        # TODO
        # Remove any measurements with temp = NaN
        # Do this when save file

    logging.info('Processed single type profiles')

    return new_profiles
