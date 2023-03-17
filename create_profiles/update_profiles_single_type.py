import logging
import copy

from variable_naming.meta_param_mapping import get_source_independent_meta_names


def add_meta_wo_data_type(meta, data_type):

    # Already have meta with the source independent meta names

    # Just want to remove data_type off the other meta and save it

    source_independent_meta_names = get_source_independent_meta_names()

    meta_wo_data_type = {}

    for key, val in meta.items():

        if key not in source_independent_meta_names:
            new_key = key.replace(f"_{data_type}", '')

            if new_key == 'source_info':
                # Remove the data type suffix from all the keys of
                # the source_info obj
                meta_wo_data_type[new_key] = {}
                for sub_key, sub_val in val.items():
                    new_sub_key = sub_key.replace(f"_{data_type}", '')
                    meta_wo_data_type[new_key][new_sub_key] = sub_val
            else:
                meta_wo_data_type[new_key] = val

    return meta_wo_data_type


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

        meta_wo_data_type = add_meta_wo_data_type(
            meta, data_type)

        if data_type == 'btl':
            new_meta = {**meta_wo_data_type, **meta}

        elif data_type == 'ctd':
            new_meta = {**meta_wo_data_type, **meta}

        new_meta['instrument'] = ship_data_type

        new_profile_dict['meta'] = new_meta

        # TODO
        # remove any measurements objs with temp = nan
        # I do this when I save to zip. Can I do that here instead?

        new_profile['profile_dict'] = new_profile_dict

        new_profiles.append(new_profile)

    return new_profiles


def update_profiles_single_type(profiles_objs):

    for profiles_obj in profiles_objs:

        profiles = profiles_obj['data_type_profiles_list']

        # Add meta without data type suffix
        new_profiles = process_profiles(profiles)

    logging.info('Processed single type profiles')

    return new_profiles
