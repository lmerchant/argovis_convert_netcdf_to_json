import logging
#import pandas as pd

from create_profiles.create_meas_combined_profiles import combine_btl_ctd_measurements
#from create_profiles.create_meas_combined_profiles import get_combined_meas_source
from variable_naming.combined_mapping import get_combined_mappings
from variable_naming.meta_param_mapping import get_source_independent_meta_names


def get_meta_wo_data_type_combined_profiles(meta, data_type):

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


def get_meta_w_data_type_combined_profiles(meta, data_type):

    # Also add meta with suffix of the data_type
    # Will have duplicate information in file
    # This is because if combining files, will be using ctd meta without
    # suffix but still have ctd meta with suffix. To have a consistent
    # naming scheme across all files

    # But don't need a suffix for common meta for btl and ctd
    source_independent_meta_names = get_source_independent_meta_names()

    meta_w_data_type = {}
    meta_source_independent = {}

    for key, val in meta.items():

        if key not in source_independent_meta_names:

            if key == 'source_info':
                # Add the data type suffix to all the keys of
                # the source_info obj
                meta_w_data_type[new_key] = {}
                for sub_key, sub_val in val.items():
                    new_sub_key = f"{sub_key}_{data_type}"
                    meta_w_data_type[new_key][new_sub_key] = sub_val

            new_key = f"{key}_{data_type}"
            meta_w_data_type[new_key] = val

        else:
            meta_source_independent[key] = val

    return meta_w_data_type, meta_source_independent


def combine_btl_ctd_per_profile(btl_profile, ctd_profile):

    # For a combined dict, station_cast same for btl and ctd
    station_cast = btl_profile['station_cast']

    # May have case where bot dict or ctd dict doesn't exist for same profile
    # But they have the same station_cast

    # All profiles have a profile and station_cast key but
    # profile_dict may be empty

    btl_dict = btl_profile['profile_dict']
    ctd_dict = ctd_profile['profile_dict']

    btl_meta = {}
    ctd_meta = {}

    btl_meta_w_data_type = {}
    ctd_meta_w_data_type = {}

    btl_meta_wo_data_type = {}
    ctd_meta_wo_data_type = {}

    btl_data = []
    ctd_data = []

    if btl_dict:
        file_data_type = 'btl'

        # Single profile meta has source independent values and values with
        # data type suffix

        # meta_w_data_type, meta_source_independent = get_meta_w_data_type_combined_profiles(
        #     btl_dict['meta'], file_data_type)

        # btl_meta = {**meta_source_independent, **meta_w_data_type}

        btl_meta_wo_data_type = get_meta_wo_data_type_combined_profiles(
            btl_dict['meta'], file_data_type)

        btl_meta = btl_dict['meta']

        btl_data = btl_dict['data']

    if ctd_dict:
        file_data_type = 'ctd'

        # meta_w_data_type, meta_source_independent = get_meta_w_data_type_combined_profiles(
        #     ctd_dict['meta'], file_data_type)

        # ctd_meta = {**meta_source_independent, **meta_w_data_type}

        ctd_meta_wo_data_type = get_meta_wo_data_type_combined_profiles(
            ctd_dict['meta'], file_data_type)

        ctd_meta = ctd_dict['meta']

        ctd_data = ctd_dict['data']

    # print('ctd data keys')
    # print(ctd_dict['data_keys'])

    if btl_dict and ctd_dict:
        data_type = 'btl_ctd'
        combined_meta = {**ctd_meta_wo_data_type, **ctd_meta, **btl_meta}

        # Add instrument key to indicate profile data type
        # It's ship_btl_ctd because cruise has both a bottle and ctd file
        combined_meta['instrument'] = 'ship_ctd_btl'

    elif btl_dict and not ctd_dict:
        # TODO
        # Check if this is true
        # keep only meta without a _btl suffix since no ctd meta
        #meta = btl_meta_wo_data_type
        data_type = 'btl'
        combined_meta = {**btl_meta_wo_data_type, **btl_meta}

        combined_meta['instrument'] = 'ship_btl'

    elif ctd_dict and not btl_dict:
        data_type = 'ctd'
        combined_meta = {**ctd_meta_wo_data_type, **ctd_meta}

        combined_meta['instrument'] = 'ship_ctd'

    # meta_w_data_type = {**ctd_meta_w_data_type, **btl_meta_w_data_type}

    # combined_meta = {**meta, **meta_w_data_type}

    combined_data = [*ctd_data, *btl_data]

    # Returns a dictionary which includes sources information along with measurements
    combined_measurements = combine_btl_ctd_measurements(
        btl_dict, ctd_dict)

    combined_btl_ctd_dict = {}

    combined_btl_ctd_dict['meta'] = combined_meta
    combined_btl_ctd_dict['data'] = combined_data

    combined_btl_ctd_dict = {
        **combined_btl_ctd_dict, **combined_measurements}

    # Combine the mappings where will be combining names that are the same
    # for both btl and ctd used for argovis and also combining key names
    # that are included because they are dependent on their source file, btl or ctd

    # TODO
    # oxygen was added to btl dict that shouldn't be there. Mainly if there is
    # an oxygen var but no doxy one.

    # combined_mappings = get_combined_mappings(btl_dict, ctd_dict)

    # combined_btl_ctd_dict = {**combined_btl_ctd_dict, **combined_mappings}

    combined_btl_ctd_dict['data_type'] = data_type

    combined_profile = {}
    combined_profile['profile_dict'] = combined_btl_ctd_dict
    combined_profile['station_cast'] = station_cast

    return combined_profile


def get_same_station_cast_profile_btl_ctd(btl_profiles, ctd_profiles):

    # Want to group  on station_cast, so if number
    # of station_cast combinations is not the same,
    # Set a station_cast for that empty profile  so
    # that the number of station_cast values is the
    # same for btl and ctd.

    station_casts_btl = [btl_profile['station_cast']
                         for btl_profile in btl_profiles]

    station_casts_ctd = [ctd_profile['station_cast']
                         for ctd_profile in ctd_profiles]

    different_pairs_in_btl = set(
        station_casts_btl).difference(station_casts_ctd)
    different_pairs_in_ctd = set(
        station_casts_ctd).difference(station_casts_btl)

    for pair in different_pairs_in_btl:
        # Create matching but empty profiles for ctd
        new_profile = {}
        new_profile['profile_dict'] = {}
        new_profile['station_cast'] = pair
        ctd_profiles.append(new_profile)

    for pair in different_pairs_in_ctd:
        # Create matching but empty profiles for ctd
        new_profile = {}
        new_profile['profile_dict'] = {}
        new_profile['station_cast'] = pair
        btl_profiles.append(new_profile)

    return btl_profiles, ctd_profiles


def balance_profiles_by_station_cast(profiles_objs):

    # When combining each station cast profiles,
    # may have case where there is only a btl profile
    # since no ctd profile at that station cast

    # TODO
    # work on what I was doing here

    # profiles_objs stands for either a btl or ctd.
    # Just loop till you locate them
    # They always exist, it's just that one may be empty dict

    for profiles_obj in profiles_objs:

        if profiles_obj['data_type'] == 'btl':
            #cchdo_meta = profiles_objs['cchdo_meta']
            btl_profiles = profiles_obj['data_type_profiles_list']
        elif profiles_obj['data_type'] == 'ctd':
            #cchdo_meta = profiles_objs['cchdo_meta']
            ctd_profiles = profiles_obj['data_type_profiles_list']

    # this profile has both doxy and oxygen vars
    # selected_profile_dict = btl_profiles[15]['profile_dict']
    # data_keys = selected_profile_dict['data_keys']

    # if 'oxygen' in data_keys:
    #     print('data_keys')
    #     print(data_keys)

    for btl_profile in btl_profiles:
        # for bottle number 1, data doesn't have doxy var but has oxygen var
        selected_profile_dict = btl_profile['profile_dict']

        source_info = selected_profile_dict['meta'][f'source_info_btl']
        data_keys = source_info['data_keys']

        # if 'oxygen' in data_keys:

        #     print('station cast')
        #     print(btl_profile['station_cast'])
        #     print(f"\n\n")
        #     print('data_keys')
        #     print(data_keys)

        #     print(f"\n\n")
        #     print(selected_profile_dict['data'])

    # Get profile dicts so have the same number of profiles
    # one may be blank while the other exists at a cast
    logging.info(f"Number of btl profiles {len(btl_profiles)}")
    logging.info(f"Number of ctd profiles {len(ctd_profiles)}")

    btl_profiles, ctd_profiles = get_same_station_cast_profile_btl_ctd(
        btl_profiles, ctd_profiles)

    #  bottle  and ctd have same keys, but  different values
    # which are the profile numbers

    all_profiles = []

    # The number of station_casts are the same for btl and ctd
    station_casts = [btl_profile['station_cast']
                     for btl_profile in btl_profiles]

    for station_cast in station_casts:

        try:
            profile_dict_btl = [btl_profile['profile_dict']
                                for btl_profile in btl_profiles if btl_profile['station_cast'] == station_cast][0]

        except:
            profile_dict_btl = {}

        profile_btl = {}
        profile_btl['station_cast'] = station_cast
        profile_btl['profile_dict'] = profile_dict_btl

        try:
            profile_dict_ctd = [ctd_profile['profile_dict']
                                for ctd_profile in ctd_profiles if ctd_profile['station_cast'] == station_cast][0]

        except:
            profile_dict_ctd = {}

        profile_ctd = {}
        profile_ctd['station_cast'] = station_cast
        profile_ctd['profile_dict'] = profile_dict_ctd

        profile = {}
        profile['btl'] = profile_btl
        profile['ctd'] = profile_ctd

        all_profiles.append(profile)

    return all_profiles


def create_profiles_combined_type(profiles_objs):

    # Have a btl and ctd profile for each station cast
    # If don't have data type at a station cast,
    # set it to be an empty object

    all_profiles = balance_profiles_by_station_cast(profiles_objs)

    profiles_list_btl_ctd = []

    count = 0

    for profile in all_profiles:

        profile_btl = profile['btl']
        profile_ctd = profile['ctd']

        # count = count + 1

        # if count == 15:
        #     exit(1)

        # returned dict has keys: station_cast, data_type, profile_dict
        combined_profile_btl_ctd = combine_btl_ctd_per_profile(
            profile_btl, profile_ctd)

        profiles_list_btl_ctd.append(combined_profile_btl_ctd)

    logging.info('Processed btl and ctd combined profiles')

    return profiles_list_btl_ctd
