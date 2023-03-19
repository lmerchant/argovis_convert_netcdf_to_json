import logging
import copy


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

        meta['instrument'] = ship_data_type

        new_profile_dict['meta'] = meta

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
