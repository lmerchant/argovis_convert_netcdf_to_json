import logging
import os


from global_vars import GlobalVars
from check_and_save.check_of_ctd_vars import check_of_ctd_vars


def filter_data_type_profiles(data_type_profiles):

    checked_profiles_info, ctd_vars_flag = check_of_ctd_vars(
        data_type_profiles)

    filtered_data_type_profiles = []
    excluded_data_type_profiles = []

    # ctd_vars_flag represents entire cruise.
    # If it is False, no profiles in cruise have CTD vars

    for checked_profile_info in checked_profiles_info:

        data_type_profile = checked_profile_info['profile_checked']

        has_btl = checked_profile_info['has_all_ctd_vars']['btl']
        has_ctd = checked_profile_info['has_all_ctd_vars']['ctd']

        if has_btl or has_ctd:
            filtered_data_type_profiles.append(data_type_profile)
        else:
            excluded_data_type_profiles.append(data_type_profile)

    return filtered_data_type_profiles, excluded_data_type_profiles


def remove_non_ctd_profiles(cruises_profiles_objs):

    filtered_cruises_profiles_objs = []
    excluded_cruises_profiles_objs = []

    for cruise_profiles_obj in cruises_profiles_objs:

        expocode = cruise_profiles_obj['cruise_expocode']
        all_data_types_profile_objs = cruise_profiles_obj['all_data_types_profile_objs']

        filtered_all_data_types_profile_objs = []
        excluded_all_data_types_profile_objs = []

        for data_type_profiles_obj in all_data_types_profile_objs:

            data_type = data_type_profiles_obj['data_type']
            data_type_profiles = data_type_profiles_obj['data_type_profiles_list']

            num_data_type_profiles = len(data_type_profiles)

            # Check for one data type
            filtered_data_type_profiles, excluded_data_type_profiles = filter_data_type_profiles(
                data_type_profiles)

            # Included
            new_data_type_profiles_obj = {}
            new_data_type_profiles_obj['data_type'] = data_type
            new_data_type_profiles_obj['data_type_profiles_list'] = filtered_data_type_profiles

            filtered_all_data_types_profile_objs.append(
                new_data_type_profiles_obj)

            # Excluded
            excluded_data_type_profiles_obj = {}
            excluded_data_type_profiles_obj['data_type'] = data_type
            excluded_data_type_profiles_obj['data_type_profiles_list'] = excluded_data_type_profiles

            num_excluded_data_type_profiles = len(excluded_data_type_profiles)

            logging.info(f"Num data type profiles {num_data_type_profiles}")
            logging.info(
                f"Num excluded data type profiles {num_excluded_data_type_profiles}")

            if num_data_type_profiles == num_excluded_data_type_profiles:

                # If all of data type excluded, save to file
                # of cruises excluded
                logging.info("*** Cruise data type not converted ***")

                profile = excluded_data_type_profiles[0]
                cruise_expocode = profile['profile_dict']['meta']['expocode']

            elif num_excluded_data_type_profiles != 0:
                excluded_all_data_types_profile_objs.append(
                    excluded_data_type_profiles_obj)

            # TODO
            # Do I save profiles not included?
            # Does this even exist?

        # Included
        new_cruise_profiles_obj = {}
        new_cruise_profiles_obj['cruise_expocode'] = expocode
        new_cruise_profiles_obj['all_data_types_profile_objs'] = filtered_all_data_types_profile_objs

        filtered_cruises_profiles_objs.append(new_cruise_profiles_obj)

        # Excluded
        # If all profiles excluded for a  cruise data type,
        #  don't count as excluded profiles since
        # that data  type file not included

        if len(excluded_all_data_types_profile_objs):

            excluded_cruise_profiles_obj = {}
            excluded_cruise_profiles_obj['cruise_expocode'] = expocode
            excluded_cruise_profiles_obj['all_data_types_profile_objs'] = excluded_all_data_types_profile_objs

            excluded_cruises_profiles_objs.append(excluded_cruise_profiles_obj)
        else:
            excluded_cruises_profiles_objs = []

    return filtered_cruises_profiles_objs, excluded_cruises_profiles_objs
