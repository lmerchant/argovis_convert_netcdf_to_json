import logging

from create_profiles.create_profiles_combined_type import create_profiles_combined_type
from check_and_save.save_output import save_data_type_profiles
from check_and_save.save_output import save_data_type_profiles_combined


def post_process_cruise_objs_by_collection(cruise_objs_by_type):

    single_data_type_cruises = []

    for cruise_obj in cruise_objs_by_type:

        expocode = cruise_obj['cruise_expocode']

        all_data_types_profile_objs = cruise_obj['all_data_types_profile_objs']

        logging.info("****************************")
        logging.info(f"Post processing {expocode}")
        logging.info("****************************")

        # In profiles objs, the combined type may just
        # be btl and not ctd or ctd and not btl
        # but within all the profiles, if find a btl and ctd
        # apply combined function

        is_btl = any([True if profiles_obj['data_type'] ==
                      'btl' else False for profiles_obj in all_data_types_profile_objs])

        is_ctd = any([True if profiles_obj['data_type'] ==
                      'ctd' else False for profiles_obj in all_data_types_profile_objs])

        if is_btl and is_ctd:

            logging.info("Combining btl and ctd")

            # filter measurements by hierarchy
            #  when combine btl and ctd profiles.
            # didn't filter btl or ctd first in case need
            # a variable from both
            # Each obj in all_data_types_profile_objs is a
            # dict with keys station_cast, data_type, profile_dict
            combined_obj_profiles = create_profiles_combined_type(
                all_data_types_profile_objs)

            save_data_type_profiles_combined(combined_obj_profiles)

        else:
            single_data_type_cruises.append(cruise_obj)

            # for data_type_obj_profiles in all_data_types_profile_objs:
            #     save_data_type_profiles(data_type_obj_profiles)

    return single_data_type_cruises
