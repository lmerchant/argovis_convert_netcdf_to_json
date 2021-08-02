import logging

from create_profiles.create_profiles_combined_type import create_profiles_combined_type
from check_and_save.save_output import save_data_type_profiles
from check_and_save.save_output import save_data_type_profiles_combined


def post_process_cruise_objs_by_collection(cruises_profiles_objs):

    for cruise_profiles_obj in cruises_profiles_objs:

        expocode = cruise_profiles_obj['cruise_expocode']

        all_data_types_profile_objs = cruise_profiles_obj['all_data_types_profile_objs']

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
            profiles_btl_ctd_objs = create_profiles_combined_type(
                all_data_types_profile_objs)

            save_data_type_profiles_combined(profiles_btl_ctd_objs)

        else:
            for data_type_obj_profiles in all_data_types_profile_objs:
                save_data_type_profiles(data_type_obj_profiles)
