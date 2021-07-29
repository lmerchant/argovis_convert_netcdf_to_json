import logging

from create_profiles.create_profiles_combined_type import create_profiles_combined_type
from check_and_save.save_output import check_and_save_combined
from check_and_save.save_output import check_and_save_per_type


def post_process_cruise_objs_by_collection(cruises_profiles_objs):

    for cruise_profiles_obj in cruises_profiles_objs:

        logging.info(f"INSIDE post_process_cruise_objs_by_collection")

        expocode = cruise_profiles_obj['cruise_expocode']
        #profiles_objs = cruise_profiles_obj['profiles_objs']
        profiles_objs = cruise_profiles_obj['all_data_types_profile_objs']

        # In profiles objs, the combined type may just
        # be btl and not ctd or ctd and not btl
        # but within all the profiles, if find a btl and ctd
        # apply combined function

        is_btl = any([True if profiles_obj['data_type'] ==
                      'btl' else False for profiles_obj in profiles_objs])

        is_ctd = any([True if profiles_obj['data_type'] ==
                      'ctd' else False for profiles_obj in profiles_objs])

        if is_btl and is_ctd:

            logging.info("Combining btl and ctd")
            logging.info(f"for cruise {expocode}")

            # filter measurements by hierarchy
            #  when combine btl and ctd profiles.
            # didn't filter btl or ctd first in case need
            # a variable from both
            profiles_btl_ctd_objs = create_profiles_combined_type(
                profiles_objs)

            # Now check if profiles have CTD vars and should be saved
            # filter btl and ctd measurements separately
            check_and_save_combined(profiles_btl_ctd_objs)

        else:
            for file_obj_profiles in profiles_objs:
                check_and_save_per_type(file_obj_profiles)
