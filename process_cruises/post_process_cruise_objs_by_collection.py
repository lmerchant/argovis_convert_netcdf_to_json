import logging

from create_profiles.create_profiles_combined_type import create_profiles_combined_type
from create_profiles.update_profiles_single_type import update_profiles_single_type
from check_and_save.save_output import save_data_type_profiles


def post_process_cruise_objs_by_collection(cruise_objs_by_type):

    # All variables have been renamed prior to combining as a collection

    # TODO
    # check that I fixed this
    # Mapping is wrong. Points to oxygen when it should point to doxy
    # doxy data points occure in the data array toward the end for
    # station 1, cast 1
    # logging.info(cruise_objs_by_type)
    # logging.info(f"\n\n\n")
    # exit(1)

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

            # But for key source_info['instrument'], shouldn't
            # just be a global value of 'ship_ctd_btl' because
            # sometimes it's a combined cruise, but a data type profile may
            # not be at that station cast

            # filter measurements by hierarchy
            #  when combine btl and ctd profiles.
            # didn't filter btl or ctd first in case need
            # a variable from both

            # Each obj in all_data_types_profile_objs is a
            # dict with keys station_cast, data_type, profile_dict
            all_profiles = create_profiles_combined_type(
                all_data_types_profile_objs)

            save_data_type_profiles(all_profiles)

        else:

            if is_btl:
                data_type = 'btl'
            elif is_ctd:
                data_type = 'ctd'

            # For meta data, add meta with data_type suffix removed
            all_profiles = update_profiles_single_type(
                all_data_types_profile_objs, data_type)

            # Inside save, if not btl_ctd data type, will filter out
            # measurements objects with temp = NaN
            save_data_type_profiles(all_profiles)
