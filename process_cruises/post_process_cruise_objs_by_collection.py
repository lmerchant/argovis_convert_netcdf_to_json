import logging
from pathlib import Path

from global_vars import GlobalVars

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
        expocode = cruise_obj["cruise_expocode"]

        all_data_types_profile_objs = cruise_obj["all_data_types_profile_objs"]

        logging.info("****************************")
        logging.info(f"Post processing {expocode}")
        logging.info("****************************")

        # Save expocode processed to a file collecting all processed
        processed_cruises_file = (
            Path(GlobalVars.LOGGING_DIR) / "all_cruises_processed.txt"
        )
        with open(processed_cruises_file, "a") as f:
            f.write(f"{expocode}\n")

        # change this to skip updating single type profile since this
        # now only updated a meta profile that said the instrument type
        # of the file, which could have been btl_ctd in the past

        # For meta data, add meta with data_type suffix removed
        all_profiles = update_profiles_single_type(all_data_types_profile_objs)

        profile = all_profiles[0]

        # print(profile["profile_dict"]["data"])

        # Change save function to use cruise_obj and not profiles

        # Inside save, if not btl_ctd data type, will filter out
        # measurements objects with temp = NaN
        save_data_type_profiles(all_profiles)
