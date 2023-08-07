import logging

from process_cruises.process_cruise_objs_by_type import process_cruise_objs_by_type
from process_cruises.post_process_cruise_objs_by_type import (
    post_process_cruise_objs_by_type,
)
from check_and_save.save_output import save_cruise_objs_by_type
from check_and_save.add_vars_to_logged_collections import gather_included_excluded_vars
from check_and_save.save_output import save_included_excluded_argovis_vars


def process_batch_of_cruises(cruise_objs):
    # Return a list of objs with keys
    # cruise_expocode and profiles_objs

    # cruises_profiles_objs is for a batch of cruises

    # And within cruise, get set of profiles by type
    # If there is both a ctd and btl, will get two types back

    logging.info("---------------------------")
    logging.info("Create profiles for cruises")
    logging.info("---------------------------")

    cruise_objs_by_type = process_cruise_objs_by_type(cruise_objs)

    # Uncomment the following to see format of one profile after it has been processed
    # one_cruise = cruise_objs_by_type[0]

    # type_objs = one_cruise["all_data_types_profile_objs"]
    # one_type = type_objs[0]
    # profiles = one_type["data_type_profiles_list"]

    # one_profile = profiles[1]
    # print(one_profile["profile_dict"]["cchdo_param_names"])
    # print(f"\n")
    # print(one_profile)
    # exit(1)

    # **********************************
    # Post process cruise objs by type
    # **********************************

    # TODO
    # Make sure the precision didn't change
    # from the c_format

    (
        cruise_objs_by_type,
        kept_vars_all_cruise_objs,
        deleted_vars_all_cruise_objs,
    ) = post_process_cruise_objs_by_type(cruise_objs_by_type)

    # ***********************************
    # Write included/excluded cchdo vars
    # ***********************************

    logging.info("------------------------------")
    logging.info("Log excluded and included vars")
    logging.info("------------------------------")

    kept_vars, deleted_vars = gather_included_excluded_vars(
        kept_vars_all_cruise_objs, deleted_vars_all_cruise_objs
    )

    save_included_excluded_argovis_vars(kept_vars, deleted_vars)

    # ******************************
    # Save cruise objs to JSON files
    # ******************************

    save_cruise_objs_by_type(cruise_objs_by_type)
