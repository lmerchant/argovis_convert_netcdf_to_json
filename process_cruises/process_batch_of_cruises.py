import logging

#from process_cruises.process_cruise_objs_by_type import process_cruise_objs_by_type
from process_cruises.process_cruise_objs_by_type_series import process_cruise_objs_by_type_series
from process_cruises.post_process_cruise_objs_by_collection import post_process_cruise_objs_by_collection
from check_and_save.add_vars_to_logged_collections import gather_included_excluded_vars
from check_and_save.save_output import save_included_excluded_goship_vars
#from check_and_save.remove_non_ctd_profiles import remove_non_ctd_profiles


def process_batch_of_cruises(cruise_objs):

   # Return a list of objs with keys
    # cruise_expocode and profiles_objs

    # cruises_profiles_objs is for a batch of cruises

    # And within cruise, get set of profiles by type
    # If there is both a ctd and btl, will get two types back

    # Group together xarry objs, then dask objs, then
    # profiles
    # cruises_profiles_objs = process_cruise_objs_by_type(cruise_objs)

    # Instead of bunching by xarray type, dask type, and
    # profiles. Run through all steps in series
    # slightly slower. but same result. Easier logging this way

    logging.info('-------------------------------')
    logging.info("Create profiles for cruises")
    logging.info('-------------------------------')

    cruises_profiles_objs = process_cruise_objs_by_type_series(cruise_objs)

    # ***********************************
    # Write included/excluded goship vars
    # ***********************************

    # If profile not included, I guess that means it
    # is excluded? But does that make sense if
    # entire cruise excluded?
    logging.info('-------------------------------')
    logging.info("Log excluded and included vars")
    logging.info('-------------------------------')

    cruises_all_included, cruises_all_excluded = gather_included_excluded_vars(
        cruises_profiles_objs)

    save_included_excluded_goship_vars(
        cruises_all_included, cruises_all_excluded)

    # ***********************************************
    # Post process cruise objs by collection
    # If cruise has both ctd and btl, combine and save.
    # If cruise only has btl or ctd, save that
    # Can happen that one station_cast only exists
    # for one data type
    # ************************************************

    logging.info('-----------------------------------------')
    logging.info("Combine btl/ctd if necessary and save all")
    logging.info('-----------------------------------------')

    post_process_cruise_objs_by_collection(cruises_profiles_objs)
