import logging

#from process_cruises.process_cruise_objs_by_type import process_cruise_objs_by_type
from process_cruises.process_cruise_objs_by_type import process_cruise_objs_by_type
from process_cruises.post_process_cruise_objs_by_type import post_process_cruise_objs_by_type
from process_cruises.post_process_cruise_objs_by_collection import post_process_cruise_objs_by_collection

from check_and_save.save_output import save_data_type_profiles

# from xarray_and_dask.apply_argovis_names_and_conversions import apply_argovis_names_and_conversions

from check_and_save.add_vars_to_logged_collections import gather_included_excluded_vars
from check_and_save.save_output import save_included_excluded_goship_vars
#from check_and_save.remove_non_ctd_profiles import remove_non_ctd_profiles


def process_batch_of_cruises(cruise_objs):

   # Return a list of objs with keys
    # cruise_expocode and profiles_objs

    # cruises_profiles_objs is for a batch of cruises

    # And within cruise, get set of profiles by type
    # If there is both a ctd and btl, will get two types back

    # Instead of bunching by xarray type, dask type, and
    # profiles. Run through all steps in series
    # slightly slower. but same result. Easier logging this way

    logging.info('-------------------------------')
    logging.info("Create profiles for cruises")
    logging.info('-------------------------------')

    cruise_objs_by_type = process_cruise_objs_by_type(cruise_objs)

    # **********************************
    # Rename variables to ArgoVis names
    # (add suffix and rename core)
    # And create mapping keys for Argovis values
    # **********************************

    # Redo name and unit change functions for Argovis

    # Load dict back into pandas dataframe to rename and then
    # save back as json.
    #
    # TODO
    # Make sure the precision didn't change
    # from the c_format

    # Don't rename measurements vars from goship names
    # becase want to use it to determine hierarchy when combining
    cruise_objs_by_type = post_process_cruise_objs_by_type(
        cruise_objs_by_type)

    # one_cruise = cruise_objs_by_type[0]
    # type_objs = one_cruise['all_data_types_profile_objs']

    # one_type = type_objs[0]

    # profiles = one_type['data_type_profiles_list']

    # one_profile = profiles[0]

    # print(one_profile)

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
        cruise_objs_by_type)

    # Rename included and excluded vars and save
    save_included_excluded_goship_vars(
        cruises_all_included, cruises_all_excluded)

    # ***********************************************
    # Post process cruise objs by collection
    # If cruise has both ctd and btl, combine and save.
    # If cruise only has btl or ctd, save that.
    # It can happen that one station_cast only exists
    # for one data type, so save that uncombined
    # ************************************************

    logging.info('-----------------------------------------')
    logging.info("Combine btl/ctd if both found and save")
    logging.info("Filter out single data type to save next")
    logging.info('-----------------------------------------')

    single_data_type_cruises = post_process_cruise_objs_by_collection(
        cruise_objs_by_type)

    for cruise_obj in single_data_type_cruises:

        all_data_types_profile_objs = cruise_obj['all_data_types_profile_objs']

        for data_type_obj_profiles in all_data_types_profile_objs:
            save_data_type_profiles(data_type_obj_profiles)

    # ----------------
