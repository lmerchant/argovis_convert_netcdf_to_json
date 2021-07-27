from process_cruises.process_cruise_objs_by_type_dask import process_cruise_objs_by_type_dask
from process_cruises.process_cruise_objs_by_type_dask import process_cruise_objs_by_type_dask_alt
from process_cruises.post_process_cruise_objs_by_collection import post_process_cruise_objs_by_collection
from check_and_save.add_vars_to_logged_collections_dask import gather_included_excluded_vars
from check_and_save.save_output import save_included_excluded_goship_vars_dask


def process_batch_of_cruises_dask(cruise_objs):

   # Return a list of objs with keys
    # cruise_expocode and profiles_objs

    # cruises_profiles_objs is for a batch of cruises
    # And within cruise, get set of profiles by type
    # If there is both a ctd and btl, will get two types back
    cruises_profiles_objs = process_cruise_objs_by_type_dask(cruise_objs)

    #cruises_profiles_objs = process_cruise_objs_by_type_dask_alt(cruise_objs)

    # ***********************************
    # Write included/excluded goship vars
    # ***********************************

    cruises_all_included, cruises_all_excluded = gather_included_excluded_vars(
        cruises_profiles_objs)

    save_included_excluded_goship_vars_dask(
        cruises_all_included, cruises_all_excluded)

    # ***********************************************
    # Post process cruise objs by collection
    # If cruise has both ctd and btl, combine and save
    # If cruise only has btl or ctd, save
    # ************************************************

    post_process_cruise_objs_by_collection(cruises_profiles_objs)
