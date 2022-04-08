import logging

from process_cruises.process_cruise_objs_by_type import process_cruise_objs_by_type
from process_cruises.post_process_cruise_objs_by_type import post_process_cruise_objs_by_type
from process_cruises.post_process_cruise_objs_by_collection import post_process_cruise_objs_by_collection

#from check_and_save.save_output import save_data_type_profiles_per_type
from check_and_save.add_vars_to_logged_collections import gather_included_excluded_vars
from check_and_save.save_output import save_included_excluded_cchdo_vars


def process_batch_of_cruises(cruise_objs):

    # Return a list of objs with keys
    # cruise_expocode and profiles_objs

    # cruises_profiles_objs is for a batch of cruises

    # And within cruise, get set of profiles by type
    # If there is both a ctd and btl, will get two types back

    logging.info('-------------------------------')
    logging.info("Create profiles for cruises")
    logging.info('-------------------------------')

    cruise_objs_by_type = process_cruise_objs_by_type(cruise_objs)

    # one_cruise = cruise_objs_by_type[0]
    # type_objs = one_cruise['all_data_types_profile_objs']

    # one_type = type_objs[0]

    # profiles = one_type['data_type_profiles_list']

    # one_profile = profiles[0]

    # print(one_profile['profile_dict']['cchdo_param_names'])

    # print(f"\n")

    # print(one_profile)

    # **********************************
    # Rename variables to ArgoVis names
    # (add suffix and rename core)
    # And create mapping keys for Argovis values
    # **********************************

    # Load dict back into pandas dataframe to rename and then
    # save back as json.
    #
    # TODO
    # Make sure the precision didn't change
    # from the c_format

    # Rename variables and add argovis mappings

    # TODO
    # Should be throwing out variables with all NaN values before
    # deciding how to name things.
    # For example, a btl file dataset can have both oxygen and
    # ctd oxygen, but if ctd oxygen is all Nan, it's not renaming
    # oxygen to doxy like it should be.

    cruise_objs_by_type = post_process_cruise_objs_by_type(
        cruise_objs_by_type)

    # one_cruise = cruise_objs_by_type[0]
    # type_objs = one_cruise['all_data_types_profile_objs']

    # print(f"total objs = {len(type_objs)}")

    # # one_type = type_objs[0] # ('get btl if have btl_ctd type)
    # one_type = type_objs[1]  # (get ctd if have btl_ctd type)

    # profiles = one_type['data_type_profiles_list']

    # one_profile = profiles[15]

    # print(one_profile['profile_dict']['data_keys'])

    # print(f"\n")

    # print(one_profile)

    # exit(1)

    # ***********************************
    # Write included/excluded cchdo vars
    # ***********************************

    logging.info('-------------------------------')
    logging.info("Log excluded and included vars")
    logging.info('-------------------------------')

    cruises_all_included, cruises_all_excluded = gather_included_excluded_vars(
        cruise_objs_by_type)

    # one_cruise = cruise_objs_by_type[0]
    # type_objs = one_cruise['all_data_types_profile_objs']

    # one_type = type_objs[0]

    # profiles = one_type['data_type_profiles_list']

    # one_profile = profiles[15]

    # print(one_profile['profile_dict']['data_keys'])

    # exit(1)

    # Rename included and excluded vars and save
    save_included_excluded_cchdo_vars(
        cruises_all_included, cruises_all_excluded)

    # ***********************************************
    # Post process cruise objs by collection
    # If cruise has both ctd and btl, combine and save.
    # If cruise only has btl or ctd, save that.
    # It can happen that one station_cast only exists
    # for one data type, so save that uncombined
    #
    # Add a suffix of _btl or _ctd
    # ************************************************

    logging.info('-----------------------------------------')
    logging.info("Combine btl/ctd if both found and save")
    logging.info("Filter out single data type to save next")
    logging.info('-----------------------------------------')

    post_process_cruise_objs_by_collection(cruise_objs_by_type)
