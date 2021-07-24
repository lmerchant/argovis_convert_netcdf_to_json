import logging
import os

from global_vars import GlobalVars
from create_profiles.filter_measurements import filter_measurements
from check_and_save.check_of_ctd_vars import check_of_ctd_vars
import check_and_save.save_output as save


def check_and_save_per_type(file_obj_profile):

    # Now check if profiles have CTD vars and should be saved
    # And filter btl and ctd measurements separately

    data_type = file_obj_profile['data_type']
    file_profiles = file_obj_profile['profiles']

    # filter measurements for core using hierarchy
    file_profiles = filter_measurements(file_profiles, data_type)

    checked_ctd_variables, ctd_vars_flag = check_of_ctd_vars(file_profiles)

    if ctd_vars_flag:
        logging.info('----------------------')
        logging.info('Saving files')
        logging.info('----------------------')

        save.write_profile_goship_units(
            checked_ctd_variables)

        save.save_all_profiles_one_type(checked_ctd_variables)

    else:
        logging.info("*** Cruise not converted ***")

        profile = file_profiles[0]
        cruise_expocode = profile['profile_dict']['meta']['expocode']

        filename = 'cruises_not_converted.txt'
        filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)
        with open(filepath, 'a') as f:
            f.write(f"{cruise_expocode}\n")
