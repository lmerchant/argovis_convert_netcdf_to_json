import logging
import os


from global_vars import GlobalVars
from check_and_save.check_of_ctd_vars import check_of_ctd_vars
import check_and_save.save_output as save


def check_and_save_combined(profiles_btl_ctd, collections):

    # Now check if profiles have CTD vars and should be saved
    # And filter btl and ctd measurements separately

    checked_ctd_variables, ctd_vars_flag = check_of_ctd_vars(
        profiles_btl_ctd)

    if ctd_vars_flag:
        logging.info('----------------------')
        logging.info('Saving files')
        logging.info('----------------------')
        save.write_profile_goship_units(checked_ctd_variables)

        save.save_all_btl_ctd_profiles(
            checked_ctd_variables, collections)

    else:
        logging.info("*** Cruise not converted ***")

        profile = profiles_btl_ctd[0]
        cruise_expocode = profile['profile_dict']['meta']['expocode']

        filename = 'cruises_not_converted.txt'
        filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)
        with open(filepath, 'a') as f:
            f.write(f"{cruise_expocode}\n")
