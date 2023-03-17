# Check if files have pressure and ctd_temperature to be a
# proper file for ArgoVis

import os
import logging

from global_vars import GlobalVars


def has_two_ctd_temperatures(names):

    has_ctd_temperature = any(
        [True for name in names if name == 'ctd_temperature'])
    has_ctd_temperature_68 = any(
        [True for name in names if name == 'ctd_temperature_68'])

    if has_ctd_temperature and has_ctd_temperature_68:
        return True
    else:
        return False


def has_two_ctd_oxygens(names):

    has_ctd_oxygen = any(
        [True for name in names if name == 'ctd_oxygen'])
    has_ctd_oxygen_ml_l = any(
        [True for name in names if name == 'ctd_oxygen_ml_l'])

    if has_ctd_oxygen and has_ctd_oxygen_ml_l:
        return True
    else:
        return False


def log_ctd_var_status(file_obj, has_pres,
                       has_ctd_temp, has_ctd_temp_ref_scale, has_both_temp, has_both_oxy):

    logging_dir = GlobalVars.LOGGING_DIR

    expocode = file_obj['cchdo_cruise_meta']['expocode']
    data_type = file_obj['data_type']

    # No pressure
    if not has_pres:
        filename = 'cruise_files_no_pressure.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {expocode}\n")
            f.write(f"collection type {data_type}\n")

    if has_pres and has_ctd_temp and has_ctd_temp_ref_scale:
        has_all_ctd_vars = True
    else:
        has_all_ctd_vars = False

    # Has no ctd core variables with at least a ctd temperature
    # regardless of if it has a qc or not
    if not has_all_ctd_vars:
        # filename = 'cruises_no_core_ctd_vars.txt'
        # filepath = os.path.join(logging_dir, filename)
        # with open(filepath, 'a') as f:
        #     f.write('-----------\n')
        #     f.write(f"expocode {expocode}\n")
        #     f.write(f"collection type {data_type}\n")

        filename = 'cruise_files_not_converted.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(f"{expocode} {data_type}\n")

    # Not looking at pressure next but whether an indivdual type has ctd temperature
    if not has_ctd_temp:
        filename = 'cruise_files_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(f"expocode {expocode} {data_type}\n")

    if has_both_temp:
        filename = 'cruise_files_multiple_ctd_temp_oxy.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(
                f"expocode {expocode} {data_type} have both ctd_temperature and ctd_temperature_68\n")

    if has_both_oxy:
        filename = 'cruise_files_multiple_ctd_temp_oxy.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(
                f"expocode {expocode} {data_type} have both ctd_oxygen and ctd_oxygen_ml_l\n")


def check_has_ctd_vars(file_obj):

    nc = file_obj['nc']

    params = list(nc.keys())

    ctd_temps = ['ctd_temperature', 'ctd_temperature_68']

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    # Look for ctd_temperature
    found_ctd_temps = [param for param in params if param in ctd_temps]

    # TODO
    # What if all temperatures are not a 0 or 2 flag?
    # See 49NZ199909_2 cruise

    if found_ctd_temps:
        has_ctd_temp = True
    else:
        has_ctd_temp = False

    has_ctd_temp_ref_scale = False
    for temp in found_ctd_temps:

        try:
            ref_scale = nc[temp].attrs['reference_scale']
        except:
            ref_scale = None

        if ref_scale:
            has_ctd_temp_ref_scale = True
            break

    coords = list(nc.coords)
    has_pres = 'pressure' in coords

    # Check has only one CTD Temperature variable on one scale
    has_both_temp = has_two_ctd_temperatures(params)

    if has_both_temp:
        logging.info('Has two ctd temperature variable names')

    # Check has only one CTD oxygen with one unit value
    has_both_oxy = has_two_ctd_oxygens(params)

    if has_both_oxy:
        logging.info('Has two ctd oxygen variable names')

    if has_both_temp or has_both_oxy:
        has_single_ctd_temp_oxy = False
    else:
        has_single_ctd_temp_oxy = True

    if has_pres and has_ctd_temp and has_ctd_temp_ref_scale and has_single_ctd_temp_oxy:
        has_all_ctd_vars = True
    else:
        has_all_ctd_vars = False

    # Now find which variables that are missing and write to files
    log_ctd_var_status(file_obj, has_pres,
                       has_ctd_temp, has_ctd_temp_ref_scale, has_both_temp, has_both_oxy)

    return has_all_ctd_vars
