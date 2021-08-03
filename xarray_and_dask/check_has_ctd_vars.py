# Check if files have pressure and ctd_temperature to be a
# proper file for ArgoVis

import os
import logging

from global_vars import GlobalVars


def log_ctd_var_status(file_obj, has_pres,
                       has_ctd_temp, has_ctd_temp_ref_scale):

    logging_dir = GlobalVars.LOGGING_DIR

    expocode = file_obj['cruise_expocode']
    data_type = file_obj['data_type']

    # No pressure
    if not has_pres:
        filename = 'cruises_no_pressure.txt'
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

        filename = 'cruises_not_converted.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(f"{expocode} {data_type}\n")

    # Not looking at pressure next but whether an indivdual type has ctd temperature
    if not has_ctd_temp:
        filename = 'cruises_no_ctd_temp.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(f"expocode {expocode} {data_type}\n")


def check_has_ctd_vars(file_obj):

    nc = file_obj['nc']

    params = list(nc.keys())

    ctd_temps = ['ctd_temperature', 'ctd_temperature_68']

    # Check to see if have all ctd vars
    # CTD vars are ctd temperature and pressure

    # Look for ctd_temperature
    found_ctd_temps = [param for param in params if param in ctd_temps]

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

    if has_pres and has_ctd_temp and has_ctd_temp_ref_scale:
        has_all_ctd_vars = True
    else:
        has_all_ctd_vars = False

    # Now find which variables that are missing and write to files
    log_ctd_var_status(file_obj, has_pres,
                       has_ctd_temp, has_ctd_temp_ref_scale)

    return has_all_ctd_vars
