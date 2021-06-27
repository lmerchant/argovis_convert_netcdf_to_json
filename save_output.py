# Save output

import os
import json
import numpy as np
import logging
import re


def convert(o):

    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)


def write_profile_goship_units(profile_dict, logging_dir):

    type = profile_dict['type']

    filename = 'files_goship_units.txt'
    filepath = os.path.join(logging_dir, filename)

    if type == 'btl':
        goship_units = profile_dict['goshipUnits']

    if type == 'ctd':
        goship_units = profile_dict['goshipUnits']

    if type == 'btl_ctd':
        try:
            goship_units_btl = profile_dict['goshipUnitsBtl']
            goship_units_ctd = profile_dict['goshipUnitsCtd']
            goship_units = {**goship_units_btl, **goship_units_ctd}
        except:
            goship_units = profile_dict['goshipUnits']

    with open(filepath, 'a') as f:
        json.dump(goship_units, f, indent=4,
                  sort_keys=True, default=convert)


def write_profile_json(cruise_expocode, json_dir, profile_dict):

    station_cast = profile_dict['stationCast']

    profile_dict.pop('stationCast', None)
    profile_dict.pop('type', None)

    # Remove  time from meta since it was just used to create date variable
    profile_dict['meta'].pop('time', None)

    # Pop off meta key and use as start of data_dict
    meta_dict = profile_dict.pop('meta', None)

    # Now combine with left over profile_dict
    data_dict = {**meta_dict, **profile_dict}

    # TODO
    # ask
    # probably use cruise expocode instead of that in file

    id = data_dict['id']

    # TODO
    # When create file id, ask if use cruise expocode instead
    filename = f"{id}.json"

    expocode = data_dict['expocode']

    json_str = json.dumps(data_dict)

    # TODO
    # check did this earlier in program
    # _qc":2.0
    # If qc value in json_str matches '.0' at end, remove it to get an int qc
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

    if '/' in filename:
        filename = filename.replace('/', '_')

    if '/' in expocode:
        folder = expocode.replace('/', '_')
    else:
        folder = expocode

    path = os.path.join(json_dir, folder)

    if not os.path.exists(path):
        os.makedirs(path)

    file = os.path.join(json_dir, folder, filename)

    # TESTING
    # TODO Remove formatting when final

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable

    # TODO
    # Sort keys or not?
    # with open(file, 'w') as f:
    #     json.dump(data_dict, f, indent=4, sort_keys=True, default=convert)
    # Sort keys or not?
    with open(file, 'w') as f:
        json.dump(data_dict, f, indent=4, sort_keys=False, default=convert)

    logging.info(f"Converted json saved for {expocode} {station_cast}")


def save_output(checked_ctd_variables, logging_dir, json_directory):

    for ctd_var_check in checked_ctd_variables:

        has_all_ctd_vars = ctd_var_check['has_all_ctd_vars']
        type = ctd_var_check['type']
        station_cast = ctd_var_check['station_cast']
        profile = ctd_var_check['profile_checked']
        profile_dict = profile['profile_dict']
        expocode = profile_dict['meta']['expocode']

        # Write one profile goship units to
        # keep a record of what units need to be converted
        write_goship_units = True
        if write_goship_units:
            write_goship_units = False
            write_profile_goship_units(profile_dict, logging_dir)

        if has_all_ctd_vars[type]:
            write_profile_json(
                expocode, json_directory, profile_dict)

            print(f"Files saved for cruies {expocode} {station_cast}")
        else:
            # Write to a file the cruise not converted
            logging.info(
                f"Cruise not converted for type {type} and {expocode} {station_cast}")
            filename = 'cruises_not_converted.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode} {station_cast}\n")
                f.write(f"collection type {type}")


def save_output_btl_ctd(checked_ctd_variables, logging_dir, json_directory):

    for ctd_var_check in checked_ctd_variables:
        has_all_ctd_vars = ctd_var_check['has_all_ctd_vars']
        has_ctd_vars_no_qc = ctd_var_check['has_ctd_vars_no_qc']
        has_ctd_vars_unk_ref_scale = ctd_var_check['has_ctd_temp_unk']

        profile = ctd_var_check['profile_checked']

        profile_dict = profile['profile_dict']
        station_cast = profile['station_cast']
        expocode = profile_dict['meta']['expocode']

        # Write one profile goship units to
        # keep a record of what units need to be converted
        write_goship_units = True
        if write_goship_units:
            write_goship_units = False
            write_profile_goship_units(profile_dict, logging_dir)

        if has_all_ctd_vars['btl'] or has_all_ctd_vars['ctd']:
            write_profile_json(
                expocode, json_directory, profile_dict)
        else:
            # Write to a file the cruise not converted
            logging.info(
                f"Cruise not converted {expocode} {station_cast}")
            filename = 'cruises_not_converted.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
