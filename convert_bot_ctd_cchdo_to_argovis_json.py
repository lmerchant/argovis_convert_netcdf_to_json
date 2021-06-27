import requests
# from requests.adapters import HTTPAdapter
# from requests.adapters import proxy_from_url
import fsspec
import xarray as xr
from datetime import datetime
from datetime import date
import errno
import os
import logging
import click

import get_variable_mappings as gvm
import rename_objects as rn
import check_if_has_ctd_vars as ckvar
import filter_profiles as fp
import get_cruise_information as gi
import create_profiles_one_type as op
import create_profiles_combined_type as cbp
import save_output as sv


# In order to use xarray open_dataset

# https://github.com/pydata/xarray/issues/3653
# pip install aiohttp
# pip install h5netcdf

# url = 'https://cchdo.ucsd.edu/data/16923/318M20130321_bottle.nc'
# with fsspec.open(url) as fobj:
#     ds = xr.open_dataset(fobj)


"""

Convert CCHDO CTD and bottle CF netCDF files to ArgoVis JSON format

program by: Lynne Merchant
date: 2021

"""


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


def remove_file(filename, dir):
    filepath = os.path.join(dir, filename)

    try:
        os.remove(filepath)
    except OSError as e:
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occurred


def read_file_test(data_obj):

    data_path = data_obj['data_path']

    nc = xr.open_dataset(data_path)

    data_obj['nc'] = nc

    file_expocode = nc.coords['expocode'].data[0]

    meta_names, param_names = op.get_meta_param_names(nc)

    data_obj['meta'] = meta_names
    data_obj['param'] = param_names

    data_obj['file_expocode'] = file_expocode

    return data_obj


def read_file(data_obj):

    flag = ''

    data_path = data_obj['data_path']

    data_url = f"https://cchdo.ucsd.edu{data_path}"

    try:
        with fsspec.open(data_url) as fobj:
            nc = xr.open_dataset(fobj)
    except:
        logging.warning(f"Error reading in file {data_url}")
        flag = 'error'
        return data_obj, flag

    data_obj['nc'] = nc

    meta_names, param_names = op.get_meta_param_names(nc)

    data_obj['meta'] = meta_names
    data_obj['param'] = param_names

    file_expocode = nc.coords['expocode'].data[0]

    data_obj['file_expocode'] = file_expocode

    return data_obj, flag


def process_ctd(ctd_obj):

    print('---------------------------')
    print('Start processing ctd profiles')
    print('---------------------------')

    ctd_obj = gvm.create_goship_unit_mapping(ctd_obj)
    ctd_obj = gvm.create_goship_ref_scale_mapping(ctd_obj)

    # get c-format (string representation of numbers)
    ctd_obj = gvm.create_goship_c_format_mapping(ctd_obj)

    # TODO
    # Are there other variables to convert besides ctd_temperature?
    # And if I do, would need to note in argovis ref scale mapping
    # that this temperare is on a new scale.
    # I'm assuming goship will always refer to original vars before
    # values converted.

    # What if other temperatures not on ITS-90 scale?
    ctd_obj = op.convert_goship_to_argovis_ref_scale(ctd_obj)

    # Add convert units function

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    ctd_profiles = op.create_profiles(ctd_obj)

    # Rename with _ctd suffix unless it is an Argovis variable
    # But no _ctd suffix to meta data
    renamed_ctd_profiles = rn.rename_profiles_to_argovis(ctd_profiles, 'ctd')

    print('---------------------------')
    print('Processed ctd profiles')
    print('---------------------------')

    return renamed_ctd_profiles


def process_bottle(btl_obj):

    print('---------------------------')
    print('Start processing bottle profiles')
    print('---------------------------')

    btl_obj = gvm.create_goship_unit_mapping(btl_obj)
    btl_obj = gvm.create_goship_ref_scale_mapping(btl_obj)

    # get c-format (string representation of numbers)
    btl_obj = gvm.create_goship_c_format_mapping(btl_obj)

    # Only converting temperature so far
    btl_obj = op.convert_goship_to_argovis_ref_scale(btl_obj)

    # Add convert units function

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    btl_profiles = op.create_profiles(btl_obj)

    # Rename with _btl suffix unless it is an Argovis variable
    # But no _btl suffix to meta data
    # Add _btl when combine files
    renamed_btl_profiles = rn.rename_profiles_to_argovis(btl_profiles, 'btl')

    print('---------------------------')
    print('Processed btl profiles')
    print('---------------------------')

    return renamed_btl_profiles


def setup_test_obj(dir, filename, type):

    if type == 'btl':
        btl_obj = {}
        btl_obj['found'] = True
        btl_obj['type'] = 'btl'
        btl_obj['data_path'] = os.path.join(dir, filename)
        btl_obj['filename'] = filename

        return btl_obj

    if type == 'ctd':
        ctd_obj = {}
        ctd_obj['found'] = True
        ctd_obj['type'] = 'ctd'
        ctd_obj['data_path'] = os.path.join(dir, filename)
        ctd_obj['filename'] = filename

        return ctd_obj


def setup_testing(btl_file, ctd_file, test_btl, test_ctd):

    # TODO Set  up start and end date for logging

    input_dir = './testing_data/modify_data_for_testing'
    output_dir = './testing_output'
    os.makedirs(output_dir, exist_ok=True)

    btl_obj = {}
    ctd_obj = {}
    btl_obj['found'] = False
    ctd_obj['found'] = False

    cruise_info = {}
    cruise_info['btl'] = {}
    cruise_info['ctd'] = {}
    cruise_info['btl']['found'] = False
    cruise_info['ctd']['found'] = False
    cruise_info['expocode'] = 'testing'
    cruise_info['cruise_id'] = None

    all_cruises_info = []
    all_cruises_info.append(cruise_info)

    # Enter test files
    if test_btl:
        btl_obj = setup_test_obj(input_dir, btl_file, 'btl')

        logging.info("======================\n")
        logging.info(f"Processing btl test file {btl_file}")

    if test_ctd:
        ctd_obj = setup_test_obj(input_dir, ctd_file, 'ctd')

        logging.info("======================\n")
        logging.info(f"Processing ctd test file {ctd_file}")

    return output_dir, all_cruises_info, btl_obj, ctd_obj


def setup_logging(clear_old_logs):

    logging_dir = './logging'
    os.makedirs(logging_dir, exist_ok=True)

    if clear_old_logs:
        remove_file('output.log', logging_dir)
        remove_file('file_read_errors.txt', logging_dir)
        remove_file('found_goship_units.txt', logging_dir)
        remove_file('cruises_no_core_ctd_vars.txt', logging_dir)
        remove_file('cruises_w_ctd_temp_no_qc.txt', logging_dir)
        remove_file('cruises_w_ctd_temp_no_ref_scale.txt', logging_dir)
        remove_file('cruises_no_ctd_temp.txt', logging_dir)
        remove_file('cruises_no_expocode.txt', logging_dir)
        remove_file('cruises_no_pressure.txt', logging_dir)
        remove_file('found_cruises_with_coords_netcdf.txt', logging_dir)
        remove_file('diff_cruise_and_file_expocodes.txt', logging_dir)
        remove_file('cruises_not_converted.txt', logging_dir)

    filename = 'output.log'
    logging_path = os.path.join(logging_dir, filename)
    logging.root.handlers = []
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                        level=logging.INFO, filename=logging_path)

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

    return logging_dir, logging


@click.group(invoke_without_command=True)
@click.pass_context
@click.option('-s', '--start_year', default=1950, help='Start year')
@click.option('-e', '--end_year', default=date.today().year, help='End year')
# @click.option('-e', '--end_year', help='End year', is_flag=True, default=date.today().year, is_eager=True)
@click.option('-c', '--clear', default=True, is_flag=True, help='Clear logs')
def get_user_input(start_year, end_year, clear):

    logging.info(f"Converting years Jan 1, {start_year} to Dec 31, {end_year}")
    click.echo(f"Converting years Jan 1, {start_year} to Dec 31, {end_year}")

    start_datetime = datetime(start_year, 1, 1)
    end_datetime = datetime(end_year, 12, 31)

    return start_datetime, end_datetime, clear


@click.command()
@click.option('-s', '--start_year', default=1950, help='Start year')
@click.option('-e', '--end_year', default=date.today().year, help='End year')
# @click.option('-e', '--end_year', help='End year', is_flag=True, default=date.today().year, is_eager=True)
@click.option('-c', '--clear', default=True, is_flag=True, help='Clear logs')
def main(start_year, end_year, clear):

    program_start_time = datetime.now()

    logging_dir, logging = setup_logging(clear)
    logging.info(f"Converting years Jan 1, {start_year} to Dec 31, {end_year}")

    start_datetime = datetime(start_year, 1, 1)
    end_datetime = datetime(end_year, 12, 31)

    # First create a list of the cruises found
    # with netCDF files

    session = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount('https://', a)

    json_directory = './converted_data'
    os.makedirs(json_directory, exist_ok=True)

    # TESTING
    testing = False
    test_btl_obj = {}
    test_ctd_obj = {}

    if testing:

        # Change this to do bot and ctd separate or combined
        test_btl = False
        test_ctd = True

        btl_file = 'modified_318M20130321_bottle_no_psal.nc'
        ctd_file = 'modified_318M20130321_ctd_core_bad_flag.nc'

        json_directory, all_cruises_info, test_btl_obj, test_ctd_obj = setup_testing(
            btl_file, ctd_file, test_btl, test_ctd)

        btl_found = test_btl_obj['found']
        ctd_found = test_ctd_obj['found']

    else:
        # Loop through all cruises and grap NetCDF files
        all_cruises_info = gi.get_cruise_information(
            session, logging_dir, start_datetime, end_datetime)

        if not all_cruises_info:
            logging.info('No cruises within dates selected')
            exit(1)

    read_error_count = 0
    data_file_read_errors = []

    for cruise_info in all_cruises_info:

        if not testing:

            cruise_expocode = cruise_info['expocode']

            logging.info("--------------------------------")
            logging.info(f"Start converting Cruise: {cruise_expocode}")
            logging.info("--------------------------------")

            btl_found = cruise_info['btl']['found']
            ctd_found = cruise_info['ctd']['found']

            cruise_info['btl']['cruise_expocode'] = cruise_expocode
            cruise_info['ctd']['cruise_expocode'] = cruise_expocode

        if btl_found:

            if testing:
                btl_obj = read_file_test(test_btl_obj)

            else:
                btl_obj = cruise_info['btl']

                btl_obj, flag = read_file(btl_obj)

                if flag == 'error':
                    print("Error reading file")
                    read_error_count = read_error_count + 1
                    data_url = f"https://cchdo.ucsd.edu{btl_obj['data_path']}"
                    data_file_read_errors.append(data_url)
                    continue

                # Check if file expocode is None
                # TODO Do we want to skip these?
                # Or were they all fixed?

                cruise_expocode = btl_obj['cruise_expocode']
                file_expocode = btl_obj['file_expocode']
                if file_expocode == 'None':
                    logging.info(f'No file expocode for {cruise_expocode}')
                    filename = 'files_no_expocode.txt'
                    filepath = os.path.join(logging_dir, filename)
                    with open(filepath, 'a') as f:
                        f.write('-----------\n')
                        f.write(f"expocode {cruise_expocode}\n")
                        f.write(f"file type BTL\n")

            profiles_btl = process_bottle(btl_obj)

        if ctd_found:

            if testing:
                ctd_obj = read_file_test(test_ctd_obj)

            else:
                ctd_obj = cruise_info['ctd']

                ctd_obj, flag = read_file(ctd_obj)
                if flag == 'error':
                    print("Error reading file")
                    read_error_count = read_error_count + 1
                    data_url = f"https://cchdo.ucsd.edu{ctd_obj['data_path']}"
                    data_file_read_errors.append(data_url)
                    continue

                # Check if file expocode is None
                # TODO Do we want to skip these?
                cruise_expocode = ctd_obj['cruise_expocode']
                file_expocode = ctd_obj['file_expocode']
                if file_expocode == 'None':
                    logging.info(f'No file expocode for {cruise_expocode}')
                    filename = 'files_no_expocode.txt'
                    filepath = os.path.join(logging_dir, filename)
                    with open(filepath, 'a') as f:
                        f.write('-----------\n')
                        f.write(f"expocode {cruise_expocode}\n")
                        f.write(f"file type CTD\n")

            profiles_ctd = process_ctd(ctd_obj)

        if btl_found and ctd_found:

            # filter measurements when combine btl and ctd profiles
            # filter  btl_ctd first in case need
            # a variable from both before they are filtered out
            profiles_btl_ctd = cbp.combine_btl_ctd_profiles(
                profiles_btl, profiles_ctd)

            logging.info('---------------------------')
            logging.info('Processed btl and ctd combined profiles')
            logging.info('---------------------------')

        if btl_found and ctd_found:

            checked_ctd_variables = ckvar.check_of_ctd_variables(
                profiles_btl_ctd, logging, logging_dir)

            sv.save_output_btl_ctd(checked_ctd_variables,
                                   logging_dir, json_directory)

        elif btl_found:
            profiles_btl = fp.filter_measurements(
                profiles_btl, 'btl')

            checked_ctd_variables = ckvar.check_of_ctd_variables(
                profiles_btl, logging, logging_dir)

            sv.save_output(checked_ctd_variables, logging_dir, json_directory)

        elif ctd_found:
            profiles_ctd = fp.filter_measurements(
                profiles_ctd, 'ctd')

            checked_ctd_variables = ckvar.check_of_ctd_variables(
                profiles_ctd, logging, logging_dir)

            sv.save_output(checked_ctd_variables, logging_dir, json_directory)

        if btl_found or ctd_found:

            if btl_found:
                file_expocode = btl_obj['file_expocode']
                cruise_expocode = btl_obj['cruise_expocode']

            if ctd_found:
                file_expocode = ctd_obj['file_expocode']
                cruise_expocode = ctd_obj['cruise_expocode']

            # Write the following to a file to keep track of when
            # the cruise expocode is different from the file expocode
            if cruise_expocode != file_expocode:
                filename = 'diff_cruise_and_file_expocodes.txt'
                filepath = os.path.join(logging_dir, filename)
                with open(filepath, 'a') as f:
                    f.write(
                        f"Cruise: {cruise_expocode} File: {file_expocode}\n")

            # TODO
            # could turn info a  function and also
            # check if file expocode exists and has
            # different ship, country, and expocode
            # Would need cruise id to find cruise json

            logging.info('---------------------------')
            logging.info(
                f"Finished for cruise {cruise_expocode}")
            logging.info(f"Expocode inside file: {file_expocode}")
            logging.info('---------------------------')

            logging.info("*****************************\n")

    if read_error_count:
        filename = 'file_read_errors.txt'
        logging.warning(f"Errors reading in {read_error_count} files")
        logging.warning(f"See {filename} for files")

        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'w') as f:
            for line in data_file_read_errors:
                f.write(f"{line}\n")

    logging.info("Time to run program")
    logging.info(datetime.now() - program_start_time)


if __name__ == '__main__':
    main()
