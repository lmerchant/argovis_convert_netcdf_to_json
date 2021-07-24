import requests
from datetime import datetime
import xarray as xr
import fsspec
import logging

from global_vars import GlobalVars
from process_cruise_objs_dask import process_cruise_objs_dask
from create_profiles.create_profiles_combined_type_dask import create_profiles_combined_type_dask
from check_and_save.check_and_save_per_type import check_and_save_per_type
from check_and_save.check_and_save_combined import check_and_save_combined
from check_and_save.add_vars_to_logged_collections import add_vars_to_logged_collections_dask
from check_and_save.save_output import save_included_excluded_goship_vars_dask


def read_file(file_obj):

    file_path = file_obj['file_path']

    try:
        # with fsspec.open(data_url) as fobj:
        #     nc = xr.open_dataset(fobj, engine='h5netcdf',
        #                          chunks={"N_PROF": CHUNK_SIZE})

        # Try this way to see if program
        # doesn't hang while reading in file
        with fsspec.open(file_path) as fobj:
            nc = xr.open_dataset(fobj, engine='h5netcdf')
            nc = nc.chunk(chunks={"N_PROF": GlobalVars.CHUNK_SIZE})

    except Exception as e:
        logging.warning(f"Error reading in file {file_path}")
        logging.warning(f"Error {e}")
        logging.info(
            f"Error reading file for cruise {file_obj['cruise_expocode']}")
        logging.info(f"Data type {file_obj['data_type']}")

        return {}

    file_obj['nc'] = nc

    return file_obj


def read_file_objs(file_objs):

    # If I read all at once, how to keep track of the meta data?
    # Inside each file, can get the profile type and file expocode
    # but not the cruise_expocodes.
    # Also want the cruise id and file hash. So read one by one into
    # a cruise obj that can hold multiple files if both btl and ctd

    cruise_files_obj = {}

    # Cruise expocode same if more than one file
    cruise_files_obj['cruise_expocode'] = file_objs[0]['cruise_expocode']

    cruise_file_objs = []

    for file_obj in file_objs:

        # Add netcdf file contents to file_obj
        new_file_obj = read_file(file_obj)

        cruise_file_objs.append(new_file_obj)

    cruise_files_obj['file_objs'] = cruise_file_objs

    return cruise_files_obj


def create_file_obj(cruise_json, file_json, file_hash):

    woce_lines_list = cruise_json['collections']['woce_lines']
    woce_lines = ','.join(woce_lines_list)

    data_type = file_json['data_type']

    if data_type == 'bottle':
        data_type = 'btl'
    elif data_type == 'ctd':
        data_type = 'ctd'
    else:
        return {}

    file_path = f"https://cchdo.ucsd.edu{file_json['file_path']}"

    file_obj = {}
    file_obj['data_type'] = data_type
    file_obj['cruise_expocode'] = cruise_json['expocode']
    file_obj['cruise_id'] = cruise_json['id']
    file_obj['woce_lines'] = woce_lines
    file_obj['file_path'] = file_path
    file_obj['filename'] = file_json['file_path'].split('/')[-1]
    file_obj['file_hash'] = file_hash

    return file_obj


def check_if_netcdf_data(file_json):

    file_role = file_json['role']
    data_format = file_json['data_format']
    data_type = file_json['data_type']

    is_netcdf_file = file_role == "dataset" and data_format == 'cf_netcdf'
    is_btl = data_type = 'bottle'
    is_ctd = data_type == 'ctd'

    if is_netcdf_file and (is_btl or is_ctd):
        return True
    else:
        return False


def get_netcdf_files_json(active_cruise_file_ids):

    netcdf_files = []

    for file_id in active_cruise_file_ids:

        query = f"{GlobalVars.API_END_POINT}/file/{file_id}"
        response = requests.get(query)

        if response.status_code != 200:
            print('api not reached in for file json')
            print(response)
            exit(1)

        file_json = response.json()

        is_netcdf = check_if_netcdf_data(file_json)

        if is_netcdf:
            file = {}
            file['id'] = file_id
            file['json'] = file_json

            netcdf_files.append(file)

    return netcdf_files


def check_if_in_time_range(cruise_json, time_range):

    cruise_start_date = cruise_json['startDate']

    # Some cruises are placeholders and have a blank start date
    if not cruise_start_date:
        return False

    cruise_datetime = datetime.strptime(cruise_start_date, "%Y-%m-%d")

    in_date_range = cruise_datetime >= time_range['start'] and cruise_datetime <= time_range['end']

    if not in_date_range:
        return False

    return True


def process_cruise(cruise_json, files_info, time_range):

    active_file_ids = files_info['active_file_ids']
    file_id_hash_mapping = files_info['file_hash_mapping']

    in_time_range = check_if_in_time_range(cruise_json, time_range)

    if not in_time_range:
        return []

    # Get files attached to the cruise
    # Could be deleted ones so check if exist in all_files
    cruise_file_ids = cruise_json['files']

    # Get only file_ids in active file ids
    active_cruise_file_ids = [
        id for id in cruise_file_ids if id in active_file_ids]

    netcdf_files = get_netcdf_files_json(active_cruise_file_ids)

    file_types = []
    file_objs = []

    for file in netcdf_files:

        file_id = file['id']
        file_json = file['json']

        file_hash = file_id_hash_mapping[file_id]

        file_obj = create_file_obj(
            cruise_json, file_json, file_hash)

        file_types.append(file_obj['data_type'])
        file_objs.append(file_obj)

    return file_objs


def process_cruises_dask(cruises_json, files_info, time_range):

    # Get list of data file paths
    # Create multiple file objects first

    logging.info('Create all cruise objs')

    cruise_objs = []

    for cruise_json in cruises_json:

        file_objs = process_cruise(cruise_json, files_info, time_range)

        if file_objs:

            cruise_file_objs = read_file_objs(file_objs)

            cruise_objs.append(cruise_file_objs)

    logging.info('Process all cruise objects')

    cruises_profiles_objs = process_cruise_objs_dask(cruise_objs)

    # Determine if both btl and ctd, it there is,
    # create a combined profile
    # created after individual profiles

    cruises_all_included = []
    cruises_all_excluded = []

    for cruise_profiles_obj in cruises_profiles_objs:

        expocode = cruise_profiles_obj['cruise_expocode']
        profiles_objs = cruise_profiles_obj['profiles_objs']

        # Return values are lists of tuples (name, profile_id)
        included, excluded = add_vars_to_logged_collections_dask(profiles_objs)

        cruises_all_included.extend(included)
        cruises_all_excluded.extend(excluded)

        is_btl = any([True if profiles_obj['data_type'] ==
                      'btl' else False for profiles_obj in profiles_objs])

        is_ctd = any([True if profiles_obj['data_type'] ==
                      'ctd' else False for profiles_obj in profiles_objs])

        if is_btl and is_ctd:

            # filter measurements by hierarchy
            #  when combine btl and ctd profiles.
            # don't filter btl or ctd first in case need
            # a variable from both
            profiles_btl_ctd_objs = create_profiles_combined_type_dask(
                profiles_objs)

            # Now check if profiles have CTD vars and should be saved
            # filter btl and ctd measurements separately
            check_and_save_combined(profiles_btl_ctd_objs)

        else:
            for file_obj_profiles in profiles_objs:
                check_and_save_per_type(file_obj_profiles)

    # ***********************************
    # Write included/excluded goship vars
    # ***********************************

    logging.info("Save included and excluded goship vars")

    save_included_excluded_goship_vars_dask(
        cruises_all_included, cruises_all_excluded)
