import requests
from datetime import datetime
import os
import logging

from process_files import process_files
# from tests.setup_test_objs import setup_test_objs
from global_vars import GlobalVars


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


def process_cruise(cruise_json, files_info, time_range, cruise_count):

    included = []
    excluded = []

    active_file_ids = files_info['active_file_ids']
    file_id_hash_mapping = files_info['file_hash_mapping']

    in_time_range = check_if_in_time_range(cruise_json, time_range)

    if not in_time_range:
        return cruise_count, included, excluded

    # Get files attached to the cruise
    # Could be deleted ones so check if exist in all_files
    cruise_file_ids = cruise_json['files']

    # Get only file_ids in active file ids
    active_cruise_file_ids = [
        id for id in cruise_file_ids if id in active_file_ids]

    netcdf_files = get_netcdf_files_json(active_cruise_file_ids)

    file_types = []
    file_objs = []

    included = []
    excluded = []

    for file in netcdf_files:

        file_id = file['id']
        file_json = file['json']

        file_hash = file_id_hash_mapping[file_id]

        file_obj = create_file_obj(
            cruise_json, file_json, file_hash)

        file_types.append(file_obj['data_type'])
        file_objs.append(file_obj)

    if len(file_objs):

        logging.info("--------------------------------")
        logging.info(f"Start converting Cruise: {cruise_json['expocode']}")
        logging.info("--------------------------------")

        included, excluded = process_files(file_objs)

        filename = 'found_cruises_with_coords_netcdf.txt'
        filepath = os.path.join(GlobalVars.LOGGING_DIR, filename)
        with open(filepath, 'a') as f:
            f.write(f"expocode {cruise_json['expocode']}\n")
            f.write(f"collection type {file_types}\n")
            f.write(f"start date {cruise_json['startDate']}\n")
            f.write(f"---------------------\n")

        cruise_count = cruise_count + 1

    return cruise_count, included, excluded
