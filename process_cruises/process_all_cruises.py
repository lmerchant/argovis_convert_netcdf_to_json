import requests
import xarray as xr
import fsspec
import logging
from datetime import datetime
import math

from global_vars import GlobalVars
from process_cruises.process_batch_of_cruises import process_batch_of_cruises

session = requests.Session()
a = requests.adapters.HTTPAdapter(max_retries=3)
session.mount('https://', a)


def add_file_data(netcdf_cruise_obj):

    cruise_expocode = netcdf_cruise_obj['cruise_expocode']
    file_objs = netcdf_cruise_obj['file_objs']

    file_objs_w_data = []
    for file_obj in file_objs:

        file_path = file_obj['file_path']

        try:
            with fsspec.open(file_path) as fobj:
                nc = xr.open_dataset(fobj, engine='h5netcdf',
                                     chunks={"N_PROF": GlobalVars.CHUNK_SIZE})

            # Try this way to see if program
            # doesn't hang while reading in file
            # with fsspec.open(file_path) as fobj:
            #     nc = xr.open_dataset(fobj, engine='h5netcdf')
            #     nc = nc.chunk(chunks={"N_PROF": GlobalVars.CHUNK_SIZE})

        except Exception as e:
            logging.warning(f"Error reading in file {file_path}")
            logging.warning(f"Error {e}")
            logging.info(
                f"Error reading file for cruise {file_obj['cruise_expocode']}")
            logging.info(f"Data type {file_obj['data_type']}")

            return {}

        file_obj['nc'] = nc

        file_objs_w_data.append(file_obj)

    cruise_obj_w_data = {}
    cruise_obj_w_data['cruise_expocode'] = cruise_expocode
    cruise_obj_w_data['file_objs'] = file_objs_w_data

    return cruise_obj_w_data


def get_cruises_data_objs(netcdf_cruises_objs):

    # Add file data into objs

    cruise_objs_w_data = []
    for netcdf_cruise_obj in netcdf_cruises_objs:

        logging.info(
            f"Processing cruise {netcdf_cruise_obj['cruise_expocode']}")

        # Each object is a dict with keys
        netcdf_cruise_obj_w_data = add_file_data(netcdf_cruise_obj)

        cruise_objs_w_data.append(netcdf_cruise_obj_w_data)

    return cruise_objs_w_data


def create_file_obj(cruise_json, file_id, file_json, file_hash):

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
    file_obj['file_id'] = file_id
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


def process_all_cruises(cruises_json, files_info, time_range):

    file_id_hash_mapping = files_info['file_id_hash_mapping']
    hash_file_id_mapping = files_info['hash_file_id_mapping']

    response = requests.get(f"{GlobalVars.API_END_POINT}/file/all")

    all_files_json = response.json()

    files_jsons_obj = {}
    netcdf_file_ids = []
    for file_json in all_files_json:
        is_netcdf = check_if_netcdf_data(file_json)
        if is_netcdf:
            file_hash = file_json['file_hash']
            file_id = hash_file_id_mapping[file_hash]

            files_jsons_obj[file_id] = file_json
            netcdf_file_ids.append(file_id)

    netcdf_cruises_objs = []
    for cruise_json in cruises_json:

        in_time_range = check_if_in_time_range(cruise_json, time_range)

        if not in_time_range:
            continue

        # Get cruise obj for netcdf files
        # Get files attached to the cruise
        # Could be deleted ones so check if exist in all_files
        cruise_file_ids = cruise_json['files']

        # Get only file_ids in active file ids
        netcdf_cruise_file_ids = [
            id for id in cruise_file_ids if id in netcdf_file_ids]

        if not netcdf_cruise_file_ids:
            continue

        netcdf_cruise_file_ids = [id for id in netcdf_cruise_file_ids if id]

        file_objs = []
        for file_id in netcdf_cruise_file_ids:

            file_json = files_jsons_obj[file_id]

            file = {}
            file['id'] = file_id
            file['json'] = file_json

            file_hash = file_id_hash_mapping[file_id]

            file_obj = create_file_obj(
                cruise_json, file_id, file_json, file_hash)

            file_objs.append(file_obj)

        cruise_obj = {}
        if file_objs:
            cruise_obj['cruise_expocode'] = cruise_json['expocode']
            cruise_obj['file_objs'] = file_objs

        if cruise_obj:
            netcdf_cruises_objs.append(cruise_obj)

    num_netcdf_cruises_objs = len(netcdf_cruises_objs)
    num_in_batch = GlobalVars.NUM_IN_BATCH

    num_batches = math.floor(num_netcdf_cruises_objs/num_in_batch)
    num_leftover = num_netcdf_cruises_objs % num_in_batch

    logging.info(f"num  cruises json {num_netcdf_cruises_objs}")
    logging.info(f"num batches {num_batches} and num leftover {num_leftover}")

    if GlobalVars.TEST:
        # Testing
        num_in_batch = 1
        num_batches = 1
        num_leftover = 0
        logging.info(f"testing num in batch {num_in_batch}")
        logging.info(f"num leftover {num_leftover}")

        # This cruise is BTL_CTD
        # test_cruise_expocode = '06HF991_1'

        # This cruise is BTL_CTD
        # Don't set as BTL_CTD if
        # temp is null while using a btl value such as psal
        #test_cruise_expocode = '64TR90_3'

        # has both btl and ctd and want to filter
        # out meas objs without a temp var
        test_cruise_expocode = '09AR9601_1'

        netcdf_cruises_objs = [
            cruise_obj for cruise_obj in netcdf_cruises_objs if cruise_obj['cruise_expocode'] == test_cruise_expocode]

        # netcdf_cruises_objs = netcdf_cruises_objs[0:5]
        # num_in_batch = 2
        # num_batches = 1
        # num_leftover = len(netcdf_cruises_objs) - num_in_batch*num_batches

    for start in range(0, num_batches):

        start_batch = start * num_in_batch
        end_batch = start_batch + num_in_batch

        logging.info(f"start batch {start_batch}")
        logging.info(f"end batch {end_batch}")

        netcdf_cruises_objs_batch = netcdf_cruises_objs[start_batch: end_batch]

        # Add data to the objs
        cruises_data_objs_w_data = get_cruises_data_objs(
            netcdf_cruises_objs_batch)

        process_batch_of_cruises(cruises_data_objs_w_data)

    if num_leftover:

        logging.info("Inside cruise objs leftover loop")

        start_batch = num_batches * num_in_batch

        netcdf_cruises_objs_batch = netcdf_cruises_objs[start_batch: num_netcdf_cruises_objs]

        # try:
        #     # If continuing from a bunch, start_batch exists
        #     netcdf_cruises_objs_batch = netcdf_cruises_objs[start_batch: num_netcdf_cruises_objs]
        # except UnboundLocalError:
        #     # If start_batch doesn't exist because number
        #     # of cruises to process is less than bunch
        #     # size, Set to zero
        #     start_batch = 0
        #     netcdf_cruises_objs_batch = netcdf_cruises_objs[start_batch: num_netcdf_cruises_objs]

        # Add data to the objs
        cruises_data_objs_w_data = get_cruises_data_objs(
            netcdf_cruises_objs_batch)

        process_batch_of_cruises(cruises_data_objs_w_data)