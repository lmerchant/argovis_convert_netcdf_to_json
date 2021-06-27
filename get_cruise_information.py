
# Get cruise information to convert

from datetime import datetime
import logging
import os


API_END_POINT = "https://cchdo.ucsd.edu/api/v1"


def find_btl_ctd_file_info(file_ids, file_id_hash_mapping, session):

    # Get file meta for each file id to search for cruise doc id and bottle id

    file_info = {}

    file_info['btl_id'] = None
    file_info['btl_hash'] = None
    file_info['btl_path'] = ''
    file_info['btl_filename'] = ''

    file_info['ctd_id'] = None
    file_info['ctd_hash'] = None
    file_info['ctd_path'] = ''
    file_info['ctd_filename'] = ''

    file_info['btl_found'] = False
    file_info['ctd_found'] = False

    for file_id in file_ids:

        # Find all bottle ids and doc ids for each cruise
        # Following api only lists active files

        query = f"{API_END_POINT}/file/{file_id}"
        response = session.get(query)

        if response.status_code != 200:
            print('api not reached in function find_btl_ctd_file_info')
            print(response)
            exit(1)

        file_meta = response.json()

        file_role = file_meta['role']
        data_type = file_meta['data_type']
        data_format = file_meta['data_format']
        file_path = file_meta['file_path']

        # Only returning file info if both a bottle and doc for cruise
        if file_role == "dataset":
            if data_type == "bottle" and data_format == "cf_netcdf":
                file_info['btl_id'] = file_id
                file_info['btl_path'] = file_path
                file_info['btl_filename'] = file_path.split('/')[-1]
                file_info['btl_found'] = True
                file_info['btl_hash'] = file_id_hash_mapping[file_id]

            if data_type == "ctd" and data_format == "cf_netcdf":
                file_info['ctd_id'] = file_id
                file_info['ctd_path'] = file_path
                file_info['ctd_filename'] = file_path.split('/')[-1]
                file_info['ctd_found'] = True
                file_info['ctd_hash'] = file_id_hash_mapping[file_id]

    return file_info


def get_file_id_hash_mapping(session):

    query = f"{API_END_POINT}/file"
    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in function get_file_id_hash_mapping')
        print(response)
        exit(1)

    mapping = response.json()['files']

    # Get into form {file_id: file_hash}
    file_hash_mapping = {obj['id']: obj['hash'] for obj in mapping}

    return file_hash_mapping


def get_all_file_ids(session):

    # Use api query to get all active file ids
    query = f"{API_END_POINT}/file/all"

    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_files')
        print(response)
        exit(1)

    all_files = response.json()

    all_file_ids = [file['id'] for file in all_files]

    return all_file_ids


def get_all_cruises(session):

    # Use api query to get all cruise id with their attached file ids

    query = f"{API_END_POINT}/cruise/all"

    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_cruises')
        print(response)
        exit(1)

    all_cruises = response.json()

    # Sort on cruise id for most recent first (assuming larger id is sooner creation)
    all_cruises.sort(
        key=lambda item: item['id'], reverse=True)

    return all_cruises


def get_cruise_information(session, logging_dir, start_datetime, end_datetime):

    # To get expocodes and cruise ids, Use get cruise/all to get all cruise metadata
    # and search cruises to get Go-Ship cruises expocodes and cruise ids,
    # from attached file ids, Search file metadata from doc file id, bottle file id

    # Get all cruises and active files
    logging.info('Get CCHDO cruise information for date range')
    all_cruises = get_all_cruises(session)
    all_file_ids = get_all_file_ids(session)
    file_id_hash_mapping = get_file_id_hash_mapping(session)

    all_cruises_info = []

    cruise_count = 0

    for cruise in all_cruises:

        # TODO
        # Maybe run once and save to file for all cruises
        # with a new netcdf file. Then read this in and
        # search for date range

        logging.info(f"Looking at cruise {cruise['expocode']}")

        cruise_start_date = cruise['startDate']

        # Some cruises are placeholders and have a blank start date
        if not cruise_start_date:
            continue

        programs = cruise['collections']['programs']
        programs = [x.lower() for x in programs]
        expocode = cruise['expocode']

        # TESTING
        # Find cruise 32MW9508 to check for ctd_temperature_68 case
        # It has ctd temp on 68 scale

        # Check this to see if program freezes or if it was random
        # expocode = cruise['expocode']
        # if expocode != '320620161224':
        #     continue

        # Get files attached to the cruise
        # Could be deleted ones so check if exist in all_files
        file_ids = cruise['files']

        # Get only file_ids in all_file_ids (active files)
        active_file_ids = list(
            filter(lambda x: (x in all_file_ids), file_ids))

        # Get file meta for each file id to search for
        # cruise doc and bottle info
        file_info = find_btl_ctd_file_info(
            active_file_ids, file_id_hash_mapping, session)

        btl_found = file_info['btl_found']
        ctd_found = file_info['ctd_found']

        cruise_info = {}
        cruise_info['btl'] = {'found': False}
        cruise_info['ctd'] = {'found': False}

        if btl_found:
            btl_obj = {}
            btl_obj['found'] = True
            btl_obj['type'] = 'btl'
            btl_obj['data_path'] = file_info['btl_path']
            btl_obj['filename'] = file_info['btl_filename']
            btl_obj['file_id'] = file_info['btl_id']
            btl_obj['cruise_expocode'] = expocode

            cruise_info['btl'] = btl_obj

        if ctd_found:
            ctd_obj = {}
            ctd_obj['found'] = True
            ctd_obj['type'] = 'ctd'
            ctd_obj['data_path'] = file_info['ctd_path']
            ctd_obj['filename'] = file_info['ctd_filename']
            ctd_obj['file_id'] = file_info['ctd_id']
            ctd_obj['cruise_expocode'] = expocode

            cruise_info['ctd'] = ctd_obj

        if btl_found or ctd_found:

            cruise_datetime = datetime.strptime(cruise_start_date, "%Y-%m-%d")

            in_date_range = cruise_datetime >= start_datetime and cruise_datetime <= end_datetime

            if not in_date_range:
                continue

            cruise_count = cruise_count + 1

            if btl_found and ctd_found:
                type = 'btl_ctd'
            elif btl_found:
                type = 'btl'
            elif ctd_found:
                type = 'ctd'

            cruise_info['start_datetime'] = cruise_datetime
            cruise_info['expocode'] = cruise['expocode']
            cruise_info['cruise_id'] = cruise['id']
            cruise_info['start_date'] = cruise['startDate']
            cruise_info['type'] = type

            all_cruises_info.append(cruise_info)

            logging.info(f"Cruise {expocode} found")
            logging.info(f"start date {cruise_start_date}")
            logging.info(f"collection type: {type}")
            logging.info('=======================================')

            # TESTING
            # Used to limit number of cruises processed
            # if cruise_count == 10:
            #     break

    # Sort cruises on date to process newest first
    try:
        all_cruises_info.sort(
            key=lambda item: item['start_datetime'], reverse=True)
    except:
        pass

    # Write each cruise expocode, start date and type, to file

    filename = 'found_cruises_with_coords_netcdf.txt'
    filepath = os.path.join(logging_dir, filename)
    with open(filepath, 'a') as f:
        for cruise_info in all_cruises_info:
            f.write(f"expocode {cruise_info['expocode']}\n")
            f.write(f"collection type {cruise_info['type']}\n")
            f.write(f"start date {cruise_info['start_date']}\n")
            f.write(f"---------------------\n")

    #print(f"Total number of cruises to convert {cruise_count}")
    logging.info(f"Total number of cruises to convert {cruise_count}")
    logging.info('=======================================')

    return all_cruises_info
