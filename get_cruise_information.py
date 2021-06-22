
# Get cruise information to convert

from datetime import datetime
import logging
import os


API_END_POINT = "https://cchdo.ucsd.edu/api/v1"


def find_bot_ctd_file_info(file_ids, session):

    # Get file meta for each file id to search for cruise doc id and bottle id

    file_info = {}

    file_info['bot_id'] = None
    file_info['bot_path'] = ''
    file_info['ctd_id'] = None
    file_info['ctd_path'] = ''
    file_info['bot_found'] = False
    file_info['ctd_found'] = False

    for file_id in file_ids:

        # Find all bottle ids and doc ids for each cruise
        # Following api only lists active files

        query = f"{API_END_POINT}/file/{file_id}"
        response = session.get(query)

        if response.status_code != 200:
            print('api not reached in function find_bot_ctd_file_info')
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
                file_info['bot_id'] = file_id
                file_info['bot_path'] = file_path
                file_info['bot_filename'] = file_path.split('/')[-1]
                file_info['bot_found'] = True

            if data_type == "ctd" and data_format == "cf_netcdf":
                file_info['ctd_id'] = file_id
                file_info['ctd_path'] = file_path
                file_info['ctd_filename'] = file_path.split('/')[-1]
                file_info['ctd_found'] = True

    return file_info


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

    return all_cruises


def get_cruise_information(session, logging_dir):

    # To get expocodes and cruise ids, Use get cruise/all to get all cruise metadata
    # and search cruises to get Go-Ship cruises expocodes and cruise ids,
    # from attached file ids, Search file metadata from doc file id, bottle file id

    # Get all cruises and active files
    print('Get CCHDO cruise information')
    all_cruises = get_all_cruises(session)
    all_file_ids = get_all_file_ids(session)

    all_cruises_info = []

    # TESTING
    # For Testing. Use when wanting to limit the number of cruises processed
    cruise_count = 0

    for cruise in all_cruises:

        cruise_count = cruise_count + 1

        programs = cruise['collections']['programs']
        programs = [x.lower() for x in programs]
        country = cruise['country']
        expocode = cruise['expocode']

        # TESTING
        # Find cruise 32MW9508 to check for ctd_temperature_68 case
        # It isn't a Go-Ship cruise but has ctd temp on 68 scale
        # And change check for Go-Ship to True
        # expocode = cruise['expocode']
        # if expocode != '31HX024_1':
        #     continue

        # if expocode != '33RR20120218':
        #     continue

        # TESTING
        # Get non-goship cruise with True
        # if True:

        # Take this if statement out, looking at all coords netcdf files
        # if 'go-ship' in programs and country == 'US':

        print(f"Finding cruise information for {cruise['expocode']}")

        # Get files attached to the cruise
        # Could be deleted ones so check if exist in all_files
        file_ids = cruise['files']

        # Get only file_ids in all_file_ids (active files)
        active_file_ids = list(
            filter(lambda x: (x in all_file_ids), file_ids))

        # Get file meta for each file id to search for
        # cruise doc and bottle info
        file_info = find_bot_ctd_file_info(active_file_ids, session)

        bot_found = file_info['bot_found']
        ctd_found = file_info['ctd_found']

        # Only want cruises with both dataset btl and doc files
        if not len(file_info):
            continue

        cruise_info = {}
        cruise_info['btl'] = {'found': False}
        cruise_info['ctd'] = {'found': False}

        if bot_found:
            bot_obj = {}
            bot_obj['found'] = True
            bot_obj['type'] = 'btl'
            bot_obj['data_path'] = file_info['bot_path']
            bot_obj['filename'] = file_info['bot_filename']

            cruise_info['btl'] = bot_obj

        if ctd_found:
            ctd_obj = {}
            ctd_obj['found'] = True
            ctd_obj['type'] = 'ctd'
            ctd_obj['data_path'] = file_info['ctd_path']
            ctd_obj['filename'] = file_info['ctd_filename']

            cruise_info['ctd'] = ctd_obj

        if bot_found or ctd_found:

            cruise_info['expocode'] = cruise['expocode']
            cruise_info['cruise_id'] = cruise['id']
            cruise_info['start_date'] = cruise['startDate']

            # Get cast and station. Want to align
            # profiles combination with cast

            try:
                cruise_info['start_datetime'] = datetime.strptime(
                    cruise['startDate'], "%Y-%m-%d")
            except ValueError:
                print('No start date found in format yyyy-mm-dd')
                print(f"skipping cruise {cruise['expocode']}")
                logging.info('********************')
                logging.info('No start date found in format yyyy-mm-dd')
                logging.info(f"skipping cruise {cruise['expocode']}")
                logging.info('********************')
                continue

            print(f"Cruise start date: {cruise['startDate']}")

            all_cruises_info.append(cruise_info)

            if bot_found and ctd_found:
                type = 'btl and ctd'
            elif bot_found:
                type = 'btl'
            elif ctd_found:
                type = 'ctd'

            logging.info(f"Cruise {expocode} found with coords netCDF")
            logging.info(f"start date {cruise['startDate']}")
            logging.info(f"collection type: {type}")
            filename = 'found_cruises_with_coords_netcdf.txt'
            filepath = os.path.join(logging_dir, filename)
            with open(filepath, 'a') as f:
                f.write('-----------\n')
                f.write(f"expocode {expocode}\n")
                f.write(f"collection type {type}\n")

    print(f"Total number of cruises to convert {cruise_count}")
    logging.info('=======================================')
    logging.info(f"Total number of cruises to convert {cruise_count}")
    logging.info('=======================================')
    # TESTING
    # Used to limit number of cruises processed
    # if cruise_count == 5:  # at count 5 gives one bottle, count 1 gives both
    #     return all_cruises_info

    return all_cruises_info