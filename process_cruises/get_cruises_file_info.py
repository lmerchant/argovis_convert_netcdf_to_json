import requests
from datetime import datetime


from global_vars import GlobalVars


session = requests.Session()
a = requests.adapters.HTTPAdapter(max_retries=3)
session.mount('https://', a)


def get_file_id_hash_mapping():

    query = f"{GlobalVars.API_END_POINT}/file"
    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in function get_file_id_hash_mapping')
        print(response)
        exit(1)

    mapping = response.json()['files']

    # Get into form {file_id: file_hash}
    file_id_hash_mapping = {obj['id']: obj['hash'] for obj in mapping}

    hash_file_id_mapping = {obj['hash']: obj['id'] for obj in mapping}

    return file_id_hash_mapping, hash_file_id_mapping


def get_active_file_ids():

    # Use api query to get all active file ids
    query = f"{GlobalVars.API_END_POINT}/file/all"

    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_files')
        print(response)
        exit(1)

    all_files = response.json()

    all_file_ids = [file['id'] for file in all_files]

    return all_file_ids


def get_cruises_info():

    # Use api query to get all cruise id with their attached file ids
    query = f"{GlobalVars.API_END_POINT}/cruise/all"

    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_cruises')
        print(response)
        exit(1)

    cruises_json = response.json()

    # Remove any with no start date
    cruises_json = [cruise for cruise in cruises_json if cruise['startDate']]

    def get_date(cruise_start_date):
        return datetime.strptime(cruise_start_date, "%Y-%m-%d")

    cruises_json.sort(
        key=lambda item: get_date(item['startDate']), reverse=True)

    return cruises_json


def get_all_cruises_file_info():

    cruises_info = get_cruises_info()

    active_file_ids = get_active_file_ids()

    file_id_hash_mapping, hash_file_id_mapping = get_file_id_hash_mapping()

    files_info = {}
    files_info['active_file_ids'] = active_file_ids
    files_info['file_id_hash_mapping'] = file_id_hash_mapping
    files_info['hash_file_id_mapping'] = hash_file_id_mapping

    return cruises_info, files_info
