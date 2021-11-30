import io
from zipfile import ZipFile
from pathlib import Path
import zipfile
import json
import os
import numpy as np
import logging

from global_vars import GlobalVars


def convert(o):

    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)

    if isinstance(o, np.int64):
        return int(o)


def unzip_file(zip_folder, zip_file):

    zip_ref = zipfile.ZipFile(zip_file)  # create zipfile object
    zip_ref.extractall(zip_folder)  # extract file to dir
    zip_ref.close()  # close file
    os.remove(zip_file)  # delete zipped file


# def write_one_json_dict(zf, profile_dict, zip_folder):

#     filename = get_filename(profile_dict)
#     file_path = os.path.join(zip_folder, filename)

#     print(f"file path {file_path}")

#     with zf.open(file_path, 'w') as json_file:
#         print('inside zf open')
#         data_bytes = json.dumps(profile_dict, ensure_ascii=False,
#                                 indent=4, sort_keys=False, default=convert).encode('utf-8')

#         json_file.write(data_bytes)


def prepare_profile_json(profile_dict):

    # TODO
    # If want to remove cchdoNames list, do it here
    # profile_dict.pop('cchdoNames', None)

    # Remove station cast var used to group data
    #profile_dict.pop('stationCast', None)
    profile_dict.pop('station_cast', None)

    profile_dict.pop('data_type', None)

    # Remove station_cast var used to group data
    # profile_dict['meta'].pop('station_cast', None)

    # Remove  time from meta since it was just used to create date variable
    profile_dict['meta'].pop('time', None)

    # Pop off meta key and use as start of data_dict
    meta_dict = profile_dict.pop('meta', None)

    # Now combine with left over profile_dict
    data_dict = {**meta_dict, **profile_dict}

    return data_dict


def get_filename(profile_dict):

    # TODO
    # ask
    # probably use cruise expocode instead of that in file

    id = profile_dict['_id']

    # TODO
    # When create file id, ask if use cruise expocode instead
    filename = f"{id}.json"

    # expocode = profile_dict['expocode']

    if '/' in filename:
        filename = filename.replace('/', '_')

    return filename


def get_json_dicts(checked_profiles_info):

    json_dicts = []

    for checked_profile_info in checked_profiles_info:

        profile = checked_profile_info['profile_checked']
        profile_dict = profile['profile_dict']
        expocode = profile_dict['meta']['expocode']

        data_dict = prepare_profile_json(profile_dict)
        json_dicts.append(data_dict)

    return json_dicts, expocode


def save_as_zip(checked_profiles_info):

    json_dicts, expocode = get_json_dicts(checked_profiles_info)

    if '/' in expocode:
        folder = expocode.replace('/', '_')
    else:
        folder = expocode

    zip_folder = os.path.join(GlobalVars.JSON_DIR, folder)
    zip_file = f"{Path.cwd()}/{zip_folder}.zip"

    zf = zipfile.ZipFile(zip_file, mode='w',
                         compression=zipfile.ZIP_DEFLATED)

    with zf as f:
        for json_dict in json_dicts:
            filename = get_filename(json_dict)
            json_str = json.dumps(json_dict, ensure_ascii=False,
                                  indent=4, sort_keys=False, default=convert)
            f.writestr(filename, json_str)

    #unzip_file(zip_folder, zip_file)


def save_as_zip_data_type_profiles(data_type_profiles):

    json_dicts = []
    for data_type_profile in data_type_profiles:
        profile_dict = data_type_profile['profile_dict']
        expocode = profile_dict['meta']['expocode']

        json_dict = prepare_profile_json(profile_dict)
        json_dicts.append(json_dict)

    if '/' in expocode:
        folder = expocode.replace('/', '_')
    else:
        folder = expocode

    zip_folder = os.path.join(GlobalVars.JSON_DIR, folder)
    zip_file = f"{Path.cwd()}/{zip_folder}.zip"

    zf = zipfile.ZipFile(zip_file, mode='w',
                         compression=zipfile.ZIP_DEFLATED)

    with zf as f:
        for json_dict in json_dicts:
            filename = get_filename(json_dict)
            json_str = json.dumps(json_dict, ensure_ascii=False,
                                  indent=4, sort_keys=False, default=convert)
            f.writestr(filename, json_str)

    # TODO
    # For development (comment out later)
    #unzip_file(zip_folder, zip_file)
