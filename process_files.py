import fsspec
import xarray as xr
import logging


from global_vars import GlobalVars
from create_profiles.create_profiles_one_type import create_profiles_one_type
from create_profiles.create_profiles_combined_type import create_profiles_combined_type
from check_and_save.check_and_save_combined import check_and_save_combined
from check_and_save.check_and_save_per_type import check_and_save_per_type


def read_file(file_obj):

    err_flag = None
    CHUNK_SIZE = 20

    file_path = file_obj['file_path']

    try:
        # with fsspec.open(data_url) as fobj:
        #     nc = xr.open_dataset(fobj, engine='h5netcdf',
        #                          chunks={"N_PROF": CHUNK_SIZE})

        # Try this way to see if program
        # doesn't hang while reading in file
        with fsspec.open(file_path) as fobj:
            nc = xr.open_dataset(fobj, engine='h5netcdf')
            nc = nc.chunk(chunks={"N_PROF": CHUNK_SIZE})

    except Exception as e:
        logging.warning(f"Error reading in file {file_path}")
        logging.warning(f"Error {e}")
        err_flag = 'error'
        return file_obj, err_flag

    file_obj['nc'] = nc
    file_obj['chunk_size'] = CHUNK_SIZE

    return file_obj, err_flag


def process_files(file_objs, collections):

    read_error_count = 0
    data_file_read_errors = []

    file_profiles = []
    file_types = []

    for file_obj in file_objs:

        # Add netcdf file contents and chunk size to file_obj
        file_obj, err_flag = read_file(file_obj)
        file_path = file_obj['file_path']

        if err_flag == 'error':
            print("Error reading file")
            read_error_count = read_error_count + 1
            data_file_read_errors.append(
                f"{GlobalVars.API_END_POINT}{file_path}")
            continue

        file_obj_profile = {}
        file_obj_profile['data_type'] = file_obj['data_type']
        file_obj_profile['profiles'] = create_profiles_one_type(file_obj)

        file_profiles.append(file_obj_profile)
        file_types.append(file_obj['data_type'])

    is_btl = 'btl' in file_types
    is_ctd = 'ctd' in file_types

    if is_btl and is_ctd:

        # filter measurements by hierarchy
        #  when combine btl and ctd profiles.
        # don't filter btl or ctd first in case need
        # a variable from both
        profiles_btl_ctd = create_profiles_combined_type(
            file_profiles)

        # Now check if profiles have CTD vars and should be saved
        # filter btl and ctd measurements separately
        check_and_save_combined(profiles_btl_ctd, collections)

    else:
        for file_obj_profile in file_profiles:
            check_and_save_per_type(file_obj_profile, collections)
