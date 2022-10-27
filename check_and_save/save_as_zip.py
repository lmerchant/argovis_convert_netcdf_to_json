import io
from zipfile import ZipFile
from pathlib import Path
import zipfile
import json
import os
import numpy as np
import pandas as pd
import logging

from global_vars import GlobalVars


def convert(o):

    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)

    if isinstance(o, np.int32):
        return int(o)

    if isinstance(o, np.int64):
        return int(o)


def unzip_file(zip_folder, zip_file):

    zip_ref = zipfile.ZipFile(zip_file)  # create zipfile object
    zip_ref.extractall(zip_folder)  # extract file to dir
    zip_ref.close()  # close file
    os.remove(zip_file)  # delete zipped file


def coerce_qc_to_integer(json_dict):

    data = json_dict['data']

    # Read into a pandas dataframe and check if there are null values
    # If there are, then do a loop through and coerce qc values to integer

    # Also do a coerce if there are qc values as a list (such as extra dims parameters)

    data_df = pd.DataFrame.from_dict(data)

    columns = list(data_df.columns)

    qc_columns = [col for col in columns if col.endswith('_woceqc')]

    col_dtypes = data_df[qc_columns].dtypes.apply(lambda x: x.name).to_dict()

    if 'object' in col_dtypes.values():
        has_qc_object_columns = True
    else:
        has_qc_object_columns = False

    # Find which qc_columns have NaN values in them, and save to coerce to integer

    # Does this find columns with cells containing a list of with NaN values in it?
    qc_cols_w_nans = data_df[qc_columns].columns[data_df[qc_columns].isna(
    ).any()].tolist()

    has_nan_qc_cols = len(qc_cols_w_nans)

    if has_nan_qc_cols or has_qc_object_columns:
        coerced_data = []

        for obj in data:
            new_obj = {}
            for k, v in obj.items():
                if k in qc_columns:
                    if isinstance(v, list):
                        try:
                            new_obj[k] = [int(val) for val in v]
                        except ValueError:
                            new_obj[k] = v
                    else:
                        try:
                            new_obj[k] = int(v)
                        except ValueError:
                            new_obj[k] = v
                else:
                    new_obj[k] = v

            coerced_data.append(new_obj)

        json_dict['data'] = coerced_data

    return json_dict


def get_data_dict(profile_dict, station_cast):

    # TODO
    # count number of cruises saved (count each time this is called)

    # TODO
    # count number of btl, ctd, and btl_ctd data types profiles and sum it
    # count size of data_dict (# of profiles is number of keys under the data key)

    # logging.info(f'Creating data dict for station cast {station_cast}')

    # TODO
    # is this only called for single cruise?
    # If so,if missing temp or it's empty, do logic

    # TODO
    # Move this to post processing

    # Filter measurements by removing any points with NaN temp
    # and points with all NaN values except pressure
    measurements = profile_dict['measurements']

    data_type = profile_dict['data_type']

    # If has psal (renamed salinity)and all null, don't save in masurement data obj
    df_meas = pd.DataFrame.from_dict(measurements)

    if 'salinity' in df_meas.columns:
        all_psal_null = df_meas['salinity'].isnull().values.all()
        if all_psal_null:
            df_meas = df_meas.drop(['salinity'], axis=1)

    measurements = df_meas.to_dict('records')

    # If data_type is 'btl_ctd', can have case of temp=nan but keep data_point
    # So don't filter out temp = nan values.

    if data_type != 'btl_ctd':

        filtered_measurements = []

        for obj in measurements:
            has_temp = 'temperature' in obj.keys()

            if has_temp:
                not_null_temp = pd.notnull(obj['temperature'])
            else:
                not_null_temp = False

            if has_temp and not_null_temp:
                filtered_measurements.append(obj)
            elif not has_temp:
                logging.info(f'data type is {data_type}')
                logging.info('measurement has null temp and not included')
                pressure = obj['pressure']
                logging.info(f'pressure is {pressure}')
                logging.info(obj)

        profile_dict['measurements'] = filtered_measurements

    profile_dict.pop('data_type', None)

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


def save_as_zip_data_type_profiles(data_type_profiles):

    # Save summary information to profiles_information.txt
    profiles_file = Path(GlobalVars.LOGGING_DIR) / 'profiles_information.txt'

    if profiles_file.is_file():

        # read dataframe
        df_profiles_info = pd.read_csv(profiles_file)

        total_cruises_processed, total_btl_profiles, total_ctd_profiles, total_btl_ctd_profiles, total_profiles = df_profiles_info.loc[0, :].values.tolist(
        )

        total_cruises_processed = total_cruises_processed + 1

    else:

        # create dataframe

        columns = [('total_cruises_processed', int),
                   ('total_btl_profiles', int),
                   ('total_ctd_profiles', int),
                   ('total_btl_ctd_profiles', int),
                   ('total_profiles', int)]

        df_profiles_info = pd.DataFrame(columns=columns)

        df_profiles_info.reset_index(drop=True, inplace=True)

        total_cruises_processed = 1
        total_btl_profiles = 0
        total_ctd_profiles = 0
        total_btl_ctd_profiles = 0
        total_profiles = 0

        df_profiles_info.loc[0] = [total_cruises_processed,
                                   total_btl_profiles, total_ctd_profiles, total_btl_ctd_profiles, total_profiles]

        df_profiles_info.columns = ['total_cruises_processed', 'total_btl_profiles',
                                    'total_ctd_profiles', 'total_btl_ctd_profiles', 'total_profiles']

    json_dicts = []

    for data_type_profile in data_type_profiles:

        station_cast = data_type_profile['station_cast']
        profile_dict = data_type_profile['profile_dict']
        expocode = profile_dict['meta']['expocode']
        data_type = profile_dict['data_type']

        json_dict = get_data_dict(profile_dict, station_cast)

        # TODO
        # Add in function to coerce any qc values from float to integer
        # since floats occur if any column value was NaN in pandas dataframe
        json_dict = coerce_qc_to_integer(json_dict)

        json_dicts.append(json_dict)

        # Save summary information to df_profiles_info
        if data_type == 'btl':
            total_btl_profiles = total_btl_profiles + 1
        elif data_type == 'ctd':
            total_ctd_profiles = total_ctd_profiles + 1
        elif data_type == 'btl_ctd':
            total_btl_ctd_profiles = total_btl_ctd_profiles + 1
        else:
            logging.info(
                "Profile type not found when saving to profile information file")

    if '/' in expocode:
        folder = expocode.replace('/', '_')
    else:
        folder = expocode

    zip_folder = os.path.join(GlobalVars.JSON_DIR, folder)
    zip_file = f"{Path.cwd()}/{zip_folder}.zip"

    zf = zipfile.ZipFile(zip_file, mode='w',
                         compression=zipfile.ZIP_DEFLATED)

    # TODO
    # how much space saved if remove the indent?
    with zf as f:
        for json_dict in json_dicts:
            filename = get_filename(json_dict)
            json_str = json.dumps(json_dict, ensure_ascii=False,
                                  indent=4, sort_keys=False, default=convert)
            f.writestr(filename, json_str)

    # TODO
    # For development (comment out later)
    unzip_file(zip_folder, zip_file)

    # Save profile information to first row

    total_profiles = total_btl_profiles + total_ctd_profiles + total_btl_ctd_profiles

    df_profiles_info.loc[0] = [total_cruises_processed,
                               total_btl_profiles, total_ctd_profiles, total_btl_ctd_profiles, total_profiles]

    logging.info(df_profiles_info)

    df_profiles_info.to_csv(profiles_file, index=False)
