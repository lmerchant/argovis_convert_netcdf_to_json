import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
import logging

# Doesn't seem to do much
# https://stackoverflow.com/questions/47776936/why-is-a-computation-much-slower-within-a-dask-distributed-worker
#pd.options.mode.chained_assignment = None


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


def to_int_qc(obj):
    # _qc":2.0
    # If float qc with '.0' in qc value, remove it to get an int
    json_str = json.dumps(obj,  default=dtjson)
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)
    obj = json.loads(json_str)
    return obj


def convert_boolean(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    if isinstance(obj, (list, tuple)):
        return [convert_boolean(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_boolean(key): convert_boolean(value) for key, value in obj.items()}
    return obj


def filter_measurements(data_type_profiles):

    # For measurements, already filtered whether to
    # keep psal or salinity, but didn't rename the
    # salinity col.

    # TODO
    # Change station_parameters keys if empty meas?

    # if val_df.empty:
    #     meas_names = {}
    #     meas_names['station_cast'] = station_cast
    #     meas_names['station_parameters'] = list(val_df.columns)
    #     all_meas_names.append(meas_names)

    output_profiles_list = []

    for profile in data_type_profiles:

        profile_dict = profile['profile_dict']
        station_cast = profile['station_cast']

        measurements = profile_dict['measurements']

        # Check if all elems null in measurements besides pressure
        all_vals = []
        for obj in measurements:
            vals = [val for key, val in obj.items() if pd.notnull(val)
                    and key != 'pres']
            all_vals.extend(vals)

        if not len(all_vals):
            logging.info("All elems null so measurements = []")
            measurements = []

        profile_dict['measurements'] = measurements

        output_profile = {}
        output_profile['profile_dict'] = profile_dict
        output_profile['station_cast'] = station_cast

        output_profiles_list.append(output_profile)

    return output_profiles_list


def get_measurements_source(df_meas, meas_qc, data_type):

    using_temp = False
    using_psal = False
    using_salinity = False

    has_temp_col = 'temp' in df_meas.columns
    if has_temp_col:
        all_temp_empty = df_meas['temp'].isnull().all()
    else:
        all_temp_empty = True

    has_psal_col = 'psal' in df_meas.columns
    if has_psal_col:
        all_psal_empty = df_meas['psal'].isnull().all()
    else:
        all_psal_empty = True

    has_salinity_column = 'salinity' in df_meas.columns
    if has_salinity_column:
        all_salinity_empty = df_meas['salinity'].isnull().all()
    else:
        all_salinity_empty = True

    if has_temp_col and not all_temp_empty:
        using_temp = True
    elif has_temp_col and all_temp_empty:
        using_temp = False

    if has_psal_col and not all_psal_empty:
        using_psal = True
    elif has_psal_col and all_psal_empty and has_salinity_column and not all_salinity_empty:
        using_salinity = True
    elif not has_psal_col and has_salinity_column and not all_salinity_empty:
        using_salinity = True

    # logging.info(f"using_temp {using_temp}")
    # logging.info(f"using psal {using_psal}")
    # logging.info(f"using_salinity  {using_salinity}")

    if not using_temp and not using_psal and not using_salinity:
        logging.info(
            "For single data type file, source flag is none because no temp or psal")
        meas_source_flag = None
    elif not using_temp and using_psal and not using_salinity:
        meas_source_flag = 'CTD'
    elif not using_temp and not using_psal and using_salinity:
        meas_source_flag = 'BTL'
    elif using_temp and using_psal and not using_salinity:
        meas_source_flag = 'CTD'
    elif using_temp and not using_psal and using_salinity:
        meas_source_flag = 'BTL_CTD'  # Or is it just BTL?
    elif using_temp and not using_psal and not using_salinity:
        # using temperature so CTD meas_source_flag
        meas_source_flag = 'CTD'

    meas_source_qc = {}
    meas_source_qc['qc'] = meas_qc
    meas_source_qc[f'use_temp_{data_type}'] = using_temp
    if has_psal_col:
        meas_source_qc[f'use_psal_{data_type}'] = using_psal
    if has_salinity_column:
        meas_source_qc['use_salinity_btl'] = using_salinity

    # For json_str, convert True, False to 'true','false'
    meas_source_qc = convert_boolean(meas_source_qc)

    return meas_source_flag, meas_source_qc


def get_argovis_core_meas_values_per_type(data_type):

    # Add in bottle_salinity since will use this in
    # measurements to check if have ctd_salinity, and
    # if now, use bottle_salinity

    return ['pres', f"temp_{data_type}", f"temp_{data_type}_qc", f"psal_{data_type}",  f"psal_{data_type}_qc", 'salinity_btl', 'salinity_btl_qc']


def filter_salinity(df_meas):

    # Now filter to keep psal over salinity and if no
    # psal, keep salinity  but rename to psal

    psal_exists = 'psal' in df_meas.columns
    if psal_exists:
        is_empty_psal = df_meas['psal'].isnull().values.all()

    salinity_exists = 'salinity' in df_meas.columns
    if salinity_exists:
        is_empty_salinity = df_meas['salinity'].isnull().values.all()

    if psal_exists and salinity_exists and not is_empty_psal:
        df_meas = df_meas.drop('salinity', axis=1)
    elif psal_exists and salinity_exists and is_empty_psal and not is_empty_salinity:
        df_meas = df_meas.drop('psal', axis=1)
        df_meas = df_meas.rename(columns={'salinity': 'psal'})
    elif not psal_exists and salinity_exists and not is_empty_salinity:
        df_meas = df_meas.rename(columns={'salinity': 'psal'})

    return df_meas


def filter_meas_core_cols(df_meas):

    # Remove objs where only a pressure value exists so
    # can remove rows with no values except pressure
    core_cols = ['temp', 'psal', 'salinity']
    found_core_cols = [col for col in df_meas.columns if col in core_cols]
    df_meas = df_meas.dropna(subset=found_core_cols, how='all')

    # # TODO
    # # Check if temp is all null, if it is set df_meas = empty df
    # # Or wait to do this when combine if the logic is a psal val
    # # makes sense if there is no temperature

    no_temp = df_meas['temp'].isnull().all()
    psal_exists = 'psal' in df_meas.columns
    no_psal = False
    if psal_exists:
        no_psal = df_meas['psal'].isnull().all()

    # Keeping objs with temp: null if psal exists

    if no_temp and not psal_exists:
        df_meas = pd.DataFrame(columns=df_meas.columns)
        meas_source_qc = None
        return df_meas, meas_source_qc

    if no_temp and psal_exists and no_psal:
        df_meas = pd.DataFrame(columns=df_meas.columns)
        meas_source_qc = None
        return df_meas, meas_source_qc

    # I would have filtered out rows with qc_source != 0 or != 2
    # (means temp not valid)
    # df_meas = df_meas.query('qc_source == 0 | qc_source == 2')

    # Get meas qc source (which is the temp qc)
    meas_source_qc = pd.unique(df_meas['qc_source'])

    # remove null values
    meas_source_qc = [qc for qc in meas_source_qc if pd.notnull(qc)]

    if len(meas_source_qc):
        # Keep any good qc values 0 or 2
        meas_source_qc = [
            qc for qc in meas_source_qc if int(qc) == 0 or int(qc) == 2]

    if len(meas_source_qc) == 1:
        meas_source_qc = meas_source_qc[0]
    elif not len(meas_source_qc):
        meas_source_qc = None
    else:
        logging.info(
            f"meas source qc not 0 or 2. {meas_source_qc}")
        meas_source_qc = None

    # Remove any columns that are not core such as  station_cast
    # and add pres to core cols
    keep_cols = ['pres']
    keep_cols.extend(found_core_cols)

    df_meas = df_meas[keep_cols]

    # Following not working. Still getting NaN in json string
    # If necessary for json to be null, use simple json
    df_meas = df_meas.where(pd.notnull(df_meas), None)

    return df_meas, meas_source_qc


def create_measurements_df_all(df, data_type):

    # For measurements, keep core vars pres, temp, psal, salinity
    # Later when filter measurements, if salinity is used,
    # it will be renamed to psal

    # core values includes '_qc' vars
    core_values = get_argovis_core_meas_values_per_type(data_type)
    table_columns = list(df.columns)
    core_cols = [col for col in table_columns if col in core_values]

    core_non_qc = [elem for elem in core_cols if '_qc' not in elem]
    core_non_qc_wo_press = [
        elem for elem in core_cols if '_qc' not in elem and elem != 'pres']

    # Also include N_PROF, N_LEVELS, station_cast for unique identifiers
    cols_to_keep = core_cols
    identifier_cols = ['N_PROF', 'station_cast']
    cols_to_keep.extend(identifier_cols)

    df_meas = df[cols_to_keep].copy()

    # If qc != 0 or 2, set corresponding non_qc value to np.nan
    for col in core_non_qc:

        qc_col = f"{col}_qc"

        try:
            df_meas.loc[(df_meas[qc_col] != 0) &
                        (df_meas[qc_col] != 2), col] = np.nan

        except KeyError:
            pass

    # Get temp qc value to be meas source qc (don't know if 0 or 2)
    # Could also be a bad qc or none
    df_meas['qc_source'] = df_meas[f"temp_{data_type}_qc"]

    # drop qc columns
    for col in df_meas.columns:
        if col.endswith('_qc'):
            df_meas = df_meas.drop([col], axis=1)

    df_meas = df_meas.sort_values(by=['pres'])

    # Remove data_type ('btl', 'ctd') from  variable names
    column_mapping = {}
    column_mapping[f"psal_{data_type}"] = 'psal'
    column_mapping[f"temp_{data_type}"] = 'temp'
    column_mapping[f"salinity_btl"] = 'salinity'

    df_meas = df_meas.rename(columns=column_mapping)

    return df_meas


def create_meas_profiles(df_param, data_type):

    # *******************************
    # Create all measurement profiles
    # *******************************

    df_meas = df_param.groupby('N_PROF').apply(
        create_measurements_df_all, data_type)

    # Remove N_PROF as index and drop because
    # already have an N_PROF column used for groupby
    df_meas = df_meas.reset_index(drop=True)

    # Change NaN to None so in json, converted to null
    # Inconsistent though. Try  doing per meas obj
    # df_meas = df_meas.where(pd.notnull(df_meas), None)

    meas_df_groups = dict(tuple(df_meas.groupby('N_PROF')))

    all_meas_profiles = []
    all_meas_source_profiles = []
    all_meas_names = []

    for val_df in meas_df_groups.values():

        station_cast = val_df['station_cast'].values[0]

        # logging.info('----------------------')
        # logging.info(f"station cast {station_cast}")

        # Now filter to core meas cols
        # still includes salinity because want to see if
        # salinity will be used in measurements
        val_df, meas_qc = filter_meas_core_cols(val_df)

        meas_source_flag, meas_source_qc = get_measurements_source(
            val_df, meas_qc, data_type)

        # Now filter to keep psal over salinity and if no
        # psal, keep salinity  but rename to psal
        val_df = filter_salinity(val_df)

        # logging.info('val_df')
        # logging.info(val_df.head())

        # TODO
        # Should I filter out all values here or leave the
        # temp as null and have a psal?

        # For logging, check if both temp and psal null
        all_ctd_temp_empty = val_df['temp'].isnull().all()

        psal_exists = 'psal' in val_df.columns
        if psal_exists:
            all_psal_empty = val_df['psal'].isnull().all()

        if all_ctd_temp_empty and psal_exists and all_psal_empty:
            logging.info(f"station cast {station_cast}")
            logging.info("No temp or sal values. All null")
        elif all_ctd_temp_empty and not psal_exists:
            logging.info(f"station cast {station_cast}")
            logging.info("No non null temp values and psal not exists.")

        # if all_ctd_temp_empty and psal_exists and all_psal_empty:
        #     val_df = pd.DataFrame(columns=val_df.columns)
        # elif all_ctd_temp_empty and not psal_exists:
        #     val_df = pd.DataFrame(columns=val_df.columns)

        # if val_df.empty:
        #     meas_dict_list = []
        #     logging.info(f"station cast {station_cast}")
        #     logging.info("No temp or sal values. All null")
        #     logging.info("meas dict list is []")
        #     logging.info("station parameters key is empty")
        #     meas_names = {}
        #     meas_names['station_cast'] = station_cast
        #     meas_names['station_parameters'] = list(val_df.columns)
        #     all_meas_names.append(meas_names)

        meas_dict_list = val_df.to_dict('records')
        meas_dict_list = to_int_qc(meas_dict_list)

        meas_names = {}
        meas_names['station_cast'] = station_cast
        meas_names['station_parameters'] = list(val_df.columns)
        all_meas_names.append(meas_names)

        meas_profile = {}
        meas_profile['station_cast'] = station_cast
        meas_profile['measurements'] = meas_dict_list

        all_meas_profiles.append(meas_profile)

        meas_source_profile = {}
        meas_source_profile['station_cast'] = station_cast
        meas_source_profile['measurementsSource'] = meas_source_flag
        all_meas_source_profiles.append(meas_source_profile)

        meas_source_profile_qc = {}
        meas_source_profile_qc['station_cast'] = station_cast
        meas_source_profile_qc['measurementsSourceQC'] = meas_source_qc
        all_meas_source_profiles.append(meas_source_profile_qc)

    return all_meas_profiles, all_meas_source_profiles, all_meas_names
