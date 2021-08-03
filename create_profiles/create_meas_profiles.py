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


def get_measurements_source(df_meas, meas_qc, data_type):

    # if df.empty:
    #     meas_source_flag = None
    #     meas_source_qc = {}
    #     meas_source_qc['qc'] = None

    #     return meas_source_flag, meas_source_qc

    # # Get meas qc source (which is the temp  qc)
    # meas_source_qc = pd.unique(df_meas['qc_source'])

    # if len(meas_source_qc) == 1:
    #     meas_source_qc = meas_source_qc[0]

    has_ctd_temp_col = 'temp' in df_meas.columns
    if has_ctd_temp_col:
        all_ctd_temp_empty = df_meas['temp'].isnull().all()
    else:
        all_ctd_temp_empty = True

    has_ctd_salinity_col = 'psal' in df_meas.columns
    if has_ctd_salinity_col:
        all_ctd_sal_empty = df_meas['psal'].isnull().all()
    else:
        all_ctd_sal_empty = True

    has_salinity_column = 'salinity' in df_meas.columns
    if has_salinity_column:
        all_salinity_empty = df_meas['salinity'].isnull().all()
    else:
        all_salinity_empty = True

    if has_ctd_salinity_col and not all_ctd_sal_empty:
        use_ctd_psal = True
        use_bottle_salinity = False
    elif has_ctd_salinity_col and all_ctd_sal_empty and has_salinity_column and not all_salinity_empty:
        use_ctd_psal = False
        use_bottle_salinity = True
    elif not has_ctd_salinity_col and has_salinity_column and not all_salinity_empty:
        use_ctd_psal = False
        use_bottle_salinity = True
    else:
        use_ctd_psal = False
        use_bottle_salinity = False

    # TODO
    # What to do for the case have ctd_salinity but no temperature?
    # Here I will give it a flag of none.
    if all_ctd_temp_empty:
        logging.info("For single data type file, flag is none because no temp")
        flag = None
        use_ctd_psal = False
        use_bottle_salinity = False
    elif use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    elif not use_ctd_psal and use_bottle_salinity:
        flag = 'BTL_CTD'  # Or is it just BTL?
    elif not use_ctd_psal and not use_bottle_salinity:
        # Still using ctd_temperature so CTD flag
        flag = 'CTD'
    else:
        logging.info("For single data type file, found no flag")
        flag = None

    meas_source_flag = flag

    meas_source_qc = {}
    meas_source_qc['qc'] = meas_qc
    meas_source_qc[f'use_temp_{data_type}'] = not all_ctd_temp_empty
    meas_source_qc[f'use_psal_{data_type}'] = use_ctd_psal
    if use_bottle_salinity:
        meas_source_qc['use_salinity_btl'] = use_bottle_salinity

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

    if 'psal' in df_meas.columns:
        is_empty_psal = df_meas['psal'].isnull().values.all()

    if 'psal' in df_meas.columns and 'salinity' in df_meas.columns and not is_empty_psal:
        df_meas = df_meas.drop('salinity', axis=1)
    elif 'psal' in df_meas.columns and 'salinity' in df_meas.columns and is_empty_psal:
        df_meas = df_meas.drop('psal', axis=1)
        df_meas = df_meas.rename(columns={'salinity': 'psal'})
    elif 'psal' not in df_meas.columns and 'salinity' in df_meas.columns:
        df_meas = df_meas.rename(columns={'salinity': 'psal'})

    return df_meas


def filter_meas_core_cols(df_meas):

    # Remove objs where only a pressure value exists
    core_cols = ['temp', 'psal', 'salinity']
    found_core_cols = [col for col in df_meas.columns if col in core_cols]
    df_meas = df_meas.dropna(subset=found_core_cols, how='all')

    # TODO
    # clarify keeping objs with temp: null if psal exists

    # I would have filtered out rows with qc_source != 0 or != 2
    # (means temp not valid)
    # psal could be qc = 2, but won't make sense if no temperature
    # df_meas = df_meas.query('qc_source == 0 | qc_source == 2')

    # Set meas_source qc to None and df_meas = [] if all ctd temp are bad

    # Following not working. Still getting NaN in json string
    # If necessary for json to be null, use simple json
    df_meas = df_meas.where(pd.notnull(df_meas), None)

    # Get meas qc source (which is the temp qc)
    meas_source_qc = pd.unique(df_meas['qc_source'])

    if len(meas_source_qc) == 1:
        meas_source_qc = meas_source_qc[0]
    elif len(meas_source_qc) == 0:
        meas_source_qc = None
    else:
        logging.info(
            f"meas source qc not unique. {meas_source_qc}")
        # Look for qc = 0 or 2. If can't find any, qc = None
        # Remove null values
        qc = [qc for qc in meas_source_qc if qc]

        if len(qc):
            meas_source_qc = qc[0]
        else:
            meas_source_qc = None

        logging.info(f"Found qc {meas_source_qc}")

    # Remove any columns that are not core such as  station_cast
    keep_cols = ['pres']
    keep_cols.extend(found_core_cols)

    df_meas = df_meas[keep_cols]

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

        # Now filter to core meas cols
        # still includes salinity because want to see if
        # salinity will be used in measurements
        val_df, meas_qc = filter_meas_core_cols(val_df)

        meas_source_flag, meas_source_qc = get_measurements_source(
            val_df, meas_qc, data_type)

        # Now filter to keep psal over salinity and if no
        # psal, keep salinity  but rename to psal
        val_df = filter_salinity(val_df)

        all_ctd_temp_empty = val_df['temp'].isnull().all()
        if all_ctd_temp_empty:
            # return an empty dataframe
            #  Or one that is empty but with the same columns?
            # What if just filter out row where temp  is null?
            val_df = pd.DataFrame()

        if val_df.empty:
            meas_dict_list = []
            logging.info(f"station cast {station_cast}")
            logging.info("meas is empty")
            meas_names = {}
            meas_names['station_cast'] = station_cast
            meas_names['station_parameters'] = []
            all_meas_names.append(meas_names)
        else:
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
