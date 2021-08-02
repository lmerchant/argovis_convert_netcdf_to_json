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

    # If ignore pressure, delete all rows with all null
    # But then removing station information, too if everything
    # removed. # Can end up with an empty dataframe

    # TODO
    # Or do I want to keep all values even if pres only non null val?
    # core_cols = ['temp', 'psal', 'salinity']
    # found_core_cols = [col for col in df_meas.columns if col in core_cols]

    # df = df_meas.dropna(subset=found_core_cols, how='all')

    # if df.empty:
    #     measurements_source = None
    #     measurements_source_qc = {}
    #     measurements_source_qc['qc'] = None

    #     return measurements_source, measurements_source_qc

    # if 'qc_temp' in df_meas.columns:
    #     temp_qc_df = df_meas['qc_temp']
    #     temp_qc = pd.unique(temp_qc_df)[0]
    # else:
    #     temp_qc = None

    # if 'qc_psal' in df_meas.columns:
    #     psal_qc_df = df_meas['qc_psal']
    #     psal_qc = pd.unique(psal_qc_df)[0]
    # else:
    #     psal_qc = None

    # if 'qc_salinity' in df_meas.columns:
    #     salinity_qc_df = df_meas['qc_salinity']
    #     salinity_qc = pd.unique(salinity_qc_df)[0]
    # else:
    #     salinity_qc = None

    # # Get meas qc source (which is the temp  qc)
    # meas_source_qc = pd.unique(df_meas['qc_source'])

    # if len(meas_source_qc) == 1:
    #     meas_source_qc = meas_source_qc[0]

    # See if using ctd_salinty or bottle_salinity
    # Is ctd_salinity NaN? If it is and bottle_salinity isn't NaN, use it
    # See if all ctd_salinity are NaN, if not, use it and
    # drop bottle salinity column

    is_ctd_sal_column = 'psal' in df_meas.columns

    if 'psal' in df_meas.columns:
        is_ctd_sal_empty = df_meas['psal'].isnull().all()
    else:
        is_ctd_sal_empty = True

    is_salinity_column = 'salinity' in df_meas.columns
    if 'salinity' in df_meas.columns:
        is_salinity_empty = df_meas['salinity'].isnull().all()
    else:
        is_salinity_empty = True

    if is_ctd_sal_column and not is_ctd_sal_empty:
        use_ctd_psal = True
        use_bottle_salinity = False
    elif is_ctd_sal_column and is_ctd_sal_empty and is_salinity_column and not is_salinity_empty:
        use_ctd_psal = False
        use_bottle_salinity = True
    elif not is_ctd_sal_column and is_salinity_column and not is_salinity_empty:
        use_ctd_psal = False
        use_bottle_salinity = True
    else:
        use_ctd_psal = False
        use_bottle_salinity = False

    # # Don't want to match variables with temp in name
    # is_ctd_temp = next(
    #     (True for col in df_meas.columns if col == 'temp'), False)

    ctd_temp_is_empty = df_meas['temp'].isnull().all()

    if ctd_temp_is_empty:
        flag = None
    elif use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    elif not use_ctd_psal and use_bottle_salinity:
        flag = 'BTL_CTD'
    elif not use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    # elif data_type == 'btl' and not ctd_temp_is_empty and not use_ctd_psal and not use_bottle_salinity:
    #     flag = 'BTL'
    else:
        flag = None

    # if pd.notnull(temp_qc):
    #     qc = temp_qc
    # elif pd.notnull(psal_qc):
    #     qc = psal_qc
    # elif pd.notnull(salinity_qc):
    #     qc = salinity_qc
    # else:
    #     qc = None

    measurements_source = flag

    measurements_source_qc = {}
    measurements_source_qc['qc'] = meas_qc
    measurements_source_qc[f'use_temp_{data_type}'] = not ctd_temp_is_empty
    measurements_source_qc[f'use_psal_{data_type}'] = use_ctd_psal
    if use_bottle_salinity:
        measurements_source_qc['use_salinity_btl'] = use_bottle_salinity

    # For json_str, convert True, False to 'true','false'
    measurements_source_qc = convert_boolean(measurements_source_qc)

    return measurements_source, measurements_source_qc


def get_argovis_core_meas_values_per_type(data_type):

    # Add in bottle_salinity since will use this in
    # measurements to check if have ctd_salinity, and
    # if now, use bottle_salinity

    return ['pres', f"temp_{data_type}", f"temp_{data_type}_qc", f"psal_{data_type}",  f"psal_{data_type}_qc", 'salinity_btl', 'salinity_btl_qc']


def get_meas_qc_vals(core_cols, data_type, df_meas):

    # Check if qc = 0 or 2 for measurements source or None

    temp_qc = None
    psal_qc = None
    salinity_qc = None

    # may not have ctd temp with ref scale
    if f"temp_{data_type}" in core_cols and f"temp_{data_type}_qc" in core_cols:
        temperature_df = df_meas[[f"temp_{data_type}", f"temp_{data_type}_qc"]]
        temp_subset = temperature_df[temperature_df[f"temp_{data_type}"].notnull(
        )]
        temp_qc = pd.unique(temp_subset[f"temp_{data_type}_qc"])

        if len(temp_qc):
            temp_qc = int(temp_qc[0])
        else:
            temp_qc = None

        df_meas['qc_temp'] = temp_qc

    # Assume psal_qc has one value to get a qc value for it,
    # if not, set qc to None (or an array of values)
    if f"psal_{data_type}" in core_cols and f"psal_{data_type}_qc" in core_cols:

        psal_df = df_meas[[f"psal_{data_type}", f"psal_{data_type}_qc"]]
        psal_subset = psal_df[psal_df[f"psal_{data_type}"].notnull()]
        psal_qc = pd.unique(psal_subset[f"psal_{data_type}_qc"])

        if len(psal_qc):
            psal_qc = int(psal_qc[0])
        else:
            psal_qc = None

        df_meas['qc_psal'] = psal_qc

    # Assume salinity_btl_qc has one value. If not, set to None (or unknown or all?)
    if "salinity_btl" in core_cols and "salinity_btl_qc" in core_cols:

        salinity_df = df_meas[["salinity_btl", "salinity_btl_qc"]]
        salinity_subset = salinity_df[salinity_df["salinity_btl"].notnull()]
        salinity_qc = pd.unique(salinity_subset["salinity_btl_qc"])

        if len(salinity_qc):
            salinity_qc = int(salinity_qc[0])
        else:
            salinity_qc = None

        df_meas['qc_salinity'] = salinity_qc

    return df_meas


def filter_salinity(df_meas):

    # Now filter to keep psal over salinity and if no
    # psal, keep salinity  but rename to psal

    if 'psal' in df_meas.columns:
        is_empty_psal = df_meas['psal'].isnull().values.all()

    if 'psal' in df_meas.columns and 'salinity' in df_meas.columns and not is_empty_psal:
        df_meas = df_meas.drop('salinity', axis=1)
    elif 'psal' in df_meas.columns and 'salinity' in df_meas.columns and is_empty_psal:
        df_meas = df_meas.drop('psal', axis=1)
    elif 'psal' not in df_meas.columns and 'salinity' in df_meas.columns:
        df_meas = df_meas.rename(columns={'salinity': 'psal'})

    return df_meas


def filter_meas_df(df_meas):

    # # Now filter to keep psal over salinity and if no
    # # psal, keep salinity  but rename to psal

    # if 'psal' in df_meas.columns:
    #     is_empty_psal = df_meas['psal'].isnull().values.all()

    # if 'psal' in df_meas.columns and 'salinity' in df_meas.columns and not is_empty_psal:
    #     df_meas = df_meas.drop('salinity', axis=1)
    # elif 'psal' in df_meas.columns and 'salinity' in df_meas.columns and is_empty_psal:
    #     df_meas = df_meas.drop('psal', axis=1)
    # elif 'psal' not in df_meas.columns and 'salinity' in df_meas.columns:
    #     df_meas = df_meas.rename(columns={'salinity': 'psal'})

    # TODO
    # Or do I want to keep all values even if pres only non null val?
    core_cols = ['temp', 'psal', 'salinity']
    found_core_cols = [col for col in df_meas.columns if col in core_cols]

    df_meas = df_meas.dropna(subset=found_core_cols, how='all')

    # filter out rows with qc_source !=0 or != 2 (means temp not valid)
    # psal could be qc = 2, but won't make sense if no temperature
    df_meas = df_meas.query('qc_source == 0 | qc_source == 2')

    # Following not working. Still getting NaN in json string
    # If necessary for json to be null, use simple json
    df_meas = df_meas.where(pd.notnull(df_meas), None)

    # Get meas qc source (which is the temp  qc)
    meas_source_qc = pd.unique(df_meas['qc_source'])

    if len(meas_source_qc) == 1:
        meas_source_qc = meas_source_qc[0]
    elif len(meas_source_qc) == 0:
        meas_source_qc = None
    else:
        logging.info(f"meas source qc not unique {meas_source_qc}")

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
        elem for elem in core_cols if '_qc' not in elem and elem != 'pressure']

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

    # Check if qc = 0 or 2 for measurements source or None
    #df_meas = get_meas_qc_vals(core_cols, data_type, df_meas)

    # Get temp qc value to be meas source qc (don't know if 0 or 2)
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

    # # If look at pressure, then all null values  for  temp, etc. are kept
    # #df_meas.dropna(subset=core_non_qc, how='all', inplace=True)

    # # If ignore pressure, delete all rows with all null
    # # But then removing station information, too if everything
    # # removed. # Can end up with an empty dataframe
    # df_meas.dropna(subset=core_non_qc_wo_press, how='all', inplace=True)

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

    for val_df in meas_df_groups.values():

        station_cast = val_df['station_cast'].values[0]

        # Now filter to core meas cols
        # still includes salinity but filtered
        # for salinity after get meas source
        val_df, meas_qc = filter_meas_df(val_df)

        measurements_source, measurements_source_qc = get_measurements_source(
            val_df, meas_qc, data_type)

        # Now filter to keep psal over salinity and if no
        # psal, keep salinity  but rename to psal
        val_df = filter_salinity(val_df)

        if val_df.empty:
            meas_dict_list = []
            logging.info(f"station cast {station_cast}")
            logging.info("meas is empty")
        else:
            meas_dict_list = val_df.to_dict('records')
            meas_dict_list = to_int_qc(meas_dict_list)

        meas_profile = {}
        meas_profile['station_cast'] = station_cast
        meas_profile['measurements'] = meas_dict_list

        all_meas_profiles.append(meas_profile)

        meas_source_profile = {}
        meas_source_profile['station_cast'] = station_cast
        meas_source_profile['measurementsSource'] = measurements_source
        all_meas_source_profiles.append(meas_source_profile)

        meas_source_profile_qc = {}
        meas_source_profile_qc['station_cast'] = station_cast
        meas_source_profile_qc['measurementsSourceQC'] = measurements_source_qc
        all_meas_source_profiles.append(meas_source_profile_qc)

    return all_meas_profiles, all_meas_source_profiles
