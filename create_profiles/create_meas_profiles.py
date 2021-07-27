import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
import logging


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


def get_measurements_source(df_meas, data_type):

    if 'qc_temp' in df_meas.columns:
        temp_qc_df = df_meas['qc_temp']
        temp_qc = pd.unique(temp_qc_df)[0]
    else:
        temp_qc = None

    if 'qc_psal' in df_meas.columns:
        psal_qc_df = df_meas['qc_psal']
        psal_qc = pd.unique(psal_qc_df)[0]
    else:
        psal_qc = None

    if 'qc_salinity' in df_meas.columns:
        salinity_qc_df = df_meas['qc_salinity']
        salinity_qc = pd.unique(salinity_qc_df)[0]
    else:
        salinity_qc = None

    # See if using ctd_salinty or bottle_salinity
    # Is ctd_salinity NaN? If it is and bottle_salinity isn't NaN, use it
    # See if all ctd_salinity are NaN, if not, use it and
    # drop bottle salinity column
    is_ctd_sal_empty = True
    is_ctd_sal_column = 'psal' in df_meas.columns
    if is_ctd_sal_column:
        is_ctd_sal_empty = df_meas['psal'].isnull().all()

    is_bottle_sal_empty = True
    is_bottle_sal_column = 'salinity' in df_meas.columns
    if is_bottle_sal_column:
        # Check if not all NaN
        is_bottle_sal_empty = df_meas['salinity'].isnull().all()

    if is_ctd_sal_column and not is_ctd_sal_empty and is_bottle_sal_column:
        use_ctd_psal = True
        use_bottle_salinity = False
        df_meas = df_meas.drop(['salinity'], axis=1)
    elif is_ctd_sal_column and not is_ctd_sal_empty and not is_bottle_sal_column:
        use_ctd_psal = True
        use_bottle_salinity = False
    elif is_ctd_sal_column and is_ctd_sal_empty and is_bottle_sal_column and not is_bottle_sal_empty:
        use_ctd_psal = False
        use_bottle_salinity = True
        df_meas = df_meas.drop(['psal'], axis=1)
    elif not is_ctd_sal_column and is_bottle_sal_column and not is_bottle_sal_empty:
        use_ctd_psal = False
        use_bottle_salinity = True
    else:
        use_ctd_psal = False
        use_bottle_salinity = False

    # Don't want to match variables with temp in name
    is_ctd_temp = next(
        (True for col in df_meas.columns if col == 'temp'), False)

    if is_ctd_temp:
        is_ctd_temp_empty = df_meas['temp'].isnull().all()
    else:
        is_ctd_temp_empty = True

    if is_ctd_temp_empty:
        flag = None
    elif not is_ctd_temp_empty and use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    elif not is_ctd_temp_empty and not use_ctd_psal and use_bottle_salinity:
        flag = 'BTL'
    elif data_type == 'ctd' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    elif data_type == 'btl' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
        flag = 'BTL'
    else:
        flag = None

    if pd.notnull(temp_qc):
        qc = temp_qc
    elif pd.notnull(psal_qc):
        qc = psal_qc
    elif pd.notnull(salinity_qc):
        qc = salinity_qc
    else:
        qc = None

    measurements_source = flag

    measurements_source_qc = {}
    measurements_source_qc['qc'] = qc
    measurements_source_qc['use_ctd_temp'] = not is_ctd_temp_empty
    measurements_source_qc['use_ctd_psal'] = use_ctd_psal
    if use_bottle_salinity:
        measurements_source_qc['use_bottle_salinity'] = use_bottle_salinity

    # For json_str, convert True, False to 'true','false'
    measurements_source_qc = convert_boolean(measurements_source_qc)

    return measurements_source, measurements_source_qc


def get_argovis_core_meas_values_per_type(data_type):

    # Add in bottle_salinity since will use this in
    # measurements to check if have ctd_salinity, and
    # if now, use bottle_salinity

    return ['pres', f"temp_{data_type}", f"temp_{data_type}_qc", f"psal_{data_type}",  f"psal_{data_type}_qc", 'salinity_btl', 'salinity_btl_qc']


def create_measurements_df_all(df, data_type):

    # For measurements, keep core vars pres, temp, psal, salinity
    # Later when filter measurements, if salinity is used,
    # it will be renamed to psal

    # core values includes '_qc' vars
    core_values = get_argovis_core_meas_values_per_type(data_type)
    table_columns = list(df.columns)
    core_cols = [col for col in table_columns if col in core_values]

    core_non_qc = [elem for elem in core_cols if '_qc' not in elem]

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
    # Add qc column back in because need to know qc for
    # measurements source

    temp_qc = None
    psal_qc = None
    salinity_qc = None

    # # Assume temp_qc is one value

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

    # Assume psal_qc has one value
    if f"psal_{data_type}" in core_cols and f"psal_{data_type}_qc" in core_cols:
        psal_df = df_meas[[f"psal_{data_type}", f"psal_{data_type}_qc"]]
        psal_subset = psal_df[psal_df[f"psal_{data_type}"].notnull()]
        psal_qc = pd.unique(psal_subset[f"psal_{data_type}_qc"])

        if len(psal_qc):
            psal_qc = int(psal_qc[0])
        else:
            psal_qc = None

        df_meas['qc_psal'] = psal_qc

    # Assume salinity_btl_qc has one value
    if "salinity_btl" in core_cols and "salinity_btl_qc" in core_cols:
        psal_df = df_meas[["salinity_btl", "salinity_btl_qc"]]
        psal_subset = psal_df[psal_df["salinity_btl"].notnull()]
        salinity_qc = pd.unique(psal_subset["salinity_btl_qc"])

        if len(salinity_qc):
            salinity_qc = int(salinity_qc[0])
        else:
            salinity_qc = None

        df_meas['qc_salinity'] = salinity_qc

    # drop qc columns now that have marked non_qc column values
    for col in df_meas.columns:
        if col.endswith('_qc'):
            df_meas = df_meas.drop([col], axis=1)

    df_meas.dropna(subset=core_non_qc, how='all', inplace=True)

    df_meas = df_meas.sort_values(by=['pres'])

    # TODO
    # if all none, return empty datafeame

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

    logging.info('create all_meas profiles')

    # Returns a variable col df depending on which core vars exist
    df_meas = df_param.groupby('N_PROF').apply(
        create_measurements_df_all, data_type)

    # Remove N_PROF as index and drop because
    # already have an N_PROF column used for groupby
    df_meas = df_meas.reset_index(drop=True)

    # Change NaN to None so in json, converted to null
    df_meas = df_meas.replace({np.nan: None})

    meas_df_groups = dict(tuple(df_meas.groupby('N_PROF')))

    all_meas_profiles = []
    all_meas_source_profiles = []

    for val_df in meas_df_groups.values():

        measurements_source, measurements_source_qc = get_measurements_source(
            val_df, data_type)

        station_cast = val_df['station_cast'].values[0]

        # Drop any columns except core meas cols
        # TODO probably easier just to select columns than drop them
        # When filter later, will see if use psal or salinity if it exists

        columns = val_df.columns
        core_cols = ['pres', 'psal', 'temp', 'salinity']
        cols = [col for col in columns if col in core_cols]

        val_df = val_df[cols]

        # Change NaN to None so in json, converted to null
        #val_df = val_df.where(pd.notnull(val_df), None)

        meas_dict_list = val_df.to_dict('records')

        meas_profile = {}
        meas_profile['station_cast'] = station_cast
        meas_profile['measurements'] = to_int_qc(meas_dict_list)
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
