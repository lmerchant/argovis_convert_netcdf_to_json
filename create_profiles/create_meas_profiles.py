import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
import logging

# Doesn't seem to do much
# https://stackoverflow.com/questions/47776936/why-is-a-computation-much-slower-within-a-dask-distributed-worker
#pd.options.mode.chained_assignment = None

from create_profiles.create_cchdo_argovis_mappings import get_argovis_core_meas_values_per_type
# from create_profiles.create_cchdo_argovis_mappings import get_cchdo_core_meas_var_names

import create_profiles.create_cchdo_argovis_mappings as mapping


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


# def get_measurements_sources_orig(df_meas, meas_qc, data_type):

#     using_temp = False
#     using_psal = False
#     using_salinity = False

#     has_temp_col = 'temp' in df_meas.columns
#     if has_temp_col:
#         all_temp_empty = df_meas['temp'].isnull().all()
#     else:
#         all_temp_empty = True

#     has_psal_col = 'psal' in df_meas.columns
#     if has_psal_col:
#         all_psal_empty = df_meas['psal'].isnull().all()
#     else:
#         all_psal_empty = True

#     has_salinity_column = 'salinity' in df_meas.columns
#     if has_salinity_column:
#         all_salinity_empty = df_meas['salinity'].isnull().all()
#     else:
#         all_salinity_empty = True

#     if has_temp_col and not all_temp_empty:
#         using_temp = True
#     elif has_temp_col and all_temp_empty:
#         using_temp = False

#     if has_psal_col and not all_psal_empty:
#         using_psal = True
#     elif has_psal_col and all_psal_empty and has_salinity_column and not all_salinity_empty:
#         using_salinity = True
#     elif not has_psal_col and has_salinity_column and not all_salinity_empty:
#         using_salinity = True

#     logging.info(f"using_temp {using_temp}")
#     logging.info(f"using psal {using_psal}")
#     logging.info(f"using_salinity  {using_salinity}")

#     # if not using_temp and not using_psal and not using_salinity:
#     #     logging.info(
#     #         "For single data type file, source flag is none because no temp or psal")
#     #     meas_source_flag = None
#     # elif not using_temp and using_psal and not using_salinity:
#     #     meas_source_flag = 'CTD'
#     # elif not using_temp and not using_psal and using_salinity:
#     #     meas_source_flag = 'BTL'
#     # elif using_temp and using_psal and not using_salinity:
#     #     meas_source_flag = 'CTD'
#     # elif using_temp and not using_psal and using_salinity:
#     #     meas_source_flag = 'BTL_CTD'  # Or is it just BTL?
#     # elif using_temp and not using_psal and not using_salinity:
#     #     # using temperature so CTD meas_source_flag
#     #     meas_source_flag = 'CTD'

#     if data_type == 'btl':
#         meas_source_flag = 'BTL'

#     if data_type == 'ctd':
#         meas_source_flag = 'CTD'

#     meas_sources = {}
#     meas_sources['qc'] = meas_qc
#     meas_sources[f'use_temp_{data_type}'] = using_temp
#     if has_psal_col:
#         meas_sources[f'use_psal_{data_type}'] = using_psal
#     if has_salinity_column:
#         meas_sources['use_salinity_btl'] = using_salinity

#     # For json_str, convert True, False to 'true','false'
#     meas_sources = convert_boolean(meas_sources)

#     return meas_source_flag, meas_sources


# def get_argovis_core_meas_values_per_type(data_type):

#     # Add in bottle_salinity since will use this in
#     # measurements to check if have ctd_salinity, and
#     # if now, use bottle_salinity

#     # Since didn't rename to argovis yet, use cchdo names
#     # which means multiple temperature and oxygen names for ctd vars
#     # standing for different ref scales

#     return ['pres', f"temp_{data_type}", f"temp_{data_type}_qc", f"psal_{data_type}",  f"psal_{data_type}_qc", 'salinity_btl', 'salinity_btl_qc']

# def create_measurements_df_all_orig(df, data_type):

#     # For measurements, keep core vars pres, temp, psal, salinity
#     # Later when filter measurements, if salinity is used,
#     # it will be renamed to psal

#     # core values includes '_qc' vars
#     core_values = get_argovis_core_meas_values_per_type(data_type)
#     table_columns = list(df.columns)
#     core_cols = [col for col in table_columns if col in core_values]

#     core_non_qc = [elem for elem in core_cols if '_qc' not in elem]

#     core_non_qc_wo_press = [
#         elem for elem in core_cols if '_qc' not in elem and elem != 'pres']

#     # Also include N_PROF, N_LEVELS, station_cast for unique identifiers
#     cols_to_keep = core_cols
#     identifier_cols = ['N_PROF', 'station_cast']
#     cols_to_keep.extend(identifier_cols)

#     df_meas = df[cols_to_keep].copy()

#     # If qc != 0 or 2, set corresponding non_qc value to np.nan
#     for col in core_non_qc:

#         qc_col = f"{col}_qc"

#         try:
#             df_meas.loc[(df_meas[qc_col] != 0) &
#                         (df_meas[qc_col] != 2), col] = np.nan

#         except KeyError:
#             pass

#     # Get temp qc value to be meas source qc (don't know if 0 or 2)
#     # Could also be a bad qc or none
#     df_meas['qc_source'] = df_meas[f"temp_{data_type}_qc"]

#     # drop qc columns
#     for col in df_meas.columns:
#         if col.endswith('_qc'):
#             df_meas = df_meas.drop([col], axis=1)

#     df_meas = df_meas.sort_values(by=['pres'])

#     # Remove data_type ('btl', 'ctd') from  variable names
#     column_mapping = {}
#     column_mapping[f"psal_{data_type}"] = 'psal'
#     column_mapping[f"temp_{data_type}"] = 'temp'
#     column_mapping[f"salinity_btl"] = 'salinity'

#     # Will change name of salinity to psal later when filter it

#     df_meas = df_meas.rename(columns=column_mapping)

#     return df_meas


def get_core_cols_from_hierarchy(df):

    # TODO
    # don't include oxygen values in core cols

    # core cols includes '_qc' vars
    #core_values = get_argovis_core_meas_values_per_type(data_type)
    core_names = mapping.get_cchdo_core_meas_var_names()

    core_temperature_names = mapping.get_cchdo_core_meas_temperature_names()

    # Salinity names not filtered yet
    #core_salinity_names = mapping.get_cchdo_core_meas_salinity_names()

    # oxygen not a core value
    #core_oxygen_names = mapping.get_cchdo_core_meas_oxygen_names()

    columns = list(df.columns)
    core_cols = [col for col in columns if col in core_names]

    # There is a hierarchy of which variable to use in core cchdo names
    # if both ref scale and units variables exist
    temperature_name = mapping.choose_core_temperature_from_hierarchy(
        core_cols)

    #oxygen_name = mapping.choose_core_oxygen_from_hierarchy(core_cols)

    # hierarchy_temp_oxy_names = [
    #     temperature_name, temperature_name + '_qc', oxygen_name, oxygen_name + '_qc']

    hierarchy_temperature_names = [temperature_name, temperature_name + '_qc']

    # Use only hierarchy core value
    # get non temperature and oxygen names and then add back in
    # temperature and oxygen names from hierarchy
    # all_temperature_oxygen_names = [
    #     *core_temperature_names, *core_oxygen_names]

    # non_temperature_oxygen = [
    #     col for col in core_cols if col not in all_temperature_oxygen_names]

    non_temperature = [
        col for col in core_cols if col not in hierarchy_temperature_names]

    # core_cols = [*non_temperature_oxygen, *hierarchy_temp_oxy_names]

    core_cols = [*non_temperature, *hierarchy_temperature_names]

    #core_non_qc = [elem for elem in core_cols if '_qc' not in elem]

    # return core_cols, temperature_name, oxygen_name

    core_cols_nonqc = [col for col in core_cols if '_qc' not in col]

    return core_cols_nonqc


def filter_measurements(data_type_profiles):

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

        meas_sources = profile_dict['measurementsSources']

        # For json_str need to convert True, False to 'true','false'
        #meas_sources = convert_boolean(meas_sources)

        # Check if all elems null in measurements besides pressure
        all_vals = []
        all_temp_vals = []

        for obj in measurements:
            vals = [val for key, val in obj.items() if pd.notnull(val)
                    and key != 'pres']
            all_vals.extend(vals)

            # check if no temp vals, then set to empty measurements
            temp_vals = [val for key, val in obj.items() if pd.notnull(val)
                         and key == 'temp']
            all_temp_vals.extend(temp_vals)

        if not len(all_vals):
            logging.info(f"Station cast {station_cast}")
            logging.info("All elems null so measurements = []")
            measurements = []

        if not len(all_temp_vals):
            logging.info(f"Station cast {station_cast}")
            logging.info("All temps null so measurements = []")
            measurements = []

        profile_dict['measurements'] = measurements
        profile_dict['measurementsSources'] = meas_sources

        output_profile = {}
        output_profile['profile_dict'] = profile_dict
        output_profile['station_cast'] = station_cast

        output_profiles_list.append(output_profile)

    return output_profiles_list


def get_measurements_sources(df_meas):

    using_ctd_salinity = False
    using_btl_salinity = False
    using_ctd_temperature = False
    using_ctd_temperature_68 = False

    # Check if using good temperature
    has_ctd_temperature_col = 'ctd_temperature' in df_meas.columns
    has_ctd_temperature_68_col = 'ctd_temperature_68' in df_meas.columns

    if has_ctd_temperature_col:
        all_ctd_temperature_empty = df_meas['ctd_temperature'].isnull().all()
    else:
        all_ctd_temperature_empty = True

    if has_ctd_temperature_68_col:
        all_ctd_temperature_68_empty = df_meas['ctd_temperature_68'].isnull(
        ).all()
    else:
        all_ctd_temperature_68_empty = True

    if has_ctd_temperature_col and not all_ctd_temperature_empty:
        using_ctd_temperature = True

    if has_ctd_temperature_68_col and not all_ctd_temperature_68_empty:
        using_ctd_temperature_68 = True

    if using_ctd_temperature and has_ctd_temperature_68_col:
        using_ctd_temperature_68 = False

    # Check if ctd_salinity exists and if non null values in col
    has_ctd_salinity_col = 'ctd_salinity' in df_meas.columns

    if has_ctd_salinity_col:
        all_ctd_salinity_empty = df_meas['ctd_salinity'].isnull().all()
    else:
        all_ctd_salinity_empty = True

    has_btl_salinity_col = 'bottle_salinity' in df_meas.columns
    if has_btl_salinity_col:
        all_bottle_salinity_empty = df_meas['bottle_salinity'].isnull().all()
    else:
        all_bottle_salinity_empty = True

    if has_ctd_salinity_col and not all_ctd_salinity_empty:
        using_ctd_salinity = True
    elif has_ctd_salinity_col and all_ctd_salinity_empty and has_btl_salinity_col and not all_bottle_salinity_empty:
        using_btl_salinity = True
    elif not has_ctd_salinity_col and has_btl_salinity_col and not all_bottle_salinity_empty:
        using_btl_salinity = True

    logging.info(f"using ctd temperature {using_ctd_temperature}")
    logging.info(f"using ctd temperature 68 {using_ctd_temperature_68}")
    logging.info(f"using ctd salinity {using_ctd_salinity}")
    logging.info(f"using bottle salinity  {using_btl_salinity}")

    meas_sources = {}

    if has_ctd_temperature_col:
        meas_sources["ctd_temperature"] = using_ctd_temperature

    if has_ctd_temperature_68_col:
        meas_sources["ctd_temperature_68"] = using_ctd_temperature_68

    if has_ctd_salinity_col:
        meas_sources['ctd_salinity'] = using_ctd_salinity

    if has_btl_salinity_col:
        meas_sources['bottle_salinity'] = using_btl_salinity

    # For json_str need to convert True, False to 'true','false'
    #meas_sources = convert_boolean(meas_sources)

    return meas_sources


def filter_temperature(df_meas, meas_sources):
    if 'ctd_temperature' in meas_sources:

        using_ctd_temperature = meas_sources['ctd_temperature']

        if not using_ctd_temperature:
            df_meas = df_meas.drop('ctd_temperature', axis=1)

    if 'using_ctd_temperature_68' in meas_sources:

        using_ctd_temperature_68 = meas_sources['using_ctd_temperature_68']

        if not using_ctd_temperature_68:
            df_meas = df_meas.drop('ctd_temperature_68', axis=1)

    return df_meas


def filter_salinity(df_meas, meas_sources):

    print('meas sources')
    print(meas_sources)

    if 'ctd_salinity' in meas_sources:

        print('ctd_salinity in meas sources')

        using_ctd_salinity = meas_sources['ctd_salinity']

        if not using_ctd_salinity:
            df_meas = df_meas.drop('ctd_salinity', axis=1)

            print('not using ctd salinity')

    if 'bottle_salinity' in meas_sources:

        print('bottle_salinity in meas sources')

        using_bottle_salinity = meas_sources['bottle_salinity']

        if not using_bottle_salinity:
            df_meas = df_meas.drop('bottle_salinity', axis=1)

            print('not using bottle salinity')

    print('df meas columns after filtering')
    print(df_meas.columns)

    # ctd_salinity_exists = 'ctd_salinity' in df_meas.columns
    # if ctd_salinity_exists:
    #     is_empty_ctd_salinity = df_meas['ctd_salinity'].isnull().values.all()

    # btl_salinity_exists = 'bottle_salinity' in df_meas.columns
    # if btl_salinity_exists:
    #     is_empty_btl_salinity = df_meas['bottle_salinity'].isnull(
    #     ).values.all()

    # if ctd_salinity_exists and btl_salinity_exists and not is_empty_ctd_salinity:
    #     df_meas = df_meas.drop('bottle_salinity', axis=1)
    # elif ctd_salinity_exists and btl_salinity_exists and is_empty_ctd_salinity and not is_empty_btl_salinity:
    #     df_meas = df_meas.drop('ctd_salinity', axis=1)

    # # Now remove empty salinity columns if no salinity value exists
    # if 'ctd_salinity' in df_meas.columns and is_empty_ctd_salinity:
    #     df_meas = df_meas.drop('ctd_salinity', axis=1)

    # if 'bottle_salinity' in df_meas.columns and is_empty_btl_salinity:
    #     df_meas = df_meas.drop('bottle_salinity', axis=1)

    return df_meas


def filter_meas_core_cols(df_meas):

    # Remove cols such as 'N_PROF', 'station_cast' and qc_source cols.
    # And only keep top hierarchy of temperature and salinity
    # filter out objects with only a pressure or
    # if no temperature

    #core_cols = ['temp', 'psal', 'salinity']

    # here
    # core cols includes '_qc' vars
    # core_cols, temperature_name, oxygen_name = get_core_cols_from_hierarchy(
    #     df_meas)

    core_cols_nonqc = get_core_cols_from_hierarchy(df_meas)

    # core_cols_nonqc = [col for col in core_cols if '_qc' not in col]

    # found_core_cols = [
    #     col for col in df_meas.columns if col in core_cols_nonqc]

    # print(f"found core cols from df_meas {found_core_cols}")

    # Get temp qc value to be meas source qc (don't know if 0 or 2)
    # Could also be a bad qc or none. Will store as key in final json,
    # but not sure this is relevant

    #df_meas['qc_source'] = df_meas[f"{temperature_name}_qc"]

    # Get meas qc source col (which is the temp qc)
    #temperature_qc = pd.unique(df_meas[f"{temperature_name}_qc"])

    # # remove null values
    # temperature_qc = [qc for qc in temperature_qc if pd.notnull(qc)]

    # temperature_qc = [
    #     int(qc) for qc in temperature_qc if int(qc) == 0 or int(qc) == 2]

    # if not len(temperature_qc):
    #     temperature_qc = None

    # if no_temp:
    #     # Set to empty dataframe
    #     df_meas = pd.DataFrame()
    #     temperature_qc = None

    #logging.info(f"meas source temperature qc {temperature_qc}")

    # Remove any columns that are not core
    df_meas = df_meas[core_cols_nonqc]

    # # drop qc columns
    # for col in df_meas.columns:
    #     if col.endswith('_qc'):
    #         df_meas = df_meas.drop([col], axis=1)

    df_meas = df_meas.sort_values(by=['pressure'])

    # Remove objects that have all core values nan
    df_meas = df_meas.dropna(how='all')

    # Filter out if pressure is nan
    df_meas = df_meas[df_meas['pressure'].notnull()]

    # Filter out if temperature is nan
    # TODO
    # do this later after find meas source qc using flags
    #df_meas = df_meas[df_meas[temperature_name].notnull()]

    # Following not working. Still getting NaN in json string
    # If necessary for json to be null, use simple json
    df_meas = df_meas.where(pd.notnull(df_meas), None)

    # no_temp = df_meas[temperature_name].isnull().all()

    # if no_temp:
    #     # Set to empty dataframe
    #     df_meas = pd.DataFrame()
    #     temperature_qc = None

    return df_meas


def create_measurements_df_all(df):

    # Don't rename to argovis, so use cchdo names

    # For measurements, keep core vars pres, temp, psal, salinity
    # Later when filter measurements, if salinity is used,
    # it will be renamed to psal

    # core cols includes '_qc' vars

    # Keep all core named values and filter more when looking
    # at each profile

    core_names = mapping.get_cchdo_core_meas_var_names()

    core_cols = [col for col in df.columns if col in core_names]

    # pressure has no qc so don't need to look at it for qc=0 or 2
    core_non_qc_wo_press = [
        col for col in core_cols if '_qc' not in col and col != 'pressure']

    # Also include N_PROF, station_cast for unique identifiers
    cols_to_keep = core_cols
    identifier_cols = ['N_PROF', 'station_cast']
    cols_to_keep.extend(identifier_cols)

    df_meas = df[cols_to_keep].copy()

    # If qc != 0 or 2, set corresponding non_qc value to np.nan
    # So know to exclude them from core measurements.
    # Will filter out later for cases where
    # pressure or temperature are null. Want to filter
    # for each station_cast profile

    for col in core_non_qc_wo_press:

        qc_col = f"{col}_qc"

        try:
            df_meas.loc[(df_meas[qc_col] != 0) &
                        (df_meas[qc_col] != 2), col] = np.nan

        except KeyError:
            pass

    # # Get temp qc value to be meas source qc (don't know if 0 or 2)
    # # Could also be a bad qc or none. Will store as key in final json,
    # # but not sure this is relevant

    # df_meas['qc_source'] = df_meas[f"{temperature_name}_qc"]

    # # drop qc columns
    # for col in df_meas.columns:
    #     if col.endswith('_qc'):
    #         df_meas = df_meas.drop([col], axis=1)

    # df_meas = df_meas.sort_values(by=['pressure'])

    # Not using argovis names, so don't need this
    # # Remove data_type ('btl', 'ctd') from  variable names
    # column_mapping = {}
    # column_mapping[f"psal_{data_type}"] = 'psal'
    # column_mapping[f"temp_{data_type}"] = 'temp'
    # column_mapping[f"salinity_btl"] = 'salinity'
    # df_meas = df_meas.rename(columns=column_mapping)

    return df_meas


def create_meas_profiles(df_param, data_type):

    # ********************************
    # Create all measurements profiles
    # ********************************

    logging.info("Create all Measurements profiles")

    # df_meas = df_param.groupby('N_PROF').apply(
    #     create_measurements_df_all, data_type)

    df_meas = df_param.groupby('N_PROF').apply(
        create_measurements_df_all)

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

        # Now filter to core meas cols using temp and oxygen
        # from hierarchy
        # still includes bottle salinity because want to see if
        # it will be kept in measurements if ctd_salinity doesn't exist

        # If temperature nan for all values, return df_meas empty df
        df_meas = filter_meas_core_cols(val_df)

        # Now find which core variables are used (meas_source_flag)
        # meas_source_flag, meas_sources = get_measurements_sources(
        #     val_df, meas_qc, data_type)

        # if not df_meas.empty:
        #     meas_source_flag = {'source': data_type}
        #     meas_sources = get_measurements_sources(df_meas, meas_qc)
        # else:
        #     meas_source_flag = {'source': None}
        #     meas_sources = {'source_qc': None}

        meas_source_flag = data_type.upper()

        # measurements source is a dict with qc value and
        # flags of using temperature, ctd_salinity, bottle_salinity
        meas_sources = get_measurements_sources(df_meas)

        # Now filter to keep ctd_salinity over bottle salinity and if no
        # ctd_salinity, keep bottle_salinity (rename to argovis psal name later)

        # Filter salinity and psal using meas_sources
        # if not df_meas.empty:
        #     df_meas = filter_salinity(df_meas)
        df_meas = filter_salinity(df_meas, meas_sources)

        df_meas = filter_temperature(df_meas, meas_sources)

        # TODO
        # change to check meas source qc flag of temperature
        using_ctd_temperature = False
        if 'ctd_temperature' in meas_sources:
            using_ctd_temperature = meas_sources['ctd_temperature']

        using_ctd_temperature_68 = False
        if 'ctd_temperature_68' in meas_sources:
            using_ctd_temperature_68 = meas_sources['ctd_temperature_68']

        if not using_ctd_temperature and not using_ctd_temperature_68:
            meas_dict_list = []
        else:
            meas_dict_list = df_meas.to_dict('records')
            meas_dict_list = to_int_qc(meas_dict_list)

        # if df_meas.empty:
        #     meas_dict_list = []
        # else:
        #     meas_dict_list = df_meas.to_dict('records')
        #     meas_dict_list = to_int_qc(meas_dict_list)

        meas_names = {}
        meas_names['station_cast'] = station_cast
        meas_names['stationParameters'] = list(df_meas.columns)
        all_meas_names.append(meas_names)

        meas_profile = {}
        meas_profile['station_cast'] = station_cast
        meas_profile['measurements'] = meas_dict_list

        all_meas_profiles.append(meas_profile)

        meas_source_profile = {}
        meas_source_profile['station_cast'] = station_cast
        meas_source_profile['measurementsSource'] = meas_source_flag
        all_meas_source_profiles.append(meas_source_profile)

        meas_sources_profile = {}
        meas_sources_profile['station_cast'] = station_cast
        meas_sources_profile['measurementsSources'] = meas_sources
        all_meas_source_profiles.append(meas_sources_profile)

        logging.info('measurements sources')
        logging.info(meas_sources)

    return all_meas_profiles, all_meas_source_profiles, all_meas_names
