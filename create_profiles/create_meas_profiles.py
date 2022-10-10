import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
import logging

# Doesn't seem to do much
# https://stackoverflow.com/questions/47776936/why-is-a-computation-much-slower-within-a-dask-distributed-worker
#pd.options.mode.chained_assignment = None

#from create_profiles.create_cchdo_argovis_mappings import get_argovis_core_meas_values_per_type
# from create_profiles.create_cchdo_argovis_mappings import get_cchdo_core_meas_var_names

#import create_profiles.create_cchdo_argovis_mappings as mapping
import variable_naming.choose_names as choose_names


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


# TODO
# doesn't look to be used
def filter_measurements(data_type_profiles):

    # TODO
    # Change station_parameters keys if empty meas?

    # if val_df.empty:
    #     meas_names = {}
    #     meas_names['station_cast'] = station_cast
    #     meas_names['station_parameters'] = list(val_df.columns)
    #     all_meas_names.append(meas_names)

    # TODO
    # shouldn't this be filtering on cchdo names, and be
    # ctd_temperature and not just 'temperature'?

    output_profiles_list = []

    for profile in data_type_profiles:

        profile_dict = profile['profile_dict']
        station_cast = profile['station_cast']

        measurements = profile_dict['measurements']
        meas_sources = profile_dict['measurements_sources']

        # For json_str need to convert True, False to 'true','false'
        #meas_sources = convert_boolean(meas_sources)

        # Check if all elems null in measurements besides pressure
        all_vals = []
        all_temp_vals = []

        for obj in measurements:
            vals = [val for key, val in obj.items() if pd.notnull(val)
                    and key != 'pressure']
            all_vals.extend(vals)

            # check if no temp vals, then set to empty measurements
            temp_vals = [val for key, val in obj.items() if pd.notnull(val)
                         and key == 'temperature']
            all_temp_vals.extend(temp_vals)

        if not len(all_vals):

            measurements = []

        if not len(all_temp_vals):

            measurements = []

        profile_dict['measurements'] = measurements
        profile_dict['measurements_sources'] = meas_sources

        output_profile = {}
        output_profile['profile_dict'] = profile_dict
        output_profile['station_cast'] = station_cast

        output_profiles_list.append(output_profile)

    return output_profiles_list


def get_measurements_sources(df_meas):

    using_ctd_salinity = False
    using_bottle_salinity = False
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

    has_bottle_salinity_col = 'bottle_salinity' in df_meas.columns
    if has_bottle_salinity_col:
        all_bottle_salinity_empty = df_meas['bottle_salinity'].isnull().all()
    else:
        all_bottle_salinity_empty = True

    if has_ctd_salinity_col and not all_ctd_salinity_empty:
        using_ctd_salinity = True
    elif has_ctd_salinity_col and all_ctd_salinity_empty and has_bottle_salinity_col and not all_bottle_salinity_empty:
        using_bottle_salinity = True
    elif not has_ctd_salinity_col and has_bottle_salinity_col and not all_bottle_salinity_empty:
        using_bottle_salinity = True

    meas_sources = {}

    if has_ctd_temperature_col:
        meas_sources["ctd_temperature"] = using_ctd_temperature

    if has_ctd_temperature_68_col:
        meas_sources["ctd_temperature_68"] = using_ctd_temperature_68

    if has_ctd_salinity_col:
        meas_sources['ctd_salinity'] = using_ctd_salinity

    if has_bottle_salinity_col:
        meas_sources['bottle_salinity'] = using_bottle_salinity

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

    if 'ctd_salinity' in meas_sources:

        using_ctd_salinity = meas_sources['ctd_salinity']

        if not using_ctd_salinity:
            df_meas = df_meas.drop('ctd_salinity', axis=1)

    if 'bottle_salinity' in meas_sources:

        using_bottle_salinity = meas_sources['bottle_salinity']

        if not using_bottle_salinity:
            df_meas = df_meas.drop('bottle_salinity', axis=1)

    return df_meas


def get_core_cols_from_hierarchy(df):

    # core cols includes '_qc' vars
    # core_names = mapping.get_cchdo_core_meas_var_names()
    core_names = choose_names.get_cchdo_core_meas_var_names()

    # Salinity names not filtered yet

    columns = list(df.columns)
    core_cols = [col for col in columns if col in core_names]

    # There is a hierarchy of which variable to use in core cchdo names
    # if both ref scale and units variables exist
    temperature_name = choose_names.choose_core_temperature_from_hierarchy(
        core_cols)

    hierarchy_temperature_names = [temperature_name, temperature_name + '_qc']

    # Use only hierarchy core value

    non_temperature = [
        col for col in core_cols if col not in hierarchy_temperature_names]

    core_cols = [*non_temperature, *hierarchy_temperature_names]

    core_cols_nonqc = [col for col in core_cols if '_qc' not in col]

    return core_cols_nonqc


def filter_meas_core_cols(df_meas):

    # Remove cols such as 'N_PROF', 'station_cast' and qc_source cols.

    # And only keep top hierarchy of temperature and salinity
    # filter out objects with only a pressure or
    # if no temperature

    core_cols_nonqc = get_core_cols_from_hierarchy(df_meas)

    # Remove any columns that are not core
    df_meas = df_meas[core_cols_nonqc]

    df_meas = df_meas.sort_values(by=['pressure'])

    # Remove objects that have all core values nan
    df_meas = df_meas.dropna(how='all')

    # Filter out if pressure is nan
    df_meas = df_meas[df_meas['pressure'].notnull()]

    # Filter out if temperature is nan
    # Don't do this yet since using temp existence as part
    # of measurements cacluation when combining btl and ctd
    #df_meas = df_meas[df_meas[temperature_name].notnull()]

    # Following not working. Still getting NaN in json string
    # If necessary for json to be null, use simple json
    df_meas = df_meas.where(pd.notnull(df_meas), None)

    # no_temp = df_meas[temperature_name].isnull().all()

    # if no_temp:
    #     # Set to empty dataframe
    #     df_meas = pd.DataFrame()

    return df_meas


def create_measurements_df_all(df):

    # Don't rename to argovis, so use cchdo names

    # For measurements, keep core vars pres, temp, psal, salinity
    # Later when filter measurements, if salinity is used,
    # it will be renamed to psal

    # core cols includes '_qc' vars

    # Keep all core named values and filter more when looking
    # at each profile

    core_names = choose_names.get_cchdo_core_meas_var_names()

    core_cols = [col for col in df.columns if col in core_names]

    # pressure has no qc so don't need to look at it for qc=0 or 2
    # not true anymore
    # core_non_qc_wo_press = [
    #     col for col in core_cols if '_qc' not in col and col != 'pressure']
    core_non_qc = [
        col for col in core_cols if '_qc' not in col]

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

    # for col in core_non_qc_wo_press:
    for col in core_non_qc:

        qc_col = f"{col}_qc"

        try:
            df_meas.loc[(df_meas[qc_col] != 0) &
                        (df_meas[qc_col] != 2), col] = np.nan

        except KeyError:
            pass

    # # drop qc columns
    # for col in df_meas.columns:
    #     if col.endswith('_qc'):
    #         df_meas = df_meas.drop([col], axis=1)

    # df_meas = df_meas.sort_values(by=['pressure'])

    return df_meas


def create_meas_profiles(df_param, data_type):

    # ********************************
    # Create all measurements profiles
    # ********************************

    logging.info("Create all Measurements profiles")

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

        # Now filter to core meas cols
        # still includes bottle salinity because want to see if
        # it will be kept in measurements if ctd_salinity doesn't exist

        # If temperature nan for all values, return df_meas empty df
        df_meas = filter_meas_core_cols(val_df)

        # Now find which core variables are used (meas_source_flag)
        meas_source_flag = data_type.upper()

        # measurements source is a dict with qc value and
        # flags of using temperature, ctd_salinity, bottle_salinity
        meas_sources = get_measurements_sources(df_meas)

        # Now filter to keep ctd_salinity over bottle salinity and if no
        # ctd_salinity, keep bottle_salinity (rename to argovis psal name later)

        # Filter salinity and psal using meas_sources
        df_meas = filter_salinity(df_meas, meas_sources)

        # Filter on temperature
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

        meas_names = {}
        meas_names['station_cast'] = station_cast
        #meas_names['stationParameters'] = list(df_meas.columns)
        all_meas_names.append(meas_names)

        meas_profile = {}
        meas_profile['station_cast'] = station_cast
        meas_profile['measurements'] = meas_dict_list

        all_meas_profiles.append(meas_profile)

        meas_source_profile = {}
        meas_source_profile['station_cast'] = station_cast
        meas_source_profile['measurements_source'] = meas_source_flag
        all_meas_source_profiles.append(meas_source_profile)

        meas_sources_profile = {}
        meas_sources_profile['station_cast'] = station_cast
        meas_sources_profile['measurements_sources'] = meas_sources
        all_meas_source_profiles.append(meas_sources_profile)

    return all_meas_profiles, all_meas_source_profiles, all_meas_names
