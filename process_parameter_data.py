# process parameter data

import pandas as pd
import numpy as np
import json
import re
import logging

import filter_profiles as fp
import get_variable_mappings as gvm
import get_profile_mapping_and_conversions as pmc


# def create_bgc_meas_list(df):

#     json_str = df.to_json(orient='records')

#     # _qc":2.0
#     # If tgoship_argovis_name_mapping_btl is '.0' in qc value, remove it to get an int
#     json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)

#     data_dict_list = json.loads(json_str)

#     return data_dict_list


def get_measurements_source(df_meas, temp_qc, type):

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

    # ctd_temp_cols = ['ctd_temperature', 'ctd_temperature_68']
    # is_ctd_temp_col = any(
    #     [True if col in df_meas.columns else False for col in ctd_temp_cols])
    #is_ctd_temp_col = 'temp' in df_meas.columns
    # Don't want to match variables with temp in name
    is_ctd_temp = next(
        (True for col in df_meas.columns if col == 'temp'), False)

    if is_ctd_temp:
        is_ctd_temp_empty = df_meas['temp'].isnull().all()
    else:
        is_ctd_temp_empty = True

    # if is_ctd_temp_col:
    #     is_ctd_temp_empty = next(
    #         (df_meas[col].isnull().all() for col in df_meas.columns), False)

    if is_ctd_temp_empty:
        flag = None
    elif not is_ctd_temp_empty and use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    elif not is_ctd_temp_empty and not use_ctd_psal and use_bottle_salinity:
        flag = 'BTL'
    elif type == 'ctd' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    elif type == 'btl' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
        flag = 'BTL'
    else:
        flag = None

    # json_str = df_meas.to_json(orient='records')

    # data_dict_list = json.loads(json_str)

    measurements_source = flag

    measurements_source_qc = {}
    measurements_source_qc['qc'] = temp_qc
    measurements_source_qc['use_ctd_temp'] = not is_ctd_temp_empty
    measurements_source_qc['use_ctd_psal'] = use_ctd_psal
    if use_bottle_salinity:
        measurements_source_qc['use_bottle_salinity'] = use_bottle_salinity

    # For json_str, convert True, False to 'true','false'
    measurements_source_qc = fp.convert_boolean(measurements_source_qc)

    return measurements_source, measurements_source_qc


def create_measurements_df_all(df,  type):

    # core values includes '_qc' vars
    core_values = gvm.get_argovis_core_values_per_type(type)
    table_columns = list(df.columns)
    core_cols = [col for col in table_columns if col in core_values]

    core_non_qc = [elem for elem in core_cols if '_qc' not in elem]

    df_meas = df[core_cols].copy()

    # here

    def check_qc(row):
        if pd.notnull(row[1]) and int(row[1]) == 0:
            return row[0]
        elif pd.notnull(row[1]) and int(row[1]) == 2:
            return row[0]
        else:
            return np.nan

    # If qc != 2, set corresponding value to np.nan
    for col in core_non_qc:

        qc_key = f"{col}_qc"

        try:
            df_meas[col] = df_meas[[col, qc_key]].apply(
                check_qc, axis=1)
        except:
            pass

    # drop qc columns now that have marked non_qc column values
    for col in df_meas.columns:
        if '_qc' in col:
            df_meas = df_meas.drop([col], axis=1)

    # If all core values have nan, drop row
    # This won't work since
    df_meas = df_meas.dropna(how='all')

    df_meas = df_meas.sort_values(by=['pres'])

    # Remove type ('btl', 'ctd') from  variable names
    column_mapping = {}
    column_mapping[f"psal_{type}"] = 'psal'
    column_mapping[f"temp_{type}"] = 'temp'
    column_mapping[f"salinity_btl"] = 'salinity'

    df_meas = df_meas.rename(columns=column_mapping)

    return df_meas


def find_temp_qc_val(df, type):

    # Check if have temp_{type}_qc with qc = 0 or qc = 2 values
    has_ctd_temp_qc = f"temp_{type}_qc" in df.columns

    if has_ctd_temp_qc:

        temp_qc = df[f"temp_{type}_qc"]
        temp_qc = list(temp_qc.array)

        if 0 in temp_qc:
            qc = 0
        elif 2 in temp_qc:
            qc = 2
        else:
            qc = None

    else:
        qc = None

    return qc


def check_if_temp_qc(nc, type):

    # Now check so see if there is a 'temp_{type}'  column and a corresponding
    # qc col. 'temp_{type}_qc'. If not, add a 'temp' qc col. with values 0

    has_ctd_temp = f"temp_{type}" in nc.keys()
    has_ctd_temp_qc = f"temp_{type}_qc" in nc.keys()

    if has_ctd_temp and not has_ctd_temp_qc:
        temp_shape = np.shape(nc[f"temp_{type}"])
        shape = np.transpose(temp_shape)
        temp_qc = np.zeros(shape)

        nc[f"temp_{type}_qc"] = (['N_PROF', 'N_LEVELS'], temp_qc)

    return nc


def apply_equations_and_ref_scale(nc):

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    # Converting to argovis ref scale if needed
    nc = pmc.convert_goship_to_argovis_ref_scale(nc)

    # Apply equations to convert units
    nc = pmc.convert_goship_to_argovis_units(nc)

    return nc
