# process parameter data

import xarray as xr
import pandas as pd
import numpy as np
import json
import re
from datetime import datetime

import filter_measurements as fm
import get_variable_mappings as gvm
import rename_objects as rn


def filter_argovis_mapping(argovis_param_mapping, all_name_mapping, type):

    # Take param mapping and filter it to only  contain all_name_mapping
    # for each station_cast

    units = argovis_param_mapping['units']
    ref_scale = argovis_param_mapping['ref_scale']
    c_format = argovis_param_mapping['c_format']
    dtype = argovis_param_mapping['dtype']

    all_filtered_mappings = []

    for name_mapping in all_name_mapping:

        argovis_names = name_mapping['non_empty_cols']

        new_mapping = {}

        # ******************************
        # filter names to non empty cols
        # ******************************

        new_mapping['station_cast'] = name_mapping['station_cast']

        new_mapping['names'] = argovis_names

        new_mapping['units'] = {key: val for key,
                                val in units.items() if key in argovis_names}
        new_mapping['ref_scale'] = {
            key: val for key, val in ref_scale.items() if key in argovis_names}
        new_mapping['c_format'] = {
            key: val for key, val in c_format.items() if key in argovis_names}
        new_mapping['dtype'] = {key: val for key,
                                val in dtype.items() if key in argovis_names}

        all_filtered_mappings.append(new_mapping)

    return all_filtered_mappings


def filter_argovis_mapping(argovis_param_mapping, all_name_mapping, type):

    # Take param mapping and filter it to only  contain all_name_mapping
    # for each station_cast

    units = argovis_param_mapping['units']
    ref_scale = argovis_param_mapping['ref_scale']
    c_format = argovis_param_mapping['c_format']
    dtype = argovis_param_mapping['dtype']

    all_filtered_mappings = []

    for name_mapping in all_name_mapping:

        argovis_names = name_mapping['non_empty_cols']

        new_mapping = {}

        # ******************************
        # filter names to non empty cols
        # ******************************

        new_mapping['station_cast'] = name_mapping['station_cast']

        new_mapping['names'] = argovis_names

        new_mapping['units'] = {key: val for key,
                                val in units.items() if key in argovis_names}
        new_mapping['ref_scale'] = {
            key: val for key, val in ref_scale.items() if key in argovis_names}
        new_mapping['c_format'] = {
            key: val for key, val in c_format.items() if key in argovis_names}
        new_mapping['dtype'] = {key: val for key,
                                val in dtype.items() if key in argovis_names}

        all_filtered_mappings.append(new_mapping)

    return all_filtered_mappings


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


def process_bgc_group_lists(all_bgc):

    all_bgc_profiles = []

    for obj in all_bgc:

        bgc_list = obj['list']

        bgc_profile = {}
        bgc_profile['station_cast'] = obj['station_cast']

        bgc_profile['bgcMeas'] = to_int_qc(bgc_list)

        all_bgc_profiles.append(bgc_profile)

    return all_bgc_profiles


def remove_empty_cols(df):

    # Delete columns with all null, empty, or 'NaT' values
    null_cols = df.columns[df.isna().all()].tolist()

    try:
        empty_str_cols = [
            *filter(lambda c: df[c].str.contains('').all(), df)]

    except AttributeError:
        empty_str_cols = []

    try:
        not_a_time_cols = [
            *filter(lambda c: df[c].str.contains('NaT').all(), df)]

    except AttributeError:
        not_a_time_cols = []

    cols_to_drop = [*null_cols, *empty_str_cols, *not_a_time_cols]

    empty_cols_wo_qc = [col for col in cols_to_drop if '_qc' not in col]

    drop_qc_cols = [
        f"{col}_qc" for col in empty_cols_wo_qc if f"{col}_qc" in df.columns]

    cols_to_drop.extend(drop_qc_cols)

    df = df.drop(cols_to_drop, axis=1)

    return df


# def create_bgc_group_lists(df_param):

#     df_bgc = df_param

#     # Change NaN to None so in json, converted to null
#     df_bgc = df_bgc.where(pd.notnull(df_bgc), None)

#     bgc_df_groups = dict(tuple(df_bgc.groupby('N_PROF')))

#     all_bgc_profiles = []
#     all_name_mapping = []

#     for val_df in bgc_df_groups.values():

#         station_cast = val_df.index[0]

#         # val_df = val_df.drop(['N_PROF', 'index'],  axis=1)
#         val_df = val_df.drop(['N_PROF'],  axis=1)

#         # ***********************************************
#         # Remove cols and corresponding qc if data set is
#         # null, empty or 'NaT'
#         # ***********************************************

#         val_df = remove_empty_cols(val_df)

#         non_empty_cols = val_df.columns

#         val_df = val_df.sort_values(by=['pres'])

#         bgc_dict_list = val_df.to_dict('records')

#         bgc_obj = {}
#         bgc_obj['station_cast'] = station_cast
#         bgc_obj['bgcMeas'] = to_int_qc(bgc_dict_list)
#         all_bgc_profiles.append(bgc_obj)

#         name_mapping_obj = {}
#         name_mapping_obj['station_cast'] = station_cast
#         name_mapping_obj['non_empty_cols'] = non_empty_cols
#         all_name_mapping.append(name_mapping_obj)

#     return all_bgc_profiles, all_name_mapping


def create_bgc_profile(ddf_param):

    df_param = ddf_param.compute()

    # Sort columns so qc next to its var
    df_param = df_param.reindex(sorted(df_param.columns), axis=1)

    bgc_df_groups = dict(tuple(df_param.groupby('N_PROF')))

    all_bgc_profiles = []
    all_name_mapping = []

    for val_df in bgc_df_groups.values():

        station_cast = val_df['station_cast'].values[0]

        #val_df = val_df.drop(['N_PROF'],  axis=1)

        # ***********************************************
        # Remove cols and corresponding qc if data set is
        # null, empty or 'NaT'
        # ***********************************************

        val_df = remove_empty_cols(val_df)

        non_empty_cols = list(val_df.columns)

        val_df = val_df.sort_values(by=['pres'])

        bgc_dict_list = val_df.to_dict('records')

        bgc_obj = {}
        bgc_obj['station_cast'] = station_cast
        bgc_obj['bgcMeas'] = to_int_qc(bgc_dict_list)
        all_bgc_profiles.append(bgc_obj)

        name_mapping_obj = {}
        name_mapping_obj['station_cast'] = station_cast
        name_mapping_obj['non_empty_cols'] = non_empty_cols
        all_name_mapping.append(name_mapping_obj)

    return all_bgc_profiles, all_name_mapping


def create_measurements_profile(ddf_meas, type):

    # Returns a variable col df depending on which core vars exist
    # df_meas = df_param.groupby('N_PROF').apply(
    #     create_measurements_df_all, type)

    # df_meas = df_param.map_partitions(
    #     lambda part: part.groupby('N_PROF').apply(create_measurements_df_all, type))

    df_meas = ddf_meas.compute()

    meas_df_groups = dict(tuple(df_meas.groupby('N_PROF')))

    all_meas_profiles = []
    all_meas_source_profiles = []

    for val_df in meas_df_groups.values():

        measurements_source, measurements_source_qc = get_measurements_source(
            val_df, type)

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


def get_measurements_source(df_meas, type):

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
    elif type == 'ctd' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
        flag = 'CTD'
    elif type == 'btl' and not is_ctd_temp_empty and not use_ctd_psal and not use_bottle_salinity:
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
    measurements_source_qc = fm.convert_boolean(measurements_source_qc)

    return measurements_source, measurements_source_qc


def create_measurements_df_all(df,  type):

    # For measurements, keep core vars pres, temp, psal, salinity
    # Later when filter measurements, if salinity is used,
    # it will be renamed to psal

    # core values includes '_qc' vars
    core_values = gvm.get_argovis_core_meas_values_per_type(type)
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
    if f"temp_{type}" in core_cols and f"temp_{type}_qc" in core_cols:
        temperature_df = df_meas[[f"temp_{type}", f"temp_{type}_qc"]]
        temp_subset = temperature_df[temperature_df[f"temp_{type}"].notnull()]
        temp_qc = pd.unique(temp_subset[f"temp_{type}_qc"])

        if len(temp_qc):
            temp_qc = int(temp_qc[0])
        else:
            temp_qc = None

        df_meas['qc_temp'] = temp_qc

    # Assume psal_qc has one value
    if f"psal_{type}" in core_cols and f"psal_{type}_qc" in core_cols:
        psal_df = df_meas[[f"psal_{type}", f"psal_{type}_qc"]]
        psal_subset = psal_df[psal_df[f"psal_{type}"].notnull()]
        psal_qc = pd.unique(psal_subset[f"psal_{type}_qc"])

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


# def remove_empty_vars(ddf):

#     ddf['new_col'] = np.nan

#     print(ddf.columns)

#     ddf = ddf.groupby('N_PROF').apply(remove_empty_cols, meta=pd.DataFrame())

#     print(ddf.columns)

#     # # Delete columns with all null, empty, or 'NaT' values
#     # null_cols = ddf.columns[ddf.isna().all()].tolist()

#     # try:
#     #     # for string cols
#     #     empty_cols = [*filter(lambda c: ddf[c].str.contains('').all(), ddf)]

#     #     not_a_time_cols = [
#     #         *filter(lambda c: ddf[c].str.contains('NaT').all(), ddf)]

#     #     print(empty_cols)
#     #     print(not_a_time_cols)
#     # except AttributeError:
#     #     pass

#     # print(null_cols)

#     exit(1)


def change_units_to_argovis(nc):

    # Rename units (no conversion)

    unit_name_mapping = gvm.get_goship_argovis_unit_name_mapping()
    goship_unit_names = unit_name_mapping.keys()

    # Get reference scale to determine if salinity because there
    # can be a goship unit of '1' that is not salinity
    salinity_ref_scale = 'PSS-78'

    for var in nc.keys():

        # Change salinity  unit
        try:
            var_ref_scale = nc[var].attrs['reference_scale']
            var_units = nc[var].attrs['units']

            if var_ref_scale == salinity_ref_scale and var_units == '1':
                nc[var].attrs['units'] = 'psu'
        except KeyError:
            pass

        # Change other units
        try:
            var_units = nc[var].attrs['units']
            if var_units in goship_unit_names and var_units != 1:
                nc[var].attrs['units'] = unit_name_mapping[var_units]
        except KeyError:
            pass

    return nc


def add_qc_if_no_temp_qc(nc):

    # Now check so see if there is a ctd temperature  column and a corresponding
    # qc column. If not, add a ctd temperature qc column with values np.nan first
    # and later set = 0. Do np.nan first to make it easier to remove rows
    # with all nan values including np.nan
    is_ctd_temperature = any(
        [True if key == 'ctd_temperature' else False for key in nc.keys()])

    is_ctd_temperature_68 = any(
        [True if key == 'ctd_temperature_68' else False for key in nc.keys()])

    if is_ctd_temperature and is_ctd_temperature_68:
        temperature_var = 'ctd_temperature'
    elif is_ctd_temperature and not is_ctd_temperature_68:
        temperature_var = 'ctd_temperature'
    elif not is_ctd_temperature and is_ctd_temperature_68:
        temperature_var = 'ctd_temperature_68'
    else:
        temperature_var = ''

    qc_name = f"{temperature_var}_qc"

    has_ctd_temp_qc = qc_name in nc.keys()

    if temperature_var and not has_ctd_temp_qc and qc_name != '_qc':
        temp_shape = np.shape(nc[temperature_var])
        shape = np.transpose(temp_shape)
        temp_qc = np.empty(shape)
        temp_qc[:] = np.nan
        nc[qc_name] = (['N_PROF', 'N_LEVELS'], temp_qc)

    return nc


class FormatFloat(float):
    def __format__(self, format_spec):
        return 'nan' if pd.isnull(self) else float.__format__(self, format_spec)


def apply_c_format_param(nc, param_mapping):

    float_types = ['float64', 'float32']

    c_format_mapping = param_mapping['c_format']
    dtype_mapping = param_mapping['dtype']

    float_vars = [name for name,
                  dtype in dtype_mapping.items() if dtype in float_types]

    c_format_vars = [
        name for name in c_format_mapping.keys() if name in float_vars]

    def format_float(num, f_format):
        return float(f"{FormatFloat(num):{f_format}}")

    def apply_c_format(var, f_format):
        vfunc = np.vectorize(format_float)
        return vfunc(var, f_format)

    def apply_c_format_xr(x, f_format, dtype):
        return xr.apply_ufunc(
            apply_c_format,
            x,
            f_format,
            input_core_dims=[['N_PROF', 'N_LEVELS'], []],
            output_core_dims=[['N_PROF', 'N_LEVELS']],
            output_dtypes=[dtype],
            keep_attrs=True
        )

    for var in c_format_vars:
        c_format = c_format_mapping[var]
        f_format = c_format.lstrip('%')
        dtype = dtype_mapping[var]
        nc[var] = apply_c_format_xr(nc[var], f_format, dtype)

    return nc


def apply_c_format_param_dask(nc, chunk_size, param_mapping):

    float_types = ['float64', 'float32']

    c_format_mapping = param_mapping['c_format']
    dtype_mapping = param_mapping['dtype']

    float_vars = [name for name,
                  dtype in dtype_mapping.items() if dtype in float_types]

    c_format_vars = [
        name for name in c_format_mapping.keys() if name in float_vars]

    def format_float(num, f_format):
        return float(f"{FormatFloat(num):{f_format}}")

    def apply_c_format(var, f_format):
        vfunc = np.vectorize(format_float)
        return vfunc(var, f_format)

    def apply_c_format_xr(x, f_format, dtype):
        return xr.apply_ufunc(
            apply_c_format,
            x.chunk({'N_PROF': -1}),
            f_format,
            input_core_dims=[['N_PROF', 'N_LEVELS'], []],
            output_core_dims=[['N_PROF', 'N_LEVELS']],
            output_dtypes=[dtype],
            keep_attrs=True,
            dask="parallelized"
        )

    for var in c_format_vars:
        c_format = c_format_mapping[var]
        f_format = c_format.lstrip('%')
        dtype = dtype_mapping[var]
        nc[var] = apply_c_format_xr(nc[var], f_format, dtype)
        nc[var] = nc[var].chunk({'N_PROF': chunk_size})

    return nc
