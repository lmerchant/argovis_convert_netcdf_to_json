# process parameter data

import pandas as pd
import numpy as np
from decimal import Decimal
import json
import re
import logging
from datetime import datetime

import filter_measurements as fm
import get_variable_mappings as gvm
import get_profile_mapping_and_conversions as pmc


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


def apply_c_format_to_num(name, num, dtype_mapping, c_format_mapping):

    # Now get str in C_format. e.g. "%9.1f"
    # dtype = mapping['dtype'][name]
    # c_format = mapping['c_format'][name]

    float_types = ['float64', 'float32']

    # Could have number be NaN so use try except
    try:

        dtype = dtype_mapping[name]
        c_format = c_format_mapping[name]

        if dtype in float_types and c_format:
            f_format = c_format.lstrip('%')
            return float(f"{num:{f_format}}")

        else:
            return num

    except:
        return num


def create_bgc_profile(df_param, param_mapping_argovis_btl, param_mapping_argovis_ctd, type):

    df_bgc = df_param
    df_bgc = df_bgc.reset_index()
    df_bgc = df_bgc.drop('index', axis=1)

    bgc_df_groups = dict(tuple(df_bgc.groupby('N_PROF')))

    if type == 'btl':
        param_mapping = param_mapping_argovis_btl
    elif type == 'ctd':
        param_mapping = param_mapping_argovis_ctd

    all_bgc = []
    for key, val_df in bgc_df_groups.items():
        val_df = val_df.reset_index()
        station_cast = val_df['station_cast'].values[0]
        val_df = val_df.drop(['station_cast', 'N_PROF', 'index'],  axis=1)

        # Change NaN to None so in json, converted to null
        val_df = val_df.where(pd.notnull(val_df), None)

        val_df = val_df.sort_values(by=['pres'])

        # TODO
        # Can I run apply_c_format_to_num in df and have
        # it saved as float of limited decimal places?

        # TODO
        # Can I run apply_c_format_to_num in df and have
        # it saved as float of limited decimal places?
        # dtype_mapping = param_mapping['dtype']
        # c_format_mapping = param_mapping['c_format']
        # float_types = ['float64', 'float32']

        # float_cols = [name for name,
        #               dtype in dtype_mapping.items() if dtype in float_types]

        # c_format_cols = [
        #     name for name in c_format_mapping.keys() if name in float_cols]

        # def apply_c_format(num, c_format):
        #     if pd.notnull(num):
        #         f_format = c_format.lstrip('%')
        #         return float(f"{num:{f_format}}")
        #     else:
        #         return num

        # for col in c_format_cols:
        #     c_format = c_format_mapping[col]
        #     val_df[col] = np.vectorize(apply_c_format)(val_df[col], c_format)

        bgc_dict_list = val_df.to_dict('records')

        # start_time = datetime.now()

        formatted_list = []
        dtype_mapping = param_mapping['dtype']
        c_format_mapping = param_mapping['c_format']
        for obj in bgc_dict_list:
            new_obj = {name: apply_c_format_to_num(name, val, dtype_mapping, c_format_mapping)
                       for name, val in obj.items()}

            formatted_list.append(new_obj)

        # logging.info("Time to run c_format on obj")
        # logging.info(datetime.now() - start_time)

        bgc_obj = {}
        bgc_obj['station_cast'] = station_cast
        bgc_obj['list'] = formatted_list
        # bgc_obj['list'] = bgc_dict_list
        all_bgc.append(bgc_obj)

    all_bgc_profiles = []
    for obj in all_bgc:

        bgc_list = obj['list']

        bgc_profile = {}
        bgc_profile['station_cast'] = obj['station_cast']
        bgc_profile['bgcMeas'] = list(map(to_int_qc, bgc_list))

        all_bgc_profiles.append(bgc_profile)

    return all_bgc_profiles


def create_measurements_profile(df_param, param_mapping_argovis_btl, param_mapping_argovis_ctd, type):

    df_meas = df_param.groupby('N_PROF').apply(
        create_measurements_df_all, type)

    meas_df_groups = dict(tuple(df_meas.groupby('N_PROF')))

    if type == 'btl':
        param_mapping = param_mapping_argovis_btl
    elif type == 'ctd':
        param_mapping = param_mapping_argovis_ctd

    all_meas = []
    for key, val_df in meas_df_groups.items():

        measurements_source, measurements_source_qc = get_measurements_source(
            val_df, type)

        val_df = val_df.reset_index()
        station_cast = val_df['station_cast'].values[0]

        # Drop any columns except core meas cols
        # TODO probably easier just to select columns than drop them
        # When filter later, will see if use psal or salinity if it exists

        columns = val_df.columns
        core_cols = ['pres', 'psal', 'temp', 'salinity']
        cols = [col for col in columns if col in core_cols]

        val_df = val_df[cols]

        # Change NaN to None so in json, converted to null
        val_df = val_df.where(pd.notnull(val_df), None)

        # val_df = val_df.drop(['station_cast', 'N_PROF',
        #                'qc_temp'],  axis=1)

        # if 'qc_psal' in val_df.columns:
        #     val_df = val_df.drop(['qc_psal'], axis=1)

        # if 'qc_salinity' in val_df.columns:
        #     val_df = val_df.drop(['qc_salinity'], axis=1)

        # TODO
        # Can I run apply_c_format_to_num in df and have
        # it saved as float of limited decimal places?

        meas_dict = val_df.to_dict('records')

        # For mapping, param_mapping has suffix and
        # measurements don't
        c_format_mapping = {name.replace(
            f"_{type}", ''): val for name, val in param_mapping['c_format'].items()}
        dtype_mapping = {name.replace(
            f"_{type}", ''): val for name, val in param_mapping['dtype'].items()}

        formatted_list = []
        for obj in meas_dict:

            new_obj = {name: apply_c_format_to_num(name, val, dtype_mapping, c_format_mapping)
                       for name, val in obj.items()}

            formatted_list.append(new_obj)

        meas_obj = {}
        meas_obj['station_cast'] = station_cast
        meas_obj['source'] = measurements_source
        meas_obj['qc'] = measurements_source_qc
        meas_obj['list'] = formatted_list

        all_meas.append(meas_obj)

    all_meas_profiles = []
    all_meas_source_profiles = []
    for obj in all_meas:

        meas_list = obj['list']

        meas_profile = {}
        meas_profile['station_cast'] = obj['station_cast']
        meas_profile['measurements'] = list(map(to_int_qc, meas_list))
        all_meas_profiles.append(meas_profile)

        meas_source_profile = {}
        meas_source_profile['station_cast'] = obj['station_cast']
        meas_source_profile['measurementsSource'] = obj['source']
        all_meas_source_profiles.append(meas_source_profile)

        meas_source_profile_qc = {}
        meas_source_profile_qc['station_cast'] = obj['station_cast']
        meas_source_profile_qc['measurementsSourceQC'] = obj['qc']
        all_meas_source_profiles.append(meas_source_profile_qc)

    return all_meas_profiles, all_meas_source_profiles


def get_measurements_source(df_meas, type):

    if 'qc_temp' in df_meas.columns:
        temp_qc_df = df_meas['qc_temp']
        temp_qc = pd.unique(temp_qc_df)[0]
    else:
        temp_qc = None

    # psal_qc_df = df_meas['qc_psal']
    # psal_qc = pd.unique(psal_qc_df)[0]

    # salinity_qc_df = df_meas['qc_salinity']
    # salinity_qc = pd.unique(salinity_qc_df)[0]

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

    # ctd_temp_cols = ['ctd_temperature', 'ctd_temperature_68']
    # is_ctd_temp_col = any(
    #     [True if col in df_meas.columns else False for col in ctd_temp_cols])
    # is_ctd_temp_col = 'temp' in df_meas.columns
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
    if temp_qc:
        qc = temp_qc
    elif psal_qc:
        qc = psal_qc
    elif salinity_qc:
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

    df_meas = df[core_cols].copy()

    # def check_qc(row):
    #     if pd.notnull(row[1]) and int(row[1]) == 0:
    #         return row[0]
    #     elif pd.notnull(row[1]) and int(row[1]) == 2:
    #         return row[0]
    #     else:
    #         return np.nan

    # If qc != 0 or 2, set corresponding non_qc value to np.nan
    for col in core_non_qc:

        qc_col = f"{col}_qc"

        try:
            df_meas.loc[(df_meas[qc_col] != 0) &
                        (df_meas[qc_col] != 2), col] = np.nan
        except KeyError:
            pass

        # try:
        #     df[col] = np.where((df[qc_col] != 0) & (
        #         df[qc_col] != 2), df[col], np.nan)
        # except KeyError:
        #     pass

        # try:
        #     df_meas[col] = df_meas[[col, qc_col]].apply(
        #         check_qc, axis=1)
        # except:
        #     pass

    # Check if qc = 0 or 2 for measurements source or None
    # Add qc column back in because need to know qc for
    # measurements source

    temp_qc = None
    psal_qc = None
    salinity_qc = None

    # Assume temp_qc is one value

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

    # If all core values have nan, drop row
    # This won't work since
    df_meas = df_meas.dropna(how='all')

    df_meas = df_meas.sort_values(by=['pres'])

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


def add_qc_if_no_temp_qc(nc):

    # Now check so see if there is a ctd temperature  column and a corresponding
    # qc column. If not, add a ctd temperature qc column with values 0

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
        temp_qc = np.zeros(shape)
        nc[qc_name] = (['N_PROF', 'N_LEVELS'], temp_qc)

    return nc


def convert_oxygen(nc, var, var_goship_units, argovis_units):

    if var_goship_units == 'ml/l' and argovis_units == 'micromole/kg':

        # https://www.nodc.noaa.gov/OC5/WOD/wod18-notes.html
        # 1 ml/l of O2 is approximately 43.570 µmol/kg
        # (assumes a molar volume of O2 of 22.392 l/mole and a
        # constant seawater potential density of 1025 kg/m3).

        # Convert to micromole/kg
        oxygen = nc[var].data
        converted_oxygen = oxygen * 43.570

        try:
            c_format = nc[var].attrs['C_format']
            f_format = c_format.lstrip('%')
            new_oxygen = [float(f"{item:{f_format}}")
                          for item in converted_oxygen]

        except:
            # Use num decimal places of var
            num_decimal_places = abs(
                Decimal(str(oxygen)).as_tuple().exponent)

            new_oxygen = round(converted_oxygen, num_decimal_places)

        # Set oxygen value in nc because use it later to
        # create profile dict
        nc[var].data = new_oxygen
        nc[var].attrs['units'] = 'micromole/kg'

    return nc


def convert_goship_to_argovis_units(nc):

    params = nc.keys()

    # If goship units aren't the same as argovis units, convert
    # So far, just converting oxygen

    goship_argovis_units_mapping = gvm.get_goship_argovis_unit_mapping()

    for var in params:
        if 'oxygen' in var:

            try:
                var_goship_units = nc[var].attrs['units']
                argovis_units = goship_argovis_units_mapping[var]

                is_unit_same = var_goship_units == argovis_units

                if not is_unit_same:
                    nc = convert_oxygen(
                        nc, var, var_goship_units, argovis_units)
            except:
                pass

    return nc


def convert_sea_water_temp(nc, var, var_goship_ref_scale, argovis_ref_scale):

    # Check sea_water_temperature to have goship_reference_scale be ITS-90

    if var_goship_ref_scale == 'IPTS-68' and argovis_ref_scale == 'ITS-90':

        # Convert to ITS-90 scal
        temperature = nc[var].data

        converted_temperature = temperature/1.00024

        # Set nc var of temp to this value
        try:
            c_format = nc[var].attrs['C_format']
            f_format = c_format.lstrip('%')
            new_temperature = [float(f"{item:{f_format}}")
                               for item in converted_temperature]

        except:
            # Use num decimal places of var
            num_decimal_places = abs(
                Decimal(str(temperature)).as_tuple().exponent)

            new_temperature = round(converted_temperature, num_decimal_places)

        # Set temperature value in nc because use it later to
        # create profile dict
        nc[var].data = new_temperature
        nc[var].attrs['reference_scale'] = 'ITS-90'

    return nc


def convert_goship_to_argovis_ref_scale(nc):

    params = nc.keys()

    # If argo ref scale not equal to goship ref scale, convert

    # So far, it's only the case for temperature

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    argovis_ref_scale_per_type = gvm.get_argovis_reference_scale_per_type()

    for var in params:
        if 'temperature' in var:

            try:
                # Get goship reference scale of var
                var_goship_ref_scale = nc[var].attrs['reference_scale']

                argovis_ref_scale = argovis_ref_scale_per_type['temperature']
                is_same_scale = var_goship_ref_scale == argovis_ref_scale

                if not is_same_scale:
                    nc = convert_sea_water_temp(
                        nc, var, var_goship_ref_scale, argovis_ref_scale)
            except:
                pass

    return nc


def convert_oxygen(nc, var, var_goship_units, argovis_units):

    if var_goship_units == 'ml/l' and argovis_units == 'micromole/kg':

        # https://www.nodc.noaa.gov/OC5/WOD/wod18-notes.html
        # 1 ml/l of O2 is approximately 43.570 µmol/kg
        # (assumes a molar volume of O2 of 22.392 l/mole and a
        # constant seawater potential density of 1025 kg/m3).

        # Convert to micromole/kg
        oxygen = nc[var].data
        converted_oxygen = oxygen * 43.570

        try:
            c_format = nc[var].attrs['C_format']
            f_format = c_format.lstrip('%')
            new_oxygen = [float(f"{item:{f_format}}")
                          for item in converted_oxygen]

        except:
            # Use num decimal places of var
            num_decimal_places = abs(
                Decimal(str(oxygen)).as_tuple().exponent)

            new_oxygen = round(converted_oxygen, num_decimal_places)

        # Set oxygen value in nc because use it later to
        # create profile dict
        nc[var].data = new_oxygen
        nc[var].attrs['units'] = 'micromole/kg'

    return nc


def convert_goship_to_argovis_units(nc):

    params = nc.keys()

    # If goship units aren't the same as argovis units, convert
    # So far, just converting oxygen

    goship_argovis_units_mapping = gvm.get_goship_argovis_unit_mapping()

    for var in params:
        if 'oxygen' in var:

            try:
                var_goship_units = nc[var].attrs['units']
                argovis_units = goship_argovis_units_mapping[var]

                is_unit_same = var_goship_units == argovis_units

                if not is_unit_same:
                    nc = convert_oxygen(
                        nc, var, var_goship_units, argovis_units)
            except:
                pass

    return nc


def convert_sea_water_temp(nc, var, var_goship_ref_scale, argovis_ref_scale):

    # Check sea_water_temperature to have goship_reference_scale be ITS-90

    if var_goship_ref_scale == 'IPTS-68' and argovis_ref_scale == 'ITS-90':

        # Convert to ITS-90 scal
        temperature = nc[var].data

        converted_temperature = temperature/1.00024

        # Set nc var of temp to this value
        try:
            c_format = nc[var].attrs['C_format']
            f_format = c_format.lstrip('%')
            new_temperature = [float(f"{item:{f_format}}")
                               for item in converted_temperature]

        except:
            # Use num decimal places of var
            num_decimal_places = abs(
                Decimal(str(temperature)).as_tuple().exponent)

            new_temperature = round(converted_temperature, num_decimal_places)

        # Set temperature value in nc because use it later to
        # create profile dict
        nc[var].data = new_temperature
        nc[var].attrs['reference_scale'] = 'ITS-90'

    return nc


def convert_goship_to_argovis_ref_scale(nc):

    params = nc.keys()

    # If argo ref scale not equal to goship ref scale, convert

    # So far, it's only the case for temperature

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    argovis_ref_scale_per_type = gvm.get_argovis_reference_scale_per_type()

    for var in params:
        if 'temperature' in var:

            try:
                # Get goship reference scale of var
                var_goship_ref_scale = nc[var].attrs['reference_scale']

                argovis_ref_scale = argovis_ref_scale_per_type['temperature']
                is_same_scale = var_goship_ref_scale == argovis_ref_scale

                if not is_same_scale:
                    nc = convert_sea_water_temp(
                        nc, var, var_goship_ref_scale, argovis_ref_scale)
            except:
                pass

    return nc


def apply_equations_and_ref_scale(nc):

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    # Converting to argovis ref scale if needed
    nc = convert_goship_to_argovis_ref_scale(nc)

    # Apply equations to convert units
    nc = convert_goship_to_argovis_units(nc)

    return nc
