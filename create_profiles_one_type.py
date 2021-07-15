# Create profiles for one type

# from pandas.core.arrays import boolean
import pandas as pd
import numpy as np
# from decimal import Decimal
import logging
import re
# import dask
# import dask.bag as db
# import dask.dataframe as dd
# from dask import delayed
# from dask.diagnostics import ResourceProfiler
from datetime import datetime
# from dask.diagnostics import ProgressBar
from collections import defaultdict
# from operator import itemgetter
import itertools


import get_variable_mappings as gvm
import rename_objects as rn
import process_meta_data as proc_meta
import process_parameter_data as proc_param


# class fakefloat(float):
#     def __init__(self, value):
#         self._value = value

#     def __repr__(self):
#         return str(self._value)


# # https://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object
# def defaultencode(o):
#     if isinstance(o, Decimal):
#         # Subclass float with custom repr?
#         return fakefloat(o)
#     raise TypeError(repr(o) + " is not JSON serializable")


def combine_profiles(meta_profiles, bgc_profiles, meas_profiles, meas_source_profiles, mapping_dict, type):

    #  https://stackoverflow.com/questions/5501810/join-two-lists-of-dictionaries-on-a-single-key

    profile_dict = defaultdict(dict)
    for elem in itertools.chain(meta_profiles, bgc_profiles,  meas_profiles, meas_source_profiles):
        profile_dict[elem['station_cast']].update(elem)

    all_profiles = []
    for key, val in profile_dict.items():
        new_obj = {}
        new_obj['station_cast'] = key
        val['type'] = type
        # Insert a mapping dict
        val = {**val, **mapping_dict}
        # Add stationCast to the dict itself
        val['stationCast'] = key
        new_obj['profile_dict'] = val
        all_profiles.append(new_obj)

    return all_profiles


def remove_empty_rows(df):

    #  TODO
    # Can I make a map of where NaT, '' are
    # Replace with NaN, and remove null rows
    # Finally use map to replace NaT and '' values

    # Count # of non empty elems in each row and save to new column
    def count_elems(row):

        result = [0 if pd.isnull(
            cell) or cell == '' or cell == 'NaT' else 1 for cell in row]

        return sum(result)

    orig_cols = df.columns

    df_columns = list(df.columns)
    df_columns.remove('N_PROF')
    df_columns.remove('station_cast')
    df_columns.remove('index')
    df_subset = df[df_columns]

    new_df = df_subset.apply(count_elems, axis=1)

    new_df.columns = ['num_elems']

    df_end = pd.concat([df, new_df], axis=1)

    # name last column so know what to delete
    df_end.columns = [*df_end.columns[:-1], 'num_elems']

    # Then drop rows where num_elems is 0
    df_end = df_end.drop(df_end[df_end['num_elems'] == 0].index)

    df_end = df_end[orig_cols]

    return df_end


def remove_empty_rows_ver2(df):

    df_copy = df.copy()

    # Replace 'NaT' and '' with  NaN but first
    # make a copy of df so after remove null rows,
    # can substitute back in any 'NaT' or '' values

    df = df.replace(r'^\s*$', np.NaN, regex=True)

    df = df.replace(r'^NaT$', np.NaN, regex=True)

    exclude_columns = ['N_PROF', 'station_cast', 'index']
    df = df.dropna(subset=df.columns.difference(exclude_columns), how='all')

    df.update(df_copy)

    return df


def arrange_coords_vars_nc(nc):

    # move pressure from coordinate to variable
    nc = nc.reset_coords(names=['pressure'], drop=False)

    # move section_id and btm_depth to coordinates
    try:
        nc = nc.set_coords(names=['btm_depth'])
    except:
        pass

    try:
        nc = nc.set_coords(names=['section_id'])
    except:
        pass

    # Drop profile_type and instrument_id and geometry_container if exist

    # TODO
    # Maybe keep and then when dataframe, only keep select columns
    try:
        nc = nc.drop_vars(['profile_type'])
    except:
        pass

    try:
        nc = nc.drop_vars(['instrument_id'])
    except:
        pass

    try:
        nc = nc.drop_vars(['geometry_container'])
    except:
        pass

    return nc


def create_profiles_one_type(data_obj):

    start_time = datetime.now()

    type = data_obj['type']

    logging.info('---------------------------')
    logging.info(f'Start processing {type} profiles')
    logging.info('---------------------------')

    file_info = {}
    file_info['type'] = type
    file_info['filename'] = data_obj['filename']
    file_info['data_path'] = data_obj['data_path']
    file_info['cruise_expocode'] = data_obj['cruise_expocode']
    file_info['cruise_id'] = data_obj['cruise_id']
    file_info['woce_lines'] = data_obj['woce_lines']
    file_info['file_hash'] = data_obj['file_hash']

    # ****** Modify nc and create mappings ********

    nc = data_obj['nc']

    # Move some vars to coordinates and drop some vars
    # Want metadata stored in coordinates and parameter
    # data stored in variables section
    nc = arrange_coords_vars_nc(nc)

    # Get goship_names before add extra_coords
    meta_goship_names = [coord for coord in nc.coords]

    # Add extra coordinates for ArgoVis metadata
    nc = proc_meta.add_extra_coords(nc, file_info)

    # TODO
    # Get formula for Oxygen unit conversion
    # Apply equations before rename
    nc = proc_param.apply_equations_and_ref_scale(nc)

    # Now check so see if there is a 'temp_{type}'  column and a corresponding
    # qc col. 'temp_{type}_qc'. If not, add a 'temp' qc col. with values 0
    nc = proc_param.add_qc_if_no_temp_qc(nc)

    # Create mapping object of goship names to nc attributes
    # mapping obj: names (list), units (obj), ref_scale (obj)
    # c_format (obj), dtype (obj)
    meta_mapping = gvm.get_goship_mappings_meta(nc, meta_goship_names)
    param_mapping = gvm.get_goship_mappings_param(nc)

    logging.info('start apply_equations_and_ref_scale')
    # TODO
    # Following could be a one-off error

    # Create mapping of goship to argovis names and attributes
    # Can't do this yet using mapped argovis names because core vars
    # can change per profile. Such as using a diff ref scale for one cast
    # See expo_49K6KY9606_1_sta_021_cast_001.json
    goship_argovis_mapping_for_profile = gvm.create_mapping_for_profile(
        meta_mapping, param_mapping, type)

    # Rename goship variables to any in ArgoVis naming scheme
    # Use to get dtype and c_format mapping later and want
    # relative to ArgoVis names
    meta_mapping_argovis = rn.rename_vars_to_argovis(meta_mapping)

    param_mapping_argovis_btl = rn.rename_vars_to_argovis_by_type(
        param_mapping, 'btl')
    param_mapping_argovis_ctd = rn.rename_vars_to_argovis_by_type(
        param_mapping, 'ctd')

    # Create mapping from goship_col names to argovis names
    # and use to rename columns to argovis names
    # Quicker to renamae columns than to change later after
    # converted to objects
    goship_argovis_col_mapping = gvm.create_meta_col_name_mapping(
        list(nc.coords))
    nc = nc.rename_vars(goship_argovis_col_mapping)

    goship_argovis_col_mapping = gvm.create_param_col_name_mapping_w_type(
        list(nc.keys()), type)
    nc = nc.rename_vars(goship_argovis_col_mapping)

    # ****** Convert to Dask dataframe ********

    # Removing rows in xarray is too slow with groupby
    # since using a big loop. faster in pandas

    # process metadata and parameter data separately
    meta_keys = list(nc.coords)
    param_keys = list(nc.keys())

    # nc was read in with Dask xarray, so now save to dask dataframe
    ddf = nc.to_dask_dataframe(dim_order=['N_PROF', 'N_LEVELS'])

    # Add dimensions and have station_cast for both
    meta_keys.extend(['N_PROF', 'N_LEVELS'])
    param_keys.extend(['N_PROF', 'N_LEVELS', 'station_cast'])

    ddf_meta = ddf[meta_keys].copy()
    ddf_param = ddf[param_keys].copy()

    # -----

    ddf_param = ddf_param.reset_index()
    ddf_param = ddf_param.drop('N_LEVELS', axis=1)

    logging.info('Remove empty rows')

    meta_dask = ddf_param.dtypes.to_dict()
    # ddf_param = ddf_param.groupby("N_PROF").apply(
    #     remove_empty_rows, meta=meta_dask)

    ddf_param = ddf_param.groupby("N_PROF").apply(
        remove_empty_rows_ver2, meta=meta_dask)

    # data = [[2, 1, 'NaT', '', 4], [3, 2, 'NaT', '', np.NaN], [
    #     4, 3, 'time1', '', 5], [5, 4, 'NaT', '', np.NaN], [6, 5, 'NaT', '', np.NaN]]

    # df_test = pd.DataFrame(
    #     data, columns=['N_PROF', 'index', 'time', 'empty', 'value'])

    # df_test['station_cast'] = 'sta_cast'

    # print('before')
    # print(df_test)

    # df_test = df_test.groupby("N_PROF").apply(remove_empty_rows_ver2)

    # print(df_test)

    ddf_param = ddf_param.set_index('station_cast')

    df_param = ddf_param.compute()

    # Sort columns so qc next to its var
    df_param = df_param.reindex(sorted(df_param.columns), axis=1)

    logging.info('create all_meta profile')
    all_meta_profiles = proc_meta.create_meta_profile(
        ddf_meta, meta_mapping_argovis)

    logging.info('create all_bgc profile')
    all_bgc_profiles = proc_param.create_bgc_profile(df_param, param_mapping_argovis_btl,
                                                     param_mapping_argovis_ctd, type)

    logging.info('create all_meas profile')
    all_meas_profiles, all_meas_source_profiles = proc_param.create_measurements_profile(
        df_param, param_mapping_argovis_btl, param_mapping_argovis_ctd, type)

    # Combine
    logging.info('start combining profiles')
    all_profiles = combine_profiles(all_meta_profiles, all_bgc_profiles,
                                    all_meas_profiles, all_meas_source_profiles, goship_argovis_mapping_for_profile, type)

    logging.info("Time to run function create_profiles_one_type")
    logging.info(datetime.now() - start_time)

    logging.info('---------------------------')
    logging.info(f'End processing {type} profiles')
    logging.info(f"Shape of dims")
    logging.info(nc.dims)
    logging.info('---------------------------')

    return all_profiles
