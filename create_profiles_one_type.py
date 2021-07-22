# Create profiles for one type


import pandas as pd
import numpy as np
import logging
from datetime import datetime
from collections import defaultdict
import itertools
import os


import get_variable_mappings as gvm
import rename_objects as rn
import process_meta_data as proc_meta
import process_parameter_data as proc_param
import conversions as conv


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

def write_logs(all_meta_profiles, logging_dir):

    # Write the following to a file to keep track of when
    # the cruise expocode is different from the file expocode

    meta_profile_dict = all_meta_profiles[0]['meta']
    cruise_expocode = meta_profile_dict['expocode']
    file_expocode = meta_profile_dict['file_expocode']

    if file_expocode == 'None':
        logging.info(f'No file expocode for {cruise_expocode}')
        filename = 'files_no_expocode.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write('-----------\n')
            f.write(f"expocode {cruise_expocode}\n")
            f.write(f"file type BTL\n")

    if cruise_expocode != file_expocode:
        filename = 'diff_cruise_and_file_expocodes.txt'
        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'a') as f:
            f.write(
                f"Cruise: {cruise_expocode} File: {file_expocode}\n")


def create_goship_argovis_mapping_profiles(
        goship_meta_mapping, goship_param_mapping, argovis_meta_mapping, all_argovis_param_mapping, type):

    all_mapping_profiles = []

    for argovis_param_mapping in all_argovis_param_mapping:

        new_mapping = {}

        new_mapping['station_cast'] = argovis_param_mapping['station_cast']

        all_goship_meta_names = goship_meta_mapping['names']
        all_goship_param_names = goship_param_mapping['names']

        all_argovis_meta_names = argovis_meta_mapping['names']
        all_argovis_param_names = argovis_param_mapping['names']

        new_mapping['goshipMetaNames'] = all_goship_meta_names
        new_mapping['goshipParamNames'] = all_goship_param_names
        new_mapping['argovisMetaNames'] = all_argovis_meta_names
        new_mapping['argovisParamNames'] = all_argovis_param_names

        core_goship_argovis_name_mapping = gvm.get_goship_argovis_name_mapping_per_type(
            type)

        name_mapping = {}
        all_goship_names = [*all_goship_meta_names, *all_goship_param_names]
        all_argovis_names = [*all_argovis_meta_names, *all_argovis_param_names]

        for var in all_goship_names:
            try:
                argovis_name = core_goship_argovis_name_mapping[var]
                if argovis_name in all_argovis_names:
                    name_mapping[var] = argovis_name
                    continue
            except KeyError:
                pass

            if var.endswith('_qc'):
                argovis_name = f"{var}_{type}_qc"
                if argovis_name in all_argovis_names:
                    name_mapping[var] = argovis_name
            else:
                argovis_name = f"{var}_{type}"
                if argovis_name in all_argovis_names:
                    name_mapping[var] = argovis_name

        meta_name_mapping = {key: val for key,
                             val in name_mapping.items() if key in all_goship_meta_names}

        param_name_mapping = {key: val for key,
                              val in name_mapping.items() if key in all_goship_param_names}

        new_mapping['goshipArgovisMetaMapping'] = meta_name_mapping
        new_mapping['goshipArgovisParamMapping'] = param_name_mapping

        new_mapping['goshipReferenceScale'] = {
            **goship_meta_mapping['ref_scale'], **goship_param_mapping['ref_scale']}

        new_mapping['argovisReferenceScale'] = {
            **argovis_meta_mapping['ref_scale'], **argovis_param_mapping['ref_scale']}

        new_mapping['goshipUnits'] = {
            **goship_meta_mapping['units'], **goship_param_mapping['units']}

        new_mapping['argovisUnits'] = {
            **argovis_meta_mapping['units'], **argovis_param_mapping['units']}

        all_mapping_profiles.append(new_mapping)

    return all_mapping_profiles

    #  https://stackoverflow.com/questions/5501810/join-two-lists-of-dictionaries-on-a-single-key


def combine_profiles(meta_profiles, bgc_profiles, meas_profiles, meas_source_profiles, mapping_profiles, type):

    #  https://stackoverflow.com/questions/5501810/join-two-lists-of-dictionaries-on-a-single-key

    profile_dict = defaultdict(dict)
    for elem in itertools.chain(meta_profiles, bgc_profiles,  meas_profiles, meas_source_profiles, mapping_profiles):
        profile_dict[elem['station_cast']].update(elem)

    all_profiles = []
    for key, val in profile_dict.items():
        new_obj = {}
        new_obj['station_cast'] = key
        val['type'] = type
        # Add stationCast to the dict itself
        val['stationCast'] = key
        new_obj['profile_dict'] = val
        all_profiles.append(new_obj)

    return all_profiles


def remove_empty_rows(df):

    # If have '' and 'NaT' values, need to do more to remove these rows
    df_copy = df.copy()

    # Replace 'NaT' and '' with  NaN but first
    # make a copy of df so after remove null rows,
    # can substitute back in any 'NaT' or '' values

    df = df.replace(r'^\s*$', np.NaN, regex=True)
    df = df.replace(r'^NaT$', np.NaN, regex=True)

    exclude_columns = ['N_PROF', 'station_cast']
    df = df.dropna(subset=df.columns.difference(exclude_columns), how='all')

    df.update(df_copy)

    return df


def rearrange_coords_vars_nc(nc):

    # Move all meta data to coords
    # Move all param data to vars

    # metadata is any without N_LEVELS dimension
    meta_names = [
        coord for coord in nc.coords if 'N_LEVELS' not in nc.coords[coord].dims]

    # param  data has both N_LEVELS AND N_PROF
    param_names = [
        coord for coord in nc.coords if 'N_LEVELS' in nc.coords[coord].dims]

    meta_names_from_var = [var for var in nc if 'N_LEVELS' not in nc[var].dims]
    param_names_from_var = [var for var in nc if 'N_LEVELS' in nc[var].dims]

    meta_names.extend(meta_names_from_var)
    param_names.extend(param_names_from_var)

    # move params from coords to variables
    # Move if not in nc.coords
    coords_to_move_to_vars = [
        name for name in param_names if name not in nc.keys()]
    vars_to_move_to_coords = [
        name for name in meta_names if name not in nc.coords]

    nc = nc.reset_coords(names=coords_to_move_to_vars, drop=False)
    nc = nc.set_coords(names=vars_to_move_to_coords)

    # move pressure from coordinate to variable
    nc = nc.reset_coords(names=['pressure'], drop=False)

    # # move section_id and btm_depth to coordinates
    # try:
    #     nc = nc.set_coords(names=['btm_depth'])
    # except:
    #     pass

    # try:
    #     nc = nc.set_coords(names=['section_id'])
    # except:
    #     pass

    # # Drop profile_type and instrument_id and geometry_container if exist

    # # TODO
    # # Maybe keep and then when dataframe, only keep select columns
    # try:
    #     nc = nc.drop_vars(['profile_type'])
    # except:
    #     pass

    # try:
    #     nc = nc.drop_vars(['instrument_id'])
    # except:
    #     pass

    # try:
    #     nc = nc.drop_vars(['geometry_container'])
    # except:
    #     pass

    return nc


def create_profiles_one_type(data_obj, logging_dir):

    start_time = datetime.now()

    type = data_obj['type']

    chunk_size = data_obj['chunk_size']

    logging.info('---------------------------')
    logging.info(f'Start processing {type} profiles')
    logging.info('---------------------------')

    # ****** Modify nc and create mappings ********

    nc = data_obj['nc']

    # *****************************************
    # Rearrange xarray nc to put meta in coords
    # and put parameters as vars
    #
    # Get mapping after this
    #
    # then add extra ArgoVis meta attributes to coords
    #
    # And if there is a ctd_temperature var
    # but no qc (most bottle files), add a
    # qc variable. Fill with NaN first and
    # later change to be 0. Use NaN first to make
    # dropping null rows easier.
    # *****************************************

    # Move some vars to coordinates and drop some vars
    # Want metadata stored in coordinates and parameter
    # data stored in variables section
    nc = rearrange_coords_vars_nc(nc)

    # Now check so see if there is a 'temp_{type}'  column and a corresponding
    # qc col. 'temp_{type}_qc'. If not, add a 'temp' qc col. with nan values
    # to make it easier later when remove null rows. Later will set qc = 0
    # when looking for temp_qc all nan. Add it here so it appears in var mappings
    nc = proc_param.add_qc_if_no_temp_qc(nc)

    # Create mapping object of goship names to nc attributes
    # Before add or drop coords
    # mapping obj: names (list), units (obj), ref_scale (obj)
    # c_format (obj), and dtype (obj)
    goship_meta_mapping = gvm.get_goship_mappings_meta(nc)
    goship_param_mapping = gvm.get_goship_mappings_param(nc)

    # Add extra coordinates for ArgoVis metadata
    nc = proc_meta.add_extra_coords(nc, data_obj)

    # Drop some coordinates
    nc = proc_meta.drop_coords(nc)

    # *********************************
    # Convert units and reference scale
    # *********************************

    logging.info("Apply conversions")

    # Apply units before change unit names

    # Couldn't get apply_ufunc and gsw to work with dask
    # So load it back to pure xarray

    # Load nc from Dask for conversions calc
    nc.load()

    nc = conv.apply_conversions(nc, chunk_size)

    # ****************************
    # Apply C_format print format
    # ****************************

    logging.info("Apply c_format")

    # Convert back to Dask
    nc = nc.chunk({'N_PROF': chunk_size})

    nc = proc_meta.apply_c_format_meta_dask(
        nc, chunk_size, goship_meta_mapping)

    nc = proc_param.apply_c_format_param_dask(
        nc, chunk_size, goship_param_mapping)

    # **********************************
    # Change unit names (no conversions)
    # **********************************

    nc = proc_meta.change_units_to_argovis(nc)
    nc = proc_param.change_units_to_argovis(nc)

    # **************************
    # Rename varables to ArgoVis
    # **************************

    nc = rn.rename_to_argovis(nc, type)

    # **********************
    # Create ArgoVis Mapping
    # **********************

    argovis_meta_mapping = gvm.get_argovis_mappings_meta(nc)
    argovis_param_mapping = gvm.get_argovis_mappings_param(nc)

    # *************************
    # Convert to Dask dataframe
    # *************************

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

    logging.info('create all_meta profiles')
    all_meta_profiles = proc_meta.create_meta_profile(ddf_meta)

    # ******************************************
    # Remove empty rows so don't include in JSON
    # ******************************************

    logging.info('Remove empty rows')

    #ddf_param = ddf_param.drop('N_LEVELS', axis=1)

    dask_meta = ddf_param.dtypes.to_dict()
    ddf_param = ddf_param.groupby("N_PROF").apply(
        remove_empty_rows, meta=dask_meta)

    # Add back in temp_qc = 0 if column exists and all np.nan
    try:
        if ddf_param[f"temp_{type}_qc"].isnull().values.all():
            ddf_param[f"temp_{type}_qc"] = ddf_param[f"temp_{type}_qc"].fillna(
                0)

    except KeyError:
        pass

    #ddf_param = ddf_param.set_index('station_cast')

    # ******************************************
    # Convert from Dask to pure Pandas dataframe
    # since mainly working with dictionaries now
    # *******************************************

    df_param = ddf_param.compute()

    #df_param = df_param.reset_index()

    # Sort columns so qc next to its var
    df_param = df_param.reindex(sorted(df_param.columns), axis=1)

    # *******************************
    # Create all measurement profiles
    # *******************************

    logging.info('create all_meas profiles')
    all_meas_profiles, all_meas_source_profiles = proc_param.create_measurements_profile(
        df_param, type)

    # **********************************
    # From df_param, filter out any vars
    # with all null/empty values
    #
    # Create mapping profile for each
    # filtered df_param profile
    # **********************************

    logging.info('create all_bgc profile and get filtered name mapping')
    all_bgc_profiles, all_name_mapping = proc_param.create_bgc_profile(
        df_param)

    all_argovis_param_mapping = proc_param.filter_argovis_mapping(
        argovis_param_mapping, all_name_mapping, type)

    goship_argovis_mapping_profiles = create_goship_argovis_mapping_profiles(
        goship_meta_mapping, goship_param_mapping, argovis_meta_mapping, all_argovis_param_mapping, type)

    # *************************************************
    # Combine all the profile parts into one dictionary
    # *************************************************

    logging.info('start combining profiles')
    all_profiles = combine_profiles(all_meta_profiles, all_bgc_profiles,
                                    all_meas_profiles, all_meas_source_profiles, goship_argovis_mapping_profiles, type)

    # ***********************
    # Write some info to logs
    # ***********************

    write_logs(all_meta_profiles, logging_dir)

    logging.info("Time to run function create_profiles_one_type")
    logging.info(datetime.now() - start_time)

    logging.info('---------------------------')
    logging.info(f'End processing {type} profiles')
    logging.info(f"Num params {len(df_param.columns)}")
    logging.info(f"Shape of dims")
    logging.info(nc.dims)
    logging.info('---------------------------')

    return all_profiles
