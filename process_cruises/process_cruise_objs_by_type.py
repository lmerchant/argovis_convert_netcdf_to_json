from datetime import datetime
import logging
from collections import defaultdict
import itertools

import numpy as np


from xarray_and_dask.check_has_ctd_vars import check_has_ctd_vars
from xarray_and_dask.modify_xarray_obj import modify_xarray_obj
from create_profiles.create_meta_profiles import create_meta_profiles
from xarray_and_dask.modify_dask_obj import modify_meta_dask_obj
from xarray_and_dask.modify_dask_obj import modify_param_dask_obj
from create_profiles.create_meas_profiles import create_meas_profiles
from create_profiles.create_bgc_profiles import create_bgc_profiles
from create_profiles.create_cchdo_argovis_mappings import create_cchdo_argovis_mappings
from create_profiles.create_cchdo_argovis_mappings import filter_argovis_mapping
from create_profiles.create_cchdo_argovis_mappings import create_cchdo_mappings


def combine_profiles(meta_profiles, bgc_profiles, all_meas_profiles, meas_source_profiles, meas_names, cchdo_mapping_profiles, data_type):

    #  https://stackoverflow.com/questions/5501810/join-two-lists-of-dictionaries-on-a-single-key

    profile_dict = defaultdict(dict)
    for elem in itertools.chain(meta_profiles, bgc_profiles,  all_meas_profiles, meas_names, meas_source_profiles, cchdo_mapping_profiles):

        profile_dict[elem['station_cast']].update(elem)

    combined_profiles = []
    for key, val in profile_dict.items():
        new_obj = {}
        new_obj['station_cast'] = key
        val['data_type'] = data_type
        val['stationCast'] = key
        new_obj['profile_dict'] = val
        combined_profiles.append(new_obj)

    logging.info(f"Total profiles {len(combined_profiles)}")

    return combined_profiles


# def combine_profiles_orig(meta_profiles, bgc_profiles, all_meas_profiles, meas_source_profiles, meas_names, mapping_profiles, data_type):


#     #  https://stackoverflow.com/questions/5501810/join-two-lists-of-dictionaries-on-a-single-key

#     profile_dict = defaultdict(dict)
#     for elem in itertools.chain(meta_profiles, bgc_profiles,  all_meas_profiles, meas_names, meas_source_profiles, mapping_profiles):
#         profile_dict[elem['station_cast']].update(elem)

#     combined_profiles = []
#     for key, val in profile_dict.items():
#         new_obj = {}
#         new_obj['station_cast'] = key
#         val['data_type'] = data_type
#         val['stationCast'] = key
#         new_obj['profile_dict'] = val
#         combined_profiles.append(new_obj)

#     logging.info(f"Total profiles {len(combined_profiles)}")

#     return combined_profiles


def create_profiles_objs(cruise_ddf_obj):

    cruise_expocode = cruise_ddf_obj['cruise_expocode']
    ddf_objs = cruise_ddf_obj['ddf_objs']

    profiles_objs = []

    # Loop through each data type (btl,ctd)
    for ddf_obj in ddf_objs:

        data_type = ddf_obj['data_type']

        logging.info(f"Create profiles obj for type {data_type}")

        nc_mappings = ddf_obj['nc_mappings']
        ddf_meta = ddf_obj['ddf_meta']
        ddf_param = ddf_obj['ddf_param']
        cchdo_file_meta = ddf_obj['cchdo_file_meta']
        cchdo_cruise_meta = ddf_obj['cchdo_cruise_meta']

        logging.info('create all_meta profiles')

        df_meta = ddf_meta.compute()
        all_meta_profiles = create_meta_profiles(df_meta)

        # Change from dask dataframe to pandas by applying function compute
        df_param = ddf_param.compute()

        # TODO
        # If remove meas in future, move this code to end of program
        # since it's mainly  getting subset  of bgcMeas

        all_meas_profiles, all_meas_source_profiles, all_meas_names = create_meas_profiles(
            df_param, data_type)

        # all_name_mapping is dict with keys: station_cast and non_empty_cols
        # Want to know non_empty_cols so can keep these cchdo names
        # in the mappings and discard vars that have no values
        all_bgc_profiles, all_name_mapping = create_bgc_profiles(df_param)

        cchdo_mapping_profiles = create_cchdo_mappings(
            nc_mappings, all_name_mapping)

        # # Filter out from var name mappings any that have no values.
        # # This can happen since xarray fills variables if they
        # # don't exist in a station cast to create one array of variables
        # # for the collection
        # all_argovis_param_mapping_list = filter_argovis_mapping(
        #     nc_mappings, all_name_mapping)

        # cchdo_argovis_mapping_profiles = create_cchdo_argovis_mappings(
        #     nc_mappings, all_argovis_param_mapping_list, data_type)

        # **************************************************
        # Combine all the profile parts into one object list
        # **************************************************

        #  combined_profiles is a list of profile objs with
        # keys 'station_cast' and 'profile_dict'

        # create cchdo_argovis_mapping_profiles later
        logging.info('start combining profiles')
        # combined_profiles = combine_profiles(all_meta_profiles, all_bgc_profiles,
        #                                 all_meas_profiles, all_meas_source_profiles, all_meas_names, cchdo_argovis_mapping_profiles, data_type)

        combined_profiles = combine_profiles(all_meta_profiles, all_bgc_profiles,
                                             all_meas_profiles, all_meas_source_profiles, all_meas_names, cchdo_mapping_profiles, data_type)

        # Create profiles_obj to hold profiles for one data type
        profiles_obj = {}
        profiles_obj['data_type'] = data_type
        profiles_obj['cchdo_file_meta'] = cchdo_file_meta
        profiles_obj['cchdo_cruise_meta'] = cchdo_cruise_meta
        profiles_obj['data_type_profiles_list'] = combined_profiles

        profiles_objs.append(profiles_obj)

    return profiles_objs


def create_dask_dataframe_obj(cruise_xr_obj):

    # File objs are btl, ctd types
    xr_file_objs = cruise_xr_obj['xr_file_objs']

    logging.info(f"Converting xarray to Dask")

    cruise_ddf_obj = {}
    cruise_ddf_obj['cruise_expocode'] = cruise_xr_obj['cruise_expocode']

    ddf_objs = []

    for xr_file_obj in xr_file_objs:

        nc = xr_file_obj['nc']

        # process metadata and parameter data separately
        meta_param_names = xr_file_obj['meta_param_names']

        meta_names = meta_param_names['meta']
        param_names = meta_param_names['params']

        # argovis_meta_mapping = nc_mappings['argovis_meta']
        # argovis_param_mapping = nc_mappings['argovis_param']

        # meta_names = argovis_meta_mapping['names']
        # param_names = argovis_param_mapping['names']

        # nc was read in with Dask xarray, so now save to dask dataframe
        ddf = nc.to_dask_dataframe(dim_order=['N_PROF', 'N_LEVELS'])

        # Add dimensions and have station_cast for both
        # station_cast acts as a unique identifier

        meta_names.extend(['N_PROF', 'N_LEVELS'])
        param_names.extend(['N_PROF', 'N_LEVELS', 'station_cast'])

        ddf_meta = ddf[meta_names].copy()
        ddf_param = ddf[param_names].copy()

        ddf_obj = {}
        ddf_obj['data_type'] = xr_file_obj['data_type']
        ddf_obj['nc_mappings'] = xr_file_obj['nc_mappings']
        ddf_obj['cchdo_file_meta'] = xr_file_obj['cchdo_file_meta']
        ddf_obj['cchdo_cruise_meta'] = xr_file_obj['cchdo_cruise_meta']

        logging.info("Modify Dask meta dataframe")

        # Keep first row of meta since rest are repeats
        ddf_meta = modify_meta_dask_obj(ddf_meta)
        ddf_obj['ddf_meta'] = ddf_meta

        logging.info('Modify Dask param dataframe')

        # Faster than pandas to use dask to remove empty rows
        ddf_param = modify_param_dask_obj(ddf_param)
        ddf_obj['ddf_param'] = ddf_param

        ddf_objs.append(ddf_obj)

    cruise_ddf_obj['ddf_objs'] = ddf_objs

    return cruise_ddf_obj


def create_xr_obj(cruise_obj):

    file_objs = cruise_obj['file_objs']

    xr_file_objs = []

    for file_obj in file_objs:

        logging.info(f"Data Type {file_obj['data_type']}")

        # ******************************
        # Check if has required CTD vars
        # Must have pressure and
        # ctd temperature with ref scale
        # ******************************

        logging.info("Check has ctd vars")

        has_ctd_vars = check_has_ctd_vars(file_obj)

        if not has_ctd_vars:
            logging.info("No ctd vars")
            continue

        # ********************************
        # Modify Xarray object
        # and get before and after mappings
        # of var names to Argovis names
        # *********************************

        logging.info(f"Modify xarray cruise object")

        nc, nc_mappings, meta_param_names = modify_xarray_obj(file_obj)

        logging.info(f"File shape {nc.dims}")

        file_obj['nc'] = nc
        file_obj['nc_mappings'] = nc_mappings
        file_obj['meta_param_names'] = meta_param_names

        xr_file_objs.append(file_obj)

    cruise_xr_obj = {}
    cruise_xr_obj['cruise_expocode'] = cruise_obj['cruise_expocode']
    cruise_xr_obj['xr_file_objs'] = xr_file_objs

    return cruise_xr_obj


def process_cruise_objs_by_type(cruise_objs):

    cruises_profile_objs = []

    for cruise_obj in cruise_objs:

        start_time = datetime.now()

        logging.info('***********************')
        logging.info(f"cruise {cruise_obj['cruise_expocode']}")
        logging.info('***********************')

        # ************************************
        # Convert cruise objs into xarray objs
        # Modify xr obj and get mappings
        # *************************************

        # Don't rename until end to ArgoVis names
        # For now, get cchdo mappings to units, netcdf names, etc.

        logging.info('Process all cruise objects in xarray objects')

        cruise_xr_obj = create_xr_obj(cruise_obj)

        if not cruise_xr_obj['xr_file_objs']:
            continue

        # ***************************
        # Convert xarray objects into
        # Dask dataframe objects
        # ***************************

        logging.info('Process all xarray objects into Dask dataframe objects')

        cruise_ddf_obj = create_dask_dataframe_obj(cruise_xr_obj)

        # cruise_ddf_obj = {}
        # cruise_ddf_obj['cruise_expocode'] = cruise_xr_obj['cruise_expocode']
        # cruise_ddf_obj['ddf_objs'] = ddf_objs

        # ******************************
        # Convert Dask dataframe objects
        # into Pandas dataframe objects
        # and then python dictionaries
        # ******************************

        logging.info("Convert dask dataframe objs into profile dicts")

        profiles_objs = create_profiles_objs(cruise_ddf_obj)

        cruise_profiles_obj = {}
        cruise_profiles_obj['cruise_expocode'] = cruise_ddf_obj['cruise_expocode']
        cruise_profiles_obj['all_data_types_profile_objs'] = profiles_objs

        cruises_profile_objs.append(cruise_profiles_obj)

        # *******************************************
        # Log time to run section to see optimization
        # *******************************************

        logging.info("Time to run create cruise profiles")
        logging.info(datetime.now() - start_time)

    return cruises_profile_objs
