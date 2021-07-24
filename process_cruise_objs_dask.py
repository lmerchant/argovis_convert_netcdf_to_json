from datetime import datetime
import logging
from collections import defaultdict
import itertools


from xarray_and_dask.modify_xarray_obj import modify_xarray_obj
from create_profiles.create_meta_profiles import create_meta_profile
from xarray_and_dask.modify_dask_obj import modify_dask_obj
from create_profiles.create_meas_profiles import create_meas_profiles
from create_profiles.create_bgc_profiles import create_bgc_profiles
from create_profiles.create_goship_argovis_mappings import create_goship_argovis_mappings
from create_profiles.create_goship_argovis_mappings import filter_argovis_mapping


def combine_profiles(meta_profiles, bgc_profiles, meas_profiles, meas_source_profiles, mapping_profiles, data_type):

    #  https://stackoverflow.com/questions/5501810/join-two-lists-of-dictionaries-on-a-single-key

    profile_dict = defaultdict(dict)
    for elem in itertools.chain(meta_profiles, bgc_profiles,  meas_profiles, meas_source_profiles, mapping_profiles):
        profile_dict[elem['station_cast']].update(elem)

    all_profiles = []
    for key, val in profile_dict.items():
        new_obj = {}
        new_obj['station_cast'] = key
        val['data_type'] = data_type
        # Add stationCast to the dict itself
        val['stationCast'] = key
        new_obj['profile_dict'] = val
        all_profiles.append(new_obj)

    return all_profiles


def process_cruise_objs_dask(cruise_objs):

    logging.info('Process all cruise objects in xarray objects')

    start_time = datetime.now()

    cruises_xr_objs = []

    for cruise_obj in cruise_objs:

        file_objs = cruise_obj['file_objs']

        xr_file_objs = []

        for file_obj in file_objs:

            # ********************************
            # Modify Xarray object
            # and get before and after mappings
            # *********************************

            nc, nc_mappings = modify_xarray_obj(file_obj)

            file_obj['nc'] = nc
            file_obj['nc_mappings'] = nc_mappings

            xr_file_objs.append(file_obj)

        cruise_xr_obj = {}
        cruise_xr_obj['cruise_expocode'] = cruise_obj['cruise_expocode']
        cruise_xr_obj['xr_file_objs'] = xr_file_objs

        cruises_xr_objs.append(cruise_xr_obj)

    # ***************************
    # Convert xarray objects into
    # Dask dataframe objects
    # ***************************

    logging.info('Process all xarray objects in Dask dataframe objects')

    cruises_ddf_objs = []

    for cruise_xr_obj in cruises_xr_objs:

        # *************************
        # Convert to Dask dataframe
        # *************************

        cruise_expocode = cruise_xr_obj['cruise_expocode']
        xr_file_objs = cruise_xr_obj['xr_file_objs']

        ddf_objs = []

        for xr_file_obj in xr_file_objs:

            nc = xr_file_obj['nc']
            data_type = xr_file_obj['data_type']

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

            ddf_obj = {}

            ddf_obj['data_type'] = data_type

            logging.info('create all_meta profiles')

            ddf_obj['all_meta_profiles'] = create_meta_profile(ddf_meta)

            logging.info('Modify Dask param dataframe')
            ddf_param = modify_dask_obj(ddf_param, data_type)

            ddf_obj['ddf_param'] = ddf_param

            ddf_objs.append(ddf_obj)

        cruise_ddf_obj = {}
        cruise_ddf_obj['cruise_expocode'] = cruise_expocode
        cruise_ddf_obj['ddf_objs'] = ddf_objs

        cruises_ddf_objs.append(cruise_ddf_obj)

    # ******************************
    # Convert Dask dataframe objects
    # into Pandas dataframe objects
    # and then python dictionaries
    # ******************************

    cruises_profiles_objs = []

    for cruise_ddf_obj in cruises_ddf_objs:

        cruise_expocode = cruise_ddf_obj['cruise_expocode']
        ddf_objs = cruise_ddf_obj['ddf_objs']

        profiles_objs = []

        for ddf_obj in ddf_objs:

            data_type = ddf_obj['data_type']
            all_meta_profiles = ddf_obj['all_meta_profiles']
            ddf_param = ddf_obj['ddf_param']

            df_param = ddf_param.compute()

            all_meas_profiles, all_meas_source_profiles = create_meas_profiles(
                df_param, data_type)

            all_bgc_profiles, all_name_mapping = create_bgc_profiles(df_param)

            all_argovis_param_mapping = filter_argovis_mapping(
                nc_mappings, all_name_mapping)

            goship_argovis_mapping_profiles = create_goship_argovis_mappings(
                nc_mappings, all_argovis_param_mapping, data_type)

            # ******************************************************
            # Combine all the profile parts into one dictionary list
            # ******************************************************

            logging.info('start combining profiles')
            all_profiles = combine_profiles(all_meta_profiles, all_bgc_profiles,
                                            all_meas_profiles, all_meas_source_profiles, goship_argovis_mapping_profiles, data_type)

            profiles_obj = {}

            profiles_obj['data_type'] = data_type
            profiles_obj['profiles'] = all_profiles

            profiles_objs.append(profiles_obj)

        cruise_profiles_obj = {}
        cruise_profiles_obj['cruise_expocode'] = cruise_expocode
        cruise_profiles_obj['profiles_objs'] = profiles_objs

        cruises_profiles_objs.append(cruise_profiles_obj)

    logging.info("Time to run function create_profiles_one_type")
    logging.info(datetime.now() - start_time)

    return cruises_profiles_objs
