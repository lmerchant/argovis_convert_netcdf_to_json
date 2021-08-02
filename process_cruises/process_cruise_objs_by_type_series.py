from datetime import datetime
import logging
from collections import defaultdict
import itertools


from xarray_and_dask.check_has_ctd_vars import check_has_ctd_vars
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
        val['stationCast'] = key
        new_obj['profile_dict'] = val
        all_profiles.append(new_obj)

    logging.info(f"Total profiles {len(all_profiles)}")

    return all_profiles


def create_profiles_objs(cruise_ddf_obj):

    ddf_objs = cruise_ddf_obj['ddf_objs']

    profiles_objs = []

    # Loop through each data type
    for ddf_obj in ddf_objs:

        data_type = ddf_obj['data_type']

        logging.info(f"Create profiles obj for type {data_type}")

        nc_mappings = ddf_obj['nc_mappings']
        all_meta_profiles = ddf_obj['all_meta_profiles']
        ddf_param = ddf_obj['ddf_param']

        df_param = ddf_param.compute()

        all_meas_profiles, all_meas_source_profiles = create_meas_profiles(
            df_param, data_type)

        all_bgc_profiles, all_name_mapping = create_bgc_profiles(df_param)

        all_argovis_param_mapping_list = filter_argovis_mapping(
            nc_mappings, all_name_mapping)

        goship_argovis_mapping_profiles = create_goship_argovis_mappings(
            nc_mappings, all_argovis_param_mapping_list, data_type)

        # ******************************************************
        # Combine all the profile parts into one dictionary list
        # ******************************************************

        #  all_profiles is a list of profile objs with
        # keys 'station_cast' and 'profile_dict'
        logging.info('start combining profiles')
        all_profiles = combine_profiles(all_meta_profiles, all_bgc_profiles,
                                        all_meas_profiles, all_meas_source_profiles, goship_argovis_mapping_profiles, data_type)

        # Create profiles_obj to hold profiles for one data type
        profiles_obj = {}
        profiles_obj['data_type'] = data_type
        profiles_obj['data_type_profiles_list'] = all_profiles

        profiles_objs.append(profiles_obj)

    return profiles_objs


def create_dask_dataframe_objs(cruise_xr_obj):

    xr_file_objs = cruise_xr_obj['xr_file_objs']

    logging.info(f"Converting xarray to Dask")

    ddf_objs = []

    for xr_file_obj in xr_file_objs:

        nc = xr_file_obj['nc']

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
        ddf_obj['data_type'] = xr_file_obj['data_type']
        ddf_obj['nc_mappings'] = xr_file_obj['nc_mappings']

        logging.info('create all_meta profiles')
        ddf_obj['all_meta_profiles'] = create_meta_profile(ddf_meta)

        logging.info('Modify Dask param dataframe')

        # ******************************************
        # Remove empty rows so don't include in JSON
        # Change NaN to None for JSON to be null
        # Add back in temp_qc = 0 col if existed
        # ******************************************

        data_type = xr_file_obj['data_type']

        ddf_param = modify_dask_obj(ddf_param, data_type)

        ddf_obj['ddf_param'] = ddf_param

        ddf_objs.append(ddf_obj)

    return ddf_objs


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
        # *********************************

        logging.info(f"Modify xarray cruise object")

        nc, nc_mappings = modify_xarray_obj(file_obj)

        logging.info(f"File shape {nc.dims}")

        file_obj['nc'] = nc
        file_obj['nc_mappings'] = nc_mappings

        xr_file_objs.append(file_obj)

    cruise_xr_obj = {}
    cruise_xr_obj['cruise_expocode'] = cruise_obj['cruise_expocode']
    cruise_xr_obj['xr_file_objs'] = xr_file_objs

    return cruise_xr_obj


def process_cruise_objs_by_type_series(cruise_objs):

    cruises_profiles_objs = []

    for cruise_obj in cruise_objs:

        start_time = datetime.now()

        logging.info('***********************')
        logging.info(f"cruise {cruise_obj['cruise_expocode']}")
        logging.info('***********************')

        # ************************************
        # Convert cruise objs into xarray objs
        # Modify xr obj and get mappings
        # *************************************

        logging.info('Process all cruise objects in xarray objects')

        cruise_xr_obj = create_xr_obj(cruise_obj)

        if not cruise_xr_obj['xr_file_objs']:
            continue

        # ***************************
        # Convert xarray objects into
        # Dask dataframe objects
        # ***************************

        logging.info('Process all xarray objects into Dask dataframe objects')

        ddf_objs = create_dask_dataframe_objs(cruise_xr_obj)

        cruise_ddf_obj = {}
        cruise_ddf_obj['cruise_expocode'] = cruise_xr_obj['cruise_expocode']
        cruise_ddf_obj['ddf_objs'] = ddf_objs

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

        cruises_profiles_objs.append(cruise_profiles_obj)

        logging.info("Time to run create cruise profiles")
        logging.info(datetime.now() - start_time)

    return cruises_profiles_objs
