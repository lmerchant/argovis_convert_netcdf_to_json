from datetime import datetime
import logging
from collections import defaultdict
import itertools


from xarray_and_dask.modify_xarray_obj import modify_xarray_obj
from xarray_and_dask.modify_dask_obj import modify_dask_obj
from create_profiles.create_meta_profiles import create_meta_profile
from create_profiles.create_meas_profiles import create_meas_profiles
from create_profiles.create_bgc_profiles import create_bgc_profiles


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


def get_goship_argovis_name_mapping_per_type(data_type):

    return {
        'pressure': 'pres',
        'ctd_salinity': f'psal_{data_type}',
        'ctd_salinity_qc': f'psal_{data_type}_qc',
        'ctd_temperature': f'temp_{data_type}',
        'ctd_temperature_qc': f'temp_{data_type}_qc',
        'ctd_temperature_68': f'temp_{data_type}',
        'ctd_temperature_68_qc': f'temp_{data_type}_qc',
        'ctd_oxygen': f'doxy_{data_type}',
        'ctd_oxygen_qc': f'doxy_{data_type}_qc',
        'ctd_oxygen_ml_l': f'doxy_{data_type}',
        'ctd_oxygen_ml_l_qc': f'doxy_{data_type}_qc',
        'bottle_salinity': f'salinity_{data_type}',
        'bottle_salinity_qc': f'salinity_{data_type}_qc',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def create_goship_argovis_mapping_profiles(
        nc_mappings, all_argovis_param_mapping, data_type):

    goship_meta_mapping = nc_mappings['goship_meta']
    goship_param_mapping = nc_mappings['goship_param']

    argovis_meta_mapping = nc_mappings['argovis_meta']

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

        core_goship_argovis_name_mapping = get_goship_argovis_name_mapping_per_type(
            data_type)

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
                argovis_name = f"{var}_{data_type}_qc"
                if argovis_name in all_argovis_names:
                    name_mapping[var] = argovis_name
            else:
                argovis_name = f"{var}_{data_type}"
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


def filter_argovis_mapping(nc_mappings, all_name_mapping):

    argovis_param_mapping = nc_mappings['argovis_param']

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


def create_profiles_one_type(file_obj):

    start_time = datetime.now()

    logging.info('---------------------------')
    logging.info(f"Start processing {file_obj['data_type']} profiles")
    logging.info('---------------------------')

    # ********************************
    # Modify Xarray object
    # and get before and after mappings
    # *********************************

    nc, nc_mappings = modify_xarray_obj(file_obj)

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
    all_meta_profiles = create_meta_profile(ddf_meta)

    data_type = file_obj['data_type']

    ddf_param = modify_dask_obj(ddf_param, data_type)

    # **********************************************
    # Convert to Pandas df since converting to dicts
    # **********************************************

    df_param = ddf_param.compute()

    all_meas_profiles, all_meas_source_profiles = create_meas_profiles(
        df_param, data_type)

    all_bgc_profiles, all_name_mapping = create_bgc_profiles(df_param)

    all_argovis_param_mapping = filter_argovis_mapping(
        nc_mappings, all_name_mapping)

    goship_argovis_mapping_profiles = create_goship_argovis_mapping_profiles(
        nc_mappings, all_argovis_param_mapping, data_type)

    # ******************************************************
    # Combine all the profile parts into one dictionary list
    # ******************************************************

    logging.info('start combining profiles')
    all_profiles = combine_profiles(all_meta_profiles, all_bgc_profiles,
                                    all_meas_profiles, all_meas_source_profiles, goship_argovis_mapping_profiles, data_type)

    logging.info("Time to run function create_profiles_one_type")
    logging.info(datetime.now() - start_time)

    logging.info('---------------------------')
    logging.info(f'End processing {data_type} profiles')
    logging.info(f"Num params {len(df_param.columns)}")
    logging.info(f"Shape of dims")
    logging.info(nc.dims)
    logging.info('---------------------------')

    return all_profiles
