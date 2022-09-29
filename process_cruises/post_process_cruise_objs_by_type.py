import logging

import pandas as pd
import numpy as np

from variable_naming.filter_parameters import get_parameters_to_filter_out
from variable_naming.rename_parameters import rename_to_argovis_mapping
from variable_naming.rename_parameters import rename_with_data_type
from variable_naming.rename_parameters import rename_mapping_w_data_type
from variable_naming.meta_param_mapping import rename_mappings_source_info_keys
from variable_naming.meta_param_mapping import get_meta_mapping
from variable_naming.meta_param_mapping import rename_core_profile_keys
from variable_naming.meta_param_mapping import rename_measurements_keys
from variable_naming.meta_param_mapping import get_program_argovis_source_info_mapping
from variable_naming.meta_param_mapping import get_source_independent_meta_names
from variable_naming.meta_param_mapping import get_parameters_no_data_type
from variable_naming.rename_units import change_units_to_argovis


def reorganize_meta_and_mappings(meta, mappings, data_type):

    # put all mappings into one dictionary called source_info

    source_info = {}

    for key, val in mappings.items():
        source_info[key] = val

    if f'source' in meta.keys():
        source_info[f'source'] = meta[f'source']
        meta.pop(f'source', None)

    if f'cruise_url_{data_type}' in meta.keys():
        source_info[f'cruise_url_{data_type}'] = meta[f'cruise_url_{data_type}']
        meta.pop(f'cruise_url_{data_type}', None)

    if f'source_url_{data_type}' in meta.keys():
        source_info['source_url'] = meta[f'source_url_{data_type}']
        meta.pop(f'source_url_{data_type}', None)

    if f'file_name_{data_type}' in meta.keys():
        source_info['file_name'] = meta[f'file_name_{data_type}']
        meta.pop(f'file_name_{data_type}', None)

    # Do this later in post process combination
    # source_url_key = f"source_url_{data_type}"
    # source_info['source_url'] = source_url_key

    meta[f'source_info_{data_type}'] = source_info

    return meta


def rename_measurements(data_type, measurements):

    df_measurements = pd.DataFrame.from_dict(measurements)

    # key is current name and value is argovis name
    # TODO
    # rename function to cchdo_to_argovis_mapping
    meas_name_mapping = rename_to_argovis_mapping(
        list(df_measurements.columns))

    # Don't use suffix on measurementsSourceQC

    new_meas_name_mapping = {}
    for cchdo_name, argovis_name in meas_name_mapping.items():
        new_meas_name_mapping[cchdo_name] = argovis_name

    df_measurements = df_measurements.set_axis(
        list(new_meas_name_mapping.values()), axis='columns', inplace=False)

    # Can have either salinity instead of psal if using bottle_salinity
    # need to rename saliniy to psal
    # TODO
    # Don't rename till after use with combined types
    # since relying on salinity to exist to select it

    # if 'salinity' in df_measurements.columns:
    #     df_measurements = df_measurements.rename(
    #         {'salinity': 'psal'}, axis=1)

    # TODO
    # Or could rename if keep track of what it was.
    # Wouldn't measurements source do that for me?

    # combined is looking at key = 'measurementsSourceQC'
    # for source of psal or salinity. Look at where I
    # create this for one source

    measurements = df_measurements.to_dict('records')

    return measurements


def rename_measurements_sources(data_type, measurements_sources):
    # Rename measurementsSourceQC
    # If have two 'temperature' var names, means had both
    # ctd_temerature and ctd_temperature_68
    # Check before rename, and remove one not being used
    # if both not being used, remove one so don't end
    # up with two 'temperature' keys and should
    # be saying using_temp False

    renamed_meas_sources = {}

    # renamed_meas_sources['qc'] = measurements_sources.pop('qc', None)

    for key, val in measurements_sources.items():
        # change key to argovis name
        # e.g. 'using_ctd_temperature' to 'temperature'
        #var_in_key = key.replace('using_', '')
        # get map of cchdo name to argovis name
        # rename_to_argovis expects a list of names to map
        key_name_mapping = rename_to_argovis_mapping([key])

        new_key = f"{key_name_mapping[key]}"

        #new_key = new_key.replace(f"_{data_type}", '')

        if key == 'ctd_temperature_68':
            new_key = new_key + '_68'

        renamed_meas_sources[new_key] = val

    if ('temperature' in renamed_meas_sources) and ('temperature_68' in renamed_meas_sources):
        using_temp = renamed_meas_sources['temperature']
        using_temp_68 = renamed_meas_sources['temperature_68']

        if using_temp:
            renamed_meas_sources.pop('temperature_68', None)
        elif using_temp_68:
            renamed_meas_sources.pop('temperature', None)
            renamed_meas_sources['temperature'] = renamed_meas_sources['temperature_68']
            renamed_meas_sources.pop('temperature_68', None)
        else:
            renamed_meas_sources['temperature'] = False
            renamed_meas_sources.pop('temperature_68', None)

    elif ('temperature' not in renamed_meas_sources) and ('temperature_68' in renamed_meas_sources):
        renamed_meas_sources['temperature'] = renamed_meas_sources['temperature_68']
        renamed_meas_sources.pop('temperature_68', None)

    return renamed_meas_sources


def create_mappings(profile_dict, data_type, meta, argovis_col_names_mapping):

    mappings = {}

    # Want before and after conversions
    # before
    #cchdo_meta_names = profile_dict['cchdoMetaNames']
    cchdo_units = profile_dict['cchdo_units']
    cchdo_ref_scale = profile_dict['cchdo_reference_scale']
    cchdo_param_names = profile_dict['cchdo_param_names']
    cchdo_standard_names = profile_dict['cchdo_standard_names']

    # vars with attributes changed
    cchdo_converted_units = profile_dict['cchdoConvertedUnits']
    cchdo_converted_ref_scale = profile_dict['cchdoConvertedReferenceScale']

    # later need to add cchdo names to netcdf names mapping.
    # Do this earlier in the program since getting info from
    # netcdf file

    # key argovisMetaNames
    # (listing of all the var names in meta)
    #mappings['argovisMetaNames'] = list(meta.keys())

    # key argovisParamNames
    # (listing of all the var names in data)
    mappings['argovis_param_names'] = list(argovis_col_names_mapping.values())

    # key cchdoArgovisParamMapping
    # from above, this is argovis_col_names_mapping
    mappings['cchdo_argovis_param_mapping'] = argovis_col_names_mapping

    # key cchdoStandardNames
    mappings['cchdo_standard_names'] = cchdo_standard_names

    # key cchdo param names
    mappings['cchdo_param_names'] = cchdo_param_names

    # # key cchdoReferenceScale
    # # starting reference scales before any conversions
    # mappings['cchdo_reference_scale'] = cchdo_ref_scale

    # # key cchdoUnits
    # # starting units before any conversions
    # mappings['cchdo_units'] = cchdo_units

    # key argovisReferenceScale
    # Values are the same as cchdo except those converted
    # And rename
    cchdo_name_ref_scale_mapping = {
        **cchdo_ref_scale, **cchdo_converted_ref_scale}

    # Get mapping and then swap out keys with argovis names
    # Takes in ref_scale_mapping.keys(), cchdo names, and
    # gives back the mapping from these cchdo names to argovis names
    cchdo_to_argovis_name_mapping = rename_to_argovis_mapping(
        list(cchdo_name_ref_scale_mapping.keys()))

    # Add data_type suffix
    cchdo_to_argovis_name_mapping_w_data_type = rename_mapping_w_data_type(
        cchdo_to_argovis_name_mapping, data_type)

    mappings['argovis_reference_scale'] = {
        cchdo_to_argovis_name_mapping_w_data_type[key]: value for key, value in cchdo_name_ref_scale_mapping.items()}

    # key argovisUnits
    # Values are the same as cchdo except those converted
    # And rename
    cchdo_units_mapping = {
        **cchdo_units, **cchdo_converted_units}

    # Convert unit names to argovis names
    # Take in cchdo name to units (included those converted)
    # and return a mapping of cchdo name to argovis unit names
    # Need reference scale so can match up if variable is salinity
    # since salinity units are currently 1 which can be a unit
    # of many variables
    cchdo_name_to_argovis_unit_mapping = change_units_to_argovis(
        cchdo_units_mapping, cchdo_name_ref_scale_mapping)

    # Convert cchdo names to argovis names
    # Get mapping and then swap out keys with argovis names
    cchdo_to_argovis_name_mapping = rename_to_argovis_mapping(
        list(cchdo_name_to_argovis_unit_mapping.keys()))

    cchdo_to_argovis_name_mapping_w_data_type = rename_mapping_w_data_type(
        cchdo_to_argovis_name_mapping, data_type)

    mappings['argovis_units'] = {
        cchdo_to_argovis_name_mapping_w_data_type[key]: value for key, value in cchdo_name_to_argovis_unit_mapping.items()}

    return mappings


def filter_out_params(parameter_names):

    params_to_filter_out = get_parameters_to_filter_out()

    params_to_filter = [
        param for param in parameter_names if param in params_to_filter_out]

    return params_to_filter


def rename_data(data, data_type):

    #logging.info('inside rename_data')

    # Rename by loading dict into pandas dataframe,
    # rename cols then output back to dict
    df_data = pd.DataFrame.from_dict(data)

    #logging.info(f"column names before rename {list(df_data.columns)}")

    data_columns = list(df_data.columns)

    # First rename to any ArgoVis names
    argovis_col_names_mapping_wo_data_type = rename_to_argovis_mapping(
        data_columns)

    argovis_col_names = []
    for col_name in data_columns:
        argovis_name = argovis_col_names_mapping_wo_data_type[col_name]

        argovis_col_names.append(argovis_name)

    df_data = df_data.set_axis(
        argovis_col_names, axis='columns', inplace=False)

    # Now add suffix of data type to both data
    data_columns = list(df_data.columns)

    data_type_col_names = rename_with_data_type(data_columns, data_type)

    argovis_col_names_mapping = rename_mapping_w_data_type(
        argovis_col_names_mapping_wo_data_type, data_type)

    df_data = df_data.set_axis(
        data_type_col_names, axis='columns', inplace=False)

    # Filter out parameters not used for ArgoVis
    cols_to_filter_out = filter_out_params(list(df_data.columns))

    df_data = df_data.drop(cols_to_filter_out, axis=1)

    #logging.info(f"column names after rename {list(df_data.columns)}")

    data = df_data.to_dict('records')

    return data, argovis_col_names_mapping


def remove_deleted_vars_from_mappings(
        profile_dict, variables_deleted):

    cchdo_key_mapping = get_program_argovis_source_info_mapping()

    for key, value in profile_dict.items():

        if key in cchdo_key_mapping.keys():

            # remove variables_deleted from value
            if isinstance(value, list):
                # Remove deleted var name from list
                new_value = [
                    val for val in value if val not in variables_deleted]

            elif isinstance(value, dict):
                # Remove deleted var name from dict
                new_value = {}
                for k, v in value.items():
                    if k not in variables_deleted:
                        new_value[k] = v
            else:
                new_value = value

            profile_dict[key] = new_value

    return profile_dict


def remove_nan_variables(data):

    #logging.info('inside remove_nan_variables')

    # Read data into a pandas dataframe
    df_data = pd.DataFrame.from_dict(data)

    # column names before delete
    column_names_start = df_data.columns

    # Check if all values for column are nan,
    # and if they are, drop them
    df_data = df_data.dropna(axis=1, how='all')

    # Keep track of dropped vars to remove them from mappings
    column_names_end = df_data.columns

    variables_deleted = [
        x for x in column_names_start if x not in column_names_end]

    # Turn data back into dict
    data = df_data.to_dict('records')

    return data, variables_deleted


def add_data_type_to_meta(meta, data_type):

    ignore_keys = get_source_independent_meta_names()

    new_meta = {}

    for key, val in meta.items():
        if key not in ignore_keys:
            #new_meta[key] = val
            new_key = f"{key}_{data_type}"
            new_meta[new_key] = val
        else:
            new_meta[key] = val

    return new_meta


def get_subset_meta(meta):

    meta_subset = {}

    # keys are names used in this program at start, values are final argovis names
    rename_mapping = get_meta_mapping()

    # meta mapping only includes meta values want to keep

    meta_keys = meta.keys()

    for key, value in rename_mapping.items():
        if key in meta_keys:
            meta_subset[value] = meta[key]

    return meta_subset


def create_geolocation_dict(lat, lon):

    # "geoLocation": {
    #     "coordinates": [
    #         -158.2927,
    #         21.3693
    #     ],
    #     "type": "Point"
    # },

    coordinates = [lon, lat]

    geo_dict = {}
    geo_dict['coordinates'] = coordinates
    geo_dict['type'] = 'Point'

    return geo_dict


def add_argovis_meta(meta):

    # Add in station_cast var for later
    # processing of groups. But in
    # final JSON, it's dropped

    # lower case the station since BTL and CTD
    # could have the same station but won't compare
    # the same because case difference

    expocode = meta['expocode']
    station = meta['station']
    cast = meta['cast']

    # Create unique id
    def create_id(x, y):
        station = (str(x).zfill(3)).lower()
        cast = str(y).zfill(3)
        return f"expo_{expocode}_sta_{station}_cast_{cast}"

    meta['_id'] = create_id(station, cast)

    meta['positioning_system'] = 'GPS'
    meta['data_center'] = 'CCHDO'

    # Create a source_info dict that will hold multiple mapping keys
    meta['source_info'] = {}

    # **********************************
    # Use dates instead of time variable
    # **********************************

    profile_time = meta['time']
    meta['date_formatted'] = pd.to_datetime(profile_time).strftime("%Y-%m-%d")

    #meta['date'] = pd.to_datetime(profile_time).isoformat()
    meta['date'] = pd.to_datetime(profile_time).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Don't include cchdo meta time var
    meta.pop('time', None)

    # **************************************
    # Create ArgoVis lat/lon extra meta data
    # and geolocation key
    # **************************************

    latitude = meta['latitude']
    longitude = meta['longitude']

    round_lat = np.round(latitude, 3)
    round_lon = np.round(longitude, 3)

    meta['roundLat'] = round_lat
    meta['roundLon'] = round_lon

    meta['strLat'] = f"{round_lat} N"
    meta['strLon'] = f"{round_lon} E"

    geo_dict = create_geolocation_dict(latitude, longitude)

    meta['geoLocation'] = geo_dict

    return meta


def add_cchdo_meta(meta, cchdo_file_meta, cchdo_cruise_meta):

    # cchdo_file_meta from cchdo file json
    # cchdo_cruise_meta from  cchdo cruise json

    meta['file_expocode'] = meta.pop('expocode', None)

    meta = {**meta, **cchdo_file_meta, **cchdo_cruise_meta}

    return meta


def process_data_profiles(profiles_obj):

    data_type = profiles_obj['data_type']

    expocode = profiles_obj['cchdo_cruise_meta']['expocode']

    #logging.info(f'Processing data profiles for {expocode}')

    cchdo_file_meta = profiles_obj['cchdo_file_meta']
    cchdo_cruise_meta = profiles_obj['cchdo_cruise_meta']

    data_profiles = profiles_obj['data_type_profiles_list']

    updated_data_profiles = []

    for profile in data_profiles:

        station_cast = profile['station_cast']
        profile_dict = profile['profile_dict']

        new_profile = {}
        new_profile['station_cast'] = station_cast

        # TODO
        # remove var stationCast ?

        meta = profile_dict['meta']
        data = profile_dict['data']
        measurements = profile_dict['measurements']

        # *****************************
        # Delete variables from profile
        # if values are all NaN
        # *****************************

        data, variables_deleted = remove_nan_variables(data)

        profile_dict = remove_deleted_vars_from_mappings(
            profile_dict, variables_deleted)

        # ******************************
        # Add metadata to cchdo metadata
        # ******************************

        meta = add_cchdo_meta(meta, cchdo_file_meta, cchdo_cruise_meta)

        meta = add_argovis_meta(meta)

        # Rename meta for argovis json format and get subset
        meta = get_subset_meta(meta)

        # Also take meta data, and for items not common to a cruise,
        # Add a data_type suffix in addition to what is there
        meta = add_data_type_to_meta(meta, data_type)

        # *************************
        # Rename all_data variables
        # *************************

        # TODO
        # Why have bottle_number_qc? Saw it in one of the cruises btl/ctd files
        # Don't remember which right now

        # TODO
        # When rename all_data, ctd_salinity becomes psal unless there is none,
        # then bottle_salinity is psal. then need to update
        # cchdo argovis mapping

        data, argovis_col_names_mapping = rename_data(data, data_type)

        # ********************
        # Get current mappings
        # ********************

        # Copy over current mappings
        current_mappings = {}

        cchdo_key_mapping = get_program_argovis_source_info_mapping()

        for key, value in profile_dict.items():

            if key in cchdo_key_mapping.keys():
                current_mappings[key] = value

        # **************************************
        # Create New Mappings to include Argovis variable names
        # **************************************

        new_mappings = create_mappings(
            profile_dict, data_type, meta, argovis_col_names_mapping)

        # ****************
        # Combine mappings
        # ****************

        mappings = {**current_mappings, **new_mappings}

        mappings = rename_mappings_source_info_keys(mappings)

        # *********************************
        # Reorganize mappings into one dict
        # *********************************

        new_meta = reorganize_meta_and_mappings(meta, mappings, data_type)

        # **************
        # measurements
        # **************

        # TODO
        # Move logic of filtering by core values here since may not
        # use in final json version. Easier to change if wait till
        # end of program and not mix it in the middle

        # TODO
        # What are station parameters?
        # currently they are starting cchdo names

        measurements_source = profile_dict['measurements_source']
        measurements_sources = profile_dict['measurements_sources']
        #station_parameters = profile_dict.pop('stationParameters', None)

        # Rename measurementsSources keys
        measurements_sources = rename_measurements_sources(
            data_type, measurements_sources)

        # rename measurements variables
        measurements = rename_measurements(data_type, measurements)

        # *********************
        # Update profile values
        # *********************

        new_profile_dict = {}

        new_profile_dict['station_cast'] = station_cast
        new_profile_dict['data_type'] = data_type

        new_profile_dict['meta'] = new_meta

        new_profile_dict['data'] = data

        new_profile_dict['measurements'] = measurements
        new_profile_dict['measurements_source'] = measurements_source
        new_profile_dict['measurements_sources'] = measurements_sources
        #profile_dict['stationParameters'] = station_parameters

        renamed_profile_dict = rename_core_profile_keys(new_profile_dict)

        renamed_profile_dict = rename_measurements_keys(renamed_profile_dict)

        # updated_profile_dict = {**renamed_profile_dict, **mappings}

        new_profile['profile_dict'] = renamed_profile_dict

        updated_data_profiles.append(new_profile)

    return updated_data_profiles


def post_process_cruise_objs_by_type(cruises_profile_objs):

    # Rename variables and add argovis mappings

    updated_cruises_profile_objs = []

    for cruise_profiles_obj in cruises_profile_objs:

        cruise_expocode = cruise_profiles_obj['cruise_expocode']

        logging.info(f"Post processing expocode {cruise_expocode}")

        all_data_types_profile_objs = cruise_profiles_obj['all_data_types_profile_objs']

        updated_all_data_types_profile_objs = []

        for profiles_obj in all_data_types_profile_objs:

            data_type = profiles_obj['data_type']

            updated_data_profiles = process_data_profiles(profiles_obj)

            new_profiles_obj = {}
            new_profiles_obj['data_type'] = data_type

            new_profiles_obj['data_type_profiles_list'] = updated_data_profiles

            updated_all_data_types_profile_objs.append(new_profiles_obj)

        cruise_profiles_obj['all_data_types_profile_objs'] = updated_all_data_types_profile_objs

        updated_cruises_profile_objs.append(cruise_profiles_obj)

    return updated_cruises_profile_objs
