import logging

import pandas as pd
import numpy as np

from variable_mapping.rename_to_argovis import rename_to_argovis
from variable_mapping.meta_param_mapping import rename_mappings_keys
from variable_mapping.meta_param_mapping import get_meta_mapping
from variable_mapping.meta_param_mapping import rename_core_profile_keys
from variable_mapping.meta_param_mapping import rename_measurements_keys
from variable_mapping.meta_param_mapping import get_mappings_keys_mapping


def rename_measurements(data_type, measurements):

    df_measurements = pd.DataFrame.from_dict(measurements)

    # key is current name and value is argovis name
    # TODO
    # rename function to cchdo_to_argovis_mapping
    meas_name_mapping = rename_to_argovis(
        df_measurements.columns, data_type)

    # Remove suffix _{data_type}
    # TODO
    # Or keep since seem to be using it in when doing combined types
    # Can keep renaming.
    # Don't use suffix on measurementsSourceQC and then
    # use it in combined meas

    new_meas_name_mapping = {}
    for cchdo_name, argovis_name in meas_name_mapping.items():
        new_meas_name_mapping[cchdo_name] = argovis_name.replace(
            f"_{data_type}", '')

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
    # If have two 'temp' var names, means had both
    # ctd_temerature and ctd_temperature_68
    # Check before rename, and remove one not being used
    # if both not being used, remove one so don't end
    # up with two 'temp' keys and should
    # be saying using_temp False

    renamed_meas_sources = {}

    # renamed_meas_sources['qc'] = measurements_sources.pop('qc', None)

    for key, val in measurements_sources.items():
        # change key to argovis name
        # e.g. 'using_ctd_temperature' to 'temp'
        #var_in_key = key.replace('using_', '')
        # get map of cchdo name to argovis name
        # rename_to_argovis expects a list of names to map
        key_name_mapping = rename_to_argovis([key], data_type)

        new_key = f"{key_name_mapping[key]}"

        new_key = new_key.replace(f"_{data_type}", '')

        if key == 'ctd_temperature_68':
            new_key = new_key + '_68'

        renamed_meas_sources[new_key] = val

    if ('temp' in renamed_meas_sources) and ('temp_68' in renamed_meas_sources):
        using_temp = renamed_meas_sources['temp']
        using_temp_68 = renamed_meas_sources['temp_68']

        if using_temp:
            renamed_meas_sources.pop('temp_68', None)
        elif using_temp_68:
            renamed_meas_sources.pop('temp', None)
            renamed_meas_sources['temp'] = renamed_meas_sources['temp_68']
            renamed_meas_sources.pop('temp_68', None)
        else:
            renamed_meas_sources['temp'] = False
            renamed_meas_sources.pop('temp_68', None)

    elif ('temp' not in renamed_meas_sources) and ('temp_68' in renamed_meas_sources):
        renamed_meas_sources['temp'] = renamed_meas_sources['temp_68']
        renamed_meas_sources.pop('temp_68', None)

    return renamed_meas_sources


def create_mappings(profile_dict, data_type, meta, argovis_col_names_mapping):

    mappings = {}

    # Want before and after conversions
    # before
    #cchdo_meta_names = profile_dict['cchdoMetaNames']
    cchdo_units = profile_dict['cchdoUnits']
    cchdo_ref_scale = profile_dict['cchdoReferenceScale']

    # vars changed
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
    mappings['argovisParamNames'] = list(argovis_col_names_mapping.values())

    # key cchdoArgovisMetaMapping
    # (unchanging dictionary created by hand)
    # Not really needed. Only was there because argovis
    # changed name of latitude to lat

    # key cchdoArgovisParamMapping
    # from above, this is argovis_col_names_mapping
    mappings['cchdoArgovisParamMapping'] = argovis_col_names_mapping

    # # key cchdoReferenceScale
    # # starting reference scales before any conversions
    # mappings['cchdoReferenceScale'] = cchdo_ref_scale

    # # key cchdoUnits
    # # starting units before any conversions
    # mappings['cchdoUnits'] = cchdo_units

    # key argovisReferenceScale
    # Values are the same as cchdo except those converted
    # And rename
    argovis_ref_scale_mapping = {
        **cchdo_ref_scale, **cchdo_converted_ref_scale}

    # Get mapping and then swap out keys with argovis names
    argovis_name_mapping = rename_to_argovis(
        list(argovis_ref_scale_mapping.keys()), data_type)

    mappings['argovisReferenceScale'] = {argovis_name_mapping[key]: value for key,
                                         value in argovis_ref_scale_mapping.items()}

    # key argovisUnits
    # Values are the same as cchdo except those converted
    # And rename
    argovis_units_mapping = {
        **cchdo_units, **cchdo_converted_units}

    # Get mapping and then swap out keys with argovis names
    argovis_name_mapping = rename_to_argovis(
        list(argovis_units_mapping.keys()), data_type)

    mappings['argovisUnits'] = {argovis_name_mapping[key]: value for key,
                                value in argovis_units_mapping.items()}

    return mappings


def create_mappings_orig(profile_dict, data_type, meta, argovis_col_names_mapping):

    mappings = {}

    # Copy over current mappings
    cchdo_key_mapping = get_mappings_keys_mapping()

    for key, value in profile_dict.items():

        if key in cchdo_key_mapping.keys():
            mappings[key] = value

    # Want before and after conversions
    # before
    #cchdo_meta_names = profile_dict['cchdoMetaNames']
    cchdo_units = profile_dict['cchdoUnits']
    cchdo_ref_scale = profile_dict['cchdoReferenceScale']

    # vars changed
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
    mappings['argovisParamNames'] = list(argovis_col_names_mapping.values())

    # key cchdoArgovisMetaMapping
    # (unchanging dictionary created by hand)
    # Not really needed. Only was there because argovis
    # changed name of latitude to lat

    # key cchdoArgovisParamMapping
    # from above, this is argovis_col_names_mapping
    mappings['cchdoArgovisParamMapping'] = argovis_col_names_mapping

    # # key cchdoReferenceScale
    # # starting reference scales before any conversions
    # mappings['cchdoReferenceScale'] = cchdo_ref_scale

    # # key cchdoUnits
    # # starting units before any conversions
    # mappings['cchdoUnits'] = cchdo_units

    # key argovisReferenceScale
    # Values are the same as cchdo except those converted
    # And rename
    argovis_ref_scale_mapping = {
        **cchdo_ref_scale, **cchdo_converted_ref_scale}

    # Get mapping and then swap out keys with argovis names
    argovis_name_mapping = rename_to_argovis(
        list(argovis_ref_scale_mapping.keys()), data_type)

    mappings['argovisReferenceScale'] = {argovis_name_mapping[key]: value for key,
                                         value in argovis_ref_scale_mapping.items()}

    # key argovisUnits
    # Values are the same as cchdo except those converted
    # And rename
    argovis_units_mapping = {
        **cchdo_units, **cchdo_converted_units}

    # Get mapping and then swap out keys with argovis names
    argovis_name_mapping = rename_to_argovis(
        list(argovis_units_mapping.keys()), data_type)

    mappings['argovisUnits'] = {argovis_name_mapping[key]: value for key,
                                value in argovis_units_mapping.items()}

    return mappings


def rename_data(data, data_type):
    # For all_dataMeas, rename by loading dict into pandas dataframe,
    # rename cols then output back to dict
    df_data = pd.DataFrame.from_dict(data)

    argovis_col_names_mapping = rename_to_argovis(
        list(df_data.columns), data_type)

    argovis_col_names = list(argovis_col_names_mapping.values())

    df_data = df_data.set_axis(
        argovis_col_names, axis='columns', inplace=False)

    # df_data.columns = argovis_col_names_mapping.values()

    data = df_data.to_dict('records')

    return data, argovis_col_names_mapping


def get_subset_meta(meta):

    meta_subset = {}

    # keys are cchdo name, values are argovis names
    rename_mapping = get_meta_mapping()

    for key, value in rename_mapping.items():
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

    # **********************************
    # Use dates instead of time variable
    # **********************************

    profile_time = meta['time']
    meta['date_formatted'] = pd.to_datetime(profile_time).strftime("%Y-%m-%d")

    meta['date'] = pd.to_datetime(profile_time).isoformat()

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
    #meta['cruise_id'] = cchdo_cruise_meta.pop('id', None)

    meta = {**meta, **cchdo_file_meta, **cchdo_cruise_meta}

    return meta


def process_data_profiles(profiles_obj):

    data_type = profiles_obj['data_type']

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
        # probably remove var stationCast

        meta = profile_dict['meta']
        data = profile_dict['data']
        measurements = profile_dict['measurements']

        # ******************************
        # Add metadata to cchdo metadata
        # ******************************

        meta = add_cchdo_meta(meta, cchdo_file_meta, cchdo_cruise_meta)
        meta = add_argovis_meta(meta)

        # Rename meta for argovis json format and get subset
        meta = get_subset_meta(meta)

        # *************************
        # Rename all_data variables
        # *************************

        # TODO
        # Why have bottle_number_qc?

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

        cchdo_key_mapping = get_mappings_keys_mapping()

        for key, value in profile_dict.items():

            if key in cchdo_key_mapping.keys():
                current_mappings[key] = value

        # **************************************
        # Create New Mappings to include Argovis
        # **************************************

        new_mappings = create_mappings(
            profile_dict, data_type, meta, argovis_col_names_mapping)

        # ****************
        # Combine mappings
        # ****************

        mappings = {**current_mappings, **new_mappings}

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

        measurements_source = profile_dict['measurementsSource']
        measurements_sources = profile_dict['measurementsSources']
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

        # TODO
        # why keep station cast and data type part of profile dict
        # when can store them in the parent dict profile?

        new_profile_dict['station_cast'] = station_cast
        new_profile_dict['data_type'] = data_type

        new_profile_dict['meta'] = meta
        new_profile_dict['data'] = data

        new_profile_dict['measurements'] = measurements
        new_profile_dict['measurementsSource'] = measurements_source
        new_profile_dict['measurementsSources'] = measurements_sources
        #profile_dict['stationParameters'] = station_parameters

        # TODO
        # Change into renaming measurements
        # and renaming core profile keys
        # meta key is placeholder and not part of final JSON
        # since it is expanded out of meta dict
        renamed_profile_dict = rename_core_profile_keys(new_profile_dict)

        renamed_profile_dict = rename_measurements_keys(renamed_profile_dict)

        mappings = rename_mappings_keys(mappings)

        updated_profile_dict = {**renamed_profile_dict, **mappings}

        new_profile['profile_dict'] = updated_profile_dict

        updated_data_profiles.append(new_profile)

    return updated_data_profiles


def post_process_cruise_objs_by_type(cruises_profile_objs):

    # Rename variables and add argovis mappings

    updated_cruises_profile_objs = []

    for cruise_profiles_obj in cruises_profile_objs:

        expocode = cruise_profiles_obj['cruise_expocode']

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
