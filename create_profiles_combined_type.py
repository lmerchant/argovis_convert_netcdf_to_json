# Create profiles for combined type (btl_ctd)
import logging

import filter_profiles as fp
import rename_objects as rn


def combine_btl_ctd_measurements(btl_measurements, ctd_measurements):

    use_elems, flag = fp.find_measurements_hierarchy_btl_ctd(
        btl_measurements, ctd_measurements)

    combined_btl_ctd_measurements, measurements_source, measurements_source_qc = fp.filter_btl_ctd_combined_measurements(
        btl_measurements, ctd_measurements, use_elems, flag)

    return combined_btl_ctd_measurements, measurements_source, measurements_source_qc


def combine_output_per_profile_btl_ctd(btl_profile, ctd_profile):

    btl_meta = {}
    btl_bgc_meas = []
    btl_measurements = []
    goship_names_btl = []
    goship_argovis_name_mapping_btl = {}
    goship_ref_scale_mapping_btl = {}
    argovis_ref_scale_btl = {}
    goship_units_btl = {}
    goship_argovis_units_btl = {}

    ctd_meta = {}
    ctd_bgc_meas = []
    ctd_measurements = []
    goship_names_ctd = []
    goship_argovis_name_mapping_ctd = {}
    goship_ref_scale_mapping_ctd = {}
    argovis_ref_scale_ctd = {}
    goship_units_ctd = {}
    goship_argovis_units_ctd = {}

    combined_btl_ctd_dict = {}

    # May have case where bot dict or ctd dict doesn't exist for same profile
    # But they have the same station_cast

    # All profiles have a profile and station_cast key but
    # profile_dict may be empty

    btl_dict = btl_profile['profile_dict']
    ctd_dict = ctd_profile['profile_dict']

    station_cast = btl_profile['station_cast']

    # For a combined dict, station_cast same for btl and ctd
    combined_btl_ctd_dict['type'] = 'btl_ctd'
    combined_btl_ctd_dict['stationCast'] = station_cast

    if btl_dict:

        btl_meta = btl_dict['meta']

        btl_bgc_meas = btl_dict['bgcMeas']
        btl_measurements = btl_dict['measurements']

        goship_argovis_name_mapping_btl = btl_dict['goshipArgovisNameMapping']

        goship_names_btl = btl_dict['goshipNames']
        goship_ref_scale_mapping_btl = btl_dict['goshipReferenceScale']
        argovis_ref_scale_btl = btl_dict['argovisReferenceScale']
        goship_units_btl = btl_dict['goshipUnits']
        goship_argovis_units_btl = btl_dict['goshipArgovisUnitsMapping']

    if ctd_dict:

        ctd_meta = ctd_dict['meta']

        ctd_bgc_meas = ctd_dict['bgcMeas']
        ctd_measurements = ctd_dict['measurements']

        goship_argovis_name_mapping_ctd = ctd_dict['goshipArgovisNameMapping']

        goship_names_ctd = ctd_dict['goshipNames']
        goship_ref_scale_mapping_ctd = ctd_dict['goshipReferenceScale']
        argovis_ref_scale_ctd = ctd_dict['argovisReferenceScale']
        goship_units_ctd = ctd_dict['goshipUnits']
        goship_argovis_units_ctd = ctd_dict['goshipArgovisUnitsMapping']

    if btl_dict and ctd_dict:

        # Put suffix of '_btl' in  bottle meta
        btl_meta = rn.rename_btl_by_key_meta(btl_meta)

        # Add extension of '_btl' to lat/lon and cast in name mapping
        new_obj = {}
        for key, val in goship_argovis_name_mapping_btl.items():
            if val == 'lat':
                new_obj[key] = 'lat_btl'
            elif val == 'lon':
                new_obj[key] = 'lon_btl'
            else:
                new_obj[key] = val

        goship_argovis_name_mapping_btl = new_obj

        # Remove _btl variables that are the same as CTD
        btl_meta.pop('expocode_btl', None)
        btl_meta.pop('cruise_url_btl', None)
        btl_meta.pop('DATA_CENTRE_btl', None)

    meta = {**ctd_meta, **btl_meta}

    bgc_meas = [*ctd_bgc_meas, *btl_bgc_meas]

    measurements, measurements_source, measurements_source_qc = combine_btl_ctd_measurements(
        btl_measurements, ctd_measurements)

    goship_names = [*goship_names_btl, *goship_names_ctd]

    goship_ref_scale_mapping = {
        **goship_ref_scale_mapping_ctd, **goship_ref_scale_mapping_btl}

    argovis_reference_scale = {
        **argovis_ref_scale_ctd, **argovis_ref_scale_btl}

    goship_argovis_units_mapping = {
        **goship_argovis_units_ctd, **goship_argovis_units_btl}

    combined_btl_ctd_dict['meta'] = meta
    combined_btl_ctd_dict['bgcMeas'] = bgc_meas
    combined_btl_ctd_dict['measurements'] = measurements
    combined_btl_ctd_dict['measurementsSource'] = measurements_source
    combined_btl_ctd_dict['measurementsSourceQC'] = measurements_source_qc

    if goship_argovis_name_mapping_btl and goship_argovis_name_mapping_ctd:
        combined_btl_ctd_dict['goshipArgovisNameMappingBtl'] = goship_argovis_name_mapping_btl
        combined_btl_ctd_dict['goshipArgovisNameMappingCtd'] = goship_argovis_name_mapping_ctd

    elif goship_argovis_name_mapping_btl:
        combined_btl_ctd_dict['goshipArgovisNameMapping'] = goship_argovis_name_mapping_btl

    elif goship_argovis_name_mapping_ctd:
        combined_btl_ctd_dict['goshipArgovisNameMapping'] = goship_argovis_name_mapping_ctd

    combined_btl_ctd_dict['goshipReferenceScale'] = goship_ref_scale_mapping
    combined_btl_ctd_dict['argovisReferenceScale'] = argovis_reference_scale
    combined_btl_ctd_dict['goshipArgovisUnitsMapping'] = goship_argovis_units_mapping

    combined_btl_ctd_dict['goshipNames'] = goship_names

    if goship_units_btl and goship_units_ctd:
        combined_btl_ctd_dict['goshipUnitsBtl'] = goship_units_btl
        combined_btl_ctd_dict['goshipUnitsCtd'] = goship_units_ctd
    elif goship_units_btl:
        combined_btl_ctd_dict['goshipUnits'] = goship_units_btl
    elif goship_units_ctd:
        combined_btl_ctd_dict['goshipUnits'] = goship_units_ctd

    combined_profile = {}
    combined_profile['profile_dict'] = combined_btl_ctd_dict
    combined_profile['station_cast'] = station_cast

    return combined_profile


def get_same_station_cast_profile_btl_ctd(btl_profiles, ctd_profiles):

    station_casts_btl = [btl_profile['station_cast']
                         for btl_profile in btl_profiles]

    station_casts_ctd = [ctd_profile['station_cast']
                         for ctd_profile in ctd_profiles]

    different_pairs_in_btl = set(
        station_casts_btl).difference(station_casts_ctd)
    different_pairs_in_ctd = set(
        station_casts_ctd).difference(station_casts_btl)

    for pair in different_pairs_in_btl:
        # Create matching but empty profiles for ctd
        new_profile = {}
        new_profile['profile_dict'] = {}
        new_profile['station_cast'] = pair
        ctd_profiles.append(new_profile)

    for pair in different_pairs_in_ctd:
        # Create matching but empty profiles for ctd
        new_profile = {}
        new_profile['profile_dict'] = {}
        new_profile['station_cast'] = pair
        btl_profiles.append(new_profile)

    return btl_profiles, ctd_profiles


def combine_btl_ctd_profiles(btl_profiles, ctd_profiles):

    # Get profile dicts so have the same number of profiles
    # one may be blank while the other exists at a cast
    btl_profiles, ctd_profiles = get_same_station_cast_profile_btl_ctd(
        btl_profiles, ctd_profiles)

    #  bottle  and ctd have same keys, but  different values
    # which are the profile numbers

    all_profiles = []

    # The number of station_casts are the same for btl and ctd
    station_casts = [btl_profile['station_cast']
                     for btl_profile in btl_profiles]

    for station_cast in station_casts:

        try:
            profile_dict_btl = [btl_profile['profile_dict']
                                for btl_profile in btl_profiles if btl_profile['station_cast'] == station_cast][0]

        except:
            profile_dict_btl = {}

        profile_btl = {}
        profile_btl['station_cast'] = station_cast
        profile_btl['profile_dict'] = profile_dict_btl

        try:
            profile_dict_ctd = [ctd_profile['profile_dict']
                                for ctd_profile in ctd_profiles if ctd_profile['station_cast'] == station_cast][0]

        except:
            profile_dict_ctd = {}

        profile_ctd = {}
        profile_ctd['station_cast'] = station_cast
        profile_ctd['profile_dict'] = profile_dict_ctd

        profile = {}
        profile['btl'] = profile_btl
        profile['ctd'] = profile_ctd

        all_profiles.append(profile)

    profiles_list_btl_ctd = []

    for profile in all_profiles:
        profile_btl = profile['btl']
        profile_ctd = profile['ctd']

        combined_profile_btl_ctd = combine_output_per_profile_btl_ctd(
            profile_btl, profile_ctd)

        profiles_list_btl_ctd.append(combined_profile_btl_ctd)

    logging.info('---------------------------')
    logging.info('Processed btl and ctd combined profiles')
    logging.info('---------------------------')

    return profiles_list_btl_ctd
