# Create profiles for combined type (btl_ctd)
import logging

import filter_measurements as fm
import rename_objects as rn


def combine_btl_ctd_measurements(btl_measurements, ctd_measurements):

    use_elems, flag = fm.find_measurements_hierarchy_btl_ctd(
        btl_measurements, ctd_measurements)

    combined_btl_ctd_measurements, measurements_source, measurements_source_qc = fm.filter_btl_ctd_combined_measurements(
        btl_measurements, ctd_measurements, use_elems, flag)

    return combined_btl_ctd_measurements, measurements_source, measurements_source_qc


def combine_mapping_per_profile_btl_ctd(combined_btl_ctd_dict, btl_dict, ctd_dict):

<<<<<<< HEAD
    # *******************************
    # Create combined mapping profile
    # *******************************

    # May have case where bot dict or ctd dict doesn't exist for same profile
    # But they have the same station_cast

    # All profiles have a profile and station_cast key but
    # profile_dict may be empty

    if btl_dict and not ctd_dict:
        combined_btl_ctd_dict['argovisMetaNames'] = btl_dict['argovisMetaNames']
        combined_btl_ctd_dict['argovisParamNames'] = btl_dict['argovisParamNames']
        combined_btl_ctd_dict['argovisReferenceScale'] = btl_dict['argovisReferenceScale']
        combined_btl_ctd_dict['argovisUnits'] = btl_dict['argovisUnits']

        combined_btl_ctd_dict['goshipMetaNames'] = btl_dict['goshipMetaNames']
        combined_btl_ctd_dict['goshipParamNames'] = btl_dict['goshipParamNames']
        combined_btl_ctd_dict['goshipArgovisMetaMapping'] = btl_dict['goshipArgovisMetaMapping']
        combined_btl_ctd_dict['goshipArgovisParamMapping'] = btl_dict['goshipArgovisParamMapping']
        combined_btl_ctd_dict['goshipReferenceScale'] = btl_dict['goshipReferenceScale']
        combined_btl_ctd_dict['goshipUnits'] = btl_dict['goshipUnits']

    if ctd_dict and not btl_dict:
        combined_btl_ctd_dict['argovisMetaNames'] = ctd_dict['argovisMetaNames']
        combined_btl_ctd_dict['argovisParamNames'] = ctd_dict['argovisParamNames']
        combined_btl_ctd_dict['argovisReferenceScale'] = ctd_dict['argovisReferenceScale']
        combined_btl_ctd_dict['argovisUnits'] = ctd_dict['argovisUnits']

        combined_btl_ctd_dict['goshipMetaNames'] = ctd_dict['goshipMetaNames']
        combined_btl_ctd_dict['goshipParamNames'] = ctd_dict['goshipParamNames']
        combined_btl_ctd_dict['goshipArgovisMetaMapping'] = ctd_dict['goshipArgovisMetaMapping']
        combined_btl_ctd_dict['goshipArgovisParamMapping'] = ctd_dict['goshipArgovisParamMapping']
        combined_btl_ctd_dict['goshipReferenceScale'] = ctd_dict['goshipReferenceScale']
        combined_btl_ctd_dict['goshipUnits'] = ctd_dict['goshipUnits']

    if btl_dict and ctd_dict:

        # Put suffix of '_btl' in  bottle meta

        # Remove _btl variables that are the same as CTD
        btl_exclude = set(['expocode', 'cruise_url', 'DATA_CENTRE'])

        argovis_meta_names_btl_subset = list(
            set(btl_dict['argovisMetaNames']).difference(btl_exclude))

        goship_argovis_meta_mapping_btl = btl_dict['goshipArgovisMetaMapping']
        goship_argovis_meta_mapping_btl = {
            key: val for key, val in goship_argovis_meta_mapping_btl.items() if val in argovis_meta_names_btl_subset}

        combined_btl_ctd_dict['goshipMetaNamesBtl'] = btl_dict['goshipMetaNames']
        combined_btl_ctd_dict['goshipMetaNamesCtd'] = ctd_dict['goshipMetaNames']

        combined_btl_ctd_dict['goshipParamNamesBtl'] = btl_dict['goshipParamNames']
        combined_btl_ctd_dict['goshipParamNamesCtd'] = ctd_dict['goshipParamNames']

        combined_btl_ctd_dict['goshipArgovisMetaMappingBtl'] = goship_argovis_meta_mapping_btl
        combined_btl_ctd_dict['goshipArgovisMetaMappingCtd'] = ctd_dict['goshipArgovisMetaMapping']

        combined_btl_ctd_dict['goshipArgovisParamMappingBtl'] = btl_dict['goshipArgovisParamMapping']
        combined_btl_ctd_dict['goshipArgovisParamMappingCtd'] = ctd_dict['goshipArgovisParamMapping']

        combined_btl_ctd_dict['goshipReferenceScaleBtl'] = btl_dict['goshipReferenceScale']
        combined_btl_ctd_dict['goshipReferenceScaleCtd'] = ctd_dict['goshipReferenceScale']

        combined_btl_ctd_dict['goshipUnitsBtl'] = btl_dict['goshipUnits']
        combined_btl_ctd_dict['goshipUnitsCtd'] = ctd_dict['goshipUnits']

        argovis_meta_names = [*ctd_dict['argovisMetaNames'],
                              *btl_dict['argovisMetaNames']]

        argovis_param_names = [*ctd_dict['argovisParamNames'],
                               *btl_dict['argovisParamNames']]

        combined_btl_ctd_dict['argovisMetaNames'] = argovis_meta_names
        combined_btl_ctd_dict['argovisParamNames'] = argovis_param_names

        argovis_ref_scale_mapping = {
            **ctd_dict['argovisReferenceScale'], **btl_dict['argovisReferenceScale']}

        argovis_units_mapping = {
            **ctd_dict['argovisUnits'], **btl_dict['argovisUnits']}

        combined_btl_ctd_dict['argovisReferenceScale'] = argovis_ref_scale_mapping
        combined_btl_ctd_dict['argovisUnits'] = argovis_units_mapping

    return combined_btl_ctd_dict


def combine_output_per_profile_btl_ctd(btl_profile, ctd_profile):

    # For a combined dict, station_cast same for btl and ctd
    station_cast = btl_profile['station_cast']
=======
    btl_meta = {}
    btl_bgc_meas = []
    btl_measurements = []
    goship_names_btl = []
    goship_argovis_name_mapping_btl = {}
    goship_ref_scale_btl = {}
    argovis_ref_scale_btl = {}
    goship_units_btl = {}
    argovis_units_btl = {}

    ctd_meta = {}
    ctd_bgc_meas = []
    ctd_measurements = []
    goship_names_ctd = []
    goship_argovis_name_mapping_ctd = {}
    goship_ref_scale_ctd = {}
    argovis_ref_scale_ctd = {}
    goship_units_ctd = {}
    argovis_units_ctd = {}
>>>>>>> 689fb70962b7b22fd8af98fc653c839f744bbee8

    combined_btl_ctd_dict = {}
    combined_btl_ctd_dict['type'] = 'btl_ctd'
    combined_btl_ctd_dict['stationCast'] = station_cast

    # May have case where bot dict or ctd dict doesn't exist for same profile
    # But they have the same station_cast

    # All profiles have a profile and station_cast key but
    # profile_dict may be empty

    btl_dict = btl_profile['profile_dict']
    ctd_dict = ctd_profile['profile_dict']

    btl_meta = {}
    ctd_meta = {}

    btl_measurements = []
    ctd_measurements = []

    btl_bgc_meas = []
    ctd_bgc_meas = []

    if btl_dict:
        btl_meta = btl_dict['meta']
        btl_bgc_meas = btl_dict['bgcMeas']
        btl_measurements = btl_dict['measurements']

<<<<<<< HEAD
=======
        goship_argovis_name_mapping_btl = btl_dict['goshipArgovisNameMapping']
        goship_names_btl = btl_dict['goshipNames']
        argovis_names_btl = btl_dict['argovisNames']
        goship_ref_scale_btl = btl_dict['goshipReferenceScale']
        argovis_ref_scale_btl = btl_dict['argovisReferenceScale']
        goship_units_btl = btl_dict['goshipUnits']
        argovis_units_btl = btl_dict['argovisUnits']

>>>>>>> 689fb70962b7b22fd8af98fc653c839f744bbee8
    if ctd_dict:
        ctd_meta = ctd_dict['meta']
        ctd_bgc_meas = ctd_dict['bgcMeas']
        ctd_measurements = ctd_dict['measurements']

<<<<<<< HEAD
=======
        goship_argovis_name_mapping_ctd = ctd_dict['goshipArgovisNameMapping']
        goship_names_ctd = ctd_dict['goshipNames']
        argovis_names_ctd = ctd_dict['argovisNames']
        goship_ref_scale_ctd = ctd_dict['goshipReferenceScale']
        argovis_ref_scale_ctd = ctd_dict['argovisReferenceScale']
        goship_units_ctd = ctd_dict['goshipUnits']
        argovis_units_ctd = ctd_dict['argovisUnits']

>>>>>>> 689fb70962b7b22fd8af98fc653c839f744bbee8
    if btl_dict and ctd_dict:

        # Put suffix of '_btl' in  bottle meta
        # Remove _btl variables that are the same as CTD
        btl_exclude = set(['expocode', 'cruise_url', 'DATA_CENTRE'])

        for item in btl_exclude:
            if item in btl_meta:
                btl_meta.pop(item)

        btl_meta = rn.rename_btl_by_key_meta(btl_meta)

    meta = {**ctd_meta, **btl_meta}
    bgc_meas = [*ctd_bgc_meas, *btl_bgc_meas]

    measurements, measurements_source, measurements_source_qc = combine_btl_ctd_measurements(
        btl_measurements, ctd_measurements)

<<<<<<< HEAD
=======
    goship_names_btl = goship_names_btl
    goship_names_ctd = goship_names_ctd

    argovis_names = [*argovis_names_btl, *argovis_names_ctd]

    goship_ref_scale_mapping_btl = goship_ref_scale_btl
    goship_ref_scale_mapping_ctd = goship_ref_scale_ctd

    argovis_ref_scale_mapping = {
        **argovis_ref_scale_ctd, **argovis_ref_scale_btl}

    goship_units_mapping_btl = goship_units_btl
    goship_units_mapping_ctd = goship_units_ctd

    argovis_units_mapping = {
        **argovis_units_ctd, **argovis_units_btl}

    # ***********************
    # Create combined profile
    # ***********************

>>>>>>> 689fb70962b7b22fd8af98fc653c839f744bbee8
    combined_btl_ctd_dict['meta'] = meta
    combined_btl_ctd_dict['bgcMeas'] = bgc_meas
    combined_btl_ctd_dict['measurements'] = measurements
    combined_btl_ctd_dict['measurementsSource'] = measurements_source
    combined_btl_ctd_dict['measurementsSourceQC'] = measurements_source_qc

<<<<<<< HEAD
    combined_btl_ctd_dict = combine_mapping_per_profile_btl_ctd(combined_btl_ctd_dict,
                                                                btl_dict, ctd_dict)
=======
    if goship_names_btl and goship_names_ctd:
        combined_btl_ctd_dict['goshipNamesBtl'] = goship_names_btl
        combined_btl_ctd_dict['goshipNamesCtd'] = goship_names_ctd
    elif goship_names_btl:
        combined_btl_ctd_dict['goshipNames'] = goship_names_btl
    elif goship_names_ctd:
        combined_btl_ctd_dict['goshipNames'] = goship_names_ctd

    combined_btl_ctd_dict['argovisNames'] = argovis_names

    if goship_argovis_name_mapping_btl and goship_argovis_name_mapping_ctd:
        combined_btl_ctd_dict['goshipArgovisNameMappingBtl'] = goship_argovis_name_mapping_btl
        combined_btl_ctd_dict['goshipArgovisNameMappingCtd'] = goship_argovis_name_mapping_ctd
    elif goship_argovis_name_mapping_btl:
        combined_btl_ctd_dict['goshipArgovisNameMapping'] = goship_argovis_name_mapping_btl
    elif goship_argovis_name_mapping_ctd:
        combined_btl_ctd_dict['goshipArgovisNameMapping'] = goship_argovis_name_mapping_ctd

    if goship_ref_scale_mapping_btl and goship_ref_scale_mapping_ctd:
        combined_btl_ctd_dict['goshipReferenceScaleBtl'] = goship_ref_scale_mapping_btl
        combined_btl_ctd_dict['goshipReferenceScaleCtd'] = goship_ref_scale_mapping_ctd
    elif goship_ref_scale_mapping_btl:
        combined_btl_ctd_dict['goshipReferenceScale'] = goship_ref_scale_mapping_btl
    elif goship_ref_scale_mapping_ctd:
        combined_btl_ctd_dict['goshipReferenceScale'] = goship_ref_scale_mapping_ctd

    combined_btl_ctd_dict['argovisReferenceScale'] = argovis_ref_scale_mapping

    if goship_units_mapping_btl and goship_units_mapping_ctd:
        combined_btl_ctd_dict['goshipUnitsBtl'] = goship_units_mapping_btl
        combined_btl_ctd_dict['goshipUnitsCtd'] = goship_units_mapping_ctd
    elif goship_units_mapping_btl:
        combined_btl_ctd_dict['goshipUnits'] = goship_units_mapping_btl
    elif goship_units_mapping_ctd:
        combined_btl_ctd_dict['goshipUnits'] = goship_units_mapping_ctd

    combined_btl_ctd_dict['argovisUnits'] = argovis_units_mapping
>>>>>>> 689fb70962b7b22fd8af98fc653c839f744bbee8

    combined_profile = {}
    combined_profile['profile_dict'] = combined_btl_ctd_dict
    combined_profile['station_cast'] = station_cast

    return combined_profile


def get_same_station_cast_profile_btl_ctd(btl_profiles, ctd_profiles):

    # Want to group  on station_cast, so if number
    # of station_cast combinations is not the same,
    # Set a station_cast for that empty profile  so
    # that the number of station_cast values is the
    # same for btl and ctd.

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
