import logging
import pandas as pd


def convert_boolean(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    if isinstance(obj, (list, tuple)):
        return [convert_boolean(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_boolean(key): convert_boolean(value) for key, value in obj.items()}
    return obj


def rename_btl_by_key_meta(obj):

    new_obj = {}

    for key, val in obj.items():

        new_key = f"{key}_btl"

        new_obj[new_key] = val

    return new_obj


def filter_btl_ctd_combined_measurements(btl_measurements, ctd_measurements, use_elems, flag):

    use_temp_btl = use_elems['use_temp_btl']
    use_temp_ctd = use_elems['use_temp_ctd']
    use_psal_btl = use_elems['use_psal_btl']
    use_psal_ctd = use_elems['use_psal_ctd']
    use_salinity_btl = use_elems['use_salinity_btl']

    new_btl_measurements = []
    for obj in btl_measurements:
        new_obj = obj.copy()
        for key, val in obj.items():
            if key == 'temp' and not use_temp_btl:
                new_obj['temp'] = None
            if key == 'psal' and not use_psal_btl:
                new_obj['psal'] = None
            if key == 'salinity' and not use_salinity_btl:
                del new_obj[key]
            if key == 'salinity' and not use_psal_btl and use_salinity_btl:
                new_obj['psal'] = val

        has_sal = next((True for key in new_obj.keys()
                       if key == 'salinity'), False)

        if has_sal:
            del new_obj['salinity']

        new_btl_measurements.append(new_obj)

    is_empty = all([not elem for elem in new_btl_measurements])

    if is_empty:
        new_btl_measurements = []

    # Remove empty objects from measurements
    # TODO
    # Or leave inside with pres but empty temp and psal?
    #new_btl_measurements = [obj for obj in new_btl_measurements if obj]

    new_ctd_measurements = []
    for obj in ctd_measurements:

        new_obj = obj.copy()
        for key in obj.keys():
            if key == 'temp' and not use_temp_ctd:
                new_obj['temp'] = None
            if key == 'psal' and not use_psal_ctd:
                new_obj['psal'] = None

        # TODO
        # Remove or not empty objects that only have pressure?
        # if not has_elems:
        #     new_obj = {}

        new_ctd_measurements.append(new_obj)

    is_empty = all([not elem for elem in new_ctd_measurements])

    if is_empty:
        new_ctd_measurements = []

    # If using no temp_btl, psal_btl, or salinity, btl, remove from list

    # Get source information
    # See if using btl only, ctd only or btl and ctd
    if not use_temp_btl and not use_psal_btl and not use_salinity_btl:
        new_btl_measurements = []

    if not use_temp_ctd and not use_psal_ctd:
        new_ctd_measurements = []

    combined_measurements = [*new_ctd_measurements, *new_btl_measurements]

    # Now filter out if don't have a temp coming from btl or ctd
    # so don't include temp = None
    combined_measurements = [
        meas for meas in combined_measurements if meas['temp']]

    if not use_temp_btl and not use_temp_ctd:
        combined_measurements = []

    # TODO
    # mark what is  being used for testing
    # But remove for final product

    measurements_sources = {}

    # TODO
    # how do I know qc = 2?
    # get qc of temp used

    measurements_sources['qc'] = 2

    measurements_sources['use_temp_ctd'] = use_temp_ctd
    measurements_sources['use_psal_ctd'] = use_psal_ctd
    measurements_sources['use_temp_btl'] = use_temp_btl
    measurements_sources['use_psal_btl'] = use_psal_btl
    if use_elems['use_salinity_btl']:
        measurements_sources['use_salinty_btl'] = use_salinity_btl

    measurements_sources = convert_boolean(measurements_sources)

    measurements_source = flag

    return combined_measurements, measurements_source, measurements_sources


def find_measurements_hierarchy_btl_ctd(btl_measurements, ctd_measurements):

    has_psal_btl = False
    has_salinity_btl = False
    has_temp_btl = False

    has_psal_ctd = False
    has_temp_ctd = False

    try:
        has_temp_btl = any([
            True if pd.notnull(obj['temp']) else False for obj in btl_measurements])
    except KeyError:
        has_temp_btl = False

    try:
        has_temp_ctd = any([
            True if pd.notnull(obj['temp']) else False for obj in ctd_measurements])
    except KeyError:
        has_temp_ctd = False

    try:
        has_psal_btl = any([
            True if pd.notnull(obj['psal']) else False for obj in btl_measurements])
    except KeyError:
        has_psal_btl = False

    try:
        has_psal_ctd = any([
            True if pd.notnull(obj['psal']) else False for obj in ctd_measurements])
    except KeyError:
        has_psal_ctd = False

    try:
        has_salinity_btl = any([
            True if pd.notnull(obj['salinity']) else False for obj in btl_measurements])
    except KeyError:
        has_salinity_btl = False

    use_temp_ctd = False
    use_psal_ctd = False
    use_temp_btl = False
    use_psal_btl = False
    use_salinity_btl = False

    use_btl = False
    use_ctd = False

    if has_temp_ctd:
        use_temp_ctd = True
        use_ctd = True

    if has_psal_ctd:
        use_psal_ctd = True
        use_ctd = True

    if not has_temp_ctd and has_temp_btl:
        use_temp_btl = True
        use_btl = True

    if not has_psal_ctd and has_psal_btl:
        use_psal_btl = True
        use_btl = True

    if not has_psal_ctd and not has_psal_btl and has_salinity_btl:
        use_salinity_btl = True
        use_btl = True

    use_elems = {
        "use_temp_btl": use_temp_btl,
        "use_temp_ctd": use_temp_ctd,
        "use_psal_btl": use_psal_btl,
        "use_psal_ctd": use_psal_ctd,
        "use_salinity_btl": use_salinity_btl,
    }

    # Get measurements flag
    if not use_temp_btl and not use_temp_ctd:
        flag = None
    # elif use_temp_btl and not use_temp_ctd:
    #     flag = 'BTL'
    # elif not use_temp_btl and use_temp_ctd:
    #     flag = 'CTD'
    elif use_btl and use_ctd:
        flag = 'BTL_CTD'
    elif use_btl:
        flag = 'BTL'
    elif use_ctd:
        flag = 'CTD'
    else:
        flag = None

    return use_elems, flag


def combine_btl_ctd_measurements(btl_measurements, ctd_measurements):

    use_elems, flag = find_measurements_hierarchy_btl_ctd(
        btl_measurements, ctd_measurements)

    combined_btl_ctd_measurements, measurements_source, measurements_source_qc = filter_btl_ctd_combined_measurements(
        btl_measurements, ctd_measurements, use_elems, flag)

    return combined_btl_ctd_measurements, measurements_source, measurements_source_qc


def combine_mapping_per_profile_btl_ctd(combined_btl_ctd_dict, btl_dict, ctd_dict):

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

        # Check if have both because in combined profile,
        # can be case of only having one type for that station_cast

        # Remove _btl variables that are the same as CTD
        btl_exclude = set(['expocode', 'cruise_url', 'DATA_CENTRE'])

        argovis_meta_names_btl_subset = list(
            set(btl_dict['argovisMetaNames']).difference(btl_exclude))

        # Put suffix of '_btl' in  bottle meta except
        # for unique meta names iin btl_exclude list
        goship_argovis_meta_mapping_btl = btl_dict['goshipArgovisMetaMapping']
        goship_argovis_meta_mapping_btl = {
            f"{key}_btl": val for key, val in goship_argovis_meta_mapping_btl.items() if val in argovis_meta_names_btl_subset}

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

        # For Argovis meta names, didn't add suffix to them
        # But now that are combining, put suffix on the
        # btl dict names that  are filtered to remove
        # btl_exclude set
        argovis_meta_names_btl = btl_dict['argovisMetaNames']
        argovis_meta_names_btl = [
            f"{name}_btl" for name in argovis_meta_names_btl if name in argovis_meta_names_btl_subset]

        argovis_meta_names = [*ctd_dict['argovisMetaNames'],
                              *argovis_meta_names_btl]

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

    combined_btl_ctd_dict = {}
    combined_btl_ctd_dict['data_type'] = 'btl_ctd'
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

    if ctd_dict:
        ctd_meta = ctd_dict['meta']
        ctd_bgc_meas = ctd_dict['bgcMeas']
        ctd_measurements = ctd_dict['measurements']

    if btl_dict and ctd_dict:

        # Put suffix of '_btl' in  bottle meta
        # Remove _btl variables that are the same as CTD
        btl_exclude = set(['expocode', 'cruise_url', 'DATA_CENTRE'])

        for item in btl_exclude:
            if item in btl_meta:
                btl_meta.pop(item)

        btl_meta = rename_btl_by_key_meta(btl_meta)

    meta = {**ctd_meta, **btl_meta}
    bgc_meas = [*ctd_bgc_meas, *btl_bgc_meas]

    measurements, measurements_source, measurements_source_qc = combine_btl_ctd_measurements(
        btl_measurements, ctd_measurements)

    combined_btl_ctd_dict['meta'] = meta
    combined_btl_ctd_dict['bgcMeas'] = bgc_meas
    combined_btl_ctd_dict['measurements'] = measurements
    combined_btl_ctd_dict['measurementsSource'] = measurements_source
    combined_btl_ctd_dict['measurementsSourceQC'] = measurements_source_qc

    combined_btl_ctd_dict = combine_mapping_per_profile_btl_ctd(combined_btl_ctd_dict,
                                                                btl_dict, ctd_dict)

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


# def create_combined_type_all_cruise_objs(cruises_profiles_objs):

#     is_btl = any([True if profiles_obj['data_type'] ==
#                   'btl' else False for profiles_obj in cruises_profiles_objs])

#     is_ctd = any([True if profiles_obj['data_type'] ==
#                   'ctd' else False for profiles_obj in cruises_profiles_objs])

#     if is_btl and is_ctd:

#         logging.info("Combining btl and ctd")

#         # filter measurements by hierarchy
#         #  when combine btl and ctd profiles.
#         # didn't filter btl or ctd first in case need
#         # a variable from both

#         all_profiles_btl_ctd_objs = []

#         for curise_profiles_obj in cruises_profiles_objs:
#             profiles_btl_ctd_objs = create_profiles_combined_type(
#                 curise_profiles_obj)

#             all_profiles_btl_ctd_objs.append(profiles_btl_ctd_objs)

#         return all_profiles_btl_ctd_objs

#     else:
#         return []


def create_profiles_combined_type(profiles_objs):

    for profiles_obj in profiles_objs:

        # if profiles_obj['data_type'] == 'btl':
        #     btl_profiles = profiles_obj['profiles']
        # elif profiles_obj['data_type'] == 'ctd':
        #     ctd_profiles = profiles_obj['profiles']

        if profiles_obj['data_type'] == 'btl':
            btl_profiles = profiles_obj['data_type_profiles_list']
        elif profiles_obj['data_type'] == 'ctd':
            ctd_profiles = profiles_obj['data_type_profiles_list']

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
