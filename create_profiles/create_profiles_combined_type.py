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


def filter_btl_ctd_measurements(btl_meas, ctd_meas, use_elems, meas_sources):

    # use_elems tells what keys exist for meas
    use_temp_btl = use_elems['use_temp_btl']
    use_temp_ctd = use_elems['use_temp_ctd']
    use_psal_btl = use_elems['use_psal_btl']
    use_psal_ctd = use_elems['use_psal_ctd']

    # meas_sources tells if using a meas key value
    if use_temp_ctd:
        using_temp_ctd = meas_sources['use_temp_ctd']
    else:
        using_temp_ctd = False

    if use_temp_btl:
        using_temp_btl = meas_sources['use_temp_btl']
    else:
        using_temp_btl = False

    if use_psal_ctd:
        using_psal_ctd = meas_sources['use_psal_ctd']
    else:
        using_psal_ctd = False

    if use_psal_btl:
        using_psal_btl = meas_sources['use_psal_btl']
    else:
        using_psal_btl = False

    # If both vals are null except pre, don't use obj
    filtered_btl_meas = []
    for obj in btl_meas:
        new_obj = obj.copy()

        if using_temp_btl and not using_psal_btl:
            temp_btl = obj['temp']
            if pd.notnull(temp_btl):
                filtered_btl_meas.append(new_obj)

        if using_temp_btl and using_psal_btl:
            temp_btl = obj['temp']
            psal_btl = obj['psal']
            if not (pd.isnull(temp_btl) and pd.isnull(psal_btl)):
                filtered_btl_meas.append(new_obj)

        if not using_temp_btl and using_psal_btl:
            new_obj['temp'] = None
            if pd.notnull(obj['psal']):
                filtered_btl_meas.append(new_obj)

    filtered_ctd_meas = []
    for obj in ctd_meas:

        new_obj = obj.copy()

        if using_temp_ctd and not using_psal_ctd:
            temp_ctd = obj['temp']
            if pd.notnull(temp_ctd):
                filtered_ctd_meas.append(new_obj)

        if using_temp_ctd and using_psal_ctd:
            temp_ctd = obj['temp']
            psal_ctd = obj['psal']
            if not (pd.isnull(temp_ctd) and pd.isnull(psal_ctd)):
                filtered_ctd_meas.append(new_obj)

        if not using_temp_ctd and using_psal_ctd:
            new_obj['temp'] = None
            if pd.notnull(obj['psal']):
                filtered_ctd_meas.append(new_obj)

    return filtered_btl_meas, filtered_ctd_meas


def find_meas_hierarchy_btl_ctd(btl_meas, ctd_meas, btl_qc, ctd_qc):

    # For btl meas, if didn't have psal and had salinity
    # salinity  was renamed  psal and the flag use_salinity_btl = True.
    # If there was both psal and salinity, the salinity was dropped

    use_temp_ctd = False
    use_psal_ctd = False
    use_temp_btl = False
    use_psal_btl = False
    use_salinity_btl = False

    has_psal_btl = False
    has_temp_btl = False
    has_psal_ctd = False
    has_temp_ctd = False

    if 'use_temp_btl' in btl_qc.keys():
        use_temp_btl = True

    if 'use_temp_ctd' in ctd_qc.keys():
        use_temp_ctd = True

    if 'use_psal_btl' in btl_qc.keys():
        use_psal_btl = True

    if 'use_psal_ctd' in ctd_qc.keys():
        use_psal_ctd = True

    # Already changed name to psal if using salinity btl
    # and so if it has a qc key, means salinity_btl existed
    if 'use_salinity_btl' in btl_qc.keys():
        salinity_exists = True
    else:
        salinity_exists = False

    if use_temp_btl:
        has_temp_btl = any([
            True if pd.notnull(obj['temp']) else False for obj in btl_meas])

    if use_temp_ctd:
        has_temp_ctd = any([
            True if pd.notnull(obj['temp']) else False for obj in ctd_meas])

    if use_psal_btl:
        has_psal_btl = any([
            True if pd.notnull(obj['psal']) else False for obj in btl_meas])

    if use_psal_ctd:
        has_psal_ctd = any([
            True if pd.notnull(obj['psal']) else False for obj in ctd_meas])

    use_btl = False
    use_ctd = False

    # tell if using meas key value (whether will be null or not)
    using_temp_btl = False
    using_temp_ctd = False
    using_psal_btl = False
    using_psal_ctd = False
    using_salinity_btl = False

    if use_temp_ctd and has_temp_ctd:
        using_temp_ctd = True
        using_temp_btl = False
        use_ctd = True

    if use_temp_ctd and not has_temp_ctd and use_temp_btl and has_temp_btl:
        using_temp_ctd = False
        using_temp_btl = True
        use_ctd = True

    if use_psal_ctd and has_psal_ctd:
        using_psal_ctd = True
        using_psal_btl = False
        use_ctd = True

    if use_psal_ctd and not has_psal_ctd and use_psal_btl and has_psal_btl and not salinity_exists:
        using_psal_ctd = False
        using_psal_btl = True
        use_ctd = True

    if not use_psal_ctd and use_psal_btl and has_psal_btl and not salinity_exists:
        using_psal_btl = True
        use_ctd = True

    # salinity btl already renamed to psal_btl and know
    # was using salinity_btl for this if has_salinity_btl
    if not use_psal_ctd and not use_psal_btl and has_psal_btl and salinity_exists:
        using_psal_btl = False
        using_salinity_btl = True
        use_btl = True

    # Tells what keys exist for meas
    use_elems = {
        "use_temp_btl": use_temp_btl,
        "use_temp_ctd": use_temp_ctd,
        "use_psal_btl": use_psal_btl,
        "use_psal_ctd": use_psal_ctd
    }

    # Tells if using a meas key value
    meas_sources = {}
    if use_temp_ctd:
        meas_sources['use_temp_ctd'] = using_temp_ctd

    if use_temp_btl:
        meas_sources['use_temp_btl'] = using_temp_btl

    if use_psal_ctd:
        meas_sources['use_psal_ctd'] = using_psal_ctd

    if use_psal_btl:
        meas_sources['use_psal_btl'] = using_psal_btl

    if use_salinity_btl:
        meas_sources['use_salinty_btl'] = using_salinity_btl

    meas_names = []
    meas_names.append('pres')

    if use_temp_ctd or use_temp_btl:
        meas_names.append('temp')

    if use_psal_ctd or use_psal_btl:
        meas_names.append('psal')

    if use_salinity_btl:
        meas_names.append('psal')

    # Only want unique names and not psal twice
    meas_names = list(set(meas_names))

    # For JSON conversion later
    meas_sources = convert_boolean(meas_sources)

    # Get measurements source flag
    if not use_btl and not use_ctd:
        meas_source = None
    elif use_btl and use_ctd:
        # case where using bottle salinity instead of psal
        meas_source = 'BTL_CTD'
    elif use_btl and not use_ctd:
        meas_source = 'BTL'
    elif use_ctd and not use_btl:
        meas_source = 'CTD'
    else:
        meas_source = None

    return meas_source, meas_sources, meas_names,  use_elems


def combine_btl_ctd_measurements(btl_meas, ctd_meas, btl_qc, ctd_qc):

    meas_source, meas_sources, meas_names, use_elems = find_meas_hierarchy_btl_ctd(
        btl_meas, ctd_meas, btl_qc, ctd_qc)

    # filter measurement objs by hierarchy and if exists
    filtered_btl_meas, filtered_ctd_meas = filter_btl_ctd_measurements(
        btl_meas, ctd_meas, use_elems, meas_sources)

    # Combine filtered measurements
    combined_measurements = [
        *filtered_ctd_meas, *filtered_btl_meas]

    # Now filter out if don't have a temp coming from btl or ctd
    # so don't include temp = None
    # And filter out if there is not 'temp' var
    # combined_measurements = [
    #     meas for meas in combined_measurements if 'temp' in meas.keys() and meas['temp']]

    return combined_measurements, meas_source, meas_sources, meas_names


def add_combined_mapping(combined_btl_ctd_dict, btl_dict, ctd_dict):

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
        combined_btl_ctd_dict['bgcMeasKeys'] = btl_dict['bgcMeasKeys']

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
        combined_btl_ctd_dict['bgcMeasKeys'] = ctd_dict['bgcMeasKeys']

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

        combined_btl_ctd_dict['bgcMeasKeys'] = [
            *btl_dict['bgcMeasKeys'], *ctd_dict['bgcMeasKeys']]

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

    # ***********************************
    # Check if no CTD vars for the cruise
    # If none, remove profile and
    # set flag to removed
    # ***********************************

    btl_meta = {}
    ctd_meta = {}

    btl_meas = []
    ctd_meas = []

    btl_bgc_meas = []
    ctd_bgc_meas = []

    btl_qc = {}
    ctd_qc = {}

    if btl_dict:
        btl_meta = btl_dict['meta']
        btl_bgc_meas = btl_dict['bgcMeas']
        btl_meas = btl_dict['measurements']
        btl_qc = btl_dict['measurementsSourceQC']
        btl_qc_val = btl_qc['qc']

    if ctd_dict:
        ctd_meta = ctd_dict['meta']
        ctd_bgc_meas = ctd_dict['bgcMeas']
        ctd_meas = ctd_dict['measurements']
        ctd_qc = ctd_dict['measurementsSourceQC']
        ctd_qc_val = ctd_qc['qc']

    if btl_dict and ctd_dict:

        if btl_qc_val == 0 and ctd_qc_val == 0:
            # Would use btl values?
            # would ctd_qc_val = 2 ever?
            qc_val = 0
        elif btl_qc_val == 2 and ctd_qc_val == 2:
            # Would use ctd values
            qc_val = 2
        elif btl_qc_val == 0 and ctd_qc_val == 2:
            # Would use ctd values
            qc_val = 2
        elif pd.isnull(ctd_qc_val) and pd.notnull(btl_qc_val):
            logging.info(f"CTD qc = None for station cast {station_cast}")
            qc_val = btl_qc_val
        elif pd.isnull(btl_qc_val) and pd.notnull(ctd_qc_val):
            logging.info(f"BTL qc = None for station cast {station_cast}")
            qc_val = ctd_qc_val
        else:
            qc_val = None
            logging.info("QC MEAS IS NONE for btl and ctd")
            logging.info(f"for station cast {station_cast}")

            # logging.info(f"btl_qc  {btl_qc_val}")
            # logging.info(f"ctd_qc  {ctd_qc_val}")

            # logging.info("*** Btl meas")
            # logging.info(btl_meas)

            # logging.info(f"\n\n\n*** Ctd Meas")
            # logging.info(ctd_meas)

            # Means btl_Meas and ctd_meas both empty,
            # So do I keep the profile?

            # TODO
            # What to do in this case?

        # Put suffix of '_btl' in  bottle meta
        # Remove _btl variables that are the same as CTD
        btl_exclude = set(['expocode', 'cruise_url', 'DATA_CENTRE'])

        for item in btl_exclude:
            if item in btl_meta:
                btl_meta.pop(item)

        btl_meta = rename_btl_by_key_meta(btl_meta)

    meta = {**ctd_meta, **btl_meta}
    bgc_meas = [*ctd_bgc_meas, *btl_bgc_meas]

    measurements, meas_source, meas_sources, meas_names = combine_btl_ctd_measurements(
        btl_meas, ctd_meas, btl_qc, ctd_qc)

    if btl_dict and ctd_dict:
        meas_sources['qc'] = qc_val
    elif btl_dict and not ctd_dict:
        meas_sources = btl_qc
    elif not btl_dict and ctd_dict:
        meas_sources = ctd_qc

    combined_btl_ctd_dict['meta'] = meta
    combined_btl_ctd_dict['bgcMeas'] = bgc_meas
    combined_btl_ctd_dict['measurements'] = measurements
    combined_btl_ctd_dict['measurementsSource'] = meas_source
    combined_btl_ctd_dict['measurementsSourceQC'] = meas_sources
    combined_btl_ctd_dict['station_parameters'] = meas_names

    combined_btl_ctd_dict = add_combined_mapping(combined_btl_ctd_dict,
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


def balance_profiles_by_station_cast(profiles_objs):

    # When combining each station cast profiles,
    # may have case where there is only a btl profile
    # since no ctd profile at that station cast

    for profiles_obj in profiles_objs:

        if profiles_obj['data_type'] == 'btl':
            btl_profiles = profiles_obj['data_type_profiles_list']
        elif profiles_obj['data_type'] == 'ctd':
            ctd_profiles = profiles_obj['data_type_profiles_list']

    # Get profile dicts so have the same number of profiles
    # one may be blank while the other exists at a cast
    btl_profiles, ctd_profiles = get_same_station_cast_profile_btl_ctd(
        btl_profiles, ctd_profiles)

    logging.info(f"Number of btl profiles {len(btl_profiles)}")
    logging.info(f"Number of ctd profiles {len(ctd_profiles)}")

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

    return all_profiles


def create_profiles_combined_type(profiles_objs):

    # Have a btl and ctd profile for each station cast
    # If don't have data type at a station cast,
    # set it to be an empty object
    all_profiles = balance_profiles_by_station_cast(profiles_objs)

    profiles_list_btl_ctd = []

    for profile in all_profiles:
        profile_btl = profile['btl']
        profile_ctd = profile['ctd']

        combined_profile_btl_ctd = combine_output_per_profile_btl_ctd(
            profile_btl, profile_ctd)

        profiles_list_btl_ctd.append(combined_profile_btl_ctd)

    logging.info('Processed btl and ctd combined profiles')

    return profiles_list_btl_ctd
