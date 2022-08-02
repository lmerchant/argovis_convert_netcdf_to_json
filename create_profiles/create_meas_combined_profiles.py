import logging
import pandas as pd

from variable_naming.meta_param_mapping import get_measurements_mapping


def convert_boolean(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    if isinstance(obj, (list, tuple)):
        return [convert_boolean(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_boolean(key): convert_boolean(value) for key, value in obj.items()}
    return obj


def get_combined_measurements(btl_meas, ctd_meas, hierarchy_meas_cols, hierarchy_source_flag, hierarchy_meas_sources):

    # Convert btl_meas and ctd_meas into df and then select columns I want
    # using hierarchy_meas_cols
    # Then turn df into list of dicts

    df_meas_btl = pd.DataFrame.from_dict(btl_meas)
    df_meas_ctd = pd.DataFrame.from_dict(ctd_meas)

    btl_columns = hierarchy_meas_cols['btl']
    ctd_columns = hierarchy_meas_cols['ctd']

    if not btl_columns:
        df_meas_btl = pd.DataFrame()
    else:
        df_meas_btl = df_meas_btl[btl_columns]

    if not ctd_columns:
        df_meas_ctd = pd.DataFrame()
    else:
        df_meas_ctd = df_meas_ctd[ctd_columns]

    # Turn back into a list of dict and combine each dict for combined_meas
    btl_meas = df_meas_btl.to_dict('records')
    ctd_meas = df_meas_ctd.to_dict('records')

    # TODO
    # Exclude data point if temperature is NaN but only if not
    # in a combined source
    # look at hierarchy meas cols. Get source easier

    # print(f"hierarchy source flag {hierarchy_source_flag}")

    # print(f"hierarchy_meas_sources {hierarchy_meas_sources}")

    # num_pts_before_btl = len(btl_meas)
    # num_pts_before_ctd = len(ctd_meas)

    # btl_meas_before = btl_meas
    # ctd_meas_before = ctd_meas

    if hierarchy_source_flag != 'BTL_CTD':
        if btl_meas:
            btl_meas = [obj for obj in btl_meas if pd.notna(
                obj['temperature'])]

        if ctd_meas:
            ctd_meas = [obj for obj in ctd_meas if pd.notna(
                obj['temperature'])]

        # num_pts_after_btl = len(btl_meas)
        # num_pts_after_ctd = len(ctd_meas)

        # if num_pts_before_btl != num_pts_after_btl:
        #     logging.info('btl meas before')
        #     logging.info(btl_meas_before)
        #     logging.info(f'\n\nbtl meas after')
        #     logging.info(btl_meas)

        # if num_pts_before_ctd != num_pts_after_ctd:
        #     logging.info('ctd meas before')
        #     logging.info(ctd_meas_before)
        #     logging.info(f'\n\nctd meas after')
        #     logging.info(ctd_meas)

    btl_meas.extend(ctd_meas)
    combined_meas = []
    for obj in btl_meas:
        if obj not in combined_meas:
            combined_meas.append(obj)

    return combined_meas


def find_btl_ctd_combinations(hierarchy_btl, hierarchy_ctd):

    # TODO
    # keep track of vars not used and only list if they exist

    # Need to take into account that a btl or ctd sourceQC key can be None
    # That means temperature was all bad
    # Maybe change this to always have a source key and indicate temp bad

    hierarchy_meas_cols = {}
    source_flag = {}
    meas_sources = {}

    has_psal_btl = hierarchy_btl['has_psal']
    has_salinity = hierarchy_btl['has_salinity']
    has_psal_ctd = hierarchy_ctd['has_psal']

    # *************
    # combination 1
    # *************

    # btl temp bad and doesn't matter if psal or salinity good
    # ctd temp good, ctd psal bad
    # then use objs {pres_ctd: #, temp_ctd: #}

    btl_condition = not hierarchy_btl['good_temp']

    ctd_condition = hierarchy_ctd['good_temp'] and not hierarchy_ctd['good_psal']

    if btl_condition and ctd_condition:
        hierarchy_meas_cols = {
            'btl': [],
            'ctd': ['pressure', 'temperature']
        }

        source_flag = 'CTD'

        meas_sources = {
            'pressure_ctd': True,
            'temperature_ctd': True
        }

        if has_psal_ctd:
            meas_sources['salinity_ctd'] = False

        return hierarchy_meas_cols, source_flag, meas_sources

    # *************
    # combination 2
    # *************

    # btl temp good and psal bad and salinity good
    # ctd temp good and psal bad

    # then use objs {pres_btl: #, salinity_btl: #} and {pres_ctd: #, temp_ctd: #}

    btl_condition = hierarchy_btl['good_temp'] and not hierarchy_btl[
        'good_psal'] and has_salinity and hierarchy_btl['good_salinity']

    ctd_condition = hierarchy_ctd['good_temp'] and not hierarchy_ctd['good_psal']

    if btl_condition and ctd_condition:
        hierarchy_meas_cols = {
            'btl': ['pressure', 'bottle_salinity'],
            'ctd': ['pressure', 'temperature']
        }

        source_flag = 'BTL_CTD'

        meas_sources = {
            'pressure_btl': True,
            'temperature_btl': False,
            'bottle_salinity_btl': True,
            'pressure_ctd': True,
            'temperature_ctd': True
        }

        if has_psal_btl:
            meas_sources['salinity_btl'] = False

        if has_psal_ctd:
            meas_sources['salinity_ctd'] = False

        return hierarchy_meas_cols, source_flag, meas_sources

    # *************
    # combination 3
    # *************

    # btl temp good, btl psal good, btl salinity good
    # ctd temp good, ctd psal bad
    # then use objs {pres_btl: #, psal_btl: #} and {pres_ctd: #, temp_ctd: #}

    # don't need to check btl salinity since psal takes precedence
    btl_condition = hierarchy_btl['good_temp'] and hierarchy_btl['good_psal']

    ctd_condition = hierarchy_ctd['good_temp'] and not hierarchy_ctd['good_psal']

    if btl_condition and ctd_condition:
        hierarchy_meas_cols = {
            'btl': ['pressure', 'salinity'],
            'ctd': ['pressure', 'temperature']
        }

        source_flag = 'BTL_CTD'

        meas_sources = {
            'pressure_btl': True,
            'temperature_btl': False,
            'salinity_btl': True,
            'pressure_ctd': True,
            'temperature_ctd': True
        }

        if has_psal_ctd:
            meas_sources['salinity_ctd'] = False

        return hierarchy_meas_cols, source_flag, meas_sources

    # *************
    # combination 4
    # *************

    # btl temp good or bad, and doesn't matter what psal or salinity are like if
    #   ctd temp good and ctd psal is good
    # ctd temp good and ctd psal good
    # then use objs {pres_ctd: #, temp_ctd: #, psal_ctd: #}

    #btl_condition = hierarchy_btl['good_temp']

    ctd_condition = hierarchy_ctd['good_temp'] and hierarchy_ctd['good_psal']

    if ctd_condition:
        hierarchy_meas_cols = {
            'btl': [],
            'ctd': ['pressure', 'temperature', 'salinity']
        }

        meas_sources = {
            'pressure_ctd': True,
            'temperature_ctd': True,
            'salinity_ctd': True
        }

        source_flag = 'CTD'

        return hierarchy_meas_cols, source_flag, meas_sources

    # *************
    # combination 5
    # *************

    # btl temp good and btl psal bad and btl salinity bad
    # ctd temp bad
    # then use objs {pres_btl:#, temp_btl:#}

    btl_condition = hierarchy_btl['good_temp'] and not hierarchy_btl[
        'good_psal'] and not hierarchy_btl['good_salinity']

    ctd_condition = not hierarchy_ctd['good_temp']

    if btl_condition and ctd_condition:
        hierarchy_meas_cols = {
            'btl': ['pressure', 'temperature'],
            'ctd': []
        }

        source_flag = 'BTL'

        meas_sources = {
            'pressure_btl': True,
            'temperature_btl': True
        }

        if has_psal_btl:
            meas_sources['salinity_btl'] = False

        if has_salinity:
            meas_sources['bottle_salinity_btl'] = False

        return hierarchy_meas_cols, source_flag, meas_sources

    # *************
    # combination 6
    # *************

    # btl temp good and btl psal good, salinity doesn't matter in this case
    # ctd temp bad
    # then use objs {pres_btl:#, temp_btl:#, psal_btl:#}

    btl_condition = hierarchy_btl['good_temp'] and hierarchy_btl['good_psal']

    ctd_condition = not hierarchy_ctd['good_temp']

    if btl_condition and ctd_condition:
        hierarchy_meas_cols = {
            'btl': ['pressure', 'temperature', 'salinity'],
            'ctd': []
        }

        source_flag = 'BTL'

        meas_sources = {
            'pressure_btl': True,
            'temperature_btl': True,
            'salinity_btl': True
        }

        if has_salinity:
            meas_sources['bottle_salinity_btl'] = True

        return hierarchy_meas_cols, source_flag, meas_sources

    # *************
    # combination 7
    # *************

    # btl temp good and btl psal bad, salinity good
    # ctd temp bad
    # then use objs {pres_btl:#, temp_btl:#, salinity:#}

    btl_condition = hierarchy_btl['good_temp'] and not hierarchy_btl[
        'good_psal'] and hierarchy_btl['good_salinity']

    ctd_condition = not hierarchy_ctd['good_temp']

    if btl_condition and ctd_condition:
        hierarchy_meas_cols = {
            'btl': ['pressure', 'temperature', 'bottle_salinity'],
            'ctd': []
        }

        source_flag = 'BTL'

        meas_sources = {
            'pressure_btl': True,
            'temperature_btl': True,
            'bottle_salinity_btl': True
        }

        if has_psal_btl:
            meas_sources['salinity_btl'] = False

        return hierarchy_meas_cols, source_flag, meas_sources

    # *************
    # combination 8
    # *************

    # btl temp good, and btl psal bad and btl salinity bad
    # ctd temp good and ctd psal bad
    # then use objs {pres_ctd: #, temp_ctd: #}

    btl_condition = hierarchy_btl['good_temp'] and not hierarchy_btl[
        'good_psal'] and not hierarchy_btl['good_salinity']

    ctd_condition = hierarchy_ctd['good_temp'] and not hierarchy_ctd['good_psal']

    if btl_condition and ctd_condition:
        hierarchy_meas_cols = {
            'btl': [],
            'ctd': ['pressure', 'temperature']
        }

        source_flag = 'CTD'

        meas_sources = {
            'pressure_ctd': True,
            'temperature_ctd': True
        }

        if has_psal_ctd:
            meas_sources['salinity_ctd'] = False

        return hierarchy_meas_cols, source_flag, meas_sources

    # *************
    # combination 9
    # *************

    # btl temp bad
    # ctd temp bad
    # then use empty objs

    btl_condition = not hierarchy_btl['good_temp']

    ctd_condition = not hierarchy_ctd['good_temp']

    if btl_condition and ctd_condition:
        hierarchy_meas_cols = {
            'btl': [],
            'ctd': []
        }

        source_flag = 'None'

        meas_sources = {
            'pressure_btl': False,
            'temperature_btl': False,
            'pressure_ctd': False,
            'temperature_ctd': False
        }

        if has_psal_btl:
            meas_sources['salinity_btl'] = False

        if has_salinity:
            meas_sources['bottle_salinity_btl'] = False

        if has_psal_ctd:
            meas_sources['salinity_ctd'] = False

        return hierarchy_meas_cols, source_flag, meas_sources

    # -----------------

    # If reach here, there is a combination not accounted for, so exit

    logging.info(
        "Found hierarchy meas combination for combined btl & ctd not accounted for")

    # print('btl hierarchy')
    # print(hierarchy_btl)

    # print('ctd hierarchy')
    # print(hierarchy_ctd)

    # exit(1)


def get_hierarchy_single_source(source_dict):

    # TODO
    # redo all this logic. seems repetitive

    # Maybe logic ok if looking at single source results
    # And when renamed single type, also renamed the
    # key measurementsSourceQC, so that's why
    # looking at data_type suffix

    # TODO
    # looks like I already removed suffix source_type
    # But I didn't include use_temp

    # Gives true or false values

    # Get mapped names since btl and ctd dicts had keys renamed
    measurements_mapping = get_measurements_mapping()

    meas_sources_key = measurements_mapping['measurements_sources']

    source = source_dict[meas_sources_key]

    hierarchy = {}

    # check ctd_temperature
    has_key = 'temperature' in source

    if has_key:
        hierarchy['good_temp'] = source['temperature']
    else:
        hierarchy['good_temp'] = False

    # Check ctd_salinity
    # measurements renamed this psal
    has_key = "salinity" in source

    if has_key:
        hierarchy['good_psal'] = source['salinity']
        hierarchy['has_psal'] = True
    else:
        hierarchy['good_psal'] = False
        hierarchy['has_psal'] = False

    # check for bottle salinity
    has_key = "bottle_salinity" in source

    if has_key:
        hierarchy['good_salinity'] = source['bottle_salinity']
        hierarchy['has_salinity'] = True
    else:
        hierarchy['good_salinity'] = False
        hierarchy['has_salinity'] = False

    return hierarchy


def find_meas_hierarchy_btl_ctd(btl_dict, ctd_dict):

    # hierarchy for combining btl and ctd

    if btl_dict:
        hierarchy_btl = get_hierarchy_single_source(btl_dict)

    else:
        hierarchy_btl = {}

    if ctd_dict:
        hierarchy_ctd = get_hierarchy_single_source(ctd_dict)
    else:
        hierarchy_ctd = {}

    return hierarchy_btl, hierarchy_ctd


def combine_btl_ctd_measurements(btl_dict, ctd_dict):

    # Get mapped names since btl and ctd dicts had keys renamed
    measurements_mapping = get_measurements_mapping()

    measurements_key = measurements_mapping['measurements']
    source_flag_key = measurements_mapping['measurements_source']
    meas_sources_key = measurements_mapping['measurements_sources']

    btl_meas = {}
    ctd_meas = {}

    if btl_dict:
        btl_meas = btl_dict[measurements_key]
        hierarchy_source_flag = btl_dict[source_flag_key]
        hierarchy_meas_sources = btl_dict[meas_sources_key]

    if ctd_dict:
        ctd_meas = ctd_dict[measurements_key]
        hierarchy_source_flag = ctd_dict[source_flag_key]
        hierarchy_meas_sources = ctd_dict[meas_sources_key]

    hierarchy_btl, hierarchy_ctd = find_meas_hierarchy_btl_ctd(
        btl_dict, ctd_dict)

    if hierarchy_btl and hierarchy_ctd:
        hierarchy_meas_cols, hierarchy_source_flag, hierarchy_meas_sources = find_btl_ctd_combinations(
            hierarchy_btl, hierarchy_ctd)

        combined_measurements = get_combined_measurements(btl_meas, ctd_meas,
                                                          hierarchy_meas_cols, hierarchy_source_flag, hierarchy_meas_sources)

    # -----------

    if hierarchy_btl and not hierarchy_ctd:
        combined_measurements = btl_meas

    if not hierarchy_btl and hierarchy_ctd:
        combined_measurements = ctd_meas

    # For JSON conversion later
    hierarchy_meas_sources = convert_boolean(hierarchy_meas_sources)

    combined_mappings = {}
    combined_mappings[measurements_key] = combined_measurements
    combined_mappings[source_flag_key] = hierarchy_source_flag
    combined_mappings[meas_sources_key] = hierarchy_meas_sources

    return combined_mappings
