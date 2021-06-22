
# Filter  measurements object by element hierarchy
import numpy as np
import pandas as pd


def filter_measurements(measurements, use_elems):

    # If a ctd file, filter on whether have salinity,
    # if not, set it to  nan

    # If a bottle file, keep temp_btl and then choose psal_btl
    # first but if doesn't exist and salinity_btl does, use that

    use_temp = use_elems['use_temp']
    use_psal = use_elems['use_psal']
    use_salinity = use_elems['use_salinity']

    new_measurements = []
    for obj in measurements:
        new_obj = obj.copy()
        for key, val in obj.items():
            if key == 'temp' and not use_temp:
                new_obj['temp'] = np.nan
            if key == 'psal' and not use_psal:
                new_obj['psal'] = np.nan
            if key == 'salinity' and not use_salinity:
                del new_obj[key]
            if key == 'salinity' and not use_psal:
                new_obj['psal'] = val

        new_measurements.append(new_obj)

        keys = new_obj.keys()
        if 'salinity' in keys:
            del new_obj['salinity']

    if not use_temp:
        new_measurements = []

    return new_measurements


def filter_bot_ctd_combined(bot_measurements, ctd_measurements, use_elems):

    use_temp_btl = use_elems['use_temp_btl']
    use_temp_ctd = use_elems['use_temp_ctd']
    use_psal_btl = use_elems['use_psal_btl']
    use_psal_ctd = use_elems['use_psal_ctd']
    use_salinity_btl = use_elems['use_salinity_btl']

    new_bot_measurements = []
    for obj in bot_measurements:
        new_obj = obj.copy()
        for key, val in obj.items():
            if key == 'temp' and not use_temp_btl:
                new_obj['temp'] = np.nan
            if key == 'psal' and not use_psal_btl:
                new_obj['psal'] = np.nan
            if key == 'salinity' and not use_salinity_btl:
                del new_obj[key]
            if key == 'salinity' and not use_psal_btl and use_salinity_btl:
                new_obj['psal'] = val

        has_sal = [True for key in new_obj.keys() if key == 'salinity']
        if has_sal:
            del new_obj['salinity']

        has_elems = any([True if (pd.notnull(val) and key != 'pres')
                         else False for key, val in new_obj.items()])

        if not has_elems:
            new_obj = {}

        new_bot_measurements.append(new_obj)

    is_empty = all([not elem for elem in new_bot_measurements])

    if is_empty:
        new_bot_measurements = []

    # Remove empty objects from measurements
    new_bot_measurements = [obj for obj in new_bot_measurements if obj]

    new_ctd_measurements = []
    for obj in ctd_measurements:

        new_obj = obj.copy()
        for key in obj.keys():
            if key == 'temp' and not use_temp_ctd:
                new_obj['temp'] = np.nan
            if key == 'psal' and not use_psal_ctd:
                new_obj['psal'] = np.nan

        has_elems = any([True if (pd.notnull(val) and key != 'pres')
                         else False for key, val in new_obj.items()])

        if not has_elems:
            new_obj = {}

        new_ctd_measurements.append(new_obj)

    is_empty = all([not elem for elem in new_ctd_measurements])

    if is_empty:
        new_ctd_measurements = []

    new_ctd_measurements = [obj for obj in new_ctd_measurements if obj]

    combined_measurements = [*new_ctd_measurements, *new_bot_measurements]

    if not use_temp_btl and not use_temp_ctd:
        combined_measurements = []

    return combined_measurements


def find_measurements_hierarchy(measurements):

    has_psal = False
    has_salinity = False
    has_temp = False

    try:

        has_temp = any([True if pd.notnull(obj['temp'])
                       else False for obj in measurements])

    except:
        has_temp = False

    try:
        has_psal = any([True if pd.notnull(obj['psal'])
                       else False for obj in measurements])
    except:
        has_psal = False

    try:
        has_salinity = any([True if pd.notnull(obj['salinity'])
                            else False for obj in measurements])
    except:
        has_salinity = False

    use_temp = False
    use_psal = False
    use_salinity = False

    if has_temp:
        use_temp = True

    if has_psal:
        use_psal = True
    if not has_psal and has_salinity:
        use_salinity = True

    use_elems = {
        'use_temp': use_temp,
        'use_psal': use_psal,
        'use_salinity': use_salinity
    }

    return use_elems


def find_measurements_hierarchy_bot_ctd(bot_measurements, ctd_measurements):

    has_psal_btl = False
    has_salinity_btl = False
    has_temp_btl = False

    has_psal_ctd = False
    has_temp_ctd = False

    try:
        has_temp_btl = any([
            True if pd.notnull(obj['temp']) else False for obj in bot_measurements])
    except KeyError:
        has_temp_btl = False

    try:
        has_temp_ctd = any([
            True if pd.notnull(obj['temp']) else False for obj in ctd_measurements])
    except KeyError:
        has_temp_ctd = False

    try:
        has_psal_btl = any([
            True if pd.notnull(obj['psal']) else False for obj in bot_measurements])
    except KeyError:
        has_psal_btl = False

    try:
        has_psal_ctd = any([
            True if pd.notnull(obj['psal']) else False for obj in ctd_measurements])
    except KeyError:
        has_psal_ctd = False

    try:
        has_salinity_btl = any([
            True if pd.notnull(obj['salinity']) else False for obj in bot_measurements])
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

    use = {}
    use['btl'] = use_btl
    use['ctd'] = use_ctd

    return use, use_elems


def get_filtered_measurements_for_profile(measurements, type):

    use_elems = find_measurements_hierarchy(measurements)

    measurements = filter_measurements(measurements, use_elems)

    # Get measurements flag
    if type == 'btl':
        flag = 'BTL'
    if type == 'ctd':
        flag = 'CTD'

    return measurements, flag, use_elems


def get_filtered_measurements(profile_dicts, type):

    for profile_dict in profile_dicts:

        measurements = profile_dict['measurements']

        measurements, flag, use_elems = get_filtered_measurements_for_profile(
            measurements, type)

        measurements_source_qc = profile_dict['measurementsSourceQC']
        measurements_source_qc['source'] = flag

        if use_elems['use_psal']:
            measurements_source_qc['psal_used'] = True

        if use_elems['use_salinity']:
            measurements_source_qc['salinity_used'] = True

        profile_dict['measurements'] = measurements
        profile_dict['measurementsSourceQC'] = measurements_source_qc

    return profile_dicts
