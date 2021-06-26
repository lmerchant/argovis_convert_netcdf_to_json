
# Filter  measurements object by element hierarchy
import numpy as np
import pandas as pd


def convert_boolean(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    if isinstance(obj, (list, tuple)):
        return [convert_boolean(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_boolean(key): convert_boolean(value) for key, value in obj.items()}
    return obj


# TODO
# use None instead of np.nan to translate to null in json and not NaN


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
                new_obj['temp'] = None
            if key == 'psal' and not use_psal:
                new_obj['psal'] = None
            if key == 'salinity' and not use_salinity:
                del new_obj[key]
            if key == 'salinity' and not use_psal:
                new_obj['psal'] = val

        new_measurements.append(new_obj)

        keys = new_obj.keys()
        if 'salinity' in keys:
            del new_obj['salinity']

    # TODO
    # Remove if no temp or keep?
    # if not use_temp:
    #     new_measurements = []

    return new_measurements


def filter_btl_ctd_combined(btl_measurements, ctd_measurements, use_elems, flag):

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

        # has_elems = any([True if (pd.notnull(val) and key != 'pres')
        #                 else False for key, val in new_obj.items()])

        # if not has_elems:
        #     new_obj = {}

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

        # has_elems = any([True if (pd.notnull(val) and key != 'pres')
        #                 else False for key, val in new_obj.items()])

        # TODO
        # Remove or not empty objects that only have pressure?
        # if not has_elems:
        #     new_obj = {}

        new_ctd_measurements.append(new_obj)

    is_empty = all([not elem for elem in new_ctd_measurements])

    if is_empty:
        new_ctd_measurements = []

    #new_ctd_measurements = [obj for obj in new_ctd_measurements if obj]

    # If using no temp_btl, psal_btl, or salinity, btl, remove from list

    # Get source information
    # See if using btl only, ctd only or btl and ctd
    if not use_temp_btl and not use_psal_btl and not use_salinity_btl:
        new_btl_measurements = []

    if not use_temp_ctd and not use_psal_ctd:
        new_ctd_measurements = []

    combined_measurements = [*new_ctd_measurements, *new_btl_measurements]

    if not use_temp_btl and not use_temp_ctd:
        combined_measurements = []

    # TODO
    # Also mark what is  being used for testing
    # But remove for final product

    measurements_source = {}
    measurements_source['source'] = flag
    measurements_source['qc'] = 2
    measurements_source['use_temp_ctd'] = use_temp_ctd
    measurements_source['use_psal_ctd'] = use_psal_ctd
    measurements_source['use_temp_btl'] = use_temp_btl
    measurements_source['use_psal_btl'] = use_psal_btl
    if use_elems['use_salinity_btl']:
        measurements_source['use_salinty_btl'] = use_salinity_btl

    measurements_source = convert_boolean(measurements_source)

    return combined_measurements, measurements_source


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
    if use_btl and use_ctd:
        flag = 'BTL_CTD'
    elif use_btl:
        flag = 'BTL'
    elif use_ctd:
        flag = 'CTD'
    else:
        flag = None

    return use_elems, flag


def get_filtered_measurements_for_profile_dict(measurements, type):

    use_elems = find_measurements_hierarchy(measurements)

    measurements = filter_measurements(measurements, use_elems)

    # Get measurements flag
    if type == 'btl':
        flag = 'BTL'
    if type == 'ctd':
        flag = 'CTD'

    return measurements, flag, use_elems


def filter_measurements(profiles, type):

    output_profiles_list = []

    for profile in profiles:

        profile_dict = profile['profile_dict']
        station_cast = profile['station_cast']

        measurements = profile_dict['measurements']

        measurements, flag, use_elems = get_filtered_measurements_for_profile_dict(
            measurements, type)

        measurements_source = profile_dict['measurementsSource']
        measurements_source['source'] = flag

        if use_elems['use_salinity']:
            measurements_source['salinity_used'] = True

        profile_dict['measurements'] = measurements
        profile_dict['measurementsSource'] = convert_boolean(
            measurements_source)

        output_profile = {}
        output_profile['profile_dict'] = profile_dict
        output_profile['station_cast'] = station_cast

        output_profiles_list.append(output_profile)

    return output_profiles_list
