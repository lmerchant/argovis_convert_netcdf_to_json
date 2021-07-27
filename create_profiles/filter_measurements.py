# Filter  measurements object by element hierarchy
import pandas as pd


def convert_boolean(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    if isinstance(obj, (list, tuple)):
        return [convert_boolean(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_boolean(key): convert_boolean(value) for key, value in obj.items()}
    return obj


# used
def filter_measurements_one_profile(measurements, use_elems):

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

    return new_measurements

# used


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

# used


def get_filtered_measurements_for_profile_dict(measurements, type):

    use_elems = find_measurements_hierarchy(measurements)

    measurements = filter_measurements_one_profile(measurements, use_elems)

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

        # TODO
        # check if this flag overrules starting source flag
        source = profile_dict['measurementsSource']
        sources = profile_dict['measurementsSourceQC']
        #measurements_source['source'] = flag

        if use_elems['use_salinity']:
            sources['salinity_used'] = True

        profile_dict['measurements'] = measurements
        #profile_dict['measurementsSource'] = source

        # TODO
        # check if already did check
        # profile_dict['measurementsSourceQC'] = convert_boolean(
        #     sources)

        output_profile = {}
        output_profile['profile_dict'] = profile_dict
        output_profile['station_cast'] = station_cast

        output_profiles_list.append(output_profile)

    return output_profiles_list
