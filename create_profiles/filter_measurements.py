# Filter  measurements object by element hierarchy
import pandas as pd
import logging


def convert_boolean(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    if isinstance(obj, (list, tuple)):
        return [convert_boolean(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_boolean(key): convert_boolean(value) for key, value in obj.items()}
    return obj


def filter_measurements(data_type_profiles):

    # For measurements, already filtered whether to
    # keep psal or salinity, but didn't rename the
    # salinity col.

    output_profiles_list = []

    for profile in data_type_profiles:

        profile_dict = profile['profile_dict']
        station_cast = profile['station_cast']

        measurements = profile_dict['measurements']

        # Check if all elems null in measurements besides pressure

        all_vals = []
        for obj in measurements:
            vals = [val for key, val in obj.items() if pd.notnull(val)
                    and key != 'pressure']
            all_vals.extend(vals)

        if not len(all_vals):
            measurements = []

        profile_dict['measurements'] = measurements

        output_profile = {}
        output_profile['profile_dict'] = profile_dict
        output_profile['station_cast'] = station_cast

        output_profiles_list.append(output_profile)

    return output_profiles_list
