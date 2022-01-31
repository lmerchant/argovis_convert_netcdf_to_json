import logging
#import pandas as pd

from create_profiles.create_meas_combined_profiles import combine_btl_ctd_measurements
#from create_profiles.create_meas_combined_profiles import get_combined_meas_source
from variable_mapping.combined_mapping import get_combined_mappings
from variable_mapping.meta_param_mapping import get_source_independent_meta_names


# def convert_boolean(obj):
#     if isinstance(obj, bool):
#         return str(obj).lower()
#     if isinstance(obj, (list, tuple)):
#         return [convert_boolean(item) for item in obj]
#     if isinstance(obj, dict):
#         return {convert_boolean(key): convert_boolean(value) for key, value in obj.items()}
#     return obj


# def add_btl_suffix(obj):

#     new_obj = {}

#     for key, val in obj.items():

#         new_key = f"{key}_btl"

#         new_obj[new_key] = val

#     return new_obj


# def filter_btl_ctd_measurements(btl_meas, ctd_meas, use_cols, meas_sources):

#     # use_cols tells what keys exist for meas
#     has_temp_btl_col = use_cols["has_temp_btl_col"]
#     has_temp_ctd_col = use_cols["has_temp_ctd_col"]
#     has_psal_btl_col = use_cols["has_psal_btl_col"]
#     has_psal_ctd_col = use_cols["has_psal_ctd_col"]
#     has_salinity_col = use_cols["has_salinity_col"]

#     # meas_sources tells if using a meas key value
#     # since if all null, not using it
#     if has_temp_ctd_col:
#         using_temp_ctd = meas_sources['use_temp_ctd']
#     else:
#         using_temp_ctd = False

#     if has_temp_btl_col:
#         using_temp_btl = meas_sources['use_temp_btl']
#     else:
#         using_temp_btl = False

#     if has_psal_ctd_col:
#         using_psal_ctd = meas_sources['use_psal_ctd']
#     else:
#         using_psal_ctd = False

#     if has_psal_btl_col:
#         using_psal_btl = meas_sources['use_psal_btl']
#     else:
#         using_psal_btl = False

#     if has_salinity_col:
#         using_salinity = meas_sources['use_salinity_btl']
#     else:
#         using_salinity = False

#     # If all vals are null except pres, don't use obj
#     # logging.info("cols available")
#     # logging.info(use_cols)

#     # logging.info("meas_sources")
#     # logging.info(meas_sources)

#     # logging.info("btl_meas")
#     # logging.info(btl_meas[0:4])

#     # logging.info("ctd_meas")
#     # logging.info(ctd_meas[0:4])

#     # Filter btl_meas
#     btl_meas_df = pd.DataFrame.from_records(btl_meas)

#     is_empty_btl = btl_meas_df.empty

#     if not is_empty_btl and has_temp_btl_col and using_temp_ctd:
#         btl_meas_df = btl_meas_df.drop('temp', axis=1)

#     elif not is_empty_btl and has_temp_btl_col and not using_temp_btl:
#         btl_meas_df = btl_meas_df.drop('temp', axis=1)

#     if not is_empty_btl and has_psal_btl_col and using_psal_ctd:
#         btl_meas_df = btl_meas_df.drop('psal', axis=1)

#     elif not is_empty_btl and has_psal_btl_col and (not using_psal_btl or not using_salinity):
#         btl_meas_df = btl_meas_df.drop('psal', axis=1)

#     if not is_empty_btl and 'pres' in btl_meas_df.columns and 'temp' not in btl_meas_df.columns and 'psal' not in btl_meas_df.columns:
#         btl_meas_df = btl_meas_df.drop('pres', axis=1)

#     filtered_btl_meas = btl_meas_df.to_dict('records')

#     # Filter ctd_meas
#     ctd_meas_df = pd.DataFrame.from_records(ctd_meas)

#     is_empty_ctd = ctd_meas_df.empty

#     if not is_empty_ctd and has_temp_ctd_col and not using_temp_ctd:
#         ctd_meas_df = ctd_meas_df.drop('temp', axis=1)

#     if not is_empty_ctd and has_psal_ctd_col and not using_psal_ctd:
#         ctd_meas_df = ctd_meas_df.drop('psal', axis=1)

#     if not is_empty_ctd and 'pres' in ctd_meas_df.columns and 'temp' not in ctd_meas_df.columns and 'psal' not in ctd_meas_df.columns:
#         ctd_meas_df = ctd_meas_df.drop('pres', axis=1)

#     filtered_ctd_meas = ctd_meas_df.to_dict('records')

#     # logging.info("filtered_btl_meas")
#     # logging.info(filtered_btl_meas[0:4])

#     # logging.info("filtered_ctd_meas")
#     # logging.info(filtered_ctd_meas[0:4])

#     return filtered_btl_meas, filtered_ctd_meas


# def filter_btl_ctd_measurements_orig(btl_meas, ctd_meas, use_cols, meas_sources):

#     #  TODO
#     # Redo all this in a copy of this function

#     # use_cols tells what keys exist for meas
#     has_temp_btl_col = use_cols["has_temp_btl_col"]
#     has_temp_ctd_col = use_cols["has_temp_ctd_col"]
#     has_psal_btl_col = use_cols["has_psal_btl_col"]
#     has_psal_ctd_col = use_cols["has_psal_ctd_col"]
#     has_salinity_col = use_cols["has_salinity_col"]

#     # meas_sources tells if using a meas key value
#     # since if all null, not using it
#     if has_temp_ctd_col:
#         using_temp_ctd = meas_sources['use_temp_ctd']
#     else:
#         using_temp_ctd = False

#     if has_temp_btl_col:
#         using_temp_btl = meas_sources['use_temp_btl']
#     else:
#         using_temp_btl = False

#     if has_psal_ctd_col:
#         using_psal_ctd = meas_sources['use_psal_ctd']
#     else:
#         using_psal_ctd = False

#     if has_psal_btl_col:
#         using_psal_btl = meas_sources['use_psal_btl']
#     else:
#         using_psal_btl = False

#     if has_salinity_col:
#         using_salinity = meas_sources['use_salinity_btl']
#     else:
#         using_salinity = False

#     # If all vals are null except pres, don't use obj
#     # logging.info("cols available")
#     # logging.info(use_cols)

#     # logging.info("meas_sources")
#     # logging.info(meas_sources)

#     # logging.info("btl_meas")
#     # logging.info(btl_meas[0:4])

#     # logging.info("ctd_meas")
#     # logging.info(ctd_meas[0:4])

#     # Filter btl_meas
#     btl_meas_df = pd.DataFrame.from_records(btl_meas)

#     is_empty_btl = btl_meas_df.empty

#     if not is_empty_btl and has_temp_btl_col and using_temp_ctd:
#         btl_meas_df = btl_meas_df.drop('temp', axis=1)

#     elif not is_empty_btl and has_temp_btl_col and not using_temp_btl:
#         btl_meas_df = btl_meas_df.drop('temp', axis=1)

#     if not is_empty_btl and has_psal_btl_col and using_psal_ctd:
#         btl_meas_df = btl_meas_df.drop('psal', axis=1)

#     elif not is_empty_btl and has_psal_btl_col and (not using_psal_btl or not using_salinity):
#         btl_meas_df = btl_meas_df.drop('psal', axis=1)

#     if not is_empty_btl and 'pres' in btl_meas_df.columns and 'temp' not in btl_meas_df.columns and 'psal' not in btl_meas_df.columns:
#         btl_meas_df = btl_meas_df.drop('pres', axis=1)

#     filtered_btl_meas = btl_meas_df.to_dict('records')

#     # Filter ctd_meas
#     ctd_meas_df = pd.DataFrame.from_records(ctd_meas)

#     is_empty_ctd = ctd_meas_df.empty

#     if not is_empty_ctd and has_temp_ctd_col and not using_temp_ctd:
#         ctd_meas_df = ctd_meas_df.drop('temp', axis=1)

#     if not is_empty_ctd and has_psal_ctd_col and not using_psal_ctd:
#         ctd_meas_df = ctd_meas_df.drop('psal', axis=1)

#     if not is_empty_ctd and 'pres' in ctd_meas_df.columns and 'temp' not in ctd_meas_df.columns and 'psal' not in ctd_meas_df.columns:
#         ctd_meas_df = ctd_meas_df.drop('pres', axis=1)

#     filtered_ctd_meas = ctd_meas_df.to_dict('records')

#     # logging.info("filtered_btl_meas")
#     # logging.info(filtered_btl_meas[0:4])

#     # logging.info("filtered_ctd_meas")
#     # logging.info(filtered_ctd_meas[0:4])

#     return filtered_btl_meas, filtered_ctd_meas


# def find_meas_hierarchy_btl_ctd_orig(btl_meas, ctd_meas, btl_qc, ctd_qc):

#     # For btl meas, if didn't have psal and had salinity
#     # salinity  was renamed  psal and the flag has_salinity_btl_col = True.
#     # If there was both psal and salinity, the salinity was dropped

#     has_temp_ctd_col = False
#     has_psal_ctd_col = False
#     has_temp_btl_col = False
#     has_psal_btl_col = False
#     has_salinity_btl_col = False

#     has_psal_btl_vals = False
#     has_temp_btl_vals = False
#     has_psal_ctd_vals = False
#     has_temp_ctd_vals = False

#     if 'use_temp_btl' in btl_qc.keys():
#         has_temp_btl_col = True

#     if 'use_temp_ctd' in ctd_qc.keys():
#         has_temp_ctd_col = True

#     if 'use_psal_btl' in btl_qc.keys():
#         has_psal_btl_col = True

#     if 'use_psal_ctd' in ctd_qc.keys():
#         has_psal_ctd_col = True

#     # Already changed name to psal if using salinity btl
#     # and so if it has a qc key, means salinity_btl existed
#     if 'use_salinity_btl' in btl_qc.keys():
#         salinity_exists = True
#     else:
#         salinity_exists = False

#     if has_temp_btl_col:
#         has_temp_btl_vals = any([
#             True if pd.notnull(obj['temp']) else False for obj in btl_meas])

#     if has_temp_ctd_col:
#         has_temp_ctd_vals = any([
#             True if pd.notnull(obj['temp']) else False for obj in ctd_meas])

#     if has_psal_btl_col:
#         has_psal_btl_vals = any([
#             True if pd.notnull(obj['psal']) else False for obj in btl_meas])

#     if has_psal_ctd_col:
#         has_psal_ctd_vals = any([
#             True if pd.notnull(obj['psal']) else False for obj in ctd_meas])

#     use_btl = False
#     use_ctd = False

#     # tell if using meas key value (whether will be null or not)
#     using_temp_btl = False
#     using_temp_ctd = False
#     using_psal_btl = False
#     using_psal_ctd = False
#     using_salinity_btl = False

#     if has_temp_ctd_col and has_temp_ctd_vals:
#         using_temp_ctd = True
#         use_ctd = True

#     if has_temp_ctd_col and not has_temp_ctd_vals and has_temp_btl_col and has_temp_btl_vals:
#         using_temp_btl = True
#         use_ctd = True

#     if has_psal_ctd_col and has_psal_ctd_vals:
#         using_psal_ctd = True
#         use_ctd = True

#     if has_psal_ctd_col and not has_psal_ctd_vals and has_psal_btl_col and has_psal_btl_vals and not salinity_exists:
#         using_psal_btl = True
#         use_ctd = True

#     # salinity btl already renamed to psal_btl and know
#     # was using salinity_btl for this if salinity_exists
#     if not has_psal_ctd_col and has_psal_btl_col and has_psal_btl_vals and not salinity_exists:
#         using_psal_btl = True
#         use_ctd = True

#     if not has_psal_ctd_col and has_psal_btl_col and has_psal_btl_vals and salinity_exists:
#         using_salinity_btl = True
#         use_btl = True

#     # Tells what keys exist for meas
#     use_cols = {
#         "has_temp_btl_col": has_temp_btl_col,
#         "has_temp_ctd_col": has_temp_ctd_col,
#         "has_psal_btl_col": has_psal_btl_col,
#         "has_psal_ctd_col": has_psal_ctd_col
#     }

#     # Tells if using a meas key value
#     meas_sources = {}
#     if has_temp_ctd_col:
#         meas_sources['use_temp_ctd'] = using_temp_ctd

#     if has_temp_btl_col:
#         meas_sources['use_temp_btl'] = using_temp_btl

#     if has_psal_ctd_col:
#         meas_sources['use_psal_ctd'] = using_psal_ctd

#     if has_psal_btl_col:
#         meas_sources['use_psal_btl'] = using_psal_btl

#     if has_salinity_btl_col:
#         meas_sources['use_salinty_btl'] = using_salinity_btl

#     meas_names = []
#     meas_names.append('pres')

#     if has_temp_ctd_col or has_temp_btl_col:
#         meas_names.append('temp')

#     if has_psal_ctd_col or has_psal_btl_col:
#         meas_names.append('psal')

#     if has_salinity_btl_col:
#         meas_names.append('psal')

#     # Only want unique names and not psal twice
#     meas_names = list(set(meas_names))

#     # For JSON conversion later
#     meas_sources = convert_boolean(meas_sources)

#     # Get measurements source flag
#     if not use_btl and not use_ctd:
#         meas_source = None
#     elif use_btl and use_ctd:
#         # case where using bottle salinity instead of psal
#         meas_source = 'BTL_CTD'
#     elif use_btl and not use_ctd:
#         meas_source = 'BTL'
#     elif use_ctd and not use_btl:
#         meas_source = 'CTD'
#     else:
#         meas_source = None

#     return meas_source, meas_sources, meas_names,  use_cols


# def find_meas_hierarchy_btl_ctd(btl_qc, ctd_qc):

#     # logging.info('***********')

#     # logging.info('btl_qc')
#     # logging.info(btl_qc)

#     # logging.info('ctd_qc')
#     # logging.info(ctd_qc)

#     # For btl meas, if didn't have psal and had salinity
#     # salinity  was renamed  psal and the flag has_salinity_btl_col = True.
#     # If there was both psal and salinity, the salinity was dropped

#     # In measurements, for each key, value means there
#     # are non null values
#     has_temp_btl_col = 'use_temp_btl' in btl_qc.keys()
#     if has_temp_btl_col:
#         using_temp_btl = btl_qc['use_temp_btl']
#     else:
#         using_temp_btl = False

#     has_temp_ctd_col = 'use_temp_ctd' in ctd_qc.keys()
#     if has_temp_ctd_col:
#         using_temp_ctd = ctd_qc['use_temp_ctd']
#     else:
#         using_temp_ctd = False

#     has_psal_btl_col = 'use_psal_btl' in btl_qc.keys()
#     if has_psal_btl_col:
#         using_psal_btl = btl_qc['use_psal_btl']
#     else:
#         using_psal_btl = False

#     has_psal_ctd_col = 'use_psal_ctd' in ctd_qc.keys()
#     if has_psal_ctd_col:
#         using_psal_ctd = ctd_qc['use_psal_ctd']
#     else:
#         using_psal_ctd = False

#     # Already changed name to psal if using salinity btl
#     # and so if it has a qc key, means salinity_btl existed
#     has_salinity_col = 'use_salinity_btl' in btl_qc.keys()
#     if has_salinity_col:
#         using_salinity = btl_qc['use_salinity_btl']
#     else:
#         using_salinity = False

#     btl_flag = False
#     ctd_flag = False
#     btl_ctd_flag = False
#     no_flag = False

#     # logging.info(f"using_temp_ctd {using_temp_ctd}")
#     # logging.info(f"using_temp_btl {using_temp_btl}")
#     # logging.info(f"using_psal_ctd {using_psal_ctd}")
#     # logging.info(f"using_psal_btl {using_psal_btl}")
#     # logging.info(f"using salinity {using_salinity}")

#     # if using_temp_ctd:
#     #     using_temp_btl = False

#     if not using_temp_ctd and using_temp_btl:
#         using_temp_ctd = False

#     # if using_psal_ctd:
#     #     using_psal_btl = False
#     #     using_salinity = False

#     if not using_psal_ctd and using_psal_btl:
#         using_psal_ctd = False
#         using_salinity = False

#     if not using_psal_ctd and not using_psal_btl and using_salinity:
#         using_psal_ctd = False
#         using_psal_btl = False

#     # Determine source flags depending on what is used

#     if using_temp_ctd and not using_temp_btl:
#         ctd_flag = True
#         btl_flag = False

#     elif not using_temp_ctd and using_temp_btl:
#         ctd_flag = False
#         btl_flag = True

#     elif using_temp_ctd and using_temp_btl:
#         btl_ctd_flag = True

#     # if using_temp_ctd or using_temp_btl:
#     #     ctd_flag = True

#     # if using_psal_ctd or using_psal_btl:
#     #     ctd_flag = True

#     # elif (using_temp_ctd or using_temp_btl) and not using_psal_ctd and not using_psal_btl and using_salinity:
#     #     btl_ctd_flag = True

#     # elif (not using_temp_ctd and not using_temp_btl) and not using_psal_ctd and not using_psal_btl and using_salinity:
#     #     btl_flag = True

#     # elif (not using_temp_ctd and not using_temp_btl) and (not using_psal_ctd and not using_psal_btl) and not using_salinity:
#     #     no_flag = True

#     # Tells what cols exist for meas
#     use_cols = {
#         "has_temp_btl_col": has_temp_btl_col,
#         "has_temp_ctd_col": has_temp_ctd_col,
#         "has_psal_btl_col": has_psal_btl_col,
#         "has_psal_ctd_col": has_psal_ctd_col,
#         "has_salinity_col": has_salinity_col
#     }

#     # Tells if using a meas col value
#     meas_sources = {}
#     if has_temp_ctd_col:
#         meas_sources['use_temp_ctd'] = using_temp_ctd

#     if has_temp_btl_col:
#         meas_sources['use_temp_btl'] = using_temp_btl

#     if has_psal_ctd_col:
#         meas_sources['use_psal_ctd'] = using_psal_ctd

#     if has_psal_btl_col:
#         meas_sources['use_psal_btl'] = using_psal_btl

#     if has_salinity_col:
#         meas_sources['use_salinity_btl'] = using_salinity

#     meas_names = []
#     meas_names.append('pres')

#     if has_temp_ctd_col or has_temp_btl_col:
#         meas_names.append('temp')

#     if has_psal_ctd_col or has_psal_btl_col:
#         meas_names.append('psal')

#     if has_salinity_col:
#         meas_names.append('psal')

#     # Only want unique names and not psal twice
#     meas_names = list(set(meas_names))

#     # For JSON conversion later
#     meas_sources = convert_boolean(meas_sources)

#     # Get measurements source flag
#     if no_flag:
#         meas_source = None
#     elif btl_ctd_flag:
#         meas_source = 'BTL_CTD'
#     elif btl_flag:
#         meas_source = 'BTL'
#     elif ctd_flag:
#         meas_source = 'CTD'

#     return meas_source, meas_sources, meas_names,  use_cols


# def combine_btl_ctd_measurements(btl_meas, ctd_meas, btl_qc, ctd_qc):

#     meas_source, meas_sources, meas_names, use_cols = find_meas_hierarchy_btl_ctd(
#         btl_qc, ctd_qc)

#     # filter measurement objs by hierarchy and if exists
#     filtered_btl_meas, filtered_ctd_meas = filter_btl_ctd_measurements(
#         btl_meas, ctd_meas, use_cols, meas_sources)

#     # Combine filtered measurements
#     combined_measurements = [
#         *filtered_ctd_meas, *filtered_btl_meas]

#     # Now filter out if don't have a temp coming from btl or ctd
#     # so don't include temp = None
#     # And filter out if there is not 'temp' var
#     # combined_measurements = [
#     #     meas for meas in combined_measurements if 'temp' in meas.keys() and meas['temp']]

#     return combined_measurements, meas_source, meas_sources, meas_names

def add_meta_w_data_type(meta, data_type):

    # Also add meta with suffix of the data_type
    # Will have duplicate information in file
    # This is because if combining files, will be using ctd meta without
    # suffix but still have ctd meta with suffix. To have a consistent
    # naming scheme across all files

    # But don't need a suffix for common meta for btl and ctd
    source_independent_meta_names = get_source_independent_meta_names()

    meta_w_data_type = {}
    meta_wo_data_type = {}

    for key, val in meta.items():

        if key not in source_independent_meta_names:
            new_key = f"{key}_{data_type}"
            meta_w_data_type[new_key] = val
        else:
            meta_wo_data_type[key] = val

    return meta_w_data_type, meta_wo_data_type


def combine_btl_ctd_per_profile(btl_profile, ctd_profile):

    # For a combined dict, station_cast same for btl and ctd
    station_cast = btl_profile['station_cast']

    # TODO
    # remove stationCast
    combined_btl_ctd_dict = {}
    #combined_btl_ctd_dict['stationCast'] = station_cast

    # May have case where bot dict or ctd dict doesn't exist for same profile
    # But they have the same station_cast

    # All profiles have a profile and station_cast key but
    # profile_dict may be empty

    btl_dict = btl_profile['profile_dict']
    ctd_dict = ctd_profile['profile_dict']

    btl_meta = {}
    ctd_meta = {}

    btl_meta_w_data_type = {}
    ctd_meta_w_data_type = {}

    btl_meta_wo_data_type = {}
    ctd_meta_wo_data_type = {}

    btl_data = []
    ctd_data = []

    if btl_dict:
        file_data_type = 'btl'
        btl_meta = btl_dict['meta']
        btl_meta_w_data_type, btl_meta_wo_data_type = add_meta_w_data_type(
            btl_meta, file_data_type)

        btl_data = btl_dict['data']

    if ctd_dict:
        file_data_type = 'ctd'
        ctd_meta = ctd_dict['meta']
        ctd_meta_w_data_type, ctd_meta_wo_data_type = add_meta_w_data_type(
            ctd_meta, file_data_type)

        ctd_data = ctd_dict['data']

    if btl_dict and ctd_dict:
        meta = ctd_meta
        data_type = 'btl_ctd'
    elif btl_dict and not ctd_dict:
        # TODO
        # Check if this is true
        # keep only meta without a _btl suffix since no ctd meta
        #meta = btl_meta_wo_data_type
        meta = btl_meta
        data_type = 'btl'

    elif ctd_dict and not btl_dict:
        meta = ctd_meta
        data_type = 'ctd'

    meta_w_data_type = {**ctd_meta_w_data_type, **btl_meta_w_data_type}

    combined_meta = {**meta, **meta_w_data_type}

    # Add instrument key to indicate profile data type
    # It's ship_btl_ctd because cruise has both a bottle and ctd file
    combined_meta['instrument'] = 'ship_ctd_btl'

    combined_data = [*ctd_data, *btl_data]

    combined_measurements = combine_btl_ctd_measurements(
        btl_dict, ctd_dict)

    combined_btl_ctd_dict['meta'] = combined_meta
    combined_btl_ctd_dict['data'] = combined_data

    combined_btl_ctd_dict = {
        **combined_btl_ctd_dict, **combined_measurements}

    combined_mappings = get_combined_mappings(btl_dict, ctd_dict, data_type)

    combined_btl_ctd_dict = {**combined_btl_ctd_dict, **combined_mappings}

    combined_btl_ctd_dict['data_type'] = data_type

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
            #cchdo_meta = profiles_objs['cchdo_meta']
            btl_profiles = profiles_obj['data_type_profiles_list']
        elif profiles_obj['data_type'] == 'ctd':
            #cchdo_meta = profiles_objs['cchdo_meta']
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

        # returned dict has keys: station_cast, data_type, profile_dict
        combined_profile_btl_ctd = combine_btl_ctd_per_profile(
            profile_btl, profile_ctd)

        profiles_list_btl_ctd.append(combined_profile_btl_ctd)

    logging.info('Processed btl and ctd combined profiles')

    return profiles_list_btl_ctd
