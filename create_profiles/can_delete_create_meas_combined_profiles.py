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


# def get_combined_meas_source(station_cast, btl_qc_val, ctd_qc_val):

#     # TODO
#     # redo all this using different critea of combining
#     # for measurements object

#     # TODO
#     # use a dict instead and take into account for combining
#     # if one of those qc's is None (empty collection)
#     qc = {}
#     qc_btl = btl_qc_val
#     qc_ctd = ctd_qc_val

#     if btl_qc_val == 0 and ctd_qc_val == 0:
#         # Would use btl values?
#         # would ctd_qc_val = 2 ever?
#         qc_val = 0
#     elif btl_qc_val == 2 and ctd_qc_val == 2:
#         # Would use ctd values
#         qc_val = 2
#     elif btl_qc_val == 0 and ctd_qc_val == 2:
#         qc_val = [0, 2]
#     elif pd.isnull(ctd_qc_val) and pd.notnull(btl_qc_val):
#         logging.info(f"CTD qc = None for station cast {station_cast}")
#         qc_val = btl_qc_val
#     elif pd.isnull(btl_qc_val) and pd.notnull(ctd_qc_val):
#         logging.info(f"BTL qc = None for station cast {station_cast}")
#         qc_val = ctd_qc_val
#     else:
#         qc_val = None
#         logging.info("QC MEAS IS NONE for btl and ctd")
#         logging.info(f"for station cast {station_cast}")

#         # logging.info(f"btl_qc  {btl_qc_val}")
#         # logging.info(f"ctd_qc  {ctd_qc_val}")

#         # logging.info("*** Btl meas")
#         # logging.info(btl_meas)

#         # logging.info(f"\n\n\n*** Ctd Meas")
#         # logging.info(ctd_meas)

#         # Means btl_Meas and ctd_meas both empty,
#         # So do I keep the profile?

#         # TODO
#         # What to do in this case?

#     return qc_val


# def find_meas_hierarchy_btl_ctd_orig(btl_meas, ctd_meas, btl_dict, ctd_dict):

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

#     btl_source_qc = btl_dict['measurementsSourceQC']
#     btl_qc = btl_source_qc['qc']

#     ctd_source_qc = ctd_dict['measurementsSourceQC']
#     ctd_qc = ctd_source_qc['qc']

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

# def get_hierarchy_single_source(source_type, source_dict):

#     key = 'measurementsSourceQC'

#     # TODO
#     # redo all this logic. seems repetitive

#     # Maybe logic ok if looking at single source results
#     # And when renamed single type, also renamed the
#     # key measurementsSourceQC, so that's why
#     # looking at data_type suffix

#     # TODO
#     # looks like I already removed suffix source_type
#     # But I didn't include use_temp

#     # Gives true or false values
#     source = source_dict[key]

#     hierarchy = {}

#     # check ctd_temperature
#     check = f"use_temp_{source_type}"
#     has_key = check in source

#     if has_key:
#         hierarchy['good_temp'] = source[f'use_temp_{source_type}']
#     else:
#         hierarchy['good_temp'] = False

#     # Check ctd_salinity
#     # measurements renamed this psal
#     check = f"use_psal_{source_type}"
#     has_key = check in source

#     if has_key:
#         hierarchy['good_psal'] = source[f'use_psal_{source_type}']
#     else:
#         hierarchy['good_psal'] = False

#     # check for bottle salinity
#     check = "use_salinity_btl"
#     has_key = check in source

#     if has_key and source_type == 'btl':
#         hierarchy['good_salinity'] = source['use_salinity_btl']
#     else:
#         hierarchy['good_salinity'] = False

#     return hierarchy

def get_combined_measurements(btl_meas, ctd_meas, hierarchy_meas_objs):

    # Convert btl_meas and ctd_meas into df and then select columns I want
    # using hierarchy_meas_objs
    # Then turn df into list of dicts

    df_meas_btl = pd.DataFrame.from_dict(btl_meas)
    df_meas_ctd = pd.DataFrame.from_dict(ctd_meas)

    btl_columns = hierarchy_meas_objs['btl']
    ctd_columns = hierarchy_meas_objs['ctd']

    df_meas_btl = df_meas_btl[btl_columns]
    df_meas_ctd = df_meas_ctd[ctd_columns]

    # Turn back into a list of dict and combine each dict for combined_meas
    btl_meas = df_meas_btl.to_dict('records')
    ctd_meas = df_meas_ctd.to_dict('records')

    btl_meas.extend(ctd_meas)
    combined_meas = []
    for obj in btl_meas:
        if obj not in combined_meas:
            combined_meas.append(obj)

    return combined_meas


def filter_btl_ctd_measurements(btl_meas, ctd_meas, meas_objs):

    # At any one time, really only one combination
    # return combination key to get source_flag and meas_sources

    meas_obj = [obj for obj in meas_objs if obj['btl'] and obj['ctd']]

    combination_key = meas_obj.keys()[0]

    for meas_obj in meas_objs:

        btl_params = meas_obj['btl']
        ctd_params = meas_obj['ctd']

        # meas_objs['combination_8'] = {
        #     'btl': [],
        #     'ctd': ['pres', 'temp']
        # }

        # Are these meas dicts? If so, need to be pandas so
        # can get sub columns and then change back to dict

    # return combination_key, filtered_btl_meas, filtered_ctd_meas


def filter_btl_ctd_measurements_orig(btl_meas, ctd_meas, use_cols, meas_sources):

    # TODO
    # check logic of having pressure with temperature always
    # Should be default for creating btl and ctd measuremens

    # use_cols tells what keys exist for meas
    has_temp_btl_col = use_cols["has_temp_btl_col"]
    has_temp_ctd_col = use_cols["has_temp_ctd_col"]
    has_psal_btl_col = use_cols["has_psal_btl_col"]
    has_psal_ctd_col = use_cols["has_psal_ctd_col"]
    has_salinity_col = use_cols["has_salinity_col"]

    # meas_sources tells if using a meas key value
    # since if all null, not using it
    if has_temp_ctd_col:
        using_temp_ctd = meas_sources['use_temp_ctd']
    else:
        using_temp_ctd = False

    if has_temp_btl_col:
        using_temp_btl = meas_sources['use_temp_btl']
    else:
        using_temp_btl = False

    if has_psal_ctd_col:
        using_psal_ctd = meas_sources['use_psal_ctd']
    else:
        using_psal_ctd = False

    if has_psal_btl_col:
        using_psal_btl = meas_sources['use_psal_btl']
    else:
        using_psal_btl = False

    if has_salinity_col:
        using_salinity = meas_sources['use_salinity_btl']
    else:
        using_salinity = False

    # If all vals are null except pres, don't use obj
    # logging.info("cols available")
    # logging.info(use_cols)

    # logging.info("meas_sources")
    # logging.info(meas_sources)

    # logging.info("btl_meas")
    # logging.info(btl_meas[0:4])

    # logging.info("ctd_meas")
    # logging.info(ctd_meas[0:4])

    # Filter btl_meas
    btl_meas_df = pd.DataFrame.from_records(btl_meas)

    is_empty_btl = btl_meas_df.empty

    if not is_empty_btl and has_temp_btl_col and using_temp_ctd:
        btl_meas_df = btl_meas_df.drop('temp', axis=1)

    elif not is_empty_btl and has_temp_btl_col and not using_temp_btl:
        btl_meas_df = btl_meas_df.drop('temp', axis=1)

    if not is_empty_btl and has_psal_btl_col and using_psal_ctd:
        btl_meas_df = btl_meas_df.drop('psal', axis=1)

    elif not is_empty_btl and has_psal_btl_col and (not using_psal_btl or not using_salinity):
        btl_meas_df = btl_meas_df.drop('psal', axis=1)

    if not is_empty_btl and 'pres' in btl_meas_df.columns and 'temp' not in btl_meas_df.columns and 'psal' not in btl_meas_df.columns:
        btl_meas_df = btl_meas_df.drop('pres', axis=1)

    filtered_btl_meas = btl_meas_df.to_dict('records')

    # Filter ctd_meas
    ctd_meas_df = pd.DataFrame.from_records(ctd_meas)

    is_empty_ctd = ctd_meas_df.empty

    if not is_empty_ctd and has_temp_ctd_col and not using_temp_ctd:
        ctd_meas_df = ctd_meas_df.drop('temp', axis=1)

    if not is_empty_ctd and has_psal_ctd_col and not using_psal_ctd:
        ctd_meas_df = ctd_meas_df.drop('psal', axis=1)

    if not is_empty_ctd and 'pres' in ctd_meas_df.columns and 'temp' not in ctd_meas_df.columns and 'psal' not in ctd_meas_df.columns:
        ctd_meas_df = ctd_meas_df.drop('pres', axis=1)

    filtered_ctd_meas = ctd_meas_df.to_dict('records')

    # logging.info("filtered_btl_meas")
    # logging.info(filtered_btl_meas[0:4])

    # logging.info("filtered_ctd_meas")
    # logging.info(filtered_ctd_meas[0:4])

    return filtered_btl_meas, filtered_ctd_meas


def get_combined_meas_sources_qc(btl_dict, ctd_dict):

    meas_sources_qc = None
    btl_qc = None
    ctd_qc = None

    if btl_dict:
        btl_source_qc = btl_dict['measurementsSourceQC']
        # TODO
        # Set qc to non array value
        btl_qc = btl_source_qc['qc'][0]
        meas_sources_qc = btl_qc

        print(f"btl_source_qc {btl_source_qc}")

    if ctd_dict:
        ctd_source_qc = ctd_dict['measurementsSourceQC']
        ctd_qc = ctd_source_qc['qc'][0]
        meas_sources_qc = ctd_qc

        print(f"ctd_source_qc {ctd_source_qc}")

    if btl_dict and ctd_dict:
        # qc_val = get_combined_meas_source(station_cast, btl_qc_val, ctd_qc_val)
        # meas_sources_qc = qc_val
        # TODO
        # redo all this using different critea of combining
        # for measurements object

        print(f"btl qc = {btl_qc}")
        print(f"ctd_qc = {ctd_qc}")

        # TODO
        # use a dict instead and take into account for combining
        # if one of those qc's is None (empty collection)
        if btl_qc == 0 and ctd_qc == 0:
            # Would use btl values?
            # would ctd_qc = 2 ever?
            meas_sources_qc = 0
        elif btl_qc == 2 and ctd_qc == 2:
            # Would use ctd values
            meas_sources_qc = 2
        elif btl_qc == 0 and ctd_qc == 2:
            meas_sources_qc = [0, 2]
        elif pd.isnull(ctd_qc) and pd.notnull(btl_qc):
            meas_sources_qc = btl_qc
        elif pd.isnull(btl_qc) and pd.notnull(ctd_qc):
            meas_sources_qc = ctd_qc
        else:
            meas_sources_qc = None
            logging.info("QC MEAS IS NONE for btl and ctd")

            # logging.info(f"btl_qc  {btl_qc}")
            # logging.info(f"ctd_qc  {ctd_qc}")

            # logging.info("*** Btl meas")
            # logging.info(btl_meas)

            # logging.info(f"\n\n\n*** Ctd Meas")
            # logging.info(ctd_meas)

            # Means btl_Meas and ctd_meas both empty,
            # So do I keep the profile?

            # TODO
            # What to do in this case?

    return meas_sources_qc


def find_single_type_combinations(data_type, hierarchy):

    if data_type == 'btl':
        source_flag = 'BTL'
    elif data_type == 'ctd':
        source_flag = 'CTD'

    has_psal = 'good_psal' in hierarchy
    has_salinity = 'good_salinity' in hierarchy

    meas_objs = {}
    source_flag = {}
    meas_sources = {}

    # *************
    # combination 1
    # *************

    # temp bad and doesn't matter if psal or salinity good

    condition = not hierarchy['good_temp']

    if condition:
        meas_objs['combination_1'] = {
            f"{data_type}": {}
        }

        source_flag['combination_1'] = source_flag

        meas_sources['combination_1'] = {
            f'using_pres_{data_type}': False,
            f'using_temp_{data_type}': False
        }

    # *************
    # combination 2
    # *************

    # temp good and psal bad and salinity doesn't exist

    condition = hierarchy['good_temp'] and has_psal and not hierarchy['good_psal'] and not has_salinity

    if condition:
        meas_objs['combination_2'] = {
            f"{data_type}": {'pres', 'temp'}
        }

        source_flag['combination_2'] = source_flag

        meas_sources['combination_2'] = {
            f'using_pres_{data_type}': True,
            f'using_temp_{data_type}': True
        }

    # *************
    # combination 3
    # *************

    # temp good and psal bad and salinity bad

    condition = hierarchy['good_temp'] and has_psal and not hierarchy[
        'good_psal'] and has_salinity and not hierarchy['good_salinity']

    if condition:
        meas_objs['combination_3'] = {
            f"{data_type}": {'pres', 'temp'}
        }

        source_flag['combination_3'] = source_flag

        meas_sources['combination_3'] = {
            f'using_pres_{data_type}': True,
            f'using_temp_{data_type}': True
        }

    # *************
    # combination 4
    # *************

    # temp good and psal good

    condition = hierarchy['good_temp'] and has_psal and hierarchy['good_psal']

    if condition:
        meas_objs['combination_4'] = {
            f"{data_type}": {'pres', 'temp', 'psal'}
        }

        source_flag['combination_4'] = source_flag

        meas_sources['combination_4'] = {
            f'using_pres_{data_type}': True,
            f'using_temp_{data_type}': True,
            f'using_psal_{data_type}': True,
        }

    # *************
    # combination 5
    # *************

    # temp good and psal bad and salinity good

    condition = hierarchy['good_temp'] and has_psal and not hierarchy['good_psal'] and has_salinity and hierarchy['good_salinity']

    if condition:
        meas_objs['combination_5'] = {
            f"{data_type}": {'pres', 'temp', 'salinity'}
        }

        source_flag['combination_5'] = source_flag

        meas_sources['combination_5'] = {
            f'using_pres_{data_type}': True,
            f'using_temp_{data_type}': True,
            f'using_salinity_{data_type}': True,
        }

    # *************
    # combination 6
    # *************

    # temp good and psal not exist and salinity good

    condition = hierarchy['good_temp'] and not has_psal and has_salinity and hierarchy['good_salinity']

    if condition:
        meas_objs['combination_6'] = {
            f"{data_type}": {'pres', 'temp', 'salinity'}
        }

        source_flag['combination_6'] = source_flag

        meas_sources['combination_6'] = {
            f'using_pres_{data_type}': True,
            f'using_temp_{data_type}': True,
            f'using_salinity_{data_type}': True,
        }

    return meas_objs, source_flag, meas_sources


def find_btl_ctd_combinations(hierarchy_btl, hierarchy_ctd):

    # Need to take into account that a btl or ctd sourceQC key can be None
    # That means temperature was all bad
    # Maybe change this to always have a source key and indicate temp bad

    hierarchy_meas_objs = {}
    source_flag = {}
    meas_sources = {}

    # *************
    # combination 1
    # *************

    # btl temp bad and doesn't matter if psal or salinity good
    # ctd temp good, ctd psal bad
    # then use objs {pres_ctd: #, temp_ctd: #}

    # bad btl temperature would already have been flagged
    # TODO
    # check this that meas source var exists but meas object is empty
    has_psal_btl = 'good_psal' in hierarchy_btl
    has_salinity = 'good_salinity' in hierarchy_btl
    has_psal_ctd = 'good_psal' in hierarchy_ctd

    btl_condition = not hierarchy_btl['good_temp']

    ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and not hierarchy_ctd['good_psal']

    if btl_condition and ctd_condition:
        hierarchy_meas_objs = {
            'btl': [],
            'ctd': ['pres', 'temp']
        }

        source_flag = 'CTD'

        meas_sources = {
            'pres_ctd': True,
            'temp_ctd': True
        }

        return hierarchy_meas_objs, source_flag, meas_sources

    # *************
    # combination 2
    # *************

    # btl temp good and psal bad and salinity good
    # ctd temp good and psal bad

    # then use objs {pres_btl: #, salinity_btl: #} and {pres_ctd: #, temp_ctd: #}

    btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and not hierarchy_btl[
        'good_psal'] and has_salinity and hierarchy_btl['good_salinity']

    ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and not hierarchy_ctd['good_psal']

    if btl_condition and ctd_condition:
        hierarchy_meas_objs = {
            'btl': ['pres', 'salinity'],
            'ctd': ['pres', 'temp']
        }

        source_flag = 'BTL_CTD'

        meas_sources = {
            'pres_btl': True,
            'salinity_btl': True,
            'pres_ctd': True,
            'temp_ctd': True
        }

        return hierarchy_meas_objs, source_flag, meas_sources

    # *************
    # combination 3
    # *************

    # btl temp good, btl psal good, btl salinity good
    # ctd temp good, ctd psal bad
    # then use objs {pres_btl: #, psal_btl: #} and {pres_ctd: #, temp_ctd: #}

    # don't need to check btl salinity since psal takes precedence
    btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and hierarchy_btl['good_psal']

    ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and not hierarchy_ctd['good_psal']

    if btl_condition and ctd_condition:
        hierarchy_meas_objs = {
            'btl': ['pres', 'psal'],
            'ctd': ['pres', 'temp']
        }

        source_flag = 'BTL_CTD'

        meas_sources = {
            'pres_btl': True,
            'psal_btl': True,
            'pres_ctd': True,
            'temp_ctd': True
        }

        return hierarchy_meas_objs, source_flag, meas_sources

    # *************
    # combination 4
    # *************

    # btl temp good, and doesn't matter what psal or salinity are like if
    #   ctd psal is good
    # ctd temp good and ctd psal good
    # then use objs {pres_ctd: #, temp_ctd: #, psal_ctd: #}

    btl_condition = hierarchy_btl['good_temp']

    ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and hierarchy_ctd['good_psal']

    if btl_condition and ctd_condition:
        hierarchy_meas_objs = {
            'btl': [],
            'ctd': ['pres', 'temp', 'psal']
        }

        meas_sources = {
            'pres_ctd': True,
            'temp_ctd': True,
            'psal_ctd': True
        }

        source_flag = 'CTD'

        return hierarchy_meas_objs, source_flag, meas_sources

    # *************
    # combination 5
    # *************

    # btl temp good and btl psal and btl salinity bad
    # ctd temp bad
    # then use objs {pres_btl:#, temp_btl:#}

    btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and not hierarchy_btl[
        'good_psal'] and not hierarchy_btl['good_salinity']

    ctd_condition = not hierarchy_ctd['good_temp']

    if btl_condition and ctd_condition:
        hierarchy_meas_objs = {
            'btl': ['pres', 'temp'],
            'ctd': []
        }

        source_flag = 'BTL'

        meas_sources = {
            'pres_btl': True,
            'temp_btl': True
        }

        return hierarchy_meas_objs, source_flag, meas_sources

    # *************
    # combination 6
    # *************

    # btl temp good and btl psal good, salinity doesn't matter in this case
    # ctd temp bad
    # then use objs {pres_btl:#, temp_btl:#, psal_btl:#}

    btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and hierarchy_btl['good_psal']

    ctd_condition = not hierarchy_ctd['good_temp']

    if btl_condition and ctd_condition:
        hierarchy_meas_objs = {
            'btl': ['pres', 'temp', 'psal'],
            'ctd': []
        }

        source_flag = 'BTL'

        meas_sources = {
            'pres_btl': True,
            'temp_btl': True,
            'psal_btl': True
        }

        return hierarchy_meas_objs, source_flag, meas_sources

    # *************
    # combination 7
    # *************

    # btl temp good and btl psal bad, salinity good
    # ctd temp bad
    # then use objs {pres_btl:#, temp_btl:#, salinity:#}

    btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and not hierarchy_btl[
        'good_psal'] and has_salinity and hierarchy_btl['good_salinity']

    ctd_condition = not hierarchy_ctd['good_temp']

    if btl_condition and ctd_condition:
        hierarchy_meas_objs = {
            'btl': ['pres', 'temp', 'salinity'],
            'ctd': []
        }

        source_flag = 'BTL'

        meas_sources = {
            'pres_btl': True,
            'temp_btl': True,
            'salinity_btl': True
        }

        return hierarchy_meas_objs, source_flag, meas_sources

    # *************
    # combination 8
    # *************

    # btl temp good, and btl psal bad and btl salinity bad
    # ctd temp good and ctd psal bad
    # then use objs {pres_ctd: #, temp_ctd: #}

    btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and not hierarchy_btl[
        'good_psal'] and has_salinity and not hierarchy_btl['good_salinity']

    ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and not hierarchy_ctd['good_psal']

    if btl_condition and ctd_condition:
        hierarchy_meas_objs = {
            'btl': [],
            'ctd': ['pres', 'temp']
        }

        source_flag = 'CTD'

        meas_sources = {
            'pres_ctd': True,
            'temp_ctd': True
        }

        return hierarchy_meas_objs, source_flag, meas_sources

    # -----------------

    # If reach here, there is a combination not accounted for, so exit

    logging.info(
        "Found hierarchy combination for combined btl & ctd not accounted for")
    exit(1)


# def find_btl_ctd_combinations(hierarchy_btl, hierarchy_ctd):

#     # Need to take into account that a btl or ctd sourceQC key can be None
#     # That means temperature was all bad
#     # Maybe change this to always have a source key and indicate temp bad

#     meas_objs = {}
#     source_flag = {}
#     meas_sources = {}

#     # *************
#     # combination 1
#     # *************

#     # btl temp bad and doesn't matter if psal or salinity good
#     # ctd temp good, ctd psal bad
#     # then use objs {pres_ctd: #, temp_ctd: #}

#     # bad btl temperature would already have been flagged
#     # TODO
#     # check this that meas source var exists but meas object is empty
#     has_psal_btl = 'good_psal' in hierarchy_btl
#     has_salinity = 'good_salinity' in hierarchy_btl
#     has_psal_ctd = 'good_psal' in hierarchy_ctd

#     btl_condition = not hierarchy_btl['good_temp']

#     ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and not hierarchy_ctd['good_psal']

#     if btl_condition and ctd_condition:
#         meas_objs['combination_1'] = {
#             'btl': [],
#             'ctd': ['pres', 'temp']
#         }

#         source_flag['combination_1'] = 'CTD'

#         meas_sources['combination_1'] = {
#             'pres_ctd': True,
#             'temp_ctd': True
#         }

#     # *************
#     # combination 2
#     # *************

#     # btl temp good and psal bad and salinity good
#     # ctd temp good and psal bad

#     # then use objs {pres_btl: #, salinity_btl: #} and {pres_ctd: #, temp_ctd: #}

#     btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and not hierarchy_btl[
#         'good_psal'] and has_salinity and hierarchy_btl['good_salinity']

#     ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and not hierarchy_ctd['good_psal']

#     if btl_condition and ctd_condition:
#         meas_objs['combination_2'] = {
#             'btl': ['pres', 'salinity'],
#             'ctd': ['pres', 'temp']
#         }

#         source_flag['combination_2'] = 'BTL_CTD'

#         meas_sources['combination_2'] = {
#             'pres_btl': True,
#             'salinity_btl': True,
#             'pres_ctd': True,
#             'temp_ctd': True
#         }

#     # *************
#     # combination 3
#     # *************

#     # btl temp good, btl psal good, btl salinity good
#     # ctd temp good, ctd psal bad
#     # then use objs {pres_btl: #, psal_btl: #} and {pres_ctd: #, temp_ctd: #}

#     # don't need to check btl salinity since psal takes precedence
#     btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and hierarchy_btl['good_psal']

#     ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and not hierarchy_ctd['good_psal']

#     if btl_condition and ctd_condition:
#         meas_objs['combination_3'] = {
#             'btl': ['pres', 'psal'],
#             'ctd': ['pres', 'temp']
#         }

#         source_flag['combination_3'] = 'BTL_CTD'

#         meas_sources['combination_3'] = {
#             'pres_btl': True,
#             'psal_btl': True,
#             'pres_ctd': True,
#             'temp_ctd': True
#         }

#     # *************
#     # combination 4
#     # *************

#     # btl temp good, and doesn't matter what psal or salinity are like if
#     #   ctd psal is good
#     # ctd temp good and ctd psal good
#     # then use objs {pres_ctd: #, temp_ctd: #, psal_ctd: #}

#     btl_condition = hierarchy_btl['good_temp']

#     ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and hierarchy_ctd['good_psal']

#     if btl_condition and ctd_condition:
#         meas_objs['combination_4'] = {
#             'btl': [],
#             'ctd': ['pres', 'temp', 'psal']
#         }

#         meas_sources['combination_4'] = {
#             'pres_ctd': True,
#             'temp_ctd': True,
#             'psal_ctd': True
#         }

#         source_flag['combination_4'] = 'CTD'

#     # *************
#     # combination 5
#     # *************

#     # btl temp good and btl psal and btl salinity bad
#     # ctd temp bad
#     # then use objs {pres_btl:#, temp_btl:#}

#     btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and not hierarchy_btl[
#         'good_psal'] and not hierarchy_btl['good_salinity']

#     ctd_condition = not hierarchy_ctd['good_temp']

#     if btl_condition and ctd_condition:
#         meas_objs['combination_5'] = {
#             'btl': ['pres', 'temp'],
#             'ctd': []
#         }

#         source_flag['combination_5'] = 'BTL'

#         meas_sources['combination_5'] = {
#             'pres_btl': True,
#             'temp_btl': True
#         }

#     # *************
#     # combination 6
#     # *************

#     # btl temp good and btl psal good, salinity doesn't matter in this case
#     # ctd temp bad
#     # then use objs {pres_btl:#, temp_btl:#, psal_btl:#}

#     btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and hierarchy_btl['good_psal']

#     ctd_condition = not hierarchy_ctd['good_temp']

#     if btl_condition and ctd_condition:
#         meas_objs['combination_6'] = {
#             'btl': ['pres', 'temp', 'psal'],
#             'ctd': []
#         }

#         source_flag['combination_6'] = 'BTL'

#         meas_sources['combination_6'] = {
#             'pres_btl': True,
#             'temp_btl': True,
#             'psal_btl': True
#         }

#     # *************
#     # combination 7
#     # *************

#     # btl temp good and btl psal bad, salinity good
#     # ctd temp bad
#     # then use objs {pres_btl:#, temp_btl:#, salinity:#}

#     btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and not hierarchy_btl[
#         'good_psal'] and has_salinity and hierarchy_btl['good_salinity']

#     ctd_condition = not hierarchy_ctd['good_temp']

#     if btl_condition and ctd_condition:
#         meas_objs['combination_7'] = {
#             'btl': ['pres', 'temp', 'salinity'],
#             'ctd': []
#         }

#         source_flag['combination_7'] = 'BTL'

#         meas_sources['combination_7'] = {
#             'pres_btl': True,
#             'temp_btl': True,
#             'salinity_btl': True
#         }

#     # *************
#     # combination 8
#     # *************

#     # btl temp good, and btl psal bad and btl salinity bad
#     # ctd temp good and ctd psal bad
#     # then use objs {pres_ctd: #, temp_ctd: #}

#     btl_condition = hierarchy_btl['good_temp'] and has_psal_btl and not hierarchy_btl[
#         'good_psal'] and has_salinity and not hierarchy_btl['good_salinity']

#     ctd_condition = hierarchy_ctd['good_temp'] and has_psal_ctd and not hierarchy_ctd['good_psal']

#     if btl_condition and ctd_condition:
#         meas_objs['combination_8'] = {
#             'btl': [],
#             'ctd': ['pres', 'temp']
#         }

#         source_flag['combination_8'] = 'CTD'

#         meas_sources['combination_8'] = {
#             'pres_ctd': True,
#             'temp_ctd': True
#         }

#     # -----------------

#     return meas_objs, source_flag, meas_sources


def get_hierarchy_single_source(source_dict):

    key = 'measurementsSourceQC'

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
    source = source_dict[key]

    hierarchy = {}

    hierarchy['qc'] = source['qc']

    # check ctd_temperature
    check = f"using_temp"
    has_key = check in source

    if has_key:
        hierarchy['good_temp'] = source['using_temp']
    else:
        hierarchy['good_temp'] = False

    # Check ctd_salinity
    # measurements renamed this psal
    check = f"using_psal"
    has_key = check in source

    if has_key:
        hierarchy['good_psal'] = source['using_psal']
    else:
        hierarchy['good_psal'] = False

    # check for bottle salinity
    check = "using_salinity"
    has_key = check in source

    if has_key:
        hierarchy['good_salinity'] = source['using_salinity']
    else:
        hierarchy['good_salinity'] = False

    return hierarchy


def find_meas_hierarchy_btl_ctd(btl_dict, ctd_dict):

    # hierarchy for combining btl and ctd

    # logging.info('***********')

    # logging.info('btl_qc')
    # logging.info(btl_qc)

    # logging.info('ctd_qc')
    # logging.info(ctd_qc)

    # For btl meas, if didn't have psal and had salinity
    # salinity  was renamed  psal and the flag has_salinity_btl_col = True.
    # If there was both psal and salinity, the salinity was dropped

    # In measurements, for each key, value means there
    # are non null values

    # btl_source_qc = {}
    # ctd_source_qc = {}

    # if btl_dict:
    #     source_type = 'btl'
    #     btl_source_qc = btl_dict['measurementsSourceQC']

    # if ctd_dict:
    #     source_type = 'ctd'
    #     ctd_source_qc = ctd_dict['measurementsSourceQC']

    # if btl_dict and ctd_dict:
    #     source_type = 'btl_ctd'

    # has_temp_btl_col = 'use_temp_btl' in btl_source_qc.keys()
    # if has_temp_btl_col:
    #     using_temp_btl = btl_source_qc['use_temp_btl']
    # else:
    #     using_temp_btl = False

    # has_temp_ctd_col = 'use_temp_ctd' in ctd_source_qc.keys()
    # if has_temp_ctd_col:
    #     using_temp_ctd = ctd_source_qc['use_temp_ctd']
    # else:
    #     using_temp_ctd = False

    # has_psal_btl_col = 'use_psal_btl' in btl_source_qc.keys()
    # if has_psal_btl_col:
    #     using_psal_btl = btl_source_qc['use_psal_btl']
    # else:
    #     using_psal_btl = False

    # has_psal_ctd_col = 'use_psal_ctd' in ctd_source_qc.keys()
    # if has_psal_ctd_col:
    #     using_psal_ctd = ctd_source_qc['use_psal_ctd']
    # else:
    #     using_psal_ctd = False

    # # Already changed name to psal if using salinity btl
    # # and so if it has a qc key, means salinity_btl existed
    # has_salinity_col = 'use_salinity_btl' in btl_source_qc.keys()
    # if has_salinity_col:
    #     using_salinity = btl_source_qc['use_salinity_btl']
    # else:
    #     using_salinity = False

    if btl_dict:
        hierarchy_btl = get_hierarchy_single_source(btl_dict)
    else:
        hierarchy_btl = {}

    if ctd_dict:
        hierarchy_ctd = get_hierarchy_single_source(ctd_dict)
    else:
        hierarchy_ctd = {}

    # if hierarchy_btl and hierarchy_ctd:
    #     meas_objs_btl_ctd, source_flag_btl_ctd, meas_sources_btl_ctd = find_btl_ctd_combinations(
    #         hierarchy_btl, hierarchy_ctd)

    # TODO
    # If temp was marked bad, already set measurements to [],
    # but what was the meas sources, flag and qc?

    # And if just one or the other, what is the result?

    # Do I need to do this since already removed and renamed sources
    # if hierarchy_btl and not hierarchy_ctd:

        # meas_objs_btl, source_flag_btl, meas_sources_btl = find_single_type_combinations(
        #     'btl', hierarchy_btl)

    # if not hierarchy_btl and hierarchy_ctd:
    #     meas_objs_ctd, source_flag_ctd, meas_sources_ctd = find_single_type_combinations(
    #         'ctd', hierarchy_ctd)

    # -------------

    # # For JSON conversion later
    # meas_sources_btl_ctd = convert_boolean(meas_sources_btl_ctd)

    # # Will get one combination per call

    # return meas_objs_btl_ctd, source_flag_btl_ctd, meas_sources_btl_ctd,

    return hierarchy_btl, hierarchy_ctd


def combine_btl_ctd_measurements(btl_dict, ctd_dict):

    btl_meas = {}
    ctd_meas = {}

    if btl_dict:
        btl_meas = btl_dict['measurements']
        hierarchy_source_flag = btl_dict['measurementsSource']
        hierarchy_meas_sources = btl_dict['measurementsSourceQC']

    if ctd_dict:
        ctd_meas = ctd_dict['measurements']
        hierarchy_source_flag = ctd_dict['measurementsSource']
        hierarchy_meas_sources = ctd_dict['measurementsSourceQC']

    hierarchy_btl, hierarchy_ctd = find_meas_hierarchy_btl_ctd(
        btl_dict, ctd_dict)

    if hierarchy_btl and hierarchy_ctd:
        hierarchy_meas_objs, hierarchy_source_flag, hierarchy_meas_sources = find_btl_ctd_combinations(
            hierarchy_btl, hierarchy_ctd)

        combined_measurements = get_combined_measurements(btl_meas, ctd_meas,
                                                          hierarchy_meas_objs)

    # -----------

    if hierarchy_btl and not hierarchy_ctd:
        combined_measurements = btl_meas

    if not hierarchy_btl and hierarchy_ctd:
        combined_measurements = ctd_meas

    # TODO
    # This needs to be updated or removed since qc depends on
    # if temp btl, temp_ctd or both are used

    # use hierarchy_meas_sources
    #meas_sources_qc = get_combined_meas_sources_qc(btl_dict, ctd_dict)

    # Get meas_sources_qc from hierarchy_meas_sources
    #meas_sources['qc'] = meas_sources_qc

    # filter measurement objs by hierarchy
    # Hierarchy of using ctd_salinity over botle_salinity
    # and use ctd_temperature in ctd file over that in bottle file
    # filtered_btl_meas, filtered_ctd_meas = filter_btl_ctd_measurements(
    #     btl_meas, ctd_meas, hierarchy_meas_objs)

    # # Combine filtered measurements
    # combined_measurements = [
    #     *filtered_ctd_meas, *filtered_btl_meas]

    # Now filter out if don't have a temp coming from btl or ctd
    # so don't include temp = None
    # And filter out if there is not 'temp' var
    # combined_measurements = [
    #     meas for meas in combined_measurements if 'temp' in meas.keys() and meas['temp']]

    # TODO
    # get rid of qc value for sources.
    # too much trouble trying to combine a 0 and 2
    # instead of QC, call it measurementsSources

    combined_mappings = {}
    combined_mappings['measurements'] = combined_measurements
    combined_mappings['measurementsSource'] = hierarchy_source_flag
    combined_mappings['measurementsSourceQC'] = hierarchy_meas_sources
    #combined_mappings['stationParameters'] = meas_names

    return combined_mappings
