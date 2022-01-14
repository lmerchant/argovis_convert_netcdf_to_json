import logging
import pandas as pd

from variable_mapping.meta_param_mapping import get_measurements_mapping


def convert_boolean(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    if isinstance(obj, (list, tuple)):
        return [convert_boolean(item) for item in obj]
    if isinstance(obj, dict):
        return {convert_boolean(key): convert_boolean(value) for key, value in obj.items()}
    return obj


# def filter_btl_ctd_measurements(btl_meas, ctd_meas, meas_objs):

#     # At any one time, really only one combination
#     # return combination key to get source_flag and meas_sources

#     meas_obj = [obj for obj in meas_objs if obj['btl'] and obj['ctd']]

#     combination_key = meas_obj.keys()[0]

#     for meas_obj in meas_objs:

#         btl_params = meas_obj['btl']
#         ctd_params = meas_obj['ctd']

#         # meas_objs['combination_8'] = {
#         #     'btl': [],
#         #     'ctd': ['pres', 'temp']
#         # }

#         # Are these meas dicts? If so, need to be pandas so
#         # can get sub columns and then change back to dict

#     # return combination_key, filtered_btl_meas, filtered_ctd_meas


# def filter_btl_ctd_measurements_orig(btl_meas, ctd_meas, use_cols, meas_sources):

#     # TODO
#     # check logic of having pressure with temperature always
#     # Should be default for creating btl and ctd measuremens

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


# def get_combined_meas_sources_qc(btl_dict, ctd_dict):

#     meas_sources_qc = None
#     btl_qc = None
#     ctd_qc = None

#     if btl_dict:
#         btl_source_qc = btl_dict['measurementsSources']
#         # TODO
#         # Set qc to non array value
#         btl_qc = btl_source_qc['qc'][0]
#         meas_sources_qc = btl_qc

#         print(f"btl_source_qc {btl_source_qc}")

#     if ctd_dict:
#         ctd_source_qc = ctd_dict['measurementsSources']
#         ctd_qc = ctd_source_qc['qc'][0]
#         meas_sources_qc = ctd_qc

#         print(f"ctd_source_qc {ctd_source_qc}")

#     if btl_dict and ctd_dict:
#         # qc_val = get_combined_meas_source(station_cast, btl_qc_val, ctd_qc_val)
#         # meas_sources_qc = qc_val
#         # TODO
#         # redo all this using different critea of combining
#         # for measurements object

#         print(f"btl qc = {btl_qc}")
#         print(f"ctd_qc = {ctd_qc}")

#         # TODO
#         # use a dict instead and take into account for combining
#         # if one of those qc's is None (empty collection)
#         if btl_qc == 0 and ctd_qc == 0:
#             # Would use btl values?
#             # would ctd_qc = 2 ever?
#             meas_sources_qc = 0
#         elif btl_qc == 2 and ctd_qc == 2:
#             # Would use ctd values
#             meas_sources_qc = 2
#         elif btl_qc == 0 and ctd_qc == 2:
#             meas_sources_qc = [0, 2]
#         elif pd.isnull(ctd_qc) and pd.notnull(btl_qc):
#             meas_sources_qc = btl_qc
#         elif pd.isnull(btl_qc) and pd.notnull(ctd_qc):
#             meas_sources_qc = ctd_qc
#         else:
#             meas_sources_qc = None
#             logging.info("QC MEAS IS NONE for btl and ctd")

#             # logging.info(f"btl_qc  {btl_qc}")
#             # logging.info(f"ctd_qc  {ctd_qc}")

#             # logging.info("*** Btl meas")
#             # logging.info(btl_meas)

#             # logging.info(f"\n\n\n*** Ctd Meas")
#             # logging.info(ctd_meas)

#             # Means btl_Meas and ctd_meas both empty,
#             # So do I keep the profile?

#             # TODO
#             # What to do in this case?

#     return meas_sources_qc


# def find_single_type_combinations(data_type, hierarchy):

#     if data_type == 'btl':
#         source_flag = 'BTL'
#     elif data_type == 'ctd':
#         source_flag = 'CTD'

#     has_psal = 'good_psal' in hierarchy
#     has_salinity = 'good_salinity' in hierarchy

#     meas_objs = {}
#     source_flag = {}
#     meas_sources = {}

#     # *************
#     # combination 1
#     # *************

#     # temp bad and doesn't matter if psal or salinity good

#     condition = not hierarchy['good_temp']

#     if condition:
#         meas_objs['combination_1'] = {
#             f"{data_type}": {}
#         }

#         source_flag['combination_1'] = source_flag

#         meas_sources['combination_1'] = {
#             f'using_pres_{data_type}': False,
#             f'using_temp_{data_type}': False
#         }

#     # *************
#     # combination 2
#     # *************

#     # temp good and psal bad and salinity doesn't exist

#     condition = hierarchy['good_temp'] and has_psal and not hierarchy['good_psal'] and not has_salinity

#     if condition:
#         meas_objs['combination_2'] = {
#             f"{data_type}": {'pres', 'temp'}
#         }

#         source_flag['combination_2'] = source_flag

#         meas_sources['combination_2'] = {
#             f'using_pres_{data_type}': True,
#             f'using_temp_{data_type}': True
#         }

#     # *************
#     # combination 3
#     # *************

#     # temp good and psal bad and salinity bad

#     condition = hierarchy['good_temp'] and has_psal and not hierarchy[
#         'good_psal'] and has_salinity and not hierarchy['good_salinity']

#     if condition:
#         meas_objs['combination_3'] = {
#             f"{data_type}": {'pres', 'temp'}
#         }

#         source_flag['combination_3'] = source_flag

#         meas_sources['combination_3'] = {
#             f'using_pres_{data_type}': True,
#             f'using_temp_{data_type}': True
#         }

#     # *************
#     # combination 4
#     # *************

#     # temp good and psal good

#     condition = hierarchy['good_temp'] and has_psal and hierarchy['good_psal']

#     if condition:
#         meas_objs['combination_4'] = {
#             f"{data_type}": {'pres', 'temp', 'psal'}
#         }

#         source_flag['combination_4'] = source_flag

#         meas_sources['combination_4'] = {
#             f'using_pres_{data_type}': True,
#             f'using_temp_{data_type}': True,
#             f'using_psal_{data_type}': True,
#         }

#     # *************
#     # combination 5
#     # *************

#     # temp good and psal bad and salinity good

#     condition = hierarchy['good_temp'] and has_psal and not hierarchy['good_psal'] and has_salinity and hierarchy['good_salinity']

#     if condition:
#         meas_objs['combination_5'] = {
#             f"{data_type}": {'pres', 'temp', 'salinity'}
#         }

#         source_flag['combination_5'] = source_flag

#         meas_sources['combination_5'] = {
#             f'using_pres_{data_type}': True,
#             f'using_temp_{data_type}': True,
#             f'using_salinity_{data_type}': True,
#         }

#     # *************
#     # combination 6
#     # *************

#     # temp good and psal not exist and salinity good

#     condition = hierarchy['good_temp'] and not has_psal and has_salinity and hierarchy['good_salinity']

#     if condition:
#         meas_objs['combination_6'] = {
#             f"{data_type}": {'pres', 'temp', 'salinity'}
#         }

#         source_flag['combination_6'] = source_flag

#         meas_sources['combination_6'] = {
#             f'using_pres_{data_type}': True,
#             f'using_temp_{data_type}': True,
#             f'using_salinity_{data_type}': True,
#         }

#     return meas_objs, source_flag, meas_sources


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

    print(f"hierarchy source flag {hierarchy_source_flag}")

    print(f"hierarchy_meas_sources {hierarchy_meas_sources}")


    num_pts_before_btl = len(btl_meas)
    num_pts_before_ctd = len(ctd_meas)

    btl_meas_before = btl_meas
    ctd_meas_before = ctd_meas

    if hierarchy_source_flag != 'BTL_CTD':
        if btl_meas:
            btl_meas = [obj for obj in btl_meas if pd.notna(obj['temp'])]

        if ctd_meas:
            ctd_meas = [obj for obj in ctd_meas if pd.notna(obj['temp'])]

        num_pts_after_btl = len(btl_meas)
        num_pts_after_ctd = len(ctd_meas)

        if num_pts_before_btl != num_pts_after_btl:
            logging.info('btl meas before')
            logging.info(btl_meas_before)
            logging.info(f'\n\nbtl meas after')
            logging.info(btl_meas)

        if num_pts_before_ctd != num_pts_after_ctd:
            logging.info('ctd meas before')
            logging.info(ctd_meas_before)
            logging.info(f'\n\nctd meas after')
            logging.info(ctd_meas)


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
            'ctd': ['pres', 'temp']
        }

        source_flag = 'CTD'

        meas_sources = {
            'pres_ctd': True,
            'temp_ctd': True
        }

        if has_psal_ctd:
            meas_sources['psal_ctd'] = False

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
            'btl': ['pres', 'salinity'],
            'ctd': ['pres', 'temp']
        }

        source_flag = 'BTL_CTD'

        meas_sources = {
            'pres_btl': True,
            'temp_btl': False,
            'salinity_btl': True,
            'pres_ctd': True,
            'temp_ctd': True
        }

        if has_psal_btl:
            meas_sources['psal_btl'] = False

        if has_psal_ctd:
            meas_sources['psal_ctd'] = False

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
            'btl': ['pres', 'psal'],
            'ctd': ['pres', 'temp']
        }

        source_flag = 'BTL_CTD'

        meas_sources = {
            'pres_btl': True,
            'temp_btl': False,
            'psal_btl': True,
            'pres_ctd': True,
            'temp_ctd': True
        }

        if has_psal_ctd:
            meas_sources['psal_ctd'] = False

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
            'ctd': ['pres', 'temp', 'psal']
        }

        meas_sources = {
            'pres_ctd': True,
            'temp_ctd': True,
            'psal_ctd': True
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
            'btl': ['pres', 'temp'],
            'ctd': []
        }

        source_flag = 'BTL'

        meas_sources = {
            'pres_btl': True,
            'temp_btl': True
        }

        if has_psal_btl:
            meas_sources['psal_btl'] = False

        if has_salinity:
            meas_sources['salinity_btl'] = False

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
            'btl': ['pres', 'temp', 'psal'],
            'ctd': []
        }

        source_flag = 'BTL'

        meas_sources = {
            'pres_btl': True,
            'temp_btl': True,
            'psal_btl': True
        }

        if has_salinity:
            meas_sources['salinity_btl'] = True

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
            'btl': ['pres', 'temp', 'salinity'],
            'ctd': []
        }

        source_flag = 'BTL'

        meas_sources = {
            'pres_btl': True,
            'temp_btl': True,
            'salinity_btl': True
        }

        if has_psal_btl:
            meas_sources['psal_btl'] = False

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
            'ctd': ['pres', 'temp']
        }

        source_flag = 'CTD'

        meas_sources = {
            'pres_ctd': True,
            'temp_ctd': True
        }

        if has_psal_ctd:
            meas_sources['psal_ctd'] = False

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
            'pres_btl': False,
            'temp_btl': False,
            'pres_ctd': False,
            'temp_ctd': False
        }

        if has_psal_btl:
            meas_sources['psal_btl'] = False

        if has_salinity:
            meas_sources['salinity_btl'] = False

        if has_psal_ctd:
            meas_sources['psal_ctd'] = False

        return hierarchy_meas_cols, source_flag, meas_sources

    # -----------------

    # If reach here, there is a combination not accounted for, so exit

    logging.info(
        "Found hierarchy meas combination for combined btl & ctd not accounted for")

    print('btl hierarchy')
    print(hierarchy_btl)

    print('ctd hierarchy')
    print(hierarchy_ctd)

    exit(1)


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

    meas_sources_key = measurements_mapping['measurementsSources']

    source = source_dict[meas_sources_key]

    hierarchy = {}

    # check ctd_temperature
    has_key = "temp" in source

    if has_key:
        hierarchy['good_temp'] = source['temp']
    else:
        hierarchy['good_temp'] = False

    # Check ctd_salinity
    # measurements renamed this psal
    has_key = "psal" in source

    if has_key:
        hierarchy['good_psal'] = source['psal']
        hierarchy['has_psal'] = True
    else:
        hierarchy['good_psal'] = False
        hierarchy['has_psal'] = False

    # check for bottle salinity
    has_key = "salinity" in source

    if has_key:
        hierarchy['good_salinity'] = source['salinity']
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
    source_flag_key = measurements_mapping['measurementsSource']
    meas_sources_key = measurements_mapping['measurementsSources']

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
    #combined_mappings['stationParameters'] = meas_names

    return combined_mappings
