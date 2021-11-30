import json
import re
from datetime import datetime
import logging


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


def to_int_qc(obj):
    # _qc":2.0
    # If float qc with '.0' in qc value, remove it to get an int
    json_str = json.dumps(obj,  default=dtjson)
    json_str = re.sub(r'(_qc":\s?\d)\.0', r"\1", json_str)
    obj = json.loads(json_str)
    return obj


def remove_empty_cols(df):

    # Delete columns with all null, empty, or 'NaT' values
    null_cols = df.columns[df.isna().all()].tolist()

    try:
        empty_str_cols = [
            *filter(lambda c: df[c].str.contains('').all(), df)]

    except AttributeError:
        empty_str_cols = []

    try:
        not_a_time_cols = [
            *filter(lambda c: df[c].str.contains('NaT').all(), df)]

    except AttributeError:
        not_a_time_cols = []

    cols_to_drop = [*null_cols, *empty_str_cols, *not_a_time_cols]

    empty_cols_wo_qc = [col for col in cols_to_drop if '_qc' not in col]

    drop_qc_cols = [
        f"{col}_qc" for col in empty_cols_wo_qc if f"{col}_qc" in df.columns]

    cols_to_drop.extend(drop_qc_cols)

    df = df.drop(cols_to_drop, axis=1)

    return df


def create_data_profiles(df_param):

    # TODO
    # check columns, does N_PROF occur as an index?
    # Or is it just created after groupby

    # **********************************
    # From df_param, filter out any vars
    # with all null/empty values
    #
    # Create mapping profile for each
    # filtered df_param profile
    # **********************************

    logging.info('create data profile and get name mapping')

    # TODO
    # Sort columns so qc next to its var
    # Just for testing preview
    # remove when finished
    df_param = df_param.reindex(sorted(df_param.columns), axis=1)

    all_data_df_groups = dict(tuple(df_param.groupby('N_PROF')))

    all_data_profiles = []
    all_name_mapping = []

    for val_df in all_data_df_groups.values():

        station_cast = val_df['station_cast'].values[0]

        val_df = val_df.drop(['N_PROF'],  axis=1)
        val_df = val_df.drop(['station_cast'],  axis=1)

        # ***********************************************
        # Remove cols and corresponding qc if data set is
        # null, empty or 'NaT'
        # ***********************************************

        val_df = remove_empty_cols(val_df)

        # TODO
        # Why do this sort?
        val_df = val_df.sort_values(by=['pressure'])

        all_data_dict_list = val_df.to_dict('records')

        all_data_obj = {}
        all_data_obj['station_cast'] = station_cast
        all_data_obj['data'] = to_int_qc(all_data_dict_list)
        all_data_profiles.append(all_data_obj)

        # Do filtering out empty col names per profile later
        name_mapping_obj = {}
        name_mapping_obj['station_cast'] = station_cast
        non_empty_cols = list(val_df.columns)
        name_mapping_obj['non_empty_cols'] = non_empty_cols

        all_name_mapping.append(name_mapping_obj)

    return all_data_profiles, all_name_mapping
