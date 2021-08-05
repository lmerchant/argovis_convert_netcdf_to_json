import logging
import numpy as np


def remove_empty_rows(df):

    # TODO
    # Do qc cols mess this up or are they NaN also?

    # If have '' and 'NaT' values, replace with NaN,
    # drop the rows, then replace with previous
    # values of '' and 'NaT'

    # make a copy of df so after remove null rows,
    # can substitute back in any 'NaT' or '' values

    df_copy = df.copy()

    df = df.replace(r'^\s*$', np.NaN, regex=True)
    df = df.replace(r'^NaT$', np.NaN, regex=True)

    exclude_columns = ['N_PROF', 'N_LEVELS', 'station_cast']
    # Exlude qc columns because only relevant if there are non nan values
    qc_columns = [col for col in df.columns if col.endswith('_qc')]
    exclude_columns.extend(qc_columns)
    subset_cols = [col for col in df.columns if col not in exclude_columns]

    df_dropped = df.dropna(subset=subset_cols, how='all').copy()

    df_dropped.update(df_copy)

    return df_dropped


def modify_dask_obj(ddf_param, data_type):

    # ******************************************
    # Remove empty rows so don't include in JSON
    # Change NaN to None for JSON to be null
    # Add back in temp_qc = 0 col if existed
    # ******************************************

    logging.info('Remove empty rows')

    # https://stackoverflow.com/questions/54164879/what-is-an-efficient-way-to-use-groupby-apply-a-custom-function-for-a-huge-dat

    dask_meta = ddf_param.dtypes.to_dict()
    ddf_param = ddf_param.map_partitions(
        lambda part: part.groupby('N_PROF').apply(remove_empty_rows), meta=dask_meta)

    # Now both indexed by N_PROF  and retained as a column
    # drop the index
    ddf_param = ddf_param.drop(['N_LEVELS'], axis=1)
    ddf_param = ddf_param.reset_index(drop=True)

    # Change NaN to None so in json, converted to null
    ddf_param = ddf_param.replace({np.nan: None})

    # # Add back in temp_qc = 0 if column exists and all np.nan
    # try:
    #     if ddf_param[f"temp_{data_type}_qc"].isnull().values.all():
    #         ddf_param[f"temp_{data_type}_qc"] = ddf_param[f"temp_{data_type}_qc"].fillna(
    #             0)

    # except KeyError:
    #     pass

    # # Add back in pres_qc = 1 if column exists and all np.nan
    # try:
    #     if ddf_param[f"pres_qc"].isnull().values.all():
    #         ddf_param[f"pres_qc"] = ddf_param[f"pres_qc"].fillna(1)

    # except KeyError:
    #     pass

    return ddf_param
