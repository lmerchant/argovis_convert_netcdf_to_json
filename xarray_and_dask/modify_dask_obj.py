import logging
import numpy as np


# used
def remove_empty_rows(df):

    # If have '' and 'NaT' values, need to do more to remove these rows
    df_copy = df.copy()

    # Replace 'NaT' and '' with  NaN but first
    # make a copy of df so after remove null rows,
    # can substitute back in any 'NaT' or '' values

    df = df.replace(r'^\s*$', np.NaN, regex=True)
    df = df.replace(r'^NaT$', np.NaN, regex=True)

    exclude_columns = ['N_PROF', 'N_LEVELS', 'station_cast']
    subset_cols = [col for col in df.columns if col not in exclude_columns]

    df = df.dropna(subset=subset_cols, how='all')

    df.update(df_copy)

    return df


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

    # Add back in temp_qc = 0 if column exists and all np.nan
    try:
        if ddf_param[f"temp_{data_type}_qc"].isnull().values.all():
            ddf_param[f"temp_{data_type}_qc"] = ddf_param[f"temp_{data_type}_qc"].fillna(
                0)

    except KeyError:
        pass

    return ddf_param
