import logging
import numpy as np


def modify_meta_dask_obj(ddf_meta):

    # With meta columns, xarray exploded them
    # for all levels. Only keep one Level
    # since they repeat
    logging.info('Get level = 0 meta rows')

    # N_LEVELS is an index
    ddf_meta = ddf_meta[ddf_meta['N_LEVELS'] == 0]
    ddf_meta = ddf_meta.drop('N_LEVELS', axis=1)

    # df_meta = ddf_meta.compute()

    return ddf_meta


def remove_empty_rows(df):

    # TODO
    # Do qc cols mess this up or are they NaN also?
    # Skipping qc cols since don't hold np.nan values

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

    # Update df_copy to only retain rows existing in df_dropped
    # df_copy has all data columns
    df_dropped.update(df_copy)

    return df_dropped


def modify_param_dask_obj(ddf_param):

    # TODO
    # Is removing rows with dask faster than with pandas?

    # ******************************************
    # Remove empty rows so don't include in JSON
    # Change NaN to None for final JSON to be null.
    # ******************************************

    logging.info('Remove empty rows')

    # https://stackoverflow.com/questions/54164879/what-is-an-efficient-way-to-use-groupby-apply-a-custom-function-for-a-huge-dat

    dask_meta = ddf_param.dtypes.to_dict()
    ddf_param = ddf_param.map_partitions(
        lambda part: part.groupby('N_PROF').apply(remove_empty_rows), meta=dask_meta)

    ddf_param = ddf_param.drop(['N_LEVELS'], axis=1)
    ddf_param = ddf_param.reset_index(drop=True)

    # Change NaN to None so in json, converted to null
    ddf_param = ddf_param.replace({np.nan: None})

    return ddf_param
