import logging


# def create_geolocation_dict(lat, lon):

#     # "geolocation": {
#     #     "coordinates": [
#     #         -158.2927,
#     #         21.3693
#     #     ],
#     #     "type": "Point"
#     # },

#     coordinates = [lon, lat]

#     geo_dict = {}
#     geo_dict['coordinates'] = coordinates
#     geo_dict['type'] = 'Point'

#     return geo_dict


def create_meta_profiles(df_meta):

    # # With meta columns, xarray exploded them
    # # for all levels. Only keep one Level
    # # since they repeat
    # logging.info('Get level = 0 meta rows')

    # # N_LEVELS is an index
    # ddf_meta = ddf_meta[ddf_meta['N_LEVELS'] == 0]
    # ddf_meta = ddf_meta.drop('N_LEVELS', axis=1)

    # df_meta = ddf_meta.compute()

    logging.info('create all_meta list')

    # TODO
    # check if want group_keys = False
    grouped_meta_dict = dict(tuple(df_meta.groupby('N_PROF', group_keys=False)))

    all_meta_profiles = []
    for val_df in grouped_meta_dict.values():

        station_cast = val_df['station_cast'].values[0]
        val_df = val_df.drop(['station_cast', 'N_PROF'],  axis=1)

        meta_dict = val_df.to_dict('records')[0]

        meta_obj = {}
        meta_obj['station_cast'] = station_cast
        meta_obj['meta'] = meta_dict

        all_meta_profiles.append(meta_obj)

    return all_meta_profiles
