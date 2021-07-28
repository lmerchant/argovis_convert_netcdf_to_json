import logging


def create_geolocation_dict(lat, lon):

    # "geoLocation": {
    #     "coordinates": [
    #         -158.2927,
    #         21.3693
    #     ],
    #     "type": "Point"
    # },

    coordinates = [lon, lat]

    geo_dict = {}
    geo_dict['coordinates'] = coordinates
    geo_dict['type'] = 'Point'

    return geo_dict


def create_meta_profile(ddf_meta):

    # With meta columns, pandas exploded them
    # for all levels. Only keep one Level
    # since they repeat
    logging.info('Get level = 0 meta rows')

    # N_LEVELS is an index
    ddf_meta = ddf_meta[ddf_meta['N_LEVELS'] == 0]
    ddf_meta = ddf_meta.drop('N_LEVELS', axis=1)

    df_meta = ddf_meta.compute()

    logging.info('create all_meta list')
    large_meta_dict = dict(tuple(df_meta.groupby('N_PROF')))

    all_meta_profiles = []
    for key, val_df in large_meta_dict.items():

        station_cast = val_df['station_cast'].values[0]
        val_df = val_df.drop(['station_cast', 'N_PROF'],  axis=1)

        meta_dict = val_df.to_dict('records')[0]

        lat = meta_dict['lat']
        lon = meta_dict['lon']

        geo_dict = create_geolocation_dict(lat, lon)
        meta_dict['geoLocation'] = geo_dict

        meta_obj = {}
        meta_obj['station_cast'] = station_cast
        meta_obj['meta'] = meta_dict

        all_meta_profiles.append(meta_obj)

    return all_meta_profiles
