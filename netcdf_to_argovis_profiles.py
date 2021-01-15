import xarray as xr
import pandas as pd
import numpy as np
import os
from datetime import datetime
import json
import logging


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


def convert(o):
    if isinstance(o, np.int8): 
        return int(o)  


def get_geo_information(latitude, longitude):

    geo_dict = {}

    roundLat = round(latitude, 3)
    roundLon = round(longitude, 3)

    strLat = f"{roundLat} N"
    strLon = f"{roundLon} E"

    geolocation_dict = {}
    geolocation_dict['type'] = 'Point'
    geolocation_dict['coordinates'] = [longitude, latitude]

    geo_dict['roundLat'] = roundLat
    geo_dict['roundLon'] = roundLon

    geo_dict['strLat'] = strLat
    geo_dict['strLon'] = strLon

    geo_dict['geoLocation'] = geolocation_dict

    return geo_dict


def update_meta_mapping(df):

    # Add row for date in df and remove time row
    data = {'name': ['date'], 'unit': ['']}
    df_new = pd.DataFrame(data, columns=['name', 'unit'])

    df = pd.concat([df_new, df], ignore_index=True)

    df.drop(df[df['name'] == 'time'].index, inplace = True)

    return df


def add_extra_meta_data(df_meta, profile_number, filename):

    # Since all rows the same for meta, just
    # select first row

    meta_dict = df_meta.iloc[0].to_dict()

    # Want to substring in expocode without overwriting value in meta_dict

    # Maybe use re.sub
    # import re
    # string_a = re.sub(r'(cat|dog)', 'pet', "Mark owns a dog and Mary owns a cat.")
    expocode = meta_dict['expocode']

    if '/' in meta_dict['expocode']:
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode.replace('/', '_')}"
    elif meta_dict['expocode'] == 'None':
        logging.info(filename)
        logging.info('expocode is None')
        cruise_url = ''
    else:
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{meta_dict['expocode']}"

    meta_dict['N_PROF'] = profile_number + 1
    meta_dict['_id'] = f"{meta_dict['expocode']}_NPROF_{profile_number + 1}"
    meta_dict['id'] = f"{meta_dict['expocode']}_NPROF_{profile_number + 1}"
    meta_dict['POSITIONING_SYSTEM'] = 'GPS'
    meta_dict['DATA_CENTRE'] = 'CCHDO'
    meta_dict['cruise_url'] = cruise_url
    meta_dict['data_filename'] = filename
    meta_dict['netcdf_url'] = ''

    meta_dict['date_formatted'] = meta_dict['time'].strftime("%Y-%m-%d")

    # Remove time, convert to iso and call date
    meta_dict['date'] = meta_dict['time'].isoformat()
    meta_dict.pop('time', None)

    latitude = meta_dict['lat']
    longitude = meta_dict['lon']

    geo_dict = get_geo_information(latitude, longitude)

    meta_dict = {**meta_dict, **geo_dict}

    return meta_dict


def write_profile_json(json_dir, profile_number, profile_dict, nc_filename):

    # Write profile_dict to as json to a profile file

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable
    profile_json = json.dumps(profile_dict, default=convert)

    filename = f"{nc_filename}_NPROF_{profile_number + 1}.json"

    file = os.path.join(json_dir,filename)

    with open(file, 'w') as f:
        json.dump(profile_dict, f, indent=4, sort_keys=True, default=convert)


def create_profile_dict(nc, profile_number, df_all_mapping, filename, meta_names, param_names):

    profile = nc.isel(N_PROF=[profile_number])
    df_profile = profile.to_dataframe()

    # Get metadata and parameter subsets of df_profile
    df_meta = df_profile[meta_names]
    df_params = df_profile[param_names]

    meta_dict = add_extra_meta_data(df_meta, profile_number, filename)

    names_in_orig = df_all_mapping['goship_name'].values.tolist()

    df_param_mapping = df_all_mapping.loc[df_all_mapping['name'].isin(param_names)]
    df_param_mapping_names = df_param_mapping[['goship_name', 'name']].copy()

    param_mapping_dict = dict(zip(df_param_mapping_names['goship_name'], df_param_mapping_names['name']))

    # Turn dataframe into a dict row by row keeping type of col
    params_dict_array = df_params.astype(object).to_dict(orient='records')  


    # Drop rows without units
    # df_units = df_all_mapping[['name', 'unit']].copy()
    # df_units = df_units[df_units['unit'] != '']
    # units_dict = dict(zip(df_units['name'], df_units['unit']))    

    # show param units with lat, lon
    df_param_units = df_param_mapping[['name', 'unit']].copy()
    df_lat_lon_units = df_all_mapping.loc[df_all_mapping['name'].isin(['lat', 'lon'])][['name', 'unit']]
    df_units = pd.concat([df_lat_lon_units, df_param_units], ignore_index=True)
    units_dict = dict(zip(df_units['name'], df_units['unit']))


    #---------

    profile_dict = meta_dict
    profile_dict['measurements'] = params_dict_array   
    profile_dict['units'] = units_dict
    profile_dict['station_parameters_in_nc'] = names_in_orig
    profile_dict['parameters_in_nc_mapping_to_argovis'] = param_mapping_dict

    return profile_dict


def convert_sea_water_temp(nc, df, filename):

    # Check sea_water_temperature to be degree_Celsius and
    # have go_ship_ref_scale be ITS-90

    # So look for ref_scale = IPTS-68 or ITS-90   

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    try:
        nc['TEMP']
        is_temp = True
    except KeyError:
        is_temp = False

    if not is_temp:
        logging.info('===========')
        logging.info(filename)
        logging.info('===========')


    for var in nc:

        try: 
            unit = nc[var].attrs['units']
            ref_scale = nc[var].attrs['reference_scale']
            expocode = nc.coords['expocode'].values[0]

            if not is_temp:

                logging.info(expocode)            
                logging.info(var)
                logging.info(ref_scale)
                logging.info(unit)

                logging.info('---------')

        except KeyError:
            pass  


    # Change this to work for all temperature names
    var = 'TEMP'

    try:
        temperature = nc[var].data

        unit = nc[var].attrs['units']
        ref_scale = nc[var].attrs['reference_scale']

        argo_unit = df.loc[df['name'] == var, 'argo_unit'].values[0]

        if ref_scale == 'IPTS-68' and argo_unit == unit:

            logging.info('IPTS-68 temperature scale')

            # Convert to ITS-90 scale 
            temperature90 = temperature/1.00024

            # Set nc var of TEMP to this value
            nc[var].data = temperature90

        elif ref_scale == 'IPTS-68' and argo_unit != unit:
            logging.info('temperature unit is not equal to Argo unit on ITS-68 scale')

        elif argo_unit != unit:
            logging.info('temperature unit is not equal to Argo unit')

        return nc, df

    except KeyError:

        return nc, df


def get_meta_param_names(nc):

    # Get meta and param names
    # metadata is any without N_LEVELS dimension
    meta_names = [coord for coord in nc.coords if 'N_LEVELS' not in nc.coords[coord].dims]
    param_names = [coord for coord in nc.coords if 'N_LEVELS' in nc.coords[coord].dims]

    meta_names_from_var = [var for var in nc if 'N_LEVELS' not in nc[var].dims]
    param_names_from_var = [var for var in nc if 'N_LEVELS' in nc[var].dims]

    meta_names.extend(meta_names_from_var)
    param_names.extend(param_names_from_var) 

    return (meta_names, param_names)   


def convert_to_argo_units(nc, df_all_mapping, filename):

    # ------ Sea Water Temperature -------

    nc, df_all_mapping = convert_sea_water_temp(nc, df_all_mapping, filename)




    return nc, df_all_mapping


def rename_units_to_argo_units(nc, argo_goship_units_mapping_file, df_all_mapping):

    # Rename units so can compare same names
    # For salinity, goship unit is 1, so check reference scale to determine what it is

    df = pd.read_csv(argo_goship_units_mapping_file)

    df_units = df_all_mapping[['name', 'goship_unit']]
    goship_units = dict(df_units.values.tolist())

    new_param_units = {}

    # Change attr variable units
    for var in nc:
        try:
            var_unit = nc[var].attrs['units']
            row = df.loc[df['goship_unit'] == var_unit]

            argo_unit = row['argo_unit'].values[0]

        except (KeyError, IndexError):
            # No units or no corresponding argo unit
            continue

        try:
            var_ref_scale = nc[var].attrs['reference_scale'] 
            is_salinity = row['reference_scale'].values[0] == 'PSS-78'

            if var_unit == '1' and not is_salinity:
                logging.info(f"goship unit = 1 and not salinity {var}") 
                logging.info(nc) 
                exit(1)
            else:
                nc[var].attrs['units'] = argo_unit
                new_param_units[var] = argo_unit                            

        except KeyError:
            # There is no reference scale to check
            nc[var].attrs['units'] = argo_unit
            new_param_units[var] = argo_unit


    new_meta_units = {}

    # Change coord field units
    for coord in nc.coords:
        try:
            coord_unit = nc.coords[coord].attrs['units']
            argo_unit = df.loc[df['goship_unit'] == coord_unit, 'argo_unit']

            nc[coord].attrs['units'] = argo_unit.values[0]

            new_meta_units[coord] = argo_unit.values[0]

        except (KeyError, IndexError):
            pass
             

    new_units = {**goship_units, **new_meta_units, **new_param_units}

    df_new_units = pd.DataFrame(new_units.items())
    df_new_units.columns = ['name', 'unit']

    df_all_mapping = df_all_mapping.merge(df_new_units,how='left', left_on='name', right_on='name')

    return nc, df_all_mapping


def rename_to_argo_names(nc, df):

    # Create new column with new names
    df['name'] = df['argo_name']    

    # if argo name is nan, use goship name
    df['name'] = np.where(df['argo_name'].isna(), df['goship_name'], df['name'])

    # Change latitude and longitude names
    df.loc[df['goship_name'] == 'latitude', 'name'] = 'lat'
    df.loc[df['goship_name'] == 'longitude', 'name'] = 'lon'

    # rename variables 
    mapping_dict = dict(zip(df['goship_name'], df['name'])) 
    nc = nc.rename_vars(name_dict=mapping_dict)

    return nc, df


def add_argo_qc_names(df):

    # If goship name has a qc, rename argo name to argo name qc
    goship_names = df.loc[df['goship_name'].str.contains('_qc')]

    for name in goship_names.iloc[:,0].tolist():

        # find argo name of goship name without qc
        goship_name = name.replace('_qc','')
        argo_name = df.loc[df['goship_name'] == goship_name, 'argo_name']

        if pd.notna(argo_name.values[0]): 

            df.loc[df['goship_name'] == name, 'argo_name'] = argo_name.values[0] + '_qc'

    return df


def get_argo_mapping_df(argo_mapping_file):

    df = pd.read_csv(argo_mapping_file)

    df = df[['argo_name', 'argo_unit', 'argo_reference_scale', 'goship_name']].copy()

    df['argo_reference_scale'] = df['argo_reference_scale'].replace(np.nan, 'unknown')

    return df


def get_goship_mapping_df(nc):

    coord_names = list(nc.coords)
    var_names = list(nc.keys())
    names = [*coord_names, *var_names]   

    name_to_units = {}
    name_to_ref_scale = {}

    for name in names:

        # map name to units if units exist
        try:
            name_to_units[name] = nc[name].attrs['units']
        except KeyError:
            name_to_units[name] = ''

        try:
            name_to_ref_scale[name] = nc[name].attrs['reference_scale']
        except KeyError:
            name_to_ref_scale[name] = 'unknown'

    df_dict = {}
    df_dict['goship_unit'] = name_to_units
    df_dict['goship_reference_scale'] = name_to_ref_scale

    df = pd.DataFrame.from_dict(df_dict).reset_index()

    df = df.rename(columns = {'index':'goship_name'})

    return df

    return (names)  


def create_json(nc, json_dir, filename, argo_mapping_file, argo_units_mapping_file):

    df_goship_mapping = get_goship_mapping_df(nc)
    df_argo_mapping = get_argo_mapping_df(argo_mapping_file)
    df_all_mapping = df_goship_mapping.merge(df_argo_mapping,how='left', left_on='goship_name', right_on='goship_name')

    df_all_mapping = add_argo_qc_names(df_all_mapping)
    nc, df_all_mapping = rename_to_argo_names(nc, df_all_mapping)

    nc, df_all_mapping = rename_units_to_argo_units(nc, argo_units_mapping_file, df_all_mapping)

    nc, df_all_mapping = convert_to_argo_units(nc, df_all_mapping, filename)

    meta_names, param_names = get_meta_param_names(nc)
    num_profiles = nc.dims['N_PROF']

    for profile_number in range(num_profiles):

        profile_dict = create_profile_dict(nc, profile_number, df_all_mapping, filename, meta_names, param_names)

        write_profile_json(json_dir, profile_number, profile_dict, filename)


def main():

    start_time = datetime.now()

    logging.root.handlers = []
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO , filename='output.log')

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)


    netcdf_data_directory = './data/netcdf'
    json_data_directory = './data/go-ship_json'

    argo_mapping_file = 'argo_go-ship_mapping.csv'

    argo_units_mapping_file = 'argo_goship_units_mapping.csv'    

    for root, dirs, files in os.walk(netcdf_data_directory):

        for filename in files:

            # With TEMP var and other temperature vars
            #filename = 'pr15_jj_hy1.csv.nc'

            # Without TEMP var and other temperature vars
            #filename = '11231_ctd.nc'

            if not filename.endswith('.nc'):
                continue

            fin = os.path.join(root, filename)

            nc = xr.load_dataset(fin)

            create_json(nc, json_data_directory, filename, argo_mapping_file, argo_units_mapping_file)

            #exit(1)

    logging.info(datetime.now() - start_time)


if __name__ == '__main__':
    main()

