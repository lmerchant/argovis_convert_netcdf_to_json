import xarray as xr
import pandas as pd
import numpy as np
import os
from datetime import datetime
import json


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


def add_extra_meta_data(meta_dict, profile_number, filename):

    new_meta_dict = meta_dict.copy()

    new_meta_dict['N_PROF'] = profile_number + 1
    new_meta_dict['_id'] = f"{meta_dict['expocode']}_NPROF_{profile_number + 1}"
    new_meta_dict['id'] = f"{meta_dict['expocode']}_NPROF_{profile_number + 1}"
    new_meta_dict['POSITIONING_SYSTEM'] = 'GPS'
    new_meta_dict['DATA_CENTRE'] = 'CCHDO'
    new_meta_dict['cruise_url'] = f"https://cchdo.ucsd.edu/cruise/{meta_dict['expocode']}"
    new_meta_dict['data_filename'] = filename
    new_meta_dict['netcdf_url'] = ''

    new_meta_dict['date_formatted'] = meta_dict['time'].strftime("%Y-%m-%d")

    # Remove time, convert to iso and call date
    new_meta_dict['time'] = meta_dict['time'].isoformat()

    latitude = meta_dict['lat']
    longitude = meta_dict['lon']

    geo_dict = get_geo_information(latitude, longitude)

    new_meta_dict = {**new_meta_dict, **geo_dict}

    return new_meta_dict


def get_argo_standard_units(argo_mapping_file):

    # Then get mapping of standard names to Argo names
    df_argo_mapping = pd.read_csv(argo_mapping_file)

    # Only use unique standard names not '-'

    # So find all rows where cf_standard_name != '-'
    df_argo_standard_names = df_argo_mapping[(df_argo_mapping['cf_standard_name'] != '-')].copy()

    # Find results with unique standard names
    df_argo_unique_names = df_argo_standard_names.drop_duplicates('cf_standard_name')

    df_argo_standards_units = df_argo_unique_names[['cf_standard_name', 'unit']].copy()

    # Rename unit column to argo_unit
    df_argo_standards_units = df_argo_standards_units.rename(columns={'unit': 'argo_unit'})

    df_argo_standards_units = df_argo_standards_units.reset_index(drop=True)

    # Remove misplaced column of degrees_Celsius
    df_argo_standards_units.dropna(inplace=True)


    return (df_argo_standards_units, df_argo_unique_names)


def convert_sea_water_temp(df_temperature):

    print('Converting temperature to ITS-90 scale')

    return df_temperature


def convert_to_argo_units(nc, df_combined_standard_names_units, conversion_functions):

    # ------ Sea Water Temperature -------

    # Check sea_water_temperature to be degree_Celsius and
    # have go_ship_ref_scale be ITS-90
    row = df_combined_standard_names_units[(df_combined_standard_names_units['cf_standard_name'] == 'sea_water_temperature')]

    same_units = (row['goship_unit'] == row['argo_unit']).bool()
    ITS_90_scale = (row['goship_ref_scale'] == 'ITS-90').bool()

    if not same_units:
        # Check if no argo units, if so, use go-ship units
        goship_unit = row['goship_unit'].values[0]
        argo_unit = row['argo_unit'].values[0]

        if not argo_unit:
            unit = goship_unit

        if goship_unit:
            # map units to argo format
            pass

        # Need way to know what units are and how to 
        # convert from one unit to another
    
        # Convert units (when would this be the case for temp?)
        print('convert temperature units to degree Celsius')

    # # Now that units the same, check reference scales
    # if not ITS_90_scale:
    #     # Convert df_go_ship['sea_water_temperature'] column
    #     # Have units in degree_Celsius
    #     print('convert to ITS_90 scale')

    #     func = conversion_functions['sea_water_temperature']
    #     df_go_ship['sea_water_temperature'] = func(df_go_ship['sea_water_temperature'])

    
    return nc


def get_go_ship_mapping_df(nc, names):

    name_to_units = {}
    name_to_standard = {}
    name_to_ref_scale = {}

    for name in names:
        try:
            name_to_units[name] = nc[name].attrs['units']
        except KeyError:
            name_to_units[name] = ''

        try:
            standard_name = nc[name].attrs['standard_name']
            name_to_standard[name] = standard_name

            if standard_name == 'status_flag':
                non_qc_name = name.replace('_qc', '')
                non_qc_standard_name = nc[non_qc_name].attrs['standard_name']  
                name_to_standard[name] = f"{non_qc_standard_name}_qc"

        except KeyError:
            name_to_standard[name] = name

        try:
            name_to_ref_scale[name] = nc[name].attrs['reference_scale']
        except KeyError:
            name_to_ref_scale[name] = 'unknown'


    df_dict = {}
    df_dict['cf_standard_name'] = name_to_standard
    df_dict['goship_unit'] = name_to_units
    df_dict['goship_ref_scale'] = name_to_ref_scale

    df = pd.DataFrame.from_dict(df_dict).reset_index()

    df = df.rename(columns = {'index':'goship_name'})

    return df


def get_argo_mapping_df(argo_mapping_file):

    # Then get mapping of standard names to Argo names
    df_argo_mapping = pd.read_csv(argo_mapping_file)

    # So find all rows where cf_standard_name != '-'
    df_argo_standard_names = df_argo_mapping[(df_argo_mapping['cf_standard_name'] != '-')]

    # Find results with unique standard names since
    # will be making a key with standard name
    df_argo_standard_names = df_argo_standard_names.drop_duplicates('cf_standard_name')

    df_argo_standard_names_units = df_argo_standard_names[['parameter name', 'cf_standard_name', 'unit']].copy()

    # Rename unit column to argo_unit
    df_argo_standard_names_units = df_argo_standard_names_units.rename(columns={'unit': 'argo_unit', 'parameter name': 'argo_name'})

    df_argo_standard_names_units = df_argo_standard_names_units.reset_index(drop=True)

    # Remove misplaced column of degrees_Celsius
    df_argo_standard_names_units.dropna(inplace=True)

    return df_argo_standard_names_units


def rename_to_argovis_names(nc):

    # argovis changes latitude, longitude into lat, lon
    name_dict = {'latitude': 'lat', 'longitude': 'lon'}

    nc = nc.rename(name_dict)

    return nc


def rename_to_argo_standard_names(nc, df_combined_standard_names_units):

   # rename variables to match Argo in dataset nc

    # Get profile name to Argo name mapping
    df_argo_name_mapping = df_combined_standard_names_units[['argo_name', 'goship_name']]

    # Remove any mapping where Argo name doesn't exist
    df_argo_name_mapping = df_argo_name_mapping.dropna()


    # Add in qc_variables if none in Argo 
    df_qc = pd.DataFrame()

    for row_index, row in df_argo_name_mapping.iterrows(): 

        qc_goship = f"{row['goship_name']}_qc"
        qc_argo = f"{row['argo_name']}_qc"

        not_in_goship = (df_combined_standard_names_units[df_combined_standard_names_units['goship_name'] == qc_goship]).empty

        if not_in_goship:
            continue

        df_qc = df_qc.append({'goship_name': qc_goship, 'argo_name': qc_argo}, ignore_index=True)

    # append qc names to argo mapping df
    df_argo_name_mapping = df_argo_name_mapping.append(df_qc, ignore_index=True)

    # Fix mapping so argo names aren't duplicated
    # If goship_name == 'oxygen' and 'oxygen_qc', don't
    # convert to an argo name. Delete from mapping

    drop_list = ['oxygen', 'oxygen_qc']

    for name in drop_list:
        df_argo_name_mapping.drop(df_argo_name_mapping[df_argo_name_mapping['goship_name'] == name].index, inplace=True)

    argo_name_mapping_dict = dict(zip(df_argo_name_mapping['goship_name'], df_argo_name_mapping['argo_name'])) 

    #nc = nc.rename_vars(name_dict=argo_name_mapping_dict)

    # Rename to standard names?

    # Change following standard names so don't conflict 
    # when renaming vars to standard names
    revert_list = ['bottle_salinity', 'bottle_salinity_qc', 'ctd_salinity', 'ctd_salinity_qc']


    for name in revert_list:

        df_combined_standard_names_units.loc[df_combined_standard_names_units['goship_name'] == name, 'cf_standard_name'] = name


    standard_name_mapping_dict = dict(zip(df_combined_standard_names_units['goship_name'], df_combined_standard_names_units['cf_standard_name'])) 



    # Overwrite with argo_name_mapping_dict values
    mapping_dict = {**standard_name_mapping_dict, **argo_name_mapping_dict}

    nc = nc.rename_vars(name_dict=mapping_dict)

    return nc, mapping_dict


def rename_units_to_argo_units(nc):

    # Rename goship_units to argo_units naming convention
    mapping_units_to_argo = {}
    mapping_units_to_argo['degC'] = 'degree_Celsius'
    mapping_units_to_argo['dbar'] = 'decibar'
    mapping_units_to_argo['umol/kg'] = 'micromole/kg'

    # Change attr field units
    for var in nc:
        try:
            var_unit = nc[var].attrs['units']
            argo_unit = mapping_units_to_argo[var_unit]
            nc[var].attrs['units'] = argo_unit

        except KeyError:
            pass

    # Change coord field units
    for coord in nc.coords:
        try:
            coord_unit = nc.coords[coord].attrs['units']
            argo_unit = mapping_units_to_argo[coord_unit]
            nc.coords[coord].attrs['units'] = argo_unit

        except KeyError:
            pass

    return nc


def create_profile_dict(nc, profile_number, df_meta_standard_names_units, df_param_standard_names_units, filename):

    profile = nc.isel(N_PROF=[profile_number])

    df_profile = profile.to_dataframe()

    meta_names = df_meta_standard_names_units['goship_name'].values

    # Get metadata subset of df_profile
    df_meta = df_profile[meta_names]

    # And since all rows the same for meta, just
    # select first row
    meta_dict = df_meta.iloc[0].to_dict()

    new_meta_dict = add_extra_meta_data(meta_dict, profile_number, filename)

    # Get param data subset of df_profile
    #param_dict = df_profile[param_names].to_dict()

    df_params = df_profile.drop(columns=meta_names)

    meta_units_dict = dict(zip(df_meta_standard_names_units['goship_name'], df_meta_standard_names_units['goship_unit'])) 

    param_units_dict = dict(zip(df_param_standard_names_units['goship_name'], df_param_standard_names_units['goship_unit']))

    units_dict = {**meta_units_dict, **param_units_dict}

    # Turn dataframe into a dict row by row keeping type of col
    params_dict_array = df_params.astype(object).to_dict(orient='records')  

    # param names without qc vars
    measurement_names = [name for name in df_param_standard_names_units['goship_name'] if not name.endswith('qc')]


    #---------

    profile_dict = new_meta_dict

    profile_dict['measurements'] = params_dict_array   
    profile_dict['units'] = units_dict
    profile_dict['station_parameters_in_nc'] = measurement_names

    return profile_dict


def write_profile_json(json_dir, profile_number, profile_dict):

    # Write profile_dict to as json to a profile file

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable
    profile_json = json.dumps(profile_dict, default=convert)

    expocode = profile_dict['expocode']
    filename = f"{expocode}_N_PROF_{profile_number + 1}.json"

    file = os.path.join(json_dir,filename)

    with open(file, 'w') as f:
        json.dump(profile_dict, f, indent=4, sort_keys=True, default=convert)


def create_json(nc, json_dir, filename, argo_mapping_file):

    # Rename lat and lon
    nc = rename_to_argovis_names(nc)

    # Get meta and param names
    meta_names = [coord for coord in nc.coords if 'N_LEVELS' not in nc.coords[coord].dims]
    param_names = [coord for coord in nc.coords if 'N_LEVELS' in nc.coords[coord].dims]

    meta_names2 = [var for var in nc if 'N_LEVELS' not in nc[var].dims]
    param_names2 = [var for var in nc if 'N_LEVELS' in nc[var].dims]

    meta_names.extend(meta_names2)
    param_names.extend(param_names2)

    df_argo_standard_names_units = get_argo_mapping_df(argo_mapping_file)
    df_meta_standard_names_units = get_go_ship_mapping_df(nc, meta_names)
    df_param_standard_names_units = get_go_ship_mapping_df(nc, param_names)

    # Combine argo and goship params into one df
    df_combined_standard_names_units = df_param_standard_names_units.merge(df_argo_standard_names_units,how='left', left_on='cf_standard_name', right_on='cf_standard_name')

    # rename variables to match Argo in dataset nc
    nc, mapping_dict = rename_to_argo_standard_names(nc, df_combined_standard_names_units)

    param_names = list(mapping_dict.values())

    # ----------------

    # Rename units to argo units to compare if units 
    # are different and should be converted
    nc = rename_units_to_argo_units(nc)

    # Get new mapping of params where argo names/units included
    df_param_standard_names_units = get_go_ship_mapping_df(nc, param_names)

    # Combine argo and goship mapping into one df
    df_combined_standard_names_units = df_param_standard_names_units.merge(df_argo_standard_names_units,how='left', left_on='cf_standard_name', right_on='cf_standard_name')

    conversion_functions = {}
    conversion_functions['sea_water_temperature'] = convert_sea_water_temp

    nc = convert_to_argo_units(nc, df_combined_standard_names_units, conversion_functions)


    num_profiles = nc.dims['N_PROF']

    for profile_number in range(num_profiles):

        profile_dict = create_profile_dict(nc, profile_number, df_meta_standard_names_units, df_param_standard_names_units, filename)

        write_profile_json(json_dir, profile_number, profile_dict)

        exit(1)


def main():

    netcdf_data_directory = './data/netcdf'
    json_data_directory = './data/go-ship_json'

    for root, dirs, files in os.walk(netcdf_data_directory):

        for filename in files:

            filename = '18HU19940524_hy1.csv.nc'

            argo_mapping_file = 'argo-parameters-list-standard-names.csv'

            if not filename.endswith('.nc'):
                continue

            fin = os.path.join(root, filename)

            nc = xr.load_dataset(fin)

            create_json(nc, json_data_directory, filename, argo_mapping_file)

            exit(1)


if __name__ == '__main__':
    main()

