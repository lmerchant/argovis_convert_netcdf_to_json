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


def add_extra_meta_data(df_meta, profile_number, filename):

    # Since all rows the same for meta, just
    # select first row

    meta_dict = df_meta.iloc[0].to_dict()

    # Want to substring in expocode without overwriting value in meta_dict

    expocode = meta_dict['expocode']

    if '/' in expocode:

        expocode = expocode.replace('/', '_')
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

    elif expocode == 'None':
        logging.info(filename)
        logging.info('expocode is None')

        cruise_url = ''

    else:
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

    profile_type = meta_dict['profile_type']
    if profile_type == 'B':
        profile_type = 'BOT'
    elif profile_type == 'C':
        profile_type = 'CTD'

    _id = f"{profile_type}_{expocode}_NPROF_{profile_number + 1}"

    meta_dict['N_PROF'] = profile_number + 1
    meta_dict['_id'] = _id
    meta_dict['id'] = _id
    meta_dict['POSITIONING_SYSTEM'] = 'GPS'
    meta_dict['DATA_CENTRE'] = 'CCHDO'
    meta_dict['cruise_url'] = cruise_url
    meta_dict['data_filename'] = filename
    meta_dict['profile_type'] = profile_type
    meta_dict['netcdf_url'] = ''
    meta_dict['date_formatted'] = meta_dict['date'].strftime("%Y-%m-%d")

    # Convert date to iso
    meta_dict['date'] = meta_dict['date'].isoformat()

    latitude = meta_dict['lat']
    longitude = meta_dict['lon']

    geo_dict = get_geo_information(latitude, longitude)

    meta_dict = {**meta_dict, **geo_dict}

    return meta_dict


def write_profile_json(json_dir, profile_number, profile_dict):

    # Write profile_dict to as json to a profile file

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable
    profile_json = json.dumps(profile_dict, default=convert)

    profile_id = profile_dict['id']
    filename = profile_id + '.json'

    profile_pieces = profile_id.split('_NPROF')
    folder_name = profile_pieces[0]

    # Now remove BOT_ or CTD_ from folder name
    folder_name = folder_name[4:]

    path = os.path.join(json_dir, folder_name)

    if not os.path.exists(path):
        os.makedirs(path)
    
    file = os.path.join(json_dir, folder_name, filename)

    # TODO Remove formatting when final
    with open(file, 'w') as f:
        json.dump(profile_dict, f, indent=4, sort_keys=True, default=convert)


# def write_profile_json(json_dir, profile_number, profile_dict, nc_filename):

#     # Write profile_dict to as json to a profile file

#     # use convert function to change numpy int values into python int
#     # Otherwise, not serializable
#     profile_json = json.dumps(profile_dict, default=convert)

#     filename = f"{nc_filename}_NPROF_{profile_number + 1}.json"
#     folder_name = f"{nc_filename}_dir"

#     path = os.path.join(json_dir, folder_name)

#     if not os.path.exists(path):
#         os.makedirs(path)
        

#     file = os.path.join(json_dir, folder_name, filename)

#     # TODO Remove formatting when final
#     with open(file, 'w') as f:
#         json.dump(profile_dict, f, indent=4, sort_keys=True, default=convert)


def create_profile_dict(nc, profile_number, df_all_mapping, filename, meta_names, param_names):

    profile = nc.isel(N_PROF=[profile_number])
    df_profile = profile.to_dataframe()

    # Get metadata and parameter subsets of df_profile
    df_meta = df_profile[meta_names]
    df_params = df_profile[param_names]


    meta_dict = add_extra_meta_data(df_meta, profile_number, filename)

    names_in_orig = df_all_mapping['goship_name'].values.tolist()


    df_meta_mapping = df_all_mapping.loc[df_all_mapping['name'].isin(meta_names)]
    df_meta_mapping_names = df_meta_mapping[['goship_name', 'name']].copy()

    meta_mapping_dict = dict(zip(df_meta_mapping_names['goship_name'], df_meta_mapping_names['name']))    


    df_param_mapping = df_all_mapping.loc[df_all_mapping['name'].isin(param_names)]
    df_param_mapping_names = df_param_mapping[['goship_name', 'name']].copy()

    param_mapping_dict = dict(zip(df_param_mapping_names['goship_name'], df_param_mapping_names['name']))

    # Do both meta and params
    all_mapping_dict = dict(zip(df_all_mapping['goship_name'], df_all_mapping['name']))
    

    # Turn dataframe into a dict row by row keeping type of col
    params_dict_array = df_params.astype(object).to_dict(orient='records')  


    # Drop rows without units
    # df_units = df_all_mapping[['name', 'unit']].copy()
    # df_units = df_units[df_units['unit'] != '']
    # units_dict = dict(zip(df_units['name'], df_units['unit']))    

    # show param units with lat, lon
    # df_param_units = df_param_mapping[['name', 'unit']].copy()
    # df_lat_lon_units = df_all_mapping.loc[df_all_mapping['name'].isin(['lat', 'lon'])][['name', 'unit']]
    # df_units = pd.concat([df_lat_lon_units, df_param_units], ignore_index=True)
    # units_argo_dict = dict(zip(df_units['name'], df_units['unit']))
    # units_nc_dict = dict(zip(df_units['goship_name'], df_units['goship_unit']))

    units_argo_dict = dict(zip(df_all_mapping['name'], df_all_mapping['unit']))
    units_nc_dict = dict(zip(df_all_mapping['goship_name'], df_all_mapping['goship_unit']))

    refscales_argovis_dict = dict(zip(df_all_mapping['name'], df_all_mapping['reference_scale']))

    refscales_nc_dict = dict(zip(df_all_mapping['goship_name'], df_all_mapping['goship_reference_scale']))

    c_format_nc_dict = dict(zip(df_all_mapping['goship_name'], df_all_mapping['goship_c_format']))

    profile_dict = meta_dict
    profile_dict['measurements'] = params_dict_array
    profile_dict['parameter_units_in_nc'] = units_nc_dict
    profile_dict['parameter_units_in_argovis'] = units_argo_dict

    profile_dict['parameter_ref_scales_in_nc'] = refscales_nc_dict
    profile_dict['parameter_ref_scales_in_argovis'] = refscales_argovis_dict

    #profile_dict['station_parameters_in_nc'] = names_in_orig
    #profile_dict['parameters_in_nc_mapping_to_argovis'] = param_mapping_dict
    profile_dict['parameter_in_nc_mapping_to_argovis'] = all_mapping_dict

    profile_dict['parameter_c_formats_in_nc'] = c_format_nc_dict

    return profile_dict


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


def check_if_all_ctd_vars(nc, filename):

    # Is ctd if have both ctd temperature and pressure

    coord_names = list(nc.coords)
    var_names = list(nc.keys())  
    names = [*coord_names, *var_names]

    # TODO does it work to just loop through names?

    is_pres = False
    is_ctd_temp_w_refscale = False
    is_ctd_temp_w_no_refscale = False
    is_temp_w_no_ctd = False

    name_to_units = {}
    name_to_ref_scale = {}

    names_ctd_temps_no_refscale = []
    names_no_ctd_temps = []

    # Check for pressure
    for name in coord_names:

        # From name mapping earlier, goship pressure name mapped to Argo equivalent
        if name == 'PRES':
            is_pres = True 

        try:
            name_to_units[name] = nc.coords[name].attrs['units']
        except KeyError:
            name_to_units[name] = ''

        try:
            name_to_ref_scale[name] = nc.coords[name].attrs['reference_scale']
        except KeyError:
            name_to_ref_scale[name] = ''

    for name in var_names:

        # From name mapping earlier, goship temperature name mapped to Argo equivalent
        if name == 'TEMP':
            is_ctd_temp_w_refscale = True

        # From name mapping earlier, if didn't find reference scale for temperature
        # but was a ctd temperature, renamed to TEMP_no_refscale
        if name == 'TEMP_no_refscale':
            is_ctd_temp_w_no_refscale = True

        try:
            name_to_units[name] = nc[name].attrs['units']
        except KeyError:
            name_to_units[name] = ''

        try:
            name_to_ref_scale[name] = nc[name].attrs['reference_scale']
            if name_to_ref_scale[name] == 'unknown':
                name_to_ref_scale[name] = ''
        except KeyError:
            name_to_ref_scale[name] = ''

        unit = name_to_units[name]

        if name !='TEMP' and name != 'TEMP_no_refscale' and unit=='Celsius':
            is_temp_w_no_ctd = True


    expocode = nc.coords['expocode'].values[0]

    log_output = False

    if not is_pres or not is_ctd_temp_w_refscale:   

        logging.info('===========')
        logging.info('EXCEPTIONS FOUND')
        logging.info(expocode)
        logging.info(filename)   


    if not is_pres:
        logging.info('missing PRES')   
        with open('files_no_pressure.csv', 'a') as f:
            f.write(f"{expocode}, {filename} \n")

    if is_ctd_temp_w_no_refscale and is_ctd_temp_w_refscale:  
        logging.info("has both CTD with and without ref scale") 

    if not is_ctd_temp_w_refscale and is_ctd_temp_w_no_refscale:  
        logging.info('CTD Temp with no ref scale')
        # Write to file listing files without ctd variables
        with open('files_ctd_temps_no_refscale.csv', 'a') as f:
            f.write(f"{expocode}, {filename} \n") 

    if not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale: 
        logging.info('NO CTD Temp')

        # Write to file listing files without ctd variables
        with open('files_no_ctd_temps.csv', 'a') as f:
            f.write(f"{expocode}, {filename} \n") 


    if not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale and not is_temp_w_no_ctd:
        logging.info('NO TEMPS')


    if not is_pres or (not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale): 

        logging.info('===========')
        logging.info('OTHER PRES/TEMP VARS')
        logging.info(expocode)
        logging.info(filename)    

        for name in names:

            unit = name_to_units[name]
            ref_scale = name_to_ref_scale[name]

            if name != 'PRES' and unit=='decibar': 
                logging.info('Not CTDPRS and has decibar unit')  
                logging.info(f"{name} with Ref scale {ref_scale}")      

            if name != 'TEMP' and name != 'TEMP_no_refscale' and unit == 'Celsius':
                logging.info('Not CTDTMP and has Celsius unit')  
                logging.info(f"{name} with Ref scale {ref_scale}")  


    # Look for vars with a unit of one (PSAL already converted to psu unit)
    if '1' in name_to_units.values():

        logging.info('===========')
        logging.info('VARS with Unit = 1')
        logging.info(expocode)
        logging.info(filename)   

        for name in names:

            unit = name_to_units[name]
            ref_scale = name_to_ref_scale[name]

            if unit == '1':  
                logging.info(f"{name} with Ref scale {ref_scale}")                     

    return is_ctd_temp_w_refscale, is_ctd_temp_w_no_refscale


def convert_sea_water_temp(var, nc):

    # Check sea_water_temperature to be degree_Celsius and
    # have goship_ref_scale be ITS-90

    # So look for ref_scale = IPTS-68 or ITS-90   

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    # Change this to work for all temperature names

    reference_scale = 'unknown'

    try:

        temperature = nc[var].data
        nc_ref_scale = nc[var].attrs['reference_scale']

        if nc_ref_scale == 'IPTS-68':

            # Convert to ITS-90 scale 
            temperature90 = temperature/1.00024

            # Set nc var of TEMP to this value
            nc[var].data = temperature90

            nc[var].attrs['reference_scale'] = 'IPT-90'

            #df.loc[df['name'] == var, 'reference_scale'] = 'IPT-90'
            reference_scale = 'IPT-90'

        else:
            print('temperature not IPTS-68')
            reference_scale = 'unknown'

        return nc, reference_scale

    except KeyError:

        return nc, reference_scale


def convert_units_add_ref_scale(nc, df_all_mapping, filename):

    # If argo ref scale different from goship, convert
    # df_goship_ref_scale = df_all_mapping[['name', 'goship_reference_scale']].copy()
    # df_argo_ref_scale = df_all_mapping[['name', 'argo_reference_scale']].copy()

    # goship_ref_scale = dict(df_goship_ref_scale.values.tolist())
    # argo_ref_scale = dict(df_argo_ref_scale.values.tolist())

    df_all_mapping['reference_scale'] = 'unknown'

    coord_names = list(nc.coords)
    var_names = list(nc.keys())  

    for var in var_names:

        new_ref_scale = 'unknown'

        # If argo ref scale not equal to goship ref scale, convert  
        row = df_all_mapping.loc[df_all_mapping['name'] == var]        
        
        goship_ref_scale = row['goship_reference_scale'].values[0]
        argo_ref_scale = row['argo_reference_scale'].values[0]

        argo_is_nan = pd.isnull(argo_ref_scale)

        if goship_ref_scale == 'unknown':
            new_ref_scale = 'unknown'
            
        if goship_ref_scale == argo_ref_scale:
            new_ref_scale = goship_ref_scale

        if (goship_ref_scale != argo_ref_scale) and not argo_is_nan:
            # Convert to argo ref scale
            # then save this to add to new reference_scale column
            if argo_ref_scale == 'IPT-90' and goship_ref_scale == 'IPTS-68':
                # convert seawater temperature
                nc, new_ref_scale = convert_sea_water_temp(var, nc)
            else:
                print(var)
                print(f"argo ref scale: {argo_ref_scale}")
                print(f"goship ref scale: {goship_ref_scale}")
                new_ref_scale = 'UNKNOWN conversion'
        
        df_all_mapping.loc[df_all_mapping['name'] == var, 'reference_scale'] = new_ref_scale

    return nc, df_all_mapping


def get_new_unit_name(var, df_all_mapping, mapping_ref_scale_dict, mapping_unit_dict):

    # New unit name is argo, but if no argo name, use goship name

    row = df_all_mapping.loc[df_all_mapping['name'] == var]

    goship_unit = row['goship_unit'].values[0]
    argo_unit = row['argo_unit'].values[0]

    goship_ref_scale = row['goship_reference_scale'].values[0] 

    mapped_argo_ref_scale = mapping_ref_scale_dict.get(argo_unit, None)
    ref_scale_same = mapped_argo_ref_scale == goship_ref_scale

    mapped_unit = mapping_unit_dict.get(goship_unit, None)

    # If have a goship unit, map it to an argo unit if one exists
    if not pd.isnull(goship_unit):

        if ref_scale_same and goship_unit == '1':
            # covers case of salinity with PSS-78 ref scale
            # and goship unit = 1 which argo is psu
            new_unit = mapped_unit

        elif goship_unit == '1':
            new_unit = goship_unit

        elif mapping_unit_dict.get(goship_unit, None):
            new_unit = mapped_unit
        else:
            new_unit = goship_unit
    else:
        new_unit = goship_unit

    return new_unit


def rename_units(nc, argo_goship_units_mapping_file, df_all_mapping):

    df = pd.read_csv(argo_goship_units_mapping_file)
    mapping_unit_dict = dict(zip(df['goship_unit'], df['argo_unit'])) 
    mapping_ref_scale_dict = dict(zip(df['argo_unit'], df['reference_scale']))

    new_param_units = {}

    for var in nc:

        new_unit = get_new_unit_name(var, df_all_mapping, mapping_ref_scale_dict, mapping_unit_dict)

        nc[var].attrs['units'] = new_unit
        new_param_units[var] = new_unit 


    new_meta_units = {}

    for var in nc.coords:

        new_unit = get_new_unit_name(var, df_all_mapping, mapping_ref_scale_dict, mapping_unit_dict)

        nc.coords[var].attrs['units'] = new_unit
        new_meta_units[var] = new_unit 
             

    new_units = {**new_meta_units, **new_param_units}
    df_new_units = pd.DataFrame(new_units.items())
    df_new_units.columns = ['name', 'unit']

    df_all_mapping = df_all_mapping.merge(df_new_units,how='left', left_on='name', right_on='name')


    return nc, df_all_mapping


def rename_to_argo_names(nc, df, filename):

    # Now rename all coords and vars in nc

    # Look at var names to rename temperature and salinity
    # depending on goship name because there are multiple types
    var_names = list(nc.keys())

    is_ctd_temp = False
    is_ctd_temp_68 = False
    is_ctd_temp_unknown = False

    is_ctd_sal = False
    is_bottle_sal = False

    for name in var_names:

        # if both ctd_temperature and ctd_temperature_68, 
        # use ctd_temperature to TEMP only
        if name == 'ctd_temperature':
            is_ctd_temp = True
        if name == 'ctd_temperature_68':
            is_ctd_temp_68 = True
        if name == 'ctd_temperature_unk':
            is_ctd_temp_unknown = True
        if name == 'ctd_salinity':
            is_ctd_sal = True
        if name == 'bottle_salinity':
            is_bottle_sal = True

    # Create new column with new names. Start as argo names if exist
    df['name'] = df['argo_name']    

    # if argo name is nan, use goship name
    df['name'] = np.where(df['argo_name'].isna(), df['goship_name'], df['name'])

    # if both temp on ITS-90 scale and temp on IPTS-68 scale, 
    # just use ctd_temperature. Don't change name of ctd_temperature_68
    if not is_ctd_temp and is_ctd_temp_68:
        # change name to TEMP and convert to ITS-90 scale later
        df.loc[df['goship_name'] == 'ctd_temperature_68', 'name'] = \
            'TEMP'
        df_qc = df.isin({'goship_name': ['ctd_temperature_68_qc']}).any()

        if df_qc.any(axis=None):
            df.loc[df['goship_name'] == 'ctd_temperature_68_qc', 'name'] = \
            'TEMP_qc'

    elif not is_ctd_temp and not is_ctd_temp_68 and is_ctd_temp_unknown:
        df.loc[df['goship_name'] == 'ctd_temperature_unk', 'name'] = \
            'TEMP_no_refscale'

        df_qc = df.isin({'goship_name': ['ctd_temperature_unk_qc']}).any()
        if df_qc.any(axis=None):
            df.loc[df['goship_name'] == 'ctd_temperature_unk_qc', 'name'] = \
            'TEMP_no_refscale_qc'    


    # if both salinities, use ctd_salinity and use name bottle_salinity
    # if no ctd_sal and is_bottle_sal, already renamed bottle_sal to 
    # PSAL_bottle
    if is_ctd_sal and is_bottle_sal:
        df.loc[df['goship_name'] == 'bottle_salinity', 'name'] = \
            'bottle_salinity'

        df_qc = df.isin({'goship_name': ['bottle_salinity_qc']}).any()
        if df_qc.any(axis=None):
            df.loc[df['goship_name'] == 'bottle_salinity_qc', 'name'] = \
            'bottle_salinity_qc'                


    # Create mapping dicts to go from goship name to new name
    # Will map both coordinate and variable names
    mapping_dict = dict(zip(df['goship_name'], df['name'])) 
    nc = nc.rename_vars(name_dict=mapping_dict)

    return nc, df


def add_qc_names_to_argo_names(df):

    # If goship name has a qc, rename corresponding argo name to argo name qc
    goship_qc_names = df.loc[df['goship_name'].str.contains('_qc')]

    for goship_name in goship_qc_names.iloc[:,0].tolist():

        # find argo name of goship name without qc
        goship_base_name = goship_name.replace('_qc','')
        argo_name = df.loc[df['goship_name'] == goship_base_name, 'argo_name']

        # If argo_name not empty, add qc name
        if pd.notna(argo_name.values[0]): 
            df.loc[df['goship_name'] == goship_name, 'argo_name'] = argo_name.values[0] + '_qc'

    return df


def get_argo_mapping_df(argo_name_mapping_file):

    df = pd.read_csv(argo_name_mapping_file)

    df = df[['argo_name', 'argo_unit', 'argo_reference_scale', 'goship_name']].copy()

    df['argo_reference_scale'] = df['argo_reference_scale'].replace(np.nan, 'unknown')

    return df


def get_goship_mapping_df(nc):

    coord_names = list(nc.coords)
    var_names = list(nc.keys())
 
    name_to_units = {}
    name_to_ref_scale = {}
    name_to_c_format = {}

    for name in coord_names:
        # map name to units if units exist
        try:
            name_to_units[name] = nc.coords[name].attrs['units']
        except KeyError:
            name_to_units[name] = ''

        try:
            name_to_ref_scale[name] = nc.coords[name].attrs['reference_scale']
        except KeyError:
            name_to_ref_scale[name] = 'unknown'

        try:
            name_to_c_format[name] = nc.coords[name].attrs['C_format']
        except KeyError:
            name_to_c_format[name] = ''

    for name in var_names:
        # map name to units if units exist
        try:
            name_to_units[name] = nc[name].attrs['units']
        except KeyError:
            name_to_units[name] = ''

        try:
            name_to_ref_scale[name] = nc[name].attrs['reference_scale']
        except KeyError:
            name_to_ref_scale[name] = 'unknown'

        try:
            name_to_c_format[name] = nc[name].attrs['C_format']
        except KeyError:
            name_to_c_format[name] = ''


    df_dict = {}
    df_dict['goship_unit'] = name_to_units
    df_dict['goship_reference_scale'] = name_to_ref_scale
    df_dict['goship_c_format'] = name_to_c_format

    df = pd.DataFrame.from_dict(df_dict)
    df.index.name = 'goship_name'
    df = df.reset_index()

    return df


def create_json(nc, json_dir, filename, argo_name_mapping_file, argo_units_mapping_file):

    df_goship_mapping = get_goship_mapping_df(nc)
    df_argo_mapping = get_argo_mapping_df(argo_name_mapping_file)

    # Any empty cells filled with nan
    df_all_mapping = df_goship_mapping.merge(df_argo_mapping,how='left', left_on='goship_name', right_on='goship_name')

    # Rename index created when merging on goship_name
    df_all_mapping = df_all_mapping.rename(columns = {'index':'goship_name'})

    # Example, argo name is TEMP, add TEMP_qc if corresponding goship name has qc
    df_all_mapping = add_qc_names_to_argo_names(df_all_mapping)

    nc, df_all_mapping = rename_to_argo_names(nc, df_all_mapping, filename)

    nc, df_all_mapping = rename_units(nc, argo_units_mapping_file, df_all_mapping)

    # If different reference scale, convert to Argo scale
    nc, df_all_mapping = convert_units_add_ref_scale(nc, df_all_mapping, filename)

    # Check if all ctd vars available pressure, temperature
    # PRES, TEMP
    is_ctd_temp_w_refscale, is_ctd_temp_w_no_refscale = check_if_all_ctd_vars(nc, filename)



    # Skip making json if no expocode, if not ctd with ref scale,
    # and no ctd

    if not is_ctd_temp_w_refscale and is_ctd_temp_w_no_refscale: 
        # files with ctd temps but no ref scale
        return

    elif not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale:
        # files with no ctd temp 
        return

    expocode = nc.coords['expocode']
    if expocode.values[0] == 'None':
        return


    meta_names, param_names = get_meta_param_names(nc)
    num_profiles = nc.dims['N_PROF']

    for profile_number in range(num_profiles):

        profile_dict = create_profile_dict(nc, profile_number, df_all_mapping, filename, meta_names, param_names)

        write_profile_json(json_dir, profile_number, profile_dict)
        #write_profile_json(json_dir, profile_number, profile_dict, filename)

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

    argo_name_mapping_file = 'argo_go-ship_mapping.csv'
    argo_units_mapping_file = 'argo_goship_units_mapping.csv'    

    for root, dirs, files in os.walk(netcdf_data_directory):

        for filename in files:

            # ctd_temperature_68 only
            #filename = '33KA004_1_hy1.csv.nc'

            #only bottle salinity
            # no ctd temperature
            # Is this renamed PSAL or kept as PSAL_bottle?
            #filename = 's05_hy1.csv.nc'

            # expocode with both bot and ctd = 32NH047_1
            # Bottle file name = 32NM047_1_hy1.csv.nc
            # CTD file name = 7790_ctd.nc

            # if not filename == '32NM047_1_hy1.csv.nc' and not filename == '7790_ctd.nc':
            #     continue

            if not filename.endswith('.nc'):
                continue

            print('-------------')
            print(filename)
            print('-------------')


            fin = os.path.join(root, filename)

            nc = xr.load_dataset(fin)

            create_json(nc, json_data_directory, filename, argo_name_mapping_file, argo_units_mapping_file)

            #exit(1)

    logging.info(datetime.now() - start_time)


if __name__ == '__main__':
    main()

