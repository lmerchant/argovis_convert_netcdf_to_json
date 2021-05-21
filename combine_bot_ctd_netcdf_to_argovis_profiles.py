import xarray as xr
import pandas as pd
import numpy as np
import os
from datetime import datetime
import json
import logging
from decimal import Decimal
from io import StringIO
import re


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


def convert(o):

    if isinstance(o, np.float32):
        return np.float64(o)

    if isinstance(o, np.int8):
        return int(o)


class fakefloat(float):
    def __init__(self, value):
        self._value = value

    def __repr__(self):
        return str(self._value)


# https://stackoverflow.com/questions/1960516/python-json-serialize-a-decimal-object
def defaultencode(o):
    if isinstance(o, Decimal):
        # Subclass float with custom repr?
        return fakefloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")


def write_profile_json(json_dir, profile_number, profile_type, profile_dict):

    # Write profile_dict to as json to a profile file

    # use convert function to change numpy int values into python int
    # Otherwise, not serializable

    # profile_json = json.dumps(profile_dict, default=convert)

    # TODO: check
    # I think profile id value is the expocode?
    # profile id of type CTD_32NH047_1_NPROF_1
    # <profile_type>_<expocode>_NPROF_<profile_number>
    profile_id = profile_dict['id']
    # Split off <profile_type> and replace with passed profile_type
    profile_pieces = profile_id.split('_')
    old_profile_type = profile_pieces[0]
    profile_id = profile_id.replace(old_profile_type, profile_type)

    filename = profile_id + '.json'

    profile_pieces = profile_id.split('_NPROF')
    folder_name = profile_pieces[0]

    path = os.path.join(json_dir, folder_name)

    if not os.path.exists(path):
        os.makedirs(path)

    file = os.path.join(json_dir, folder_name, filename)

    # TODO Remove formatting when final
    with open(file, 'w') as f:
        json.dump(profile_dict, f, indent=4, sort_keys=True, default=convert)


def create_unit_mapping_json_str(df_mapping):

    # Create goship to new units mapping json string
    df_unit_mapping = df_mapping[['goship_unit', 'unit']].copy()
    # Remove rows if all NaN
    df_unit_mapping = df_unit_mapping.dropna()

    unit_mapping_dict = dict(
        zip(df_unit_mapping['goship_unit'], df_unit_mapping['unit']))

    json_str = json.dumps(unit_mapping_dict)

    return json_str


def create_goship_ref_scale_mapping_json_str(df_mapping):

    # Create goship to new names mapping json string
    df_ref_scale_mapping = df_mapping[[
        'goship_name', 'goship_reference_scale']].copy()

    # Remove rows if all NaN
    df_ref_scale_mapping = df_ref_scale_mapping.dropna()

    ref_scale_mapping_dict = dict(
        zip(df_ref_scale_mapping['goship_name'], df_ref_scale_mapping['goship_reference_scale']))

    json_str = json.dumps(ref_scale_mapping_dict)

    return json_str


def create_new_ref_scale_mapping_json_str(df_mapping):

    # Create new names to ref scale mapping json string
    df_ref_scale_mapping = df_mapping[[
        'name', 'reference_scale']].copy()

    # Remove rows if all NaN
    df_ref_scale_mapping = df_ref_scale_mapping.dropna()

    ref_scale_mapping_dict = dict(
        zip(df_ref_scale_mapping['name'], df_ref_scale_mapping['reference_scale']))

    json_str = json.dumps(ref_scale_mapping_dict)

    return json_str


def create_name_mapping_json_str(df_mapping):

    # Create goship to new names mapping json string
    df_name_mapping = df_mapping[['goship_name', 'name']].copy()
    # Remove rows if all NaN
    df_name_mapping = df_name_mapping.dropna()

    name_mapping_dict = dict(
        zip(df_name_mapping['goship_name'], df_name_mapping['name']))

    json_str = json.dumps(name_mapping_dict)

    return json_str


def create_bgcMeas_json_str(param_entries):

    # Now split up param_entries into multiple json dicts

    json_dict = json.loads(param_entries)

    df = pd.DataFrame.from_dict(json_dict)

    # Replace '' with nan
    df = df.replace(r'^\s*$', np.nan, regex=True)

    # Drop rows if all null except profile_type
    mask = df.drop("profile_type", axis=1).isna().all(
        1) & df['profile_type'].notna()

    df = df[~mask]

    # Change qc columns to integer
    qc_column_names = list(filter(lambda x: x.endswith('_qc'), df.columns))

    try:
        col_type_dict = {col_name: int for col_name in qc_column_names}

        # throws error if try to convert NaN to int
        # but if still have NaN in qc, rest are float
        df = df.astype(col_type_dict)

    except:
        pass

    # TODO: change sample columns to integer if ends with '.0'
    # or keep as is?
    try:
        col_type_dict = {'sample': int}

        # throws error if try to convert NaN to int
        # but if still have NaN in qc, rest are float
        df = df.astype(col_type_dict)

    except:
        pass

    json_str = df.to_json(orient='records')

    # TODO
    # find values like ',"silicate_qc":2.0' and strip of .0 from end
    # regex pattern is "(),\".*_qc\":\d).0" and replace with capture group

    # # Remove outer brackets
    # json_str = json_str.lstrip('{').rstrip('}')

    return json_str


def create_geolocation_json_str(nc):

    # "geoLocation": {
    #     "coordinates": [
    #         -158.2927,
    #         21.3693
    #     ],
    #     "type": "Point"
    # },

    lat = nc.coords['lat'].astype('str').values
    lon = nc.coords['lon'].astype('str').values

    lat = Decimal(lat.item(0))
    lon = Decimal(lon.item(0))

    coordinates = [lon, lat]

    geo_dict = {}
    geo_dict['coordinates'] = coordinates
    geo_dict['type'] = 'Point'

    geolocation_dict = {}
    geolocation_dict['geoLocation'] = geo_dict

    json_str = json.dumps(geolocation_dict, default=defaultencode)

    return json_str


def create_json_entries(profile_group, names):

    # If NaN in column, int qc becomes float

    json_entry = {}

    for name in names:

        # name either coord or data var
        # Test if float to convert to string and
        # keep precision

        is_int = False
        is_float = False

        float_types = ['float64', 'float32']
        int_types = ['int8', 'int64']

        try:
            var = profile_group.coords[name]

            if var.dtype in float_types:
                vals = var.astype('str').values
                is_float = True
            elif var.dtype in int_types:
                vals = var.astype('str').values
                is_int = True
            else:
                vals = var.astype('str').values

        except KeyError:
            var = profile_group.data_vars[name]

            if var.dtype in float_types:
                vals = var.astype('str').values
                is_float = True
            elif var.dtype in int_types:
                vals = var.astype('str').values
                is_int = True
            else:
                vals = var.astype('str').values

        if vals.size == 1:
            val = vals.item(0)

            if is_float:
                result = Decimal(val)
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)
            elif is_int:
                result = int(val)
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)
            else:
                result = val
                name_dict = {name: result}
                json_str = json.dumps(name_dict)
        else:

            if is_float:
                result = [Decimal(x) for x in vals]
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)
            elif is_int:
                result = [int(x) for x in vals]
                name_dict = {name: result}
                json_str = json.dumps(name_dict, default=defaultencode)
            else:
                result = vals.tolist()
                name_dict = {name: result}
                json_str = json.dumps(name_dict)

        json_entry[name] = json_str

    json_entries = ''

    for entry in json_entry.values():
        entry = entry.lstrip('{')
        bare_entry = entry.rstrip('}')
        json_entries = json_entries + ', ' + bare_entry

    # Strip off starting ', ' of string
    json_entries = '{' + json_entries.strip(', ') + '}'

    return json_entries


def add_extra_coords(nc, profile_number, filename):

    expocode = nc['expocode'].values

    if '/' in expocode:
        expocode = expocode.replace('/', '_')
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"
    elif expocode == 'None':
        logging.info(filename)
        logging.info('expocode is None')
        cruise_url = ''
    else:
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

    new_coords = {}

    if nc['profile_type'] == 'B':
        profile_type = 'BOT'
        nc.assign_coords(profile_type='BOT')
    else:
        profile_type = 'CTD'
        nc.assign_coords(profile_type='CTD')

    _id = f"{profile_type}_{expocode}_NPROF_{profile_number + 1}"
    new_coords['_id'] = _id
    new_coords['id'] = _id

    new_coords['PROF_#'] = profile_number + 1
    new_coords['POSITIONING_SYSTEM'] = 'GPS'
    new_coords['DATA_CENTRE'] = 'CCHDO'
    new_coords['cruise_url'] = cruise_url
    new_coords['netcdf_url'] = ''

    # TODO, will contain multiple filenames if combine BOT and CTD files
    new_coords['data_filename'] = filename

    datetime64 = nc['date'].values
    date = pd.to_datetime(datetime64)

    new_coords['date_formatted'] = date.strftime("%Y-%m-%d")

    # Convert date to iso
    new_coords['date'] = date.isoformat()

    latitude = nc['lat'].values
    longitude = nc['lon'].values

    roundLat = np.round(latitude, 3)
    roundLon = np.round(longitude, 3)

    strLat = f"{roundLat} N"
    strLon = f"{roundLon} E"

    new_coords['roundLat'] = roundLat
    new_coords['roundLon'] = roundLon

    new_coords['strLat'] = strLat
    new_coords['strLon'] = strLon

    nc = nc.assign_coords(new_coords)

    return nc


def create_profile_dict(profile_number, profile_group, df_mapping, meta_param_names, filename):

    # Create json without translating to dataframe first

    # Consider this as way to get values out of the xarray
    # xr_strs = profile_group[name].astype('str')
    # np_arr = xr_strs.values

    # Rename variables
    name_mapping_dict = dict(
        zip(df_mapping['goship_name'], df_mapping['name']))

    profile_group = profile_group.rename(name_mapping_dict)

    profile_group = add_extra_coords(profile_group, profile_number, filename)

    meta_names, param_names = get_meta_param_names(profile_group)

    meta_param_names = {}
    meta_param_names['meta'] = meta_names
    meta_param_names['param'] = param_names

    # Copy profile_type into param_names
    param_names.append('profile_type')

    meta_entries = create_json_entries(profile_group, meta_names)
    param_entries = create_json_entries(profile_group, param_names)

    geolocation_json_str = create_geolocation_json_str(profile_group)

    # Include geolocation dict into meta json string
    meta_left_str = meta_entries.rstrip('}')
    meta_geo_str = geolocation_json_str.lstrip('{')
    meta_json_str = f"{meta_left_str}, {meta_geo_str}"

    # Expand profile type name from 'B' to 'BOT' and 'C' to 'CTD'
    # Look for "profile_type": "C" and change to "profile_type": "CTD"
    meta_json_str = meta_json_str.replace(
        '"profile_type": "B"', '"profile_type": "BOT"')

    meta_json_str = meta_json_str.replace(
        '"profile_type": "C"', '"profile_type": "CTD"')

    bgcMeas_json_str = create_bgcMeas_json_str(param_entries)

    name_mapping_json_str = create_name_mapping_json_str(df_mapping)

    # Map new name to reference scale
    new_ref_scale_mapping_json_str = create_new_ref_scale_mapping_json_str(
        df_mapping)

    goship_ref_scale_mapping_json_str = create_goship_ref_scale_mapping_json_str(
        df_mapping)

    unit_mapping_json_str = create_unit_mapping_json_str(df_mapping)

    json_entry = {}
    json_entry['meta'] = meta_json_str
    json_entry['bgcMeas'] = bgcMeas_json_str
    json_entry['goship_name_mapping'] = name_mapping_json_str
    json_entry['goship_name_unit_mapping'] = unit_mapping_json_str
    json_entry['new_ref_scale_mapping'] = new_ref_scale_mapping_json_str
    json_entry['goship_ref_scale_mapping'] = goship_ref_scale_mapping_json_str

    meta = json.loads(meta_json_str)
    bgcMeas = json.loads(bgcMeas_json_str)
    goship_name_mapping = json.loads(name_mapping_json_str)
    goship_name_unit_mapping = json.loads(unit_mapping_json_str)
    new_ref_scale_mapping = json.loads(new_ref_scale_mapping_json_str)
    goship_ref_scale_mapping = json.loads(goship_ref_scale_mapping_json_str)

    # Get subset of bgcMeas
    # 'PRES', 'TEMP', 'PSAL', 'profile_type' and if have qc value
    keep_values = ['PRES', 'TEMP', 'PSAL',
                   'profile_type', 'TEMP_qc', 'PSAL_qc']

    subset_bgcMeas = []

    # TODO: Could just get values if qc = 2
    for meas_dict in bgcMeas:
        keep_meas = {
            key: val for key, val in meas_dict.items() if key in keep_values}

        subset_bgcMeas.append(keep_meas)

    profile_dict = meta
    profile_dict['bgcMeas'] = bgcMeas
    profile_dict['measurements'] = subset_bgcMeas
    profile_dict['goship_name_mapping'] = goship_name_mapping
    profile_dict['goship_name_unit_mapping'] = goship_name_unit_mapping
    profile_dict['new_ref_scale_mapping'] = new_ref_scale_mapping
    profile_dict['goship_ref_scale_mapping'] = goship_ref_scale_mapping

    return profile_dict, json_entry


def check_if_all_ctd_vars(nc, df_mapping):

    # Has all ctd vars if have both ctd temperature and pressure

    logging_filename = 'logging.txt'

    is_pres = False
    is_ctd_temp_w_refscale = False
    is_ctd_temp_w_no_refscale = False

    has_ctd_vars = True

    # is_temp_w_no_ctd_for = False
    # is_temp_w_no_ctd_for_ctd = False

    # name_to_units = {}
    # name_to_units_ctd = {}
    # name_to_ref_scale = {}
    # name_to_ref_scale_ctd = {}

    # names_ctd_temps_no_refscale = []
    # names_no_ctd_temps = []

    names = df_mapping['name'].tolist()

    for name in names:

        # From name mapping earlier, goship pressure name mapped to Argo equivalent
        if name == 'PRES':
            is_pres = True

        # From name mapping earlier, goship temperature name mapped to Argo equivalent
        if name == 'TEMP':
            is_ctd_temp_w_refscale = True

        # # From name mapping earlier, if didn't find reference scale for temperature
        # # but was a ctd temperature, was renamed to TEMP_no_refscale
        # if name == 'TEMP_no_refscale':
        #     is_ctd_temp_w_no_refscale = True

        # Was checking if no ctd temperatures at all
        # row = df_mapping.loc[df_mapping['name'] == name]
        # unit = row['unit'].values[0]

        # if name != 'TEMP' and name != 'TEMP_no_refscale' and unit == 'Celsius':
        #     is_temp_w_no_ctd_for = True

        # if is_temp_w_no_ctd_for:
        #     is_temp_w_no_ctd = True

    expocode = nc.coords['expocode'].data[0]

    if not is_pres or not is_ctd_temp_w_refscale:
        logging.info('===========')
        logging.info('EXCEPTIONS FOUND')
        logging.info(expocode)
        logging.info(logging_filename)

    if not is_pres:
        logging.info('missing PRES')
        with open('files_no_pressure.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    # if is_ctd_temp_w_no_refscale and is_ctd_temp_w_refscale:
    #     logging.info("has both CTD with and without ref scale")

    # If possiblity have both a ctd temperature with and without a refscale
    # Check what condition
    if not is_ctd_temp_w_refscale and is_ctd_temp_w_no_refscale:
        logging.info('CTD Temp with no ref scale')
        # Write to file listing files without ctd variables
        with open('files_ctd_temps_no_refscale.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    if not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale:
        logging.info('NO CTD Temp')
        # Write to file listing files without ctd variables
        with open('files_no_ctd_temps.csv', 'a') as f:
            f.write(f"{expocode}, {logging_filename} \n")

    # if not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale and not is_temp_w_no_ctd:
    #     logging.info('NO TEMPS')

    # if not is_pres or (not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale):
    #     logging.info('===========')
    #     logging.info('OTHER PRES/TEMP VARS')
    #     logging.info(expocode)
    #     logging.info(logging_filename)

    # TODO log these exceptions

    # Skip making json if no expocode, no pressure or no ctd temp with ref scale
    if not is_pres:
        has_ctd_vars = False

    if not is_ctd_temp_w_refscale:
        has_ctd_vars = False

    # elif not is_ctd_temp_w_refscale and not is_ctd_temp_w_no_refscale:
    #     # files with no ctd temp
    #     return

    expocode = nc.coords['expocode'].data[0]

    if expocode == 'None':
        has_ctd_vars = False

    # return is_pres, is_ctd_temp_w_refscale

    return has_ctd_vars


def create_json(nc, meta_param_names, df_mapping, argo_units_mapping_file, json_dir, filename):

    # Consider this as way to get values out of the xarray
    # xr_strs = nc[name].astype('str')
    # np_arr = xr_strs.values

    # Check if all ctd vars available: pressure and temperature
    has_ctd_vars = check_if_all_ctd_vars(nc, df_mapping)

    if not has_ctd_vars:
        return

    all_profile_dicts = []
    all_json_entries = []

    for nc_group in nc.groupby('N_PROF'):

        profile_number = nc_group[0]
        profile_group = nc_group[1]

        profile_dict, json_entry = create_profile_dict(
            profile_number, profile_group, df_mapping, meta_param_names, filename)

        all_profile_dicts.append(profile_dict)
        all_json_entries.append(json_entry)

    return all_profile_dicts, all_json_entries


def convert_sea_water_temp(nc, var, goship_ref_scale):

    # Check sea_water_temperature to be degree_Celsius and
    # have goship_ref_scale be ITS-90

    # So look for ref_scale = IPTS-68 or ITS-90

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    # Change this to work for all temperature names

    reference_scale = np.nan

    try:
        temperature = nc[var].data

        if goship_ref_scale == 'IPTS-68':

            # Convert to ITS-90 scale
            temperature90 = temperature/1.00024

            # Set nc var of TEMP to this value

            # TODO: check if can set precision.
            # want the same as the orig temperature precision

            nc[var].data = temperature90

            reference_scale = 'IPT-90'

        else:
            print('temperature not IPTS-68')
            reference_scale = np.nan

        return nc, reference_scale

    except KeyError:

        return nc, reference_scale


def convert_units_add_ref_scale(nc, df_mapping, meta_param_names):

    # Create new column to hold new reference scales
    df_mapping['reference_scale'] = df_mapping['goship_reference_scale']

    # TODO
    # Is it correct only looking at params and not meta?

    for var in meta_param_names['param']:

        row = df_mapping.loc[df_mapping['goship_name'] == var]

        # new_ref_scale = row['goship_reference_scale']

        # If argo ref scale not equal to goship ref scale, convert
        # So far, it's only the case for temperature
        # row = df_mapping.loc[print(
        #     df_mapping['goship_reference_scale']) == var]

        try:
            goship_ref_scale = row['goship_reference_scale'].values[0]
        except IndexError:
            goship_ref_scale = np.nan

        try:
            argo_ref_scale = row['argo_reference_scale'].values[0]
        except IndexError:
            argo_ref_scale = np.nan

        goship_is_nan = pd.isnull(goship_ref_scale)
        argo_is_nan = pd.isnull(argo_ref_scale)

        if goship_is_nan and argo_is_nan:
            new_ref_scale = np.nan

        elif goship_ref_scale == argo_ref_scale:
            new_ref_scale = goship_ref_scale

        elif (goship_ref_scale != argo_ref_scale) and not argo_is_nan and not goship_is_nan:
            # Convert to argo ref scale
            # then save this to add to new reference_scale column

            # TODO: are there other ref scales to convert?

            if argo_ref_scale == 'IPT-90' and goship_ref_scale == 'IPTS-68':
                # convert seawater temperature
                # TODO
                # Use C-format or precision of IPT-90 to get same precision
                nc, new_ref_scale = convert_sea_water_temp(nc,
                                                           var, goship_ref_scale)

        elif not goship_is_nan and argo_is_nan:
            new_ref_scale = goship_ref_scale

        df_mapping.loc[df_mapping['name'] == var,
                       'reference_scale'] = new_ref_scale

    return nc, df_mapping


def get_new_unit_name(var, df_mapping, df_mapping_unit_ref_scale):

    # New unit name is argo unit, but if no argo unit, use goship unit
    row = df_mapping.loc[df_mapping['goship_name'] == var]

    goship_unit = row['goship_unit'].values[0]
    argo_unit = row['argo_unit'].values[0]

    row_argo_ref_scale_for_unit = df_mapping_unit_ref_scale[
        df_mapping_unit_ref_scale['argo_unit'] == argo_unit]

    row_goship_ref_scale_for_unit = df_mapping_unit_ref_scale[
        df_mapping_unit_ref_scale['goship_unit'] == goship_unit]

    try:
        argo_ref_scale = row_argo_ref_scale_for_unit['reference_scale'].values[0]
    except IndexError:
        argo_ref_scale = np.nan

    try:
        goship_ref_scale = row_goship_ref_scale_for_unit['reference_scale'].values[0]
    except IndexError:
        goship_ref_scale = np.nan

    # compare values of goship and argo reference scales
    is_goship_ref_scale_nan = pd.isnull(goship_ref_scale)
    is_argo_ref_scale_nan = pd.isnull(argo_ref_scale)
    is_same_ref_scale = argo_ref_scale == goship_ref_scale

    if is_goship_ref_scale_nan and is_argo_ref_scale_nan:
        # Unit could be temperature or pressure where
        # converting say from degC to Celsius
        # New unit name is Argo unit name
        new_unit = argo_unit

    elif is_same_ref_scale:
        # New unit is argo unit
        new_unit = argo_unit

    else:
        # New units are goship units
        new_unit = goship_unit

    return new_unit


def create_new_units_mapping(df_mapping, argo_units_mapping_file, meta_param_names):

    # Mapping goship unit names to argo units
    # Since unit of 1 for Salinity maps to psu,
    # check reference scale to be PSS-78, otherwise
    # leave unit as 1

    # 3 columns
    # goship_unit, argo_unit, reference_scale
    df_mapping_unit_ref_scale = pd.read_csv(argo_units_mapping_file)

    # mapping_unit_dict = dict(zip(df['goship_unit'], df['argo_unit']))

    # mapping_ref_scale_dict = dict(
    #     zip(df['goship_reference_scale'], df['argo_reference_scale']))

    new_meta_units = {}
    for name in meta_param_names['meta']:

        new_unit = get_new_unit_name(
            name, df_mapping, df_mapping_unit_ref_scale)

        new_meta_units[name] = new_unit

    new_param_units = {}
    for name in meta_param_names['param']:

        new_unit = get_new_unit_name(
            name, df_mapping, df_mapping_unit_ref_scale)

        new_param_units[name] = new_unit

    new_units = {**new_meta_units, **new_param_units}

    df_new_units = pd.DataFrame(new_units.items())
    df_new_units.columns = ['name', 'unit']

    df_mapping = df_mapping.merge(
        df_new_units, how='left', left_on='name', right_on='name')

    return df_mapping


def create_new_names_mapping(df, meta_param_names):

    # Look at var names to rename temperature and salinity
    # depending on goship name because there are multiple types
    params = meta_param_names['param']

    is_ctd_temp = False
    is_ctd_temp_68 = False
    # is_ctd_temp_unknown = False

    is_ctd_sal = False
    is_bottle_sal = False

    for name in params:

        # if both ctd_temperature and ctd_temperature_68,
        # use ctd_temperature to TEMP only
        if name == 'ctd_temperature':
            is_ctd_temp = True
        if name == 'ctd_temperature_68':
            is_ctd_temp_68 = True
        # if name == 'ctd_temperature_unk':
        #     is_ctd_temp_unknown = True
        if name == 'ctd_salinity':
            is_ctd_sal = True
        if name == 'bottle_salinity':
            is_bottle_sal = True

    # Create new column with new names. Start as argo names if exist
    df['name'] = df['argo_name']

    # if argo name is nan, use goship name
    df['name'] = np.where(df['argo_name'].isna(),
                          df['goship_name'], df['name'])

    # if both temp on ITS-90 scale and temp on IPTS-68 scale,
    # just change name of ctd_temperature (ITS-90) to TEMP, and
    # don't change name of ctd_temperature_68. So keep names as is.

    # If don't have an argo name of TEMP yet and do have a ctd temperature
    # on the 68 scale, call it TEMP
    if not is_ctd_temp and is_ctd_temp_68:
        # change name to TEMP and convert to ITS-90 scale later
        df.loc[df['goship_name'] == 'ctd_temperature_68', 'name'] = 'TEMP'
        df_qc = df.isin({'goship_name': ['ctd_temperature_68_qc']}).any()

        if df_qc.any(axis=None):
            df.loc[df['goship_name'] ==
                   'ctd_temperature_68_qc', 'name'] = 'TEMP_qc'

    # elif not is_ctd_temp and not is_ctd_temp_68 and is_ctd_temp_unknown:
    #     # TODO: Maybe don't rename and just exclude this netcdf file
    #     df.loc[df['goship_name'] == 'ctd_temperature_unk',
    #            'name'] = 'TEMP_no_refscale'

    #     df_qc = df.isin({'goship_name': ['ctd_temperature_unk_qc']}).any()
    #     if df_qc.any(axis=None):
    #         df.loc[df['goship_name'] == 'ctd_temperature_unk_qc',
    #                'name'] = 'TEMP_no_refscale_qc'

    # if no ctd_sal and is_bottle_sal, rename bottle_salinity to PSAL_bottle
    if not is_ctd_sal and is_bottle_sal:
        df.loc[df['goship_name'] == 'bottle_salinity', 'name'] = 'PSAL_bottle'
        df_qc = df.isin({'goship_name': ['bottle_salinity_qc']}).any()

        if df_qc.any(axis=None):
            df.loc[df['goship_name'] ==
                   'bottle_salinity_qc', 'name'] = 'PSAL_bottle_qc'

    # # TODO: change this so don't use PSAL_bottle
    # # If don't have ctd_sal, then change to PSAL_bottle

    # # if both salinities, use ctd_salinity and use name bottle_salinity
    # # if no ctd_sal and is_bottle_sal, already renamed bottle_sal to
    # # PSAL_bottle, so no change
    # if is_ctd_sal and is_bottle_sal:
    #     df.loc[df['goship_name'] == 'bottle_salinity',
    #            'name'] = 'bottle_salinity'

    #     df_qc = df.isin({'goship_name': ['bottle_salinity_qc']}).any()
    #     if df_qc.any(axis=None):
    #         df.loc[df['goship_name'] == 'bottle_salinity_qc',
    #                'name'] = 'bottle_salinity_qc'

    # # Create mapping dicts to go from goship name to new name
    # # Will map both coordinate and variable names
    # mapping_dict = dict(zip(df['goship_name'], df['name']))
    # nc = nc.rename_vars(name_dict=mapping_dict)

    return df


def add_qc_names_to_argo_names(df_mapping):

    # If goship name has a qc, rename corresponding argo name to argo name qc
    goship_qc_names = df_mapping.loc[df_mapping['goship_name'].str.contains(
        '_qc')]['goship_name'].tolist()

    for goship_qc_name in goship_qc_names:

        # find argo name of goship name without qc
        goship_base_name = goship_qc_name.replace('_qc', '')
        argo_name = df_mapping.loc[df_mapping['goship_name']
                                   == goship_base_name, 'argo_name']

        # If argo_name not empty, add qc name
        if pd.notna(argo_name.values[0]):
            df_mapping.loc[df_mapping['goship_name'] == goship_qc_name,
                           'argo_name'] = argo_name.values[0] + '_qc'

    return df_mapping


def rename_mapping_names_units(df_mapping, meta_param_names, argo_units_mapping_file):

    # Add qc names since Argo names don't have any
    # example: for argo name TEMP, add TEMP_qc if corresponding goship name has qc
    df_mapping = add_qc_names_to_argo_names(df_mapping)

    # Take mapping of goship to new Name and use Argo Names if exist
    # Otherwise, use goship name
    df_mapping = create_new_names_mapping(df_mapping, meta_param_names)

    # Convert goship unit names into argo unit names
    # Unit names rely on reference scale if unit = 1
    df_mapping = create_new_units_mapping(
        df_mapping, argo_units_mapping_file, meta_param_names)

    # Later, compare ref scales and see if need to convert

    return df_mapping


def get_goship_mapping_df(nc):

    # Get units, reference scale and c_format from nc
    coord_names = list(nc.coords)
    var_names = list(nc.keys())

    name_to_units = {}
    name_to_ref_scale = {}
    name_to_c_format = {}

    for coord in coord_names:

        try:
            name_to_units[coord] = nc.coords[coord].attrs['units']
        except KeyError:
            name_to_units[coord] = np.nan

        try:
            name_to_ref_scale[coord] = nc.coords[coord].attrs['reference_scale']
        except KeyError:
            name_to_ref_scale[coord] = np.nan

        try:
            name_to_c_format[coord] = nc.coords[coord].attrs['C_format']
        except KeyError:
            name_to_c_format[coord] = np.nan

    for var in var_names:

        try:
            name_to_units[var] = nc[var].attrs['units']
        except KeyError:
            name_to_units[var] = np.nan

        try:
            name_to_ref_scale[var] = nc[var].attrs['reference_scale']
            # Replace 'unknown' with np.nan
            if name_to_ref_scale[var] == 'unknown':
                name_to_ref_scale[var] = np.nan
        except KeyError:
            name_to_ref_scale[var] = np.nan

        try:
            name_to_c_format[var] = nc[var].attrs['C_format']
        except KeyError:
            name_to_c_format[var] = np.nan

    df_dict = {}

    df_dict['goship_unit'] = name_to_units
    df_dict['goship_reference_scale'] = name_to_ref_scale
    df_dict['goship_c_format'] = name_to_c_format

    df = pd.DataFrame.from_dict(df_dict)
    df.index.name = 'goship_name'
    df = df.reset_index()

    return df


def get_argo_mapping_df(argo_name_mapping_file):

    df = pd.read_csv(argo_name_mapping_file)

    # Only want part of the csv file
    df = df[['argo_name', 'argo_unit', 'argo_reference_scale', 'goship_name']].copy()

    return df


def get_argo_goship_mapping(nc, argo_name_mapping_file):

    df_argo_mapping = get_argo_mapping_df(argo_name_mapping_file)

    df_goship_mapping = get_goship_mapping_df(nc)

    # Any empty cells are filled with nan
    df_all_mapping = df_goship_mapping.merge(
        df_argo_mapping, how='left', left_on='goship_name', right_on='goship_name')

    # Rename index created when merging on goship_name
    df_all_mapping = df_all_mapping.rename(
        columns={'index': 'goship_name'})

    return df_all_mapping


def get_meta_param_names(nc):

    # Meta names have size N_PROF and no N_LEVELS
    # Parameter names have size N_PROF AND N_LEVELS

    meta_names = []
    param_names = []

    # check coords
    for name in list(nc.coords):
        size = nc[name].sizes

        try:
            size['N_LEVELS']
            param_names.append(name)
        except KeyError:
            meta_names.append(name)

    # check params
    for name in list(nc.keys()):
        size = nc[name].sizes

        try:
            size['N_LEVELS']
            param_names.append(name)
        except KeyError:
            meta_names.append(name)

    return meta_names, param_names


def process_folder(nc_folder):

    # folder_name = nc_folder.name

    nc_files = os.scandir(nc_folder)

    nc_dict = {}
    nc_dict['bot'] = None
    nc_dict['ctd'] = None

    filenames = {}

    for file in nc_files:

        filename = file.name

        if not filename.endswith('.nc'):
            continue

        print('-------------')
        print(filename)
        print('-------------')

        filepath = os.path.join(nc_folder, filename)
        nc = xr.load_dataset(filepath)

        profile_type = nc['profile_type'].values[0]

        if profile_type == 'B':
            nc_dict['bot'] = nc
            filenames['bot'] = filename
        elif profile_type == 'C':
            nc_dict['ctd'] = nc
            filenames['ctd'] = filename
        else:
            print('No bottle or ctd files')
            exit(1)

    nc_files.close()

    nc_bot = nc_dict['bot']
    nc_ctd = nc_dict['ctd']

    print(nc_bot)
    print(nc_ctd)

    print('=====================')

    bot_names = {}
    ctd_names = {}

    if nc_bot:
        bot_meta_names, bot_param_names = get_meta_param_names(nc_bot)
        bot_names['meta'] = bot_meta_names
        bot_names['param'] = bot_param_names

    if nc_ctd:
        ctd_meta_names, ctd_param_names = get_meta_param_names(nc_ctd)
        ctd_names['meta'] = ctd_meta_names
        ctd_names['param'] = ctd_param_names

    return (filenames, nc_dict, bot_names, ctd_names)


def main():

    # TODO: Need to add in bot and ctd files used in creating the json

    start_time = datetime.now()

    logging.root.handlers = []
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                        level=logging.INFO, filename='output.log')

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

    # TODO
    # For now, grabbed by hand a bot and ctd file of one expocode and put
    # in a folder. Later generalize to match up all files by looping
    # through them
    input_netcdf_data_directory = './data/same_expocode_bot_ctd_netcdf'
    # output_netcdf_data_directory = './data/same_expocode_combined_netcdf'
    json_data_directory = './data/same_expocode_json'
    # csv_data_directory = './data/same_expocode_csv'

    argo_name_mapping_file = 'argo_go-ship_mapping.csv'
    argo_units_mapping_file = 'argo_goship_units_mapping.csv'

    nc_data_entry = os.scandir(input_netcdf_data_directory)

    for nc_folder in nc_data_entry:

        if nc_folder.name != '32NH047_1':
            continue

        # Process folder to combine bot and ctd without
        # caring about setting new coords and vals
        if nc_folder.is_dir():
            filenames, nc_dict, bot_names, ctd_names = process_folder(
                nc_folder)

            if nc_dict['bot']:
                df_bot_mapping = get_argo_goship_mapping(nc_dict['bot'],
                                                         argo_name_mapping_file)

                df_bot_mapping = rename_mapping_names_units(
                    df_bot_mapping, bot_names, argo_units_mapping_file)

                nc_dict['bot'], df_bot_mapping = convert_units_add_ref_scale(
                    nc_dict['bot'], df_bot_mapping, bot_names)

                # Create ArgoVis json
                bot_profile_dicts, bot_json_entries = create_json(
                    nc_dict['bot'], bot_names, df_bot_mapping, argo_name_mapping_file, json_data_directory, filenames['bot'])

            if nc_dict['ctd']:
                df_ctd_mapping = get_argo_goship_mapping(nc_dict['ctd'],
                                                         argo_name_mapping_file)

                df_ctd_mapping = rename_mapping_names_units(
                    df_ctd_mapping, ctd_names, argo_units_mapping_file)

                nc_dict['ctd'], df_ctd_mapping = convert_units_add_ref_scale(
                    nc_dict['ctd'], df_ctd_mapping, ctd_names)

                # Create ArgoVis json
                ctd_profile_dicts, ctd_json_entries = create_json(
                    nc_dict['ctd'], ctd_names, df_ctd_mapping, argo_name_mapping_file, json_data_directory, filenames['ctd'])

            if nc_dict['bot'] and not nc_dict['ctd']:
                for profile_number, profile_dict in enumerate(bot_profile_dicts):
                    profile_type = 'BOT'
                    # write_profile_json(json_data_directory,
                    #                    profile_number, profile_type, profile_dict)

            elif not nc_dict['bot'] and nc_dict['ctd']:
                for profile_number, profile_dict in enumerate(ctd_profile_dicts):
                    profile_type = 'CTD'

                    # write_profile_json(json_data_directory,
                    #                    profile_number, profile_type, profile_dict)

            else:
                # Assuming same number of profiles
                # Is this true? Do a check

                bot_num_profiles = len(bot_profile_dicts)
                ctd_num_profiles = len(ctd_profile_dicts)

               # TODO: what to do in the case when not equal?
                if bot_num_profiles != ctd_num_profiles:
                    # Log it and save ctd and bot separate
                    # print and exit for now
                    print(
                        f"bot profiles {bot_num_profiles} and ctd profiles {ctd_num_profiles} are different")
                    exit(1)

                for profile_number, ctd_profile_dict in enumerate(ctd_profile_dicts):

                    combined_dict = {}

                    # Combine bot values into ctd values
                    bot_profile_dict = bot_profile_dicts[profile_number]

                    bot_json_entry = bot_json_entries[profile_number]
                    ctd_json_entry = ctd_json_entries[profile_number]

                    # dicts included in json_entry
                    # meta, bgcMeas, goship_name_mapping,
                    # goship_name_unit_mapping, new_ref_scale_mapping,
                    # goship_ref_scale_mapping

                    bot_meta_json = bot_json_entry['meta']
                    ctd_meta_json = ctd_json_entry['meta']

                    bot_meta_dict = json.loads(bot_meta_json)
                    ctd_meta_dict = json.loads(ctd_meta_json)

                    combined_dict = ctd_meta_dict
                    combined_dict['bot'] = bot_meta_dict

                    # Combine bgcMeas dicts
                    bot_bgcMeas_array = bot_profile_dict['bgcMeas']
                    ctd_bgcMeas_array = ctd_profile_dict['bgcMeas']

                    bgcMeas_array = [
                        *ctd_bgcMeas_array, *bot_bgcMeas_array]

                    combined_dict['bgcMeas'] = bgcMeas_array

                    # Combine measurements dicts
                    bot_measurements_array = bot_profile_dict['measurements']
                    ctd_measurements_array = ctd_profile_dict['measurements']

                    measurements_array = [
                        *ctd_measurements_array, *bot_measurements_array]

                    # sort measurements on PRES value
                    sorted_measurements_array = sorted(measurements_array,
                                                       key=lambda k: k['PRES'])

                    combined_dict['measurements'] = sorted_measurements_array

                    # Change to store profile json in an array and return that
                    # Then when have both ctd and bot json, combine them and then
                    # write to files
                    profile_type = 'BOT_CTD'
                    write_profile_json(json_data_directory,
                                       profile_number, profile_type, combined_dict)

                    exit(1)

            exit(1)

        break

    nc_data_entry.close()

    logging.info(datetime.now() - start_time)


if __name__ == '__main__':
    main()
