import logging

import pandas as pd
import numpy as np

from variable_naming.filter_parameters import get_parameters_to_filter_out
from variable_naming.rename_parameters import rename_to_argovis_mapping

from variable_naming.meta_param_mapping import rename_mappings_data_info_keys
from variable_naming.meta_param_mapping import get_meta_mapping

from variable_naming.meta_param_mapping import get_program_argovis_source_info_mapping
from variable_naming.meta_param_mapping import get_program_argovis_data_info_mapping

from variable_naming.rename_units import change_units_to_argovis


def reformat_data_info(data_info, standard_names_mapping):
    new_data_info = []

    # Argovis info
    data_keys = data_info["data_keys"]
    data_units = data_info["data_units"]
    data_reference_scale = data_info["data_reference_scale"]

    # CCHDO info
    data_source_units = data_info["data_source_units"]
    data_source_reference_scale = data_info["data_source_reference_scale"]

    new_data_info.append(data_keys)

    new_data_info.append(
        [
            "units",
            "reference_scale",
            "data_keys_mapping",
            "data_source_standard_names",
            "data_source_units",
            "data_source_reference_scale",
        ]
    )

    # mapping where keys = cchdo names, values = argovis names
    data_keys_mapping = data_info["data_keys_mapping"]

    # Swap this so that keys are argovis names, and values are cchdo names
    mapping_argovis_to_cchdo = {value: key for key, value in data_keys_mapping.items()}

    data_rows = []
    for var in data_keys:
        new_data_row = []

        # Add units
        if var in data_units.keys():
            data_unit = data_units[var]
        else:
            data_unit = np.nan

        new_data_row.append(data_unit)

        # Add reference scale
        if var in data_reference_scale.keys():
            ref_scale = data_reference_scale[var]
        else:
            ref_scale = np.nan

        new_data_row.append(ref_scale)

        # Add source name from mapping
        cchdo_var_name = mapping_argovis_to_cchdo[var]
        new_data_row.append(cchdo_var_name)

        # Add data source corresponding standard name
        try:
            cchdo_standard_name = standard_names_mapping[cchdo_var_name]
        except KeyError:
            cchdo_standard_name = np.nan

        new_data_row.append(cchdo_standard_name)

        # Add data source units
        if cchdo_var_name in data_source_units.keys():
            data_source_unit = data_source_units[cchdo_var_name]

        else:
            data_source_unit = np.nan

        new_data_row.append(data_source_unit)

        # Add data source reference scale
        if cchdo_var_name in data_source_reference_scale.keys():
            data_source_ref_scale = data_source_reference_scale[cchdo_var_name]
        else:
            data_source_ref_scale = np.nan

        new_data_row.append(data_source_ref_scale)

        # Add new row
        data_rows.append(new_data_row)

    new_data_info.append(data_rows)

    return new_data_info


def modify_meta_for_cdom(new_meta, data):
    # Remove cdom_wavelengths meta variable when cdom data vars don't exist
    # This meta variable exists because it was defined for the whole
    # netCDF data file when read in with xarray and thus is will exist
    # for each N_PROF dimension.
    for obj in data:
        keys = list(obj.keys())
        keys_str = " ".join(keys)

        # Check for flattened variable CDOM name
        # 'cdom_wavelength_325' where 325 is the wavelength for that column
        if "cdom_wavelength_" not in keys_str:
            # remove cdom_wavelengths meta entry because there are no
            # cdom variables
            new_meta.pop("cdom_wavelengths", None)

    return new_meta


def reorganize_meta_and_mappings(meta, mappings):
    # put mappings into dictionaries called source and data_info

    source_info = {}

    # Get key names that will be in the output key "source_info"
    # source_info_keys = get_program_argovis_source_info_mapping().values()

    standard_names_mapping = mappings["data_source_standard_names"]

    # for key in source_info_keys:
    #     source_info[key] = mappings[key]

    # source is what "programs" was mapped to
    if "source" in meta.keys():
        source_info["source"] = meta["source"]
        meta.pop("source", None)

    if "cruise_url" in meta.keys():
        source_info["cruise_url"] = meta["cruise_url"]
        meta.pop("cruise_url", None)

    if "source_url" in meta.keys():
        source_info["url"] = meta["source_url"]
        meta.pop("source_url", None)

    if "file_name" in meta.keys():
        source_info["file_name"] = meta["file_name"]
        meta.pop("file_name", None)

    meta["source"] = [source_info]

    data_info = {}

    # Get key names used by Argovis from key names used in this program
    data_info_keys = get_program_argovis_data_info_mapping().values()

    for key in data_info_keys:
        data_info[key] = mappings[key]

    data_info = reformat_data_info(data_info, standard_names_mapping)

    meta["data_info"] = data_info

    return meta


def create_mappings(profile_dict, argovis_col_names_mapping):
    # Creates argovis mappings and
    # adds in cchdoConvertedUnits & cchdoConvertedReferenceScale if there are any

    # So mappings is a combination of cchdo mappings and argovis mappings

    mappings = {}

    # Want before and after conversions

    # keys before conversion of cchdo file
    cchdo_units = profile_dict["cchdo_units"]
    cchdo_ref_scale = profile_dict["cchdo_reference_scale"]
    cchdo_param_names = profile_dict["cchdo_param_names"]
    cchdo_standard_names = profile_dict["cchdo_standard_names"]

    # vars with attributes changed
    # Want to incorporate these into cchdo_units and cchdo_reference_scale
    cchdo_converted_units = profile_dict["cchdoConvertedUnits"]
    cchdo_converted_ref_scale = profile_dict["cchdoConvertedReferenceScale"]

    # later need to add cchdo names to netcdf names mapping.
    # Do this earlier in the program since getting info from
    # netcdf file

    # (listing of all the var names in data)
    mappings["argovis_param_names"] = list(argovis_col_names_mapping.values())

    # from above, this is argovis_col_names_mapping
    mappings["cchdo_argovis_param_mapping"] = argovis_col_names_mapping

    mappings["cchdo_standard_names"] = cchdo_standard_names

    mappings["cchdo_param_names"] = cchdo_param_names

    # Values are the same as cchdo except those converted
    # And rename
    cchdo_name_ref_scale_mapping = {**cchdo_ref_scale, **cchdo_converted_ref_scale}

    # Get mapping and then swap out keys with argovis names
    # Takes in ref_scale_mapping.keys(), cchdo names, and
    # gives back the mapping from these cchdo names to argovis names
    data_type = profile_dict["data_type"]
    cchdo_to_argovis_name_mapping = rename_to_argovis_mapping(
        list(cchdo_name_ref_scale_mapping.keys()), data_type
    )

    mappings["argovis_reference_scale"] = {
        cchdo_to_argovis_name_mapping[key]: value
        for key, value in cchdo_name_ref_scale_mapping.items()
    }

    # key argovisUnits
    # Values are the same as cchdo except those converted
    # And rename
    cchdo_units_mapping = {**cchdo_units, **cchdo_converted_units}

    # Convert unit names to argovis names
    # Take in cchdo name to units (included those converted)
    # and return a mapping of cchdo name to argovis unit names
    # Need reference scale so can match up if variable is salinity
    # since salinity units are currently 1 which can be a unit
    # of many variables
    cchdo_name_to_argovis_unit_mapping = change_units_to_argovis(
        cchdo_units_mapping, cchdo_name_ref_scale_mapping
    )

    # Convert cchdo names to argovis names
    # Get mapping and then swap out keys with argovis names
    cchdo_to_argovis_name_mapping = rename_to_argovis_mapping(
        list(cchdo_name_to_argovis_unit_mapping.keys()), data_type
    )

    mappings["argovis_units"] = {
        cchdo_to_argovis_name_mapping[key]: value
        for key, value in cchdo_name_to_argovis_unit_mapping.items()
    }

    return mappings


def filter_out_params(parameter_names):
    params_to_filter_out = get_parameters_to_filter_out()

    params_to_filter = [
        param for param in parameter_names if param in params_to_filter_out
    ]

    return params_to_filter


def rename_data(data_type, data):
    # Rename by loading dict into pandas dataframe,
    # rename cols then output back to dict
    df_data = pd.DataFrame.from_dict(data)

    # Reorganize columns by name in preparation for saving them to JSON in
    # alphabetical order
    df_data = df_data.reindex(sorted(df_data.columns), axis=1)

    # logging.info(f"column names before rename {list(df_data.columns)}")

    data_columns = list(df_data.columns)

    # First rename to any ArgoVis names and change _qc to _woceqc
    argovis_col_names_mapping = rename_to_argovis_mapping(data_columns, data_type)

    argovis_col_names = []
    for col_name in data_columns:
        argovis_name = argovis_col_names_mapping[col_name]

        argovis_col_names.append(argovis_name)

    df_data = df_data.set_axis(argovis_col_names, axis="columns")

    data_columns = list(df_data.columns)

    df_data = df_data.set_axis(data_columns, axis="columns")

    data = df_data.to_dict("records")

    return data, argovis_col_names_mapping


# def remove_extra_dim_vars_from_mappings(
#     profile_dict, variables_to_delete_in_mappings, variables_to_add_in_mappings
# ):
#     # Get the names of extra dim vars
#     extra_dim_vars = variables_to_add_in_mappings.keys()

#     for extra_dim in extra_dim_vars:
#         # First keep mapping attribute values of one variable out of the exploded vars
#         # Then delete and add variables

#         variables_to_delete = variables_to_delete_in_mappings[extra_dim]
#         variables_to_add = variables_to_add_in_mappings[extra_dim]

#         variables_to_include = []

#         non_qc_var_to_add = variables_to_add["non_qc_var"]

#         variables_to_include.append(non_qc_var_to_add)

#         try:
#             qc_var_to_add = variables_to_add["qc_var"]
#             variables_to_include.append(qc_var_to_add)
#         except:
#             qc_var_to_add = None

#         col_naming_var = variables_to_add["col_naming_var"]
#         variables_to_include.append(col_naming_var)

#         non_qc_vars_to_delete = [
#             var for var in variables_to_delete if not var.endswith("_qc")
#         ]
#         qc_vars_to_delete = [var for var in variables_to_delete if var.endswith("_qc")]

#         non_qc_vars_to_delete.sort()
#         qc_vars_to_delete.sort()

#         sample_non_qc_var = non_qc_vars_to_delete[0]
#         sample_qc_var = qc_vars_to_delete[0]

#         for key, value in profile_dict.items():
#             if key == "cchdo_param_names":
#                 new_value = [val for val in value if val not in variables_to_delete]

#                 new_value.extend(variables_to_include)

#                 profile_dict[key] = new_value

#             if key == "cchdo_standard_names":
#                 new_value = {}
#                 for k, v in value.items():
#                     if k == sample_non_qc_var:
#                         new_value[non_qc_var_to_add] = v

#                     elif k == sample_qc_var:
#                         new_value[qc_var_to_add] = v

#                     else:
#                         new_value[k] = v

#                 profile_dict[key] = {
#                     k: v for k, v in new_value.items() if k not in variables_to_delete
#                 }

#             if key == "cchdo_reference_scale":
#                 new_value = {}
#                 for k, v in value.items():
#                     if k == sample_non_qc_var:
#                         new_value[non_qc_var_to_add] = v
#                     else:
#                         new_value[k] = v

#                 profile_dict[key] = {
#                     k: v for k, v in new_value.items() if k not in variables_to_delete
#                 }

#             if key == "cchdo_units":
#                 new_value = {}
#                 for k, v in value.items():
#                     if k == sample_non_qc_var:
#                         new_value[non_qc_var_to_add] = v
#                     else:
#                         new_value[k] = v

#                 profile_dict[key] = {
#                     k: v for k, v in new_value.items() if k not in variables_to_delete
#                 }

#     return profile_dict


def remove_deleted_vars_from_mappings(profile_dict, variables_deleted):
    # keys are names used in this program
    # values are the final key names to be used for Argovis
    cchdo_key_mapping = get_program_argovis_source_info_mapping()

    # argovis_key_mapping = get_program_argovis_data_info_mapping()

    # key_mapping = {**cchdo_key_mapping, **argovis_key_mapping}

    key_mapping = cchdo_key_mapping

    for key, value in profile_dict.items():
        if key in key_mapping.keys():
            # remove variables_deleted from value
            if isinstance(value, list):
                # Remove deleted var name from list
                new_value = [val for val in value if val not in variables_deleted]

            elif isinstance(value, dict):
                # Remove deleted var name from dict
                new_value = {}
                for k, v in value.items():
                    if k not in variables_deleted:
                        new_value[k] = v
            else:
                new_value = value

            profile_dict[key] = new_value

    return profile_dict


# def remove_empty_and_nan_variables(data):
#     # Read data into a pandas dataframe
#     df_data = pd.DataFrame.from_dict(data)

#     # column names before delete
#     column_names_start = list(df_data.columns)

#     # Check if all values for column are nan,
#     # and if they are, drop them
#     df_data = df_data.dropna(axis=1, how="all")

#     columns_not_nan = set(df_data.columns)

#     # Create copy of df and drop empty columns (full of blanks)
#     df_data_copy = df_data.copy()

#     df_data_copy = df_data_copy.replace(r"^\s*$", np.nan, regex=True)
#     df_data_copy = df_data_copy.dropna(axis=1, how="all")

#     columns_after_dropped = set(df_data_copy.columns)

#     empty_columns = columns_not_nan.difference(columns_after_dropped)

#     empty_variables_to_delete = list(empty_columns)

#     # search for any corresponding qc columns to delete
#     new_empty_variables_to_delete = []

#     for var in empty_variables_to_delete:
#         new_empty_variables_to_delete.append(var)

#         qc_var = f"{var}_qc"
#         if qc_var in column_names_start:
#             new_empty_variables_to_delete.append(qc_var)

#     variables_to_keep = list(columns_not_nan)

#     for var in new_empty_variables_to_delete:
#         variables_to_keep.remove(var)

#     df_data = df_data[variables_to_keep]

#     # Drop any vars not to be included in a final Argovis JSON profile
#     params_to_filter_out = get_parameters_to_filter_out()

#     cols_to_filter_out = [
#         param for param in variables_to_keep if param in params_to_filter_out
#     ]

#     df_data = df_data.drop(columns=cols_to_filter_out)
#     variables_kept = list(df_data.columns)

#     # Keep track of dropped vars to remove them from mappings
#     variables_deleted = [x for x in column_names_start if x not in variables_kept]

#     # Turn data back into dict
#     data = df_data.to_dict("records")

#     return data, variables_deleted


def remove_empty_and_nan_variables(data):
    # Read data into a pandas dataframe
    df_data = pd.DataFrame.from_dict(data)

    # column names before delete
    column_names_start = list(df_data.columns)

    df_data_copy = df_data.copy()

    variables_to_delete = []

    # Drop any vars not to be included in a final Argovis JSON profile
    params_to_filter_out = get_parameters_to_filter_out()

    filtered_cols_to_drop = [
        param for param in column_names_start if param in params_to_filter_out
    ]

    df_data_copy = df_data_copy.drop(columns=filtered_cols_to_drop)

    # columns remaining in df_data
    remaining_cols = list(df_data_copy.columns)

    variables_to_delete.extend(filtered_cols_to_drop)

    # Check if all values for column are nan,
    # and if they are, drop them
    df_data_copy = df_data_copy.dropna(axis=1, how="all")

    nan_columns = set(remaining_cols).difference(df_data_copy.columns)
    nan_columns = list(nan_columns)

    variables_to_delete.extend(nan_columns)

    remaining_cols = list(df_data_copy.columns)

    # Drop empty columns (full of blanks)
    df_data_copy = df_data_copy.replace(r"^\s*$", np.nan, regex=True)
    df_data_copy = df_data_copy.dropna(axis=1, how="all")

    empty_columns = set(remaining_cols).difference(df_data_copy.columns)
    empty_columns = list(empty_columns)

    variables_to_delete.extend(empty_columns)

    # search for any corresponding qc columns to delete
    qc_vars_to_delete = []

    for var in variables_to_delete:
        qc_var = f"{var}_qc"
        if qc_var in column_names_start:
            qc_vars_to_delete.append(qc_var)

    variables_to_delete.extend(qc_vars_to_delete)

    cols_to_drop = [
        param for param in column_names_start if param in variables_to_delete
    ]

    df_data = df_data.drop(columns=cols_to_drop)

    # Turn data back into dict
    data = df_data.to_dict("records")

    return data, variables_to_delete


def get_subset_meta(meta):
    # meta mapping only includes meta values want to keep

    meta_subset = {}

    # keys are names used in this program at start, values are final argovis names
    rename_mapping = get_meta_mapping()

    meta_keys = meta.keys()

    for key, value in rename_mapping.items():
        if key in meta_keys:
            meta_subset[value] = meta[key]

    return meta_subset


def create_geolocation_dict(lat, lon):
    # "geolocation": {
    #     "coordinates": [
    #         -158.2927,
    #         21.3693
    #     ],
    #     "type": "Point"
    # },

    coordinates = [lon, lat]

    geo_dict = {}
    geo_dict["coordinates"] = coordinates
    geo_dict["type"] = "Point"

    return geo_dict


def add_argovis_meta(meta, data_type):
    # Add in station_cast var for later
    # processing of groups. But in
    # final JSON, it's dropped

    # lower case the station since BTL and CTD
    # could have the same station but won't compare
    # the same because case difference

    expocode = meta["expocode"]
    station = meta["station"]
    cast = meta["cast"]

    # Create unique id
    def create_id(x, y, data_type):
        station = (str(x).zfill(3)).lower()
        cast = str(y).zfill(3)
        return f"expo_{expocode}_sta_{station}_cast_{cast}_type_{data_type}"

    meta["_id"] = create_id(station, cast, data_type)

    # Do I put this here or later when looking at the 'collection'?
    # They could want instrument to refer to cruise having btl & ctd
    # even if they aren't combined into one file
    # meta['instrument'] = f"ship_{data_type}"

    meta["positioning_system"] = "GPS"
    meta["data_center"] = "CCHDO"

    # # Create a source_info and data_info dict that will hold multiple mapping keys
    # meta['source'] = {}
    # meta['data_info'] = {}

    meta["instrument"] = f"ship_{data_type}"

    # **********************************
    # Use dates instead of time variable
    # **********************************

    profile_time = meta["time"]
    meta["date_formatted"] = pd.to_datetime(profile_time).strftime("%Y-%m-%d")

    # meta['date'] = pd.to_datetime(profile_time).isoformat()
    meta["date"] = pd.to_datetime(profile_time).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Don't include cchdo meta time var
    meta.pop("time", None)

    # **************************************
    # Create ArgoVis lat/lon extra meta data
    # and geolocation key
    # **************************************

    latitude = meta["latitude"]
    longitude = meta["longitude"]

    # round_lat = np.round(latitude, 3)
    # round_lon = np.round(longitude, 3)

    # meta["roundLat"] = round_lat
    # meta["roundLon"] = round_lon

    # meta["strLat"] = f"{round_lat} N"
    # meta["strLon"] = f"{round_lon} E"

    geo_dict = create_geolocation_dict(latitude, longitude)

    meta["geolocation"] = geo_dict

    # then remove latitude and longitude since it is stored in geolocation
    # meta.pop("latitude", None)
    # meta.pop("longitude", None)

    return meta


def add_cchdo_meta(meta, cchdo_file_meta, cchdo_cruise_meta):
    # cchdo_file_meta from cchdo file json
    # cchdo_cruise_meta from  cchdo cruise json

    meta["file_expocode"] = meta.pop("expocode", None)

    meta = {**meta, **cchdo_file_meta, **cchdo_cruise_meta}

    return meta


# def coalesce_extra_dim_variables(profile_dict):
#     extra_dim_obj = profile_dict["extra_dim_obj"]

#     data = profile_dict["data"]

#     # Put data in a pandas dataframe, pull out the extra dim columns,
#     # combine them into one column as an array (number of WAVELENGTHS)
#     # Then save as json

#     df = pd.DataFrame.from_records(data)

#     # Get list of the renamed variables that are no longer needed since
#     # are coalescing into single value arrays and qc arrays and an array
#     # identifying the columns such as the wavelength number
#     variables_to_delete_in_mappings = {}
#     variables_to_add_in_mappings = {}

#     for key, value in extra_dim_obj.items():
#         if key == "cdom":
#             wavelengths = value["wavelengths"]
#             cdom_variables = value["variables"]

#             non_qc = [var for var in cdom_variables if not var.endswith("_qc")]
#             qc = [var for var in cdom_variables if var.endswith("_qc")]

#             # Sort on column number
#             qc.sort()
#             non_qc.sort()

#             df["cdom"] = df[non_qc].values.tolist()
#             df["cdom_qc"] = df[qc].values.tolist()

#             df = df.drop(cdom_variables, axis=1)

#             # Save cdom wavelengths as a list in each cell of a column of the dataframe
#             indices = list(df.index)

#             df.loc[indices, "cdom_wavelengths"] = pd.Series(
#                 [wavelengths] * len(indices), index=indices
#             )

#             df["cdom_wavelengths"] = df["cdom_wavelengths"].apply(lambda x: list(x))

#             variables_to_delete_in_mappings["cdom"] = cdom_variables

#             # Will need to use attributes extracted from starting xarray file object
#             # in the mappings when remove variables and add these in.
#             # cdom and cdom_qc will have the same attributes as the deleted expanded vars
#             variables_to_add_in_mappings["cdom"] = {
#                 "non_qc_var": "cdom",
#                 "qc_var": "cdom_qc",
#                 "col_naming_var": "cdom_wavelengths",
#             }

#     data = df.to_dict("records")

#     profile_dict["data"] = data

#     return profile_dict, variables_to_delete_in_mappings, variables_to_add_in_mappings


def process_data_profiles(profiles_obj):
    data_type = profiles_obj["data_type"]

    expocode = profiles_obj["cchdo_cruise_meta"]["expocode"]

    # logging.info(f'Processing data profiles for {expocode}')

    cchdo_file_meta = profiles_obj["cchdo_file_meta"]
    cchdo_cruise_meta = profiles_obj["cchdo_cruise_meta"]

    data_profiles = profiles_obj["data_type_profiles_list"]

    updated_data_profiles = []

    # Keep track of deleted variables in each data profile and data type
    deleted_vars = {}
    deleted_vars["data_type"] = data_type
    deleted_vars["vars_per_profile"] = []

    kept_vars = {}
    kept_vars["data_type"] = data_type
    kept_vars["vars_per_profile"] = []

    for profile in data_profiles:
        station_cast = profile["station_cast"]
        profile_dict = profile["profile_dict"]

        new_profile = {}
        new_profile["station_cast"] = station_cast

        meta = profile_dict["meta"]
        data = profile_dict["data"]

        # has_extra_dim = profile_dict["has_extra_dim"]

        # **************
        # Combine variables with extra dims into two variables

        # One containing the values of each column and the
        # other a two dimensional array of the coalesced variables that were
        # previously exploded into separate one dim variables of the starting xarray
        # ****************

        # For cdom
        # One containing the wavelengths of each column and the
        # other a two dimensional array of the coalesced variables that were
        # previously exploded into separate one dim variables of the starting xarray

        # july 28, comment out

        # if has_extra_dim:
        #     (
        #         profile_dict,
        #         variables_to_delete_in_mappings,
        #         variables_to_add_in_mappings,
        #     ) = coalesce_extra_dim_variables(profile_dict)

        #     data = profile_dict["data"]

        # else:
        #     variables_to_delete_in_mappings = []
        #     variables_to_add_in_mappings = []

        # variables_to_delete_in_mappings = []
        # variables_to_add_in_mappings = []

        # *****************************
        # Delete variables from profile
        # if values are all NaN and if
        # they are to be removed from final
        # Argovis format. Also delete
        # the corresponding qc column
        # *****************************

        data, variables_deleted = remove_empty_and_nan_variables(data)

        # *********************************
        # Remove deleted vars from mappings
        # *********************************

        profile_dict = remove_deleted_vars_from_mappings(
            profile_dict, variables_deleted
        )

        # ****************************************
        # Remove exploded var names
        # if expanded a variable having extra dims
        # ****************************************

        # if has_extra_dim:
        #     profile_dict = remove_extra_dim_vars_from_mappings(
        #         profile_dict,
        #         variables_to_delete_in_mappings,
        #         variables_to_add_in_mappings,
        #     )

        # ******************************
        # Add metadata to cchdo metadata
        # ******************************

        meta = add_cchdo_meta(meta, cchdo_file_meta, cchdo_cruise_meta)

        meta = add_argovis_meta(meta, data_type)

        # Rename meta for argovis json format and get subset
        meta = get_subset_meta(meta)

        profile_id = meta["_id"]

        # *************************
        # Rename all_data variables
        # *************************

        data, argovis_col_names_mapping = rename_data(data_type, data)

        # **************************************
        # Save kept and deleted vars per profile
        # **************************************

        # Get renamed variables deleted
        argovis_vars_deleted_mapping = rename_to_argovis_mapping(
            variables_deleted, data_type
        )

        argovis_vars_deleted = list(argovis_vars_deleted_mapping.values())

        profile_deleted_vars = {}
        profile_deleted_vars["profile_id"] = profile_id
        profile_deleted_vars["param_names"] = argovis_vars_deleted

        deleted_vars["vars_per_profile"].append(profile_deleted_vars)

        argovis_vars_kept = list(data[0].keys())

        profile_kept_vars = {}
        profile_kept_vars["profile_id"] = profile_id
        profile_kept_vars["param_names"] = argovis_vars_kept

        kept_vars["vars_per_profile"].append(profile_kept_vars)

        # ********************
        # Get current mappings
        # ********************

        # Copy over current mappings
        current_mappings = {}

        # get mapping of keys used in this program to final Argovis key names
        cchdo_key_mapping = get_program_argovis_source_info_mapping()

        for key, value in profile_dict.items():
            if key in cchdo_key_mapping.keys():
                current_mappings[key] = value

        # **************************************
        # Create New Mappings to include Argovis variable names
        # and add in cchdoConvertedUnits & cchdoConvertedReferenceScale
        # to cchdo mappings if there are any
        # **************************************

        new_mappings = create_mappings(profile_dict, argovis_col_names_mapping)

        # ****************
        # Combine mappings
        # ****************

        mappings = {**current_mappings, **new_mappings}

        # Rename data_info mapping names used in this program to those used for Argovis output
        mappings = rename_mappings_data_info_keys(mappings)

        # *********************************
        # Reorganize mappings into one dict
        # *********************************

        new_meta = reorganize_meta_and_mappings(meta, mappings)

        # ******************
        # Remove any meta variables associated with CDOM that are
        # not in the data section
        # ******************

        new_meta = modify_meta_for_cdom(new_meta, data)

        # *********************
        # Update profile values
        # *********************

        new_profile_dict = {}

        new_profile_dict["station_cast"] = station_cast
        new_profile_dict["data_type"] = data_type

        new_profile_dict["meta"] = new_meta

        new_profile_dict["data"] = data

        # renamed_profile_dict = rename_core_profile_keys(new_profile_dict)
        # new_profile["profile_dict"] = renamed_profile_dict

        new_profile["profile_dict"] = new_profile_dict

        updated_data_profiles.append(new_profile)

    return updated_data_profiles, kept_vars, deleted_vars


def post_process_cruise_objs_by_type(cruises_profile_objs):
    # Rename variables and add argovis mappings

    updated_cruises_profile_objs = []

    kept_vars_all_cruise_objs = []
    deleted_vars_all_cruise_objs = []

    for cruise_profiles_obj in cruises_profile_objs:
        cruise_expocode = cruise_profiles_obj["cruise_expocode"]

        logging.info("****************************")
        logging.info(f"Post processing {cruise_expocode}")
        logging.info("****************************")

        all_data_types_profile_objs = cruise_profiles_obj["all_data_types_profile_objs"]

        updated_all_data_types_profile_objs = []

        kept_vars_cruise_obj = {}
        kept_vars_cruise_obj["cruise_expocode"] = cruise_expocode
        kept_vars_cruise_obj["vars"] = []

        deleted_vars_cruise_obj = {}
        deleted_vars_cruise_obj["cruise_expocode"] = cruise_expocode
        deleted_vars_cruise_obj["vars"] = []

        for profiles_obj in all_data_types_profile_objs:
            data_type = profiles_obj["data_type"]

            updated_data_profiles, kept_vars, deleted_vars = process_data_profiles(
                profiles_obj
            )

            kept_vars_cruise_obj["vars"].append(kept_vars)
            deleted_vars_cruise_obj["vars"].append(deleted_vars)

            new_profiles_obj = {}
            new_profiles_obj["data_type"] = data_type

            new_profiles_obj["data_type_profiles_list"] = updated_data_profiles

            updated_all_data_types_profile_objs.append(new_profiles_obj)

        cruise_profiles_obj[
            "all_data_types_profile_objs"
        ] = updated_all_data_types_profile_objs

        updated_cruises_profile_objs.append(cruise_profiles_obj)

        kept_vars_all_cruise_objs.append(kept_vars_cruise_obj)
        deleted_vars_all_cruise_objs.append(deleted_vars_cruise_obj)

    return (
        updated_cruises_profile_objs,
        kept_vars_all_cruise_objs,
        deleted_vars_all_cruise_objs,
    )
