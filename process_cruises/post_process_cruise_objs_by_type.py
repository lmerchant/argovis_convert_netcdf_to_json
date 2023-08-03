import logging

import pandas as pd
import numpy as np
from pathlib import Path

from global_vars import GlobalVars

from variable_naming.filter_parameters import get_parameters_to_filter_out
from variable_naming.rename_parameters import rename_to_argovis_mapping
from variable_naming.meta_param_mapping import rename_mappings_source_info_keys
from variable_naming.meta_param_mapping import rename_mappings_data_info_keys
from variable_naming.meta_param_mapping import get_meta_mapping
from variable_naming.meta_param_mapping import rename_core_profile_keys

# from variable_naming.meta_param_mapping import rename_measurements_keys
from variable_naming.meta_param_mapping import get_program_argovis_source_info_mapping
from variable_naming.meta_param_mapping import get_program_argovis_data_info_mapping
from variable_naming.meta_param_mapping import get_source_independent_meta_names
from variable_naming.rename_units import change_units_to_argovis

from create_profiles.update_profiles_single_type import update_profiles_single_type
from check_and_save.save_output import save_data_type_profiles


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

    # new_data_info.append(
    #     [
    #         "data_keys_mapping",
    #         "units",
    #         "data_source_standard_names",
    #         "reference_scale",
    #     ]
    # )

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


# def rename_measurements(measurements):
#     df_measurements = pd.DataFrame.from_dict(measurements)

#     # If there is both ctd_temperature and ctd_temperature_68, remove
#     # the ctd_temperature_68 column
#     # check which temperature to use if both exist
#     col_names = list(df_measurements.columns)
#     is_ctd_temp = "ctd_temperature" in col_names
#     is_ctd_temp_68 = "ctd_temperature_68" in col_names

#     if is_ctd_temp and is_ctd_temp_68:
#         df_measurements.drop("ctd_temperature_68", axis=1, inplace=True)

#     # if there is both a ctd_oxygen and ctd_oxygen_ml_l, remove the
#     # oxygen_ml_l column
#     col_names = list(df_measurements.columns)
#     is_ctd_oxy = "ctd_oxygen" in col_names
#     is_ctd_oxy_ml_l = "ctd_oxygen_ml_l" in col_names

#     if is_ctd_oxy and is_ctd_oxy_ml_l:
#         df_measurements.drop("ctd_oxygen_ml_l", axis=1, inplace=True)

#     # key is current name and value is argovis name
#     # TODO
#     # rename function to cchdo_to_argovis_mapping
#     meas_name_mapping = rename_to_argovis_mapping(list(df_measurements.columns))

#     # Don't use suffix on measurementsSourceQC

#     new_meas_name_mapping = {}
#     for cchdo_name, argovis_name in meas_name_mapping.items():
#         new_meas_name_mapping[cchdo_name] = argovis_name

#     # df_measurements = df_measurements.set_axis(
#     #     list(new_meas_name_mapping.values()), axis='columns', inplace=False)

#     df_measurements = df_measurements.set_axis(
#         list(new_meas_name_mapping.values()), axis="columns"
#     )

#     # Can have either salinity instead of psal if using bottle_salinity
#     # need to rename saliniy to psal
#     # TODO
#     # Don't rename till after use with combined types
#     # since relying on salinity to exist to select it

#     # if 'salinity' in df_measurements.columns:
#     #     df_measurements = df_measurements.rename(
#     #         {'salinity': 'psal'}, axis=1)

#     # TODO
#     # Or could rename if keep track of what it was.
#     # Wouldn't measurements source do that for me?

#     # combined is looking at key = 'measurementsSourceQC'
#     # for source of psal or salinity. Look at where I
#     # create this for one source

#     measurements = df_measurements.to_dict("records")

#     return measurements


# def rename_measurements_sources(measurements_sources):
#     # Rename measurementsSourceQC
#     # If have two 'temperature' var names, means had both
#     # ctd_temerature and ctd_temperature_68
#     # Check before rename, and remove one not being used
#     # if both not being used, remove one so don't end
#     # up with two 'temperature' keys and should
#     # be saying using_temp False

#     renamed_meas_sources = {}

#     # renamed_meas_sources['qc'] = measurements_sources.pop('qc', None)

#     for key, val in measurements_sources.items():
#         # change key to argovis name
#         # e.g. 'using_ctd_temperature' to 'temperature'
#         # var_in_key = key.replace('using_', '')
#         # get map of cchdo name to argovis name
#         # rename_to_argovis expects a list of names to map
#         key_name_mapping = rename_to_argovis_mapping([key])

#         new_key = f"{key_name_mapping[key]}"

#         if key == "ctd_temperature_68":
#             new_key = new_key + "_68"

#         renamed_meas_sources[new_key] = val

#     if ("temperature" in renamed_meas_sources) and (
#         "temperature_68" in renamed_meas_sources
#     ):
#         using_temp = renamed_meas_sources["temperature"]
#         using_temp_68 = renamed_meas_sources["temperature_68"]

#         if using_temp:
#             renamed_meas_sources.pop("temperature_68", None)
#         elif using_temp_68:
#             renamed_meas_sources.pop("temperature", None)
#             renamed_meas_sources["temperature"] = renamed_meas_sources["temperature_68"]
#             renamed_meas_sources.pop("temperature_68", None)
#         else:
#             renamed_meas_sources["temperature"] = False
#             renamed_meas_sources.pop("temperature_68", None)

#     elif ("temperature" not in renamed_meas_sources) and (
#         "temperature_68" in renamed_meas_sources
#     ):
#         renamed_meas_sources["temperature"] = renamed_meas_sources["temperature_68"]
#         renamed_meas_sources.pop("temperature_68", None)

#     return renamed_meas_sources


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

    # df_data = df_data.set_axis(
    #     argovis_col_names, axis='columns', inplace=False)

    df_data = df_data.set_axis(argovis_col_names, axis="columns")

    data_columns = list(df_data.columns)

    # df_data = df_data.set_axis(data_columns, axis='columns', inplace=False)
    df_data = df_data.set_axis(data_columns, axis="columns")

    # ----- delete start

    # Filter out parameters not used for ArgoVis
    # cols_to_filter_out = filter_out_params(list(df_data.columns))

    # print(cols_to_filter_out)

    # if filter out column from params, need to filter from mappings
    # do this earlier in processing

    # df_data = df_data.drop(cols_to_filter_out, axis=1)

    # ----- delete to here

    data = df_data.to_dict("records")

    return data, argovis_col_names_mapping


def remove_extra_dim_vars_from_mappings(
    profile_dict, variables_to_delete_in_mappings, variables_to_add_in_mappings
):
    # Get the names of extra dim vars
    extra_dim_vars = variables_to_add_in_mappings.keys()

    for extra_dim in extra_dim_vars:
        # First keep mapping attribute values of one variable out of the exploded vars
        # Then delete and add variables

        variables_to_delete = variables_to_delete_in_mappings[extra_dim]
        variables_to_add = variables_to_add_in_mappings[extra_dim]

        variables_to_include = []

        non_qc_var_to_add = variables_to_add["non_qc_var"]

        variables_to_include.append(non_qc_var_to_add)

        try:
            qc_var_to_add = variables_to_add["qc_var"]
            variables_to_include.append(qc_var_to_add)
        except:
            pass

        col_naming_var = variables_to_add["col_naming_var"]
        variables_to_include.append(col_naming_var)

        non_qc_vars_to_delete = [
            var for var in variables_to_delete if not var.endswith("_qc")
        ]
        qc_vars_to_delete = [var for var in variables_to_delete if var.endswith("_qc")]

        non_qc_vars_to_delete.sort()
        qc_vars_to_delete.sort()

        sample_non_qc_var = non_qc_vars_to_delete[0]
        sample_qc_var = qc_vars_to_delete[0]

        for key, value in profile_dict.items():
            if key == "cchdo_param_names":
                new_value = [val for val in value if val not in variables_to_delete]

                new_value.extend(variables_to_include)

                profile_dict[key] = new_value

            if key == "cchdo_standard_names":
                new_value = {}
                for k, v in value.items():
                    if k == sample_non_qc_var:
                        new_value[non_qc_var_to_add] = v

                    elif k == sample_qc_var:
                        new_value[qc_var_to_add] = v

                    else:
                        new_value[k] = v

                profile_dict[key] = {
                    k: v for k, v in new_value.items() if k not in variables_to_delete
                }

            if key == "cchdo_reference_scale":
                new_value = {}
                for k, v in value.items():
                    if k == sample_non_qc_var:
                        new_value[non_qc_var_to_add] = v
                    else:
                        new_value[k] = v

                profile_dict[key] = {
                    k: v for k, v in new_value.items() if k not in variables_to_delete
                }

            if key == "cchdo_units":
                new_value = {}
                for k, v in value.items():
                    if k == sample_non_qc_var:
                        new_value[non_qc_var_to_add] = v
                    else:
                        new_value[k] = v

                profile_dict[key] = {
                    k: v for k, v in new_value.items() if k not in variables_to_delete
                }

    return profile_dict


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


def remove_empty_and_nan_variables(data):
    # Read data into a pandas dataframe
    df_data = pd.DataFrame.from_dict(data)

    # column names before delete
    column_names_start = df_data.columns

    # Check if all values for column are nan,
    # and if they are, drop them
    df_data = df_data.dropna(axis=1, how="all")

    columns_not_nan = set(df_data.columns)

    # Check if column values are empty
    df_data_copy = df_data.copy()

    df_data_copy = df_data_copy.replace(r"^\s*$", np.nan, regex=True)
    df_data_copy = df_data_copy.dropna(axis=1, how="all")

    columns_empty = set(df_data_copy.columns)

    empty_columns = columns_not_nan.difference(columns_empty)

    empty_variables_to_delete = list(empty_columns)
    variables_to_keep = list(columns_not_nan)

    for var in empty_variables_to_delete:
        variables_to_keep.remove(var)

    df_data = df_data[variables_to_keep]

    # Delete vars not used in Argovis final file
    # cols_to_filter_out = filter_out_params(variables_kept)

    params_to_filter_out = get_parameters_to_filter_out()

    cols_to_filter_out = [
        param for param in variables_to_keep if param in params_to_filter_out
    ]

    df_data = df_data.drop(columns=cols_to_filter_out)
    variables_kept = df_data.columns

    # Keep track of dropped vars to remove them from mappings
    variables_deleted = [x for x in column_names_start if x not in variables_kept]

    # Turn data back into dict
    data = df_data.to_dict("records")

    return data, variables_deleted


def add_data_type_to_meta(meta, data_type):
    ignore_keys = get_source_independent_meta_names()

    new_meta = {}

    for key, val in meta.items():
        if key not in ignore_keys:
            # new_meta[key] = val
            new_key = f"{key}_{data_type}"
            new_meta[new_key] = val
        else:
            new_meta[key] = val

    return new_meta


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


def coalesce_extra_dim_variables(profile_dict):
    extra_dim_obj = profile_dict["extra_dim_obj"]

    data = profile_dict["data"]

    # Put data in a pandas dataframe, pull out the extra dim columns,
    # combine them into one column as an array (number of WAVELENGTHS)
    # Then save as json

    df = pd.DataFrame.from_records(data)

    # Get list of the renamed variables that are no longer needed since
    # are coalescing into single value arrays and qc arrays and an array
    # identifying the columns such as the wavelength number
    variables_to_delete_in_mappings = {}
    variables_to_add_in_mappings = {}

    for key, value in extra_dim_obj.items():
        if key == "cdom":
            wavelengths = value["wavelengths"]
            cdom_variables = value["variables"]

            non_qc = [var for var in cdom_variables if not var.endswith("_qc")]
            qc = [var for var in cdom_variables if var.endswith("_qc")]

            # Sort on column number
            qc.sort()
            non_qc.sort()

            df["cdom"] = df[non_qc].values.tolist()
            df["cdom_qc"] = df[qc].values.tolist()

            df = df.drop(cdom_variables, axis=1)

            # Save cdom wavelengths as a list in each cell of a column of the dataframe
            indices = list(df.index)

            df.loc[indices, "cdom_wavelengths"] = pd.Series(
                [wavelengths] * len(indices), index=indices
            )

            df["cdom_wavelengths"] = df["cdom_wavelengths"].apply(lambda x: list(x))

            variables_to_delete_in_mappings["cdom"] = cdom_variables

            # Will need to use attributes extracted from starting xarray file object
            # in the mappings when remove variables and add these in.
            # cdom and cdom_qc will have the same attributes as the deleted expanded vars
            variables_to_add_in_mappings["cdom"] = {
                "non_qc_var": "cdom",
                "qc_var": "cdom_qc",
                "col_naming_var": "cdom_wavelengths",
            }

    data = df.to_dict("records")

    profile_dict["data"] = data

    return profile_dict, variables_to_delete_in_mappings, variables_to_add_in_mappings


def process_data_profiles(profiles_obj):
    data_type = profiles_obj["data_type"]

    expocode = profiles_obj["cchdo_cruise_meta"]["expocode"]

    # logging.info(f'Processing data profiles for {expocode}')

    cchdo_file_meta = profiles_obj["cchdo_file_meta"]
    cchdo_cruise_meta = profiles_obj["cchdo_cruise_meta"]

    data_profiles = profiles_obj["data_type_profiles_list"]

    updated_data_profiles = []

    for profile in data_profiles:
        station_cast = profile["station_cast"]
        profile_dict = profile["profile_dict"]

        new_profile = {}
        new_profile["station_cast"] = station_cast

        meta = profile_dict["meta"]
        data = profile_dict["data"]
        # measurements = profile_dict["measurements"]

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

        variables_to_delete_in_mappings = []
        variables_to_add_in_mappings = []

        # *****************************
        # Delete variables from profile
        # if values are all NaN and if
        # they are to be removed from final
        # Argovis format
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

        # Also take meta data, and for items not common to a cruise,
        # Add a data_type suffix in addition to what is there
        # meta = add_data_type_to_meta(meta, data_type)

        # *************************
        # Rename all_data variables
        # *************************

        data, argovis_col_names_mapping = rename_data(data_type, data)

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
        # mappings = rename_mappings_source_info_keys(mappings)

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

        # **************
        # measurements
        # **************

        # TODO
        # Move logic of filtering by core values here since may not
        # use in final json version. Easier to change if wait till
        # end of program and not mix it in the middle

        # TODO
        # What are station parameters?
        # currently they are starting cchdo names

        # measurements_source = profile_dict["measurements_source"]
        # measurements_sources = profile_dict["measurements_sources"]

        # station_parameters = profile_dict.pop('stationParameters', None)

        # Rename measurementsSources keys
        # measurements_sources = rename_measurements_sources(measurements_sources)

        # rename measurements variables
        # measurements = rename_measurements(measurements)

        # *********************
        # Update profile values
        # *********************

        new_profile_dict = {}

        new_profile_dict["station_cast"] = station_cast
        new_profile_dict["data_type"] = data_type

        new_profile_dict["meta"] = new_meta

        new_profile_dict["data"] = data

        # new_profile_dict["measurements"] = measurements
        # new_profile_dict["measurements_source"] = measurements_source
        # new_profile_dict["measurements_sources"] = measurements_sources

        # profile_dict['stationParameters'] = station_parameters

        renamed_profile_dict = rename_core_profile_keys(new_profile_dict)

        # renamed_profile_dict = rename_measurements_keys(renamed_profile_dict)

        # updated_profile_dict = {**renamed_profile_dict, **mappings}

        new_profile["profile_dict"] = renamed_profile_dict

        updated_data_profiles.append(new_profile)

    return updated_data_profiles


# def orig_post_process_cruise_objs_by_type(cruises_profile_objs):
#     # Rename variables and add argovis mappings

#     updated_cruises_profile_objs = []

#     for cruise_profiles_obj in cruises_profile_objs:
#         cruise_expocode = cruise_profiles_obj["cruise_expocode"]

#         logging.info(f"Post processing expocode {cruise_expocode}")

#         all_data_types_profile_objs = cruise_profiles_obj["all_data_types_profile_objs"]

#         updated_all_data_types_profile_objs = []

#         for profiles_obj in all_data_types_profile_objs:
#             data_type = profiles_obj["data_type"]

#             updated_data_profiles = process_data_profiles(profiles_obj)

#             new_profiles_obj = {}
#             new_profiles_obj["data_type"] = data_type

#             new_profiles_obj["data_type_profiles_list"] = updated_data_profiles

#             updated_all_data_types_profile_objs.append(new_profiles_obj)

#         cruise_profiles_obj[
#             "all_data_types_profile_objs"
#         ] = updated_all_data_types_profile_objs

#         updated_cruises_profile_objs.append(cruise_profiles_obj)

#     return updated_cruises_profile_objs


def post_process_cruise_objs_by_type(cruises_profile_objs):
    # Rename variables and add argovis mappings

    updated_cruises_profile_objs = []

    for cruise_profiles_obj in cruises_profile_objs:
        cruise_expocode = cruise_profiles_obj["cruise_expocode"]

        logging.info("****************************")
        logging.info(f"Post processing {cruise_expocode}")
        logging.info("****************************")

        all_data_types_profile_objs = cruise_profiles_obj["all_data_types_profile_objs"]

        updated_all_data_types_profile_objs = []

        for profiles_obj in all_data_types_profile_objs:
            data_type = profiles_obj["data_type"]

            updated_data_profiles = process_data_profiles(profiles_obj)

            new_profiles_obj = {}
            new_profiles_obj["data_type"] = data_type

            new_profiles_obj["data_type_profiles_list"] = updated_data_profiles

            updated_all_data_types_profile_objs.append(new_profiles_obj)

        cruise_profiles_obj[
            "all_data_types_profile_objs"
        ] = updated_all_data_types_profile_objs

        updated_cruises_profile_objs.append(cruise_profiles_obj)

    return updated_cruises_profile_objs
