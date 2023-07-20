def get_source_independent_meta_names():
    # This is used when combining btl and ctd profiles which have already been renamed
    # So use the argovis names here

    names = [
        "expocode",
        "station",
        "cast",
        "cchdo_cruise_id",
        "woce_lines",
        "source",
        "data_center",
        "positioning_system",
        "_id",
        "country",
        "pi_name",
        "cruise_url",
    ]

    return names


# def get_source_info_meta_names():
#     # keys going in the meta 'source' key
#     # this is information about the CCHDO input source file
#     names = [
#         "data_keys_source",
#         "data_source_standard_names",
#         "data_source_reference_scale",
#         "data_source_units",
#     ]

#     return names


def get_data_info_meta_names():
    # keys going in the meta 'data_info' key
    # this is information about the Argovis output data file

    names = ["data_keys", "data_keys_mapping", "data_reference_scale", "data_units"]

    return names


def get_parameters_no_data_type():
    return ["pressure", "pressure_qc"]


def get_cchdo_core_meas_var_names():
    return [
        "pressure",
        "pressure_qc",
        "ctd_salinity",
        "ctd_salinity_qc",
        "bottle_salinity",
        "bottle_salinity_qc",
        "ctd_temperature",
        "ctd_temperature_qc",
        "ctd_temperature_68",
        "ctd_temperature_68_qc",
    ]


# TODO
# fix so can remove this. Note renaming it
def get_measurements_mapping():
    return {
        "measurements": "measurements",
        "measurements_source": "measurements_source",
        "measurements_sources": "measurements_sources",
    }


def get_meta_mapping():
    # keys are names used in this program
    # values are argovis names
    return {
        "btm_depth": "btm_depth",
        "latitude": "latitude",
        "longitude": "longitude",
        "expocode": "expocode",
        "file_expocode": "file_expocode",
        "station": "station",
        "cast": "cast",
        "file_hash": "file_hash",
        "cruise_id": "cchdo_cruise_id",
        # "programs": "data_source",
        "programs": "source",
        "woce_lines": "woce_lines",
        "chief_scientists": "pi_name",
        "country": "country",
        "positioning_system": "positioning_system",
        "data_center": "data_center",
        "cruise_url": "cruise_url",
        "file_path": "source_url",
        "file_name": "file_name",
        "_id": "_id",
        "date_formatted": "date_formatted",
        "date": "timestamp",
        "roundLat": "roundLat",
        "roundLon": "roundLon",
        "strLat": "strLat",
        "strLon": "strLon",
        "geolocation": "geolocation",
        "source": "source",
    }


def get_program_argovis_source_info_mapping():
    # keys are names used in this program
    # values are the final key names to be used for Argovis

    return {
        "cchdo_param_names": "data_keys_source",
        "cchdo_reference_scale": "data_source_reference_scale",
        "cchdo_units": "data_source_units",
        "cchdo_standard_names": "data_source_standard_names",
    }


def get_program_argovis_data_info_mapping():
    # keys are names used in this program
    # values are the final key names to be used for Argovis

    # return {
    #     "argovis_param_names": "data_keys",
    #     "cchdo_argovis_param_mapping": "data_keys_mapping",
    #     "argovis_reference_scale": "data_reference_scale",
    #     "argovis_units": "data_units",
    # }

    return {
        "argovis_param_names": "data_keys",
        "argovis_units": "data_units",
        "argovis_reference_scale": "data_reference_scale",
        "cchdo_argovis_param_mapping": "data_keys_mapping",
        "cchdo_standard_names": "data_source_standard_names",
        "cchdo_units": "data_source_units",
        "cchdo_reference_scale": "data_source_reference_scale",
    }


def get_combined_mappings_keys():
    # Get mapping keys that can be combined
    # such as names. Even though the btl file and ctd file may have same names,
    # want to just have one set of names
    # But for the data units, keep separate since no guarantee the units will be the same
    # use argovis names

    return [
        "data_keys_source",
        "data_keys",
        "data_keys_mapping",
        "data_reference_scale",
        "data_units",
        "data_source_standard_names",
    ]


def get_cchdo_argovis_name_mapping():
    return {
        "ctd_salinity": f"salinity",
        "ctd_salinity_qc": f"salinity_woceqc",
        "ctd_temperature": f"temperature",
        "ctd_temperature_qc": f"temperature_woceqc",
        "ctd_temperature_68": f"temperature",
        "ctd_temperature_68_qc": f"temperature_woceqc",
        "ctd_oxygen": f"doxy",
        "ctd_oxygen_qc": f"doxy_woceqc",
        "ctd_oxygen_ml_l": f"doxy",
        "ctd_oxygen_ml_l_qc": f"doxy_woceqc",
    }


def get_core_profile_keys_mapping():
    return {"data": "data"}


def rename_mappings_source_info_keys(mappings):
    # keys are CCHDO and values are Argovis
    key_mapping = get_program_argovis_source_info_mapping()

    new_mappings = {}
    for key, value in mappings.items():
        if key in key_mapping:
            new_key = key_mapping[key]
            new_mappings[new_key] = value
        else:
            new_mappings[key] = value

    return new_mappings


def rename_mappings_data_info_keys(mappings):
    # keys are CCHDO and values are Argovis
    key_mapping = get_program_argovis_data_info_mapping()

    new_mappings = {}
    for key, value in mappings.items():
        if key in key_mapping:
            new_key = key_mapping[key]
            new_mappings[new_key] = value
        else:
            new_mappings[key] = value

    return new_mappings


def rename_core_profile_keys(profile):
    key_mapping = get_core_profile_keys_mapping()

    # keys are CCHDO and values are Argovis
    new_profile = {}
    for key, value in profile.items():
        if key in key_mapping:
            new_key = key_mapping[key]
            new_profile[new_key] = value
        else:
            new_profile[key] = value

    return new_profile


def rename_measurements_keys(profile):
    key_mapping = get_measurements_mapping()

    # keys are CCHDO and values are Argovis
    new_profile = {}
    for key, value in profile.items():
        if key in key_mapping:
            new_key = key_mapping[key]
            new_profile[new_key] = value
        else:
            new_profile[key] = value

    return new_profile
