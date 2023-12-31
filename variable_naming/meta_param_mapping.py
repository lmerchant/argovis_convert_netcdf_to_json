def get_parameters_no_data_type():
    # No btl or ctd data type suffix
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


def get_meta_mapping():
    # Values to keep in final meta data

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
        "date": "timestamp",
        "roundLat": "roundLat",
        "roundLon": "roundLon",
        "strLat": "strLat",
        "strLon": "strLon",
        "geolocation": "geolocation",
        "source": "source",
        "instrument": "insturment",
        "cdom_wavelengths": "cdom_wavelengths",
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

    return {
        "argovis_param_names": "data_keys",
        "argovis_units": "data_units",
        "argovis_reference_scale": "data_reference_scale",
        "cchdo_argovis_param_mapping": "data_keys_mapping",
        "cchdo_standard_names": "data_source_standard_names",
        "cchdo_units": "data_source_units",
        "cchdo_reference_scale": "data_source_reference_scale",
    }


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
