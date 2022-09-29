def get_cchdo_core_meas_var_names():
    return ['pressure', 'ctd_salinity', 'ctd_salinity_qc', 'bottle_salinity', 'bottle_salinity_qc', 'ctd_temperature', 'ctd_temperature_qc', 'ctd_temperature_68', 'ctd_temperature_68_qc']


# def get_cchdo_core_meas_salinity_names():
#     return ['ctd_salinity', 'ctd_salinity_qc', 'bottle_salinity', 'bottle_salinity_qc']


# def get_cchdo_core_meas_temperature_names():
#     return ['ctd_temperature', 'ctd_temperature_qc', 'ctd_temperature_68', 'ctd_temperature_68_qc']


# def get_cchdo_core_meas_oxygen_names():
#     return ['ctd_oxygen', 'ctd_oxygen_qc', 'ctd_oxygen_ml_l', 'ctd_oxygen_ml_l_qc']


def choose_core_temperature_from_hierarchy(col_names):

    # check which temperature to use if both exist
    is_ctd_temp = 'ctd_temperature' in col_names
    is_ctd_temp_68 = 'ctd_temperature_68' in col_names

    if (is_ctd_temp and is_ctd_temp_68) or is_ctd_temp:
        use_temperature = 'ctd_temperature'
    elif is_ctd_temp_68:
        use_temperature = 'ctd_temperature_68'
    else:
        use_temperature = ''

    return use_temperature


# def choose_core_oxygen_from_hierarchy(col_names):

#     # TODO
#     # do i need this if oxygen not part of core meas values?

#     # check which oxygen to use if both exist
#     is_ctd_oxy = 'ctd_oxygen' in col_names
#     is_ctd_oxy_ml_l = 'ctd_oxygen_ml_l' in col_names

#     if (is_ctd_oxy and is_ctd_oxy_ml_l) or is_ctd_oxy:
#         use_oxygen = 'ctd_oxygen'
#     elif is_ctd_oxy_ml_l:
#         use_oxygen = 'ctd_oxygen_ml_l'
#     else:
#         use_oxygen = ''

#     return use_oxygen


# def choose_core_salinity_from_hierarchy(col_names):

#     # check which salinity to use if both exist
#     is_ctd_sal = 'ctd_salinity' in col_names
#     is_bottle_sal = 'bottle_salinity' in col_names

#     if (is_ctd_sal and is_bottle_sal) or is_ctd_sal:
#         use_salinity = 'ctd_salinity'
#     elif is_bottle_sal:
#         use_salinity = 'bottle_salinity'
#     else:
#         use_salinity = ''

#     return use_salinity

# def get_argovis_core_meas_values_per_type(data_type):

#     # Add in bottle_salinity since will use this in
#     # measurements to check if have ctd_salinity, and
#     # if now, use bottle_salinity

#     # Since didn't rename to argovis yet, use cchdo names
#     # which means multiple temperature and oxygen names for ctd vars
#     # standing for different ref scales

#     # return ['pres', f"temp_{data_type}", f"temp_{data_type}_qc", f"psal_{data_type}",  f"psal_{data_type}_qc", 'salinity_btl', 'salinity_btl_qc']

#     return ['pressure', f"temperature_{data_type}", f"temperature_{data_type}_qc", f"salinity_{data_type}",  f"salinity_{data_type}_qc", 'bottle_salinity_btl', 'bottle_salinity_btl_qc']
