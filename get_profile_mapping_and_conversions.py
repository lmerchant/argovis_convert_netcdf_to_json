from decimal import Decimal

import get_variable_mappings as gvm


# ***** Is this used? *******

# def get_meta_param_names(nc):

#     # Meta names have size N_PROF and no N_LEVELS
#     # Parameter names have size N_PROF AND N_LEVELS

#     meta_names = []
#     param_names = []

#     # check coords
#     for name in list(nc.coords):
#         size = nc[name].sizes

#         try:
#             size['N_LEVELS']
#             param_names.append(name)
#         except KeyError:
#             meta_names.append(name)

#     # check params
#     for name in list(nc.keys()):
#         size = nc[name].sizes

#         try:
#             size['N_LEVELS']
#             param_names.append(name)
#         except KeyError:
#             meta_names.append(name)

#     # Remove variables not wanted
#     meta_names.remove('profile_type')
#     meta_names.remove('geometry_container')

#     return meta_names, param_names


# # ***** Is this used? ******
# def get_profile_conversions(nc):

#     # Rename converted temperature later.
#     # Keep 68 in name and show it maps to temp_ctd
#     # and ref scale show what scale it was converted to

#     # Only converting temperature so far
#     #data_obj = convert_goship_to_argovis_ref_scale(data_obj)
#     nc = convert_goship_to_argovis_ref_scale(nc)

#     # Add convert units function
#     nc = convert_goship_to_argovis_units(nc)

#     return nc


# # **** Is this used ******
# def get_profile_mapping(nc, station_cast):

#     goship_units = gvm.create_goship_unit_mapping(nc)

#     goship_ref_scale = gvm.create_goship_ref_scale_mapping(nc)

#     # get c-format (string representation of numbers)
#     goship_c_format = gvm.create_goship_c_format_mapping(nc)

#     profile_mapping = {}
#     profile_mapping['station_cast'] = station_cast
#     profile_mapping['goship_c_format'] = goship_c_format
#     profile_mapping['goship_ref_scale'] = goship_ref_scale
#     profile_mapping['goship_units'] = goship_units

#     return profile_mapping


# def get_profile_mapping_and_conversions(data_obj):

#     data_obj = gvm.create_goship_unit_mapping(data_obj)
#     data_obj = gvm.create_goship_ref_scale_mapping(data_obj)
#     data_obj = gvm.create_goship_c_format_mapping(data_obj)

#     return data_obj
