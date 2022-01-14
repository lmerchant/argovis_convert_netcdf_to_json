
# def get_argovis_mappings_param(nc):

#     param_mapping = {}

#     param_units = {}
#     param_ref_scale = {}
#     param_c_format = {}
#     param_dtype = {}

#     # Param: Save units, ref_scale, and c_format, dtype

#     for var in nc.keys():
#         try:
#             param_units[var] = nc[var].attrs['units']
#         except KeyError:
#             pass

#         try:
#             if nc[var].attrs['reference_scale'] != 'unknown':
#                 param_ref_scale[var] = nc[var].attrs['reference_scale']
#         except KeyError:
#             pass

#         try:
#             param_c_format[var] = nc[var].attrs['C_format']
#         except KeyError:
#             pass

#         try:
#             param_dtype[var] = nc[var].dtype
#         except KeyError:
#             pass

#     param_mapping['names'] = list(nc.keys())
#     param_mapping['units'] = param_units
#     param_mapping['ref_scale'] = param_ref_scale
#     param_mapping['c_format'] = param_c_format
#     param_mapping['dtype'] = param_dtype

#     return param_mapping


# def get_argovis_mappings_meta(nc):

#     meta_mapping = {}

#     meta_units = {}
#     meta_ref_scale = {}
#     meta_c_format = {}
#     meta_dtype = {}

#     # Meta: Save units, ref_scale, c_format, dtype
#     for var in nc.coords:

#         try:
#             meta_units[var] = nc[var].attrs['units']
#         except KeyError:
#             pass

#         try:
#             meta_ref_scale[var] = nc[var].attrs['reference_scale']
#         except KeyError:
#             pass

#         try:
#             meta_c_format[var] = nc[var].attrs['C_format']
#         except KeyError:
#             pass

#         try:
#             meta_dtype[var] = nc[var].dtype
#         except KeyError:
#             pass

#     meta_mapping['names'] = list(nc.coords)
#     meta_mapping['units'] = meta_units
#     meta_mapping['ref_scale'] = meta_ref_scale
#     meta_mapping['c_format'] = meta_c_format
#     meta_mapping['dtype'] = meta_dtype

#     return meta_mapping


def get_var_mapping(nc, var_names):

    # TODO
    # Need to update with oxygen not converted to units
    # Doesn't show after conversion because all units
    # needed to be set to the same value

    mapping = {}

    var_units = {}
    var_ref_scale = {}
    var_c_format = {}
    var_dtype = {}
    var_netcdf_name = {}

    for var in var_names:

        try:
            var_units[var] = nc[var].attrs['units']
        except KeyError:
            pass

        try:
            if nc[var].attrs['reference_scale'] != 'unknown':
                var_ref_scale[var] = nc[var].attrs['reference_scale']
        except KeyError:
            pass

        if 'source_C_format' in nc[var].attrs:
            c_format_attr = 'source_C_format'
        else:
            c_format_attr = 'C_format'

        try:
            var_c_format[var] = nc[var].attrs[c_format_attr]
        except KeyError:
            pass

        try:
            var_dtype[var] = nc[var].dtype
        except KeyError:
            pass

        try:
            var_netcdf_name[var] = nc[var].attrs['standard_name']
        except KeyError:
            pass

    mapping['units'] = var_units
    mapping['ref_scale'] = var_ref_scale
    mapping['c_format'] = var_c_format
    mapping['dtype'] = var_dtype
    mapping['netcdf_name'] = var_netcdf_name

    return mapping


def get_nc_variable_mappings(nc, coords_vars_names):

    # get names, units, ref_scale, c_format, dtype
    # and netcdf standard_name attribute

    coords_vars_mapping = get_var_mapping(nc, coords_vars_names)

    mapping = {}

    keys = ['units', 'ref_scale', 'c_format', 'dtype', 'netcdf_name']

    for key in keys:
        mapping[key] = coords_vars_mapping[key]

    mapping['names'] = coords_vars_names

    return mapping
