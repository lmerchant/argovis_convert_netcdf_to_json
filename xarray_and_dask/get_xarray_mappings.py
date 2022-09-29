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
