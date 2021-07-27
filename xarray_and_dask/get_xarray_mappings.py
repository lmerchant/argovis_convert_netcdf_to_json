
def get_argovis_mappings_param(nc):

    param_mapping = {}

    param_units = {}
    param_ref_scale = {}
    param_c_format = {}
    param_dtype = {}

    # Param: Save units, ref_scale, and c_format, dtype

    for var in nc.keys():
        try:
            param_units[var] = nc[var].attrs['units']
        except KeyError:
            pass

        try:
            if nc[var].attrs['reference_scale'] != 'unknown':
                param_ref_scale[var] = nc[var].attrs['reference_scale']
        except KeyError:
            pass

        try:
            param_c_format[var] = nc[var].attrs['C_format']
        except KeyError:
            pass

        try:
            param_dtype[var] = nc[var].dtype
        except KeyError:
            pass

    param_mapping['names'] = list(nc.keys())
    param_mapping['units'] = param_units
    param_mapping['ref_scale'] = param_ref_scale
    param_mapping['c_format'] = param_c_format
    param_mapping['dtype'] = param_dtype

    return param_mapping


def get_argovis_mappings_meta(nc):

    meta_mapping = {}

    meta_units = {}
    meta_ref_scale = {}
    meta_c_format = {}
    meta_dtype = {}

    # Meta: Save units, ref_scale, c_format, dtype
    for var in nc.coords:

        try:
            meta_units[var] = nc[var].attrs['units']
        except KeyError:
            pass

        try:
            meta_ref_scale[var] = nc[var].attrs['reference_scale']
        except KeyError:
            pass

        try:
            meta_c_format[var] = nc[var].attrs['C_format']
        except KeyError:
            pass

        try:
            meta_dtype[var] = nc[var].dtype
        except KeyError:
            pass

    meta_mapping['names'] = list(nc.coords)
    meta_mapping['units'] = meta_units
    meta_mapping['ref_scale'] = meta_ref_scale
    meta_mapping['c_format'] = meta_c_format
    meta_mapping['dtype'] = meta_dtype

    return meta_mapping


def get_goship_mappings_param(nc):

    param_mapping = {}

    param_units = {}
    param_ref_scale = {}
    param_c_format = {}
    param_dtype = {}

    # Param: Save units, ref_scale, and c_format, dtype

    for var in nc.keys():
        try:
            param_units[var] = nc[var].attrs['units']
        except KeyError:
            pass

        try:
            if nc[var].attrs['reference_scale'] != 'unknown':
                param_ref_scale[var] = nc[var].attrs['reference_scale']
        except KeyError:
            pass

        try:
            param_c_format[var] = nc[var].attrs['C_format']
        except KeyError:
            pass

        try:
            param_dtype[var] = nc[var].dtype
        except KeyError:
            pass

    param_mapping['names'] = list(nc.keys())
    param_mapping['units'] = param_units
    param_mapping['ref_scale'] = param_ref_scale
    param_mapping['c_format'] = param_c_format
    param_mapping['dtype'] = param_dtype

    return param_mapping


def get_goship_mappings_meta(nc):

    meta_mapping = {}

    meta_units = {}
    meta_ref_scale = {}
    meta_c_format = {}
    meta_dtype = {}

    # Meta: Save units, ref_scale, c_format, dtype
    for var in nc.coords:

        try:
            meta_units[var] = nc[var].attrs['units']
        except KeyError:
            pass

        try:
            meta_ref_scale[var] = nc[var].attrs['reference_scale']
        except KeyError:
            pass

        try:
            meta_c_format[var] = nc[var].attrs['C_format']
        except KeyError:
            pass

        try:
            meta_dtype[var] = nc[var].dtype
        except KeyError:
            pass

    meta_mapping['names'] = list(nc.coords)
    meta_mapping['units'] = meta_units
    meta_mapping['ref_scale'] = meta_ref_scale
    meta_mapping['c_format'] = meta_c_format
    meta_mapping['dtype'] = meta_dtype

    return meta_mapping
