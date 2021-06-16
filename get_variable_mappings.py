import rename_objects as rn

# Objects storing core Objects


def get_goship_salniity_reference_scale():

    return {
        'goship_ref_scale': 'PSS-78'
    }


def get_argovis_reference_scale_per_type():

    return {
        'temperature': 'ITS-90',
        'salinity': 'PSS-78'
    }


def get_argovis_reference_scale():

    return {
        'ctd_temperature': 'ITS-90',
        'ctd_salinity': 'PSS-78'
    }


def get_goship_argovis_unit_mapping():

    return {
        'dbar': 'decibar',
        '1': 'psu',
        'degC': 'Celsius',
        'umol/kg': 'micromole/kg',
        'meters': 'meters'
    }


def get_goship_argovis_core_values_mapping_bot():

    return {
        'pressure': 'pres',
        'ctd_salinity': 'psal_btl',
        'ctd_temperature': 'temp_btl',
        'ctd_temperature_68': 'temp_btl',
        'ctd_oxygen': 'doxy_btl',
        'bottle_salinity': 'salinity_btl',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def get_goship_argovis_core_values_mapping_ctd():

    return {
        'pressure': 'pres',
        'ctd_salinity': 'psal_ctd',
        'ctd_temperature': 'temp_ctd',
        'ctd_temperature_68': 'temp_btl',
        'ctd_oxygen': 'doxy_ctd',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def get_goship_argovis_core_values_mapping(type):

    if type == 'bot':

        return {
            'pressure': 'pres',
            'ctd_salinity': 'psal_btl',
            'ctd_temperature': 'temp_btl',
            'ctd_temperature_68': 'temp_btl',
            'ctd_oxygen': 'doxy_btl',
            'bottle_salinity': 'salinity_btl',
            'latitude': 'lat',
            'longitude': 'lon'
        }

    if type == 'ctd':

        return {
            'pressure': 'pres',
            'ctd_salinity': 'psal_ctd',
            'ctd_temperature': 'temp_ctd',
            'ctd_temperature_68': 'temp_btl',
            'ctd_oxygen': 'doxy_ctd',
            'latitude': 'lat',
            'longitude': 'lon'
        }


def get_core_values_no_qc():

    return ['pressure', 'ctd_temperature', 'ctd_salinity', 'ctd_oxygen']


def get_goship_core_values():
    return ['pressure', 'ctd_temperature', 'ctd_temperature_qc', 'ctd_salinity', 'ctd_oxygen', 'ctd_temperature_qc', 'ctd_salinity_qc', 'ctd_oxygen_qc', 'ctd_temperature_68', 'ctd_temperature_68_qc']


def get_argovis_meta_mapping():
    return {'latitude': 'lat',
            'longitude': 'lon'}


def get_argovis_ref_scale_mapping_dict():

    return {
        'temp_ctd': 'ITS-90',
        'psal_ctd': 'PSS-78'
    }


def get_argovis_ref_scale_mapping_dict_bot():

    return {
        'temp_btl': 'ITS-90',
        'psal_btl': 'PSS-78',
        'salinity_btl': 'PSS-78'
    }


def create_goship_ref_scale(data_obj):

    nc = data_obj['nc']
    vars = nc.keys()
    ref_scale = {}

    for var in vars:
        try:
            var_ref_scale = nc[var].attrs['reference_scale']

            ref_scale[var] = var_ref_scale

        except KeyError:
            pass

    data_obj['goship_ref_scale'] = ref_scale

    return data_obj


def create_goship_argovis_core_values_mapping(data_obj):

    if data_obj['type'] == 'bot':

        core_mapping = {
            'pressure': 'pres',
            'ctd_salinity': 'psal_btl',
            'ctd_temperature': 'temp_btl',
            'ctd_temperature_68': 'temp_btl',
            'ctd_oxygen': 'doxy_btl',
            'bottle_salinity': 'salinity_btl',
            'latitude': 'lat',
            'longitude': 'lon'
        }

    elif data_obj['type'] == 'ctd':

        core_mapping = {
            'pressure': 'pres',
            'ctd_salinity': 'psal_ctd',
            'ctd_temperature': 'temp_ctd',
            'ctd_temperature_68': 'temp_ctd',
            'ctd_oxygen': 'doxy_ctd',
            'latitude': 'lat',
            'longitude': 'lon'
        }

    meta_names = data_obj['meta']
    param_names = data_obj['param']

    # If meta or param in the core_mapping, keep in
    # the returned mapping
    core_names = core_mapping.keys()

    new_mapping = {}

    for name in meta_names:
        if name in core_names:
            new_mapping[name] = core_mapping[name]

    for name in meta_names:
        if name in core_names:
            new_mapping[name] = core_mapping[name]

    new_mapping_meta = {key: val for key,
                        val in core_mapping.items() if key in meta_names}
    new_mapping_param = {key: val for key,
                         val in core_mapping.items() if key in param_names}

    name_mapping = {**new_mapping_meta, **new_mapping_param}

    return name_mapping


def create_goship_unit_mapping(data_obj):

    nc = data_obj['nc']
    meta = data_obj['meta']
    params = data_obj['param']

    goship_units = {}

    for var in meta:

        # Not all vars have units
        try:
            # Get goship units of var
            var_goship_unit = nc.coords[var].attrrs['units']
            goship_units[var] = var_goship_unit
        except:
            pass

    for var in params:

        # Not all vars have units
        try:
            # Get goship units of var
            var_goship_unit = nc[var].attrrs['units']
            goship_units[var] = var_goship_unit
        except:
            pass

    data_obj['goship_units'] = goship_units

    return data_obj


def create_argovis_ref_scale_mapping(data_obj):

    # here

    type = data_obj['type']

    core_mapping = {
        'ctd_temperature': 'ITS-90',
        'ctd_salinity': 'PSS-78',
        'bottle_salinity': 'PSS-78'
    }

    meta_names = data_obj['meta']
    param_names = data_obj['param']

    meta_mapping = {key: val for key,
                    val in core_mapping.items() if key in meta_names}

    param_mapping = {key: val for key,
                     val in core_mapping.items() if key in param_names}

    ref_scale_mapping = {**meta_mapping, **param_mapping}

    # Now convert to argovis names
    new_ref_scale_mapping = rn.rename_argovis_not_meta(ref_scale_mapping, type)

    return new_ref_scale_mapping
