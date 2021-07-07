import rename_objects as rn

import get_profile_mapping_and_conversions as pm

# Objects storing core Objects

#  TODO
# consolidate into less objects and use a core
# and expand that


def get_goship_salnity_reference_scale():

    return {
        'goship_ref_scale': 'PSS-78'
    }


def get_argovis_reference_scale_per_type():

    return {
        'temperature': 'ITS-90',
        'salinity': 'PSS-78'
    }


def get_goship_argovis_unit_mapping():

    return {
        'dbar': 'decibar',
        '1': 'psu',
        'degC': 'Celsius',
        'umol/kg': 'micromole/kg',
        'meters': 'meters',
        'ml/l': 'micromole/kg'
    }


def get_goship_argovis_name_mapping_per_type(type):

    return {
        'pressure': 'pres',
        'ctd_salinity': f'psal_{type}',
        'ctd_salinity_qc': f'psal_{type}_qc',
        'ctd_temperature': f'temp_{type}',
        'ctd_temperature_qc': f'temp_{type}_qc',
        'ctd_temperature_68': f'temp_{type}',
        'ctd_temperature_68_qc': f'temp_{type}_qc',
        'ctd_oxygen': f'doxy_{type}',
        'ctd_oxygen_qc': f'doxy_{type}_qc',
        'ctd_oxygen_ml_l': f'doxy_{type}',
        'ctd_oxygen_ml_l_qc': f'doxy_{type}_qc',
        'bottle_salinity': f'salinity_{type}',
        'bottle_salinity_qc': f'salinity_{type}_qc',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def get_goship_argovis_measurements_mapping():

    # No extension for measurements

    return {
        'pressure': 'pres',
        'ctd_salinity': 'psal',
        'ctd_temperature': 'temp',
        'ctd_temperature_68': 'temp',
        'ctd_oxygen': 'doxy',
        'ctd_oxygen_ml_l': 'doxy',
        'bottle_salinity': 'salinity',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def get_goship_argovis_core_values_mapping(type):

    return {
        'pressure': 'pres',
        'ctd_salinity': f'psal_{type}',
        'ctd_temperature': f'temp_{type}',
        'ctd_temperature_68': f'temp_{type}',
        'ctd_oxygen': f'doxy_{type}',
        'ctd_oxygen_ml_l': f'doxy_{type}',
        'bottle_salinity': f'salinity_{type}',
        'latitude': 'lat',
        'longitude': 'lon'
    }


# Add in bottle_salinity since will use this in
# measurements to check if have ctd_salinity, and
# if now, use bottle_salinity
def get_argovis_core_values_per_type(type):
    if type == 'btl':
        return ['pres', 'temp_btl', 'temp_btl_qc', 'psal_btl',  'psal_btl_qc', 'salinity_btl', 'salinity_btl_qc']

    if type == 'ctd':
        return ['pres', 'temp_ctd', 'temp_ctd_qc', 'psal_ctd',  'psal_ctd_qc']


def get_argovis_core_values():
    return ['pres', 'temp', 'temp_qc', 'ctd_salinity', 'temp_qc', 'ctd_salinity_qc', 'temp_68', 'temp_68_qc', 'bottle_salinity', 'bottle_salinity_qc']


def get_argovis_meta_mapping():
    return {'latitude': 'lat',
            'longitude': 'lon'}


def get_goship_argovis_name_mapping():
    return {
        'pressure': 'pres',
        'ctd_salinity': 'psal',
        'ctd_salinity': 'psal_qc',
        'ctd_temperature': 'temp',
        'ctd_temperature_qc': 'temp_qc',
        'ctd_temperature_68': 'temp',
        'ctd_temperature_68_qc': 'temp_qc',
        'ctd_oxygen': 'doxy',
        'ctd_oxygen_qc': 'doxy_qc',
        'ctd_oxygen_ml_l': 'doxy',
        'ctd_oxygen_ml_l_qc': 'doxy_qc',
        'bottle_salinity': 'salinity',
        'bottle_salinity_qc': 'salinity_qc',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def create_goship_ref_scale_mapping(nc):

    vars = nc.keys()
    ref_scale = {}

    for var in vars:
        try:
            var_ref_scale = nc[var].attrs['reference_scale']

            if var_ref_scale != 'unknown':
                ref_scale[var] = var_ref_scale

        except KeyError:
            pass

    return ref_scale


def create_goship_argovis_core_values_mapping(type):

    return {
        'pressure': f'pres',
        'ctd_salinity': f'psal_{type}',
        'ctd_salinity_qc': f'psal_{type}_qc',
        'ctd_temperature': f'temp_{type}',
        'ctd_temperature_qc': f'temp_{type}_qc',
        'ctd_temperature_68': f'temp_{type}',
        'ctd_temperature_68_qc': f'temp_{type}_qc',
        'ctd_oxygen': f'doxy_{type}',
        'ctd_oxygen_qc': f'doxy_{type}_qc',
        'ctd_oxygen_ml_l': f'doxy',
        'ctd_oxygen_ml_l_qc': f'doxy_qc',
        'bottle_salinity': f'salinity_{type}',
        'bottle_salinity_qc': f'salinity_{type}_qc',
        'latitude': 'lat',
        'longitude': 'lon'
    }


def create_goship_c_format_mapping(nc):

    meta_names, param_names = pm.get_meta_param_names(nc)

    goship_c_format = {}

    # TODO
    # What about bottom depth?

    for var in meta_names:

        # Not all vars have c_format
        try:
            # Get goship c_format of var
            var_goship_format = nc.coords[var].attrs['C_format']
            goship_c_format[var] = var_goship_format
        except:
            pass

    for var in param_names:

        # Not all vars have c_format
        try:
            # Get goship c_format of var
            var_goship_format = nc[var].attrs['C_format']
            goship_c_format[var] = var_goship_format
        except:
            pass

    return goship_c_format


def create_goship_unit_mapping(nc):

    meta_names, param_names = pm.get_meta_param_names(nc)

    goship_units = {}

    # TODO
    # What about bottom depth?

    for var in meta_names:

        # Not all vars have units
        try:
            # Get goship units of var
            var_goship_unit = nc.coords[var].attrs['units']
            goship_units[var] = var_goship_unit
        except:
            pass

    for var in param_names:

        # Not all vars have units
        try:
            # Get goship units of var
            var_goship_unit = nc[var].attrs['units']
            goship_units[var] = var_goship_unit
        except:
            pass

    return goship_units


def get_argovis_ref_scale_mapping(goship_names, type):

    # Want to use ship names as a key and then rename according to obj type

    # By the time this mapping is created, ctd_temperature_68 was
    # already converted to the ITS-90 scale

    core_mapping = {
        'ctd_temperature': 'ITS-90',
        'ctd_temperature_68': 'ITS-90',
        'ctd_salinity': 'PSS-78',
        'bottle_salinity': 'PSS-78'
    }

    ref_scale_mapping = {key: val for key,
                         val in core_mapping.items() if key in goship_names}

    # Now convert to argovis names depending on type
    new_ref_scale_mapping = rn.rename_goship_on_key_not_meta(
        ref_scale_mapping, type)

    return new_ref_scale_mapping
