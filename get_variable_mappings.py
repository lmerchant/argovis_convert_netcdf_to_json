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

    # TODO
    # is this necessary to split

    if type == 'btl':

        return {
            'pressure': 'pres',
            'ctd_salinity': 'psal_btl',
            'ctd_temperature': 'temp_btl',
            'ctd_temperature_68': 'temp_btl',
            'ctd_oxygen': 'doxy_btl',
            'ctd_oxygen_ml_l': 'doxy',
            'bottle_salinity': 'salinity_btl',
            'latitude': 'lat',
            'longitude': 'lon'
        }

    if type == 'ctd':

        return {
            'pressure': 'pres',
            'ctd_salinity': 'psal_ctd',
            'ctd_temperature': 'temp_ctd',
            'ctd_temperature_68': 'temp_ctd',
            'ctd_oxygen': 'doxy_ctd',
            'ctd_oxygen_ml_l': 'doxy',
            'latitude': 'lat',
            'longitude': 'lon'
        }


# Add in bottle_salinity since will use this in
# measurements to check if have ctd_salinity, and
# if now, use bottle_salinity
def get_goship_core_values():
    return ['pressure', 'ctd_temperature', 'ctd_temperature_qc', 'ctd_salinity', 'ctd_temperature_qc', 'ctd_salinity_qc', 'ctd_temperature_68', 'ctd_temperature_68_qc', 'bottle_salinity', 'bottle_salinity_qc']


def get_argovis_meta_mapping():
    return {'latitude': 'lat',
            'longitude': 'lon'}


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


def create_goship_argovis_core_values_mapping(goship_names, type):

    # TODO
    # is this necessary to split?

    if type == 'btl':

        core_mapping = {
            'pressure': 'pres',
            'ctd_salinity': 'psal_btl',
            'ctd_salinity_qc': 'psal_btl_qc',
            'ctd_temperature': 'temp_btl',
            'ctd_temperature_qc': 'temp_btl_qc',
            'ctd_temperature_68': 'temp_btl',
            'ctd_temperature_68_qc': 'temp_btl_qc',
            'ctd_oxygen': 'doxy_btl',
            'ctd_oxygen_qc': 'doxy_btl_qc',
            'ctd_oxygen_ml_l': 'doxy',
            'ctd_oxygen_ml_l_qc': 'doxy_qc',
            'bottle_salinity': 'salinity_btl',
            'bottle_salinity_qc': 'salinity_btl_qc',
            'latitude': 'lat',
            'longitude': 'lon'
        }

    if type == 'ctd':

        core_mapping = {
            'pressure': 'pres',
            'ctd_salinity': 'psal_ctd',
            'ctd_salinity_qc': 'psal_ctd_qc',
            'ctd_temperature': 'temp_ctd',
            'ctd_temperature_qc': 'temp_ctd_qc',
            'ctd_temperature_68': 'temp_ctd',
            'ctd_temperature_68_qc': 'temp_ctd_qc',
            'ctd_oxygen': 'doxy_ctd',
            'ctd_oxygen_qc': 'doxy_ctd_qc',
            'ctd_oxygen_ml_l': 'doxy',
            'ctd_oxygen_ml_l_qc': 'doxy_qc',
            'latitude': 'lat',
            'longitude': 'lon'
        }

    # If meta or param in the core_mapping, keep in
    # the returned mapping
    core_names = core_mapping.keys()

    new_mapping = {}

    for name in goship_names:
        if name in core_names:
            new_mapping[name] = core_mapping[name]

    name_mapping = {key: val for key,
                    val in core_mapping.items() if key in goship_names}

    return name_mapping


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
