import pandas as pd


import get_profile_mapping_and_conversions as pmc
import rename_objects as rn

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
def get_argovis_core_meas_values_per_type(type):

    return ['pres', f"temp_{type}", f"temp_{type}_qc", f"psal_{type}",  f"psal_{type}_qc", 'salinity_btl', 'salinity_btl_qc']

    # if type == 'btl':
    #     return ['pres', 'temp_btl', 'temp_btl_qc', 'psal_btl',  'psal_btl_qc', 'salinity_btl', 'salinity_btl_qc']

    # if type == 'ctd':
    #     return ['pres', 'temp_ctd', 'temp_ctd_qc', 'psal_ctd',  'psal_ctd_qc']


# def get_argovis_core_values():
#     return ['pres', 'temp', 'temp_qc', 'ctd_salinity', 'ctd_salinity_qc', 'temp_68', 'temp_68_qc', 'bottle_salinity', 'bottle_salinity_qc']


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


def get_goship_argovis_core_values_mapping(type):

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

    meta_names, param_names = pmc.get_meta_param_names(nc)

    goship_c_format = {}

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

    meta_names, param_names = pmc.get_meta_param_names(nc)

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


def create_meta_col_name_mapping(cols):

    core_values_mapping = get_goship_argovis_name_mapping()

    # core_values = core_values_mapping.keys()
    core_values = [key for key in core_values_mapping.keys() if key in cols]

    col_mapping = {}

    for name in cols:

        if name in core_values:
            mapped_name = core_values_mapping[name]

        else:
            mapped_name = name

        col_mapping[name] = mapped_name

    return col_mapping


def create_param_col_name_mapping_w_type(cols, type):

    # names including '_qc'
    core_values_mapping = get_goship_argovis_core_values_mapping(type)

    # check if both ctd_temperature  and ctd_temperature_68 are
    # in the file, if it is, only rename ctd_temperature as core.
    # If not, rename ctd_temperature_68
    # Same for oxygen, check for ctd_oxygen and ctd_oxygen_ml_l

    has_both_temp = 'ctd_temperature' in cols and 'ctd_temperature_68' in cols
    has_both_oxygen = 'ctd_oxygen' in cols and 'ctd_oxygen_ml_l' in cols

    primary_temp = ['ctd_temperature']
    primary_oxygen = ['ctd_oxygen']

    if has_both_temp:
        core_values_mapping.pop('ctd_temperature_68', None)

    if has_both_oxygen:
        core_values_mapping.pop('ctd_oxygen_ml_l', None)

    core_values = [core for core in core_values_mapping.keys() if core in cols]

    col_mapping = {}

    for name in cols:

        if name.endswith('_qc'):
            non_qc_name = name.replace('_qc', '')

            if has_both_temp and non_qc_name in primary_temp:
                new_name = f"{core_values_mapping[non_qc_name]}_qc"

            elif has_both_oxygen and non_qc_name in primary_oxygen:
                new_name = f"{core_values_mapping[non_qc_name]}_qc"

            elif non_qc_name in core_values:
                new_name = f"{core_values_mapping[non_qc_name]}_qc"
            else:
                new_name = f"{non_qc_name}_{type}_qc"

        elif has_both_temp and name in primary_temp:
            new_name = core_values_mapping[name]
        elif has_both_oxygen and name in primary_oxygen:
            new_name = core_values_mapping[name]

        elif name in core_values:
            new_name = core_values_mapping[name]
        else:
            new_name = f"{name}_{type}"

        col_mapping[name] = new_name

    return col_mapping


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

    # for var in nc.keys():
    #     try:
    #         param_units[var] = nc[var].attrs['units']
    #     except:
    #         param_units[var] = None

    #     try:
    #         param_ref_scale[var] = nc[var].attrs['reference_scale']
    #     except:
    #         param_ref_scale[var] = None

    #     try:
    #         param_c_format[var] = nc[var].attrs['C_format']
    #     except:
    #         param_c_format[var] = None

    #     try:
    #         param_dtype[var] = nc[var].dtype
    #     except KeyError:
    #         param_dtype[var] = None

    # param_mapping['names'] = list(nc.keys())
    # param_mapping['units'] = {key: val for key,
    #                           val in param_units.items() if val}
    # param_mapping['ref_scale'] = {
    #     key: val for key, val in param_ref_scale.items() if val}
    # param_mapping['c_format'] = {key: val for key,
    #                              val in param_c_format.items() if val}
    # param_mapping['dtype'] = {key: val for key,
    #                           val in param_dtype.items() if val}


def get_goship_mappings_meta(nc, meta_goship_names):

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

    meta_mapping['names'] = meta_goship_names
    meta_mapping['units'] = meta_units
    meta_mapping['ref_scale'] = meta_ref_scale
    meta_mapping['c_format'] = meta_c_format
    meta_mapping['dtype'] = meta_dtype

    return meta_mapping

    # for var in nc.coords:
    #     try:
    #         meta_units[var] = nc[var].attrs['units']
    #     except:
    #         meta_units[var] = None

    #     try:
    #         meta_ref_scale[var] = nc[var].attrs['reference_scale']
    #     except:
    #         meta_ref_scale[var] = None

    #     try:
    #         meta_c_format[var] = nc[var].attrs['C_format']
    #     except:
    #         meta_c_format[var] = None

    #     try:
    #         meta_dtype[var] = nc[var].dtype
    #     except KeyError:
    #         meta_dtype[var] = None

    # meta_mapping['units'] = {key: val for key,
    #                          val in meta_units.items() if val}
    # meta_mapping['ref_scale'] = {key: val for key,
    #                              val in meta_ref_scale.items() if val}
    # # meta_mapping['c_format'] = {key: val for key,
    # #                             val in meta_c_format.items() if val and val != 'station_cast'}
    # meta_mapping['c_format'] = {key: val for key,
    #                             val in meta_c_format.items() if val}
    # meta_mapping['dtype'] = {key: val for key,
    #                          val in meta_dtype.items() if val}


def create_goship_argovis_mapping(goship_names, type):

    goship_argovis_mapping = get_goship_argovis_core_values_mapping(
        type)

    # Keep only keys that are in goship_names
    goship_argovis_mapping = {
        key: val for key, val in goship_argovis_mapping.items() if key in goship_names}

    has_both_temp = 'ctd_temperature' in goship_names and 'ctd_temperature_68' in goship_names

    has_both_oxygen = 'ctd_oxygen' in goship_names and 'ctd_oxygen_ml_l' in goship_names

    if has_both_temp:
        goship_argovis_mapping.pop('ctd_temperature_68')
        if 'ctd_temperature_68_qc' in goship_names:
            goship_argovis_mapping.pop('ctd_temperature_68_qc')

    if has_both_oxygen:
        goship_argovis_mapping.pop('ctd_oxygen_ml_l')
        if 'ctd_oxygen_ml_l_qc' in goship_names:
            goship_argovis_mapping.pop('ctd_oxygen_ml_l_qc')

    return goship_argovis_mapping


def create_mapping_for_profile(meta_mapping, param_mapping, type):

    # This function is for one station_cast
    meta_names = meta_mapping['names']
    meta_units = meta_mapping['units']
    meta_ref_scale = meta_mapping['ref_scale']
    meta_c_format = meta_mapping['c_format']

    param_names = param_mapping['names']
    param_units = param_mapping['units']
    param_ref_scale = param_mapping['ref_scale']
    param_c_format = param_mapping['c_format']

    goship_names = [*meta_names, *param_names]
    goship_units = {**meta_units, **param_units}
    goship_ref_scale = {**meta_ref_scale, **param_ref_scale}
    goship_c_format = {**meta_c_format, **param_c_format}

    # Remove null values
    goship_units = {key: val for key,
                    val in goship_units.items() if pd.notnull(val)}

    goship_ref_scale = {key: val for key,
                        val in goship_ref_scale.items() if pd.notnull(val)}
    goship_c_format = {key: val for key,
                       val in goship_c_format.items() if pd.notnull(val)}

    mapping_dict = {}

    mapping_dict['goshipArgovisNameMapping'] = create_goship_argovis_mapping(
        goship_names, type)

    mapping_dict['argovisReferenceScale'] = get_argovis_ref_scale_mapping(
        goship_names, type)

    mapping_dict['goshipNames'] = goship_names
    mapping_dict['goshipReferenceScale'] = goship_ref_scale
    mapping_dict['goshipUnits'] = goship_units
    mapping_dict['goshipCformat'] = goship_c_format

    mapping_dict['goshipArgovisUnitsMapping'] = get_goship_argovis_unit_mapping()

    return mapping_dict
