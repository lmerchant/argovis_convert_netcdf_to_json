import get_variable_mappings as gvm


# Get variable mappings and convert ref scales and units to argovis

def convert_oxygen(nc, var, var_goship_units, argovis_units):

    if var_goship_units == 'ml/l' and argovis_units == 'micromole/kg':

        # Convert to micromole/kg
        oxygen = nc[var].data
        converted_oxygen = oxygen * 44.6596

        try:
            c_format = nc[var].attrs['C_format']
            f_format = c_format.lstrip('%')
            new_oxygen = [float(f"{item:{f_format}}")
                          for item in converted_oxygen]

        except:
            # Use num decimal places of var
            num_decimal_places = abs(
                Decimal(str(oxygen)).as_tuple().exponent)

            new_oxygen = round(converted_oxygen, num_decimal_places)

        # Set oxygen value in nc because use it later to
        # create profile dict
        nc[var].data = new_oxygen
        nc[var].attrs['units'] = 'micromole/kg'

    return nc


def convert_goship_to_argovis_units(data_obj):

    nc = data_obj['nc']
    params = data_obj['param']

    # If goship units aren't the same as argovis units, convert
    # So far, just converting oxygen

    goship_argovis_units_mapping = gvm.get_goship_argovis_unit_mapping()

    for var in params:
        if 'oxygen' in var:

            try:
                var_goship_units = nc[var].attrs['units']
                argovis_units = goship_argovis_units_mapping[var]

                is_unit_same = var_goship_units == argovis_units

                if not is_unit_same:
                    nc = convert_oxygen(
                        nc, var, var_goship_units, argovis_units)
            except:
                pass

    data_obj['nc'] = nc

    return data_obj


def convert_sea_water_temp(nc, var, var_goship_ref_scale, argovis_ref_scale):

    # Check sea_water_temperature to have goship_reference_scale be ITS-90

    if var_goship_ref_scale == 'IPTS-68' and argovis_ref_scale == 'ITS-90':

        # Convert to ITS-90 scal
        temperature = nc[var].data

        converted_temperature = temperature/1.00024

        # Set nc var of temp to this value
        try:
            c_format = nc[var].attrs['C_format']
            f_format = c_format.lstrip('%')
            new_temperature = [float(f"{item:{f_format}}")
                               for item in converted_temperature]

        except:
            # Use num decimal places of var
            num_decimal_places = abs(
                Decimal(str(temperature)).as_tuple().exponent)

            new_temperature = round(converted_temperature, num_decimal_places)

        # Set temperature value in nc because use it later to
        # create profile dict
        nc[var].data = new_temperature
        nc[var].attrs['reference_scale'] = 'ITS-90'

    return nc


def convert_goship_to_argovis_ref_scale(data_obj):

    nc = data_obj['nc']
    params = data_obj['param']

    # If argo ref scale not equal to goship ref scale, convert

    # So far, it's only the case for temperature

    # loop through variables and look at reference scale,
    # if it is IPTS-68 then convert

    argovis_ref_scale_per_type = gvm.get_argovis_reference_scale_per_type()

    for var in params:
        if 'temperature' in var:

            try:
                # Get goship reference scale of var
                var_goship_ref_scale = nc[var].attrs['reference_scale']

                argovis_ref_scale = argovis_ref_scale_per_type['temperature']
                is_same_scale = var_goship_ref_scale == argovis_ref_scale

                if not is_same_scale:
                    nc = convert_sea_water_temp(
                        nc, var, var_goship_ref_scale, argovis_ref_scale)
            except:
                pass

    data_obj['nc'] = nc

    return data_obj


def get_meta_param_names(nc):

    # Meta names have size N_PROF and no N_LEVELS
    # Parameter names have size N_PROF AND N_LEVELS

    #  TODO
    # Why not find btm_depth? Not finding it for units mapping

    # It is  N_PROF dimension only. Maybe need to look
    # for a size  not N_LEVELS
    # Does it find cast and station for meta names?

    meta_names = []
    param_names = []

    # check coords
    for name in list(nc.coords):
        size = nc[name].sizes

        try:
            size['N_LEVELS']
            param_names.append(name)
        except KeyError:
            meta_names.append(name)

    # check params
    for name in list(nc.keys()):
        size = nc[name].sizes

        try:
            size['N_LEVELS']
            param_names.append(name)
        except KeyError:
            meta_names.append(name)

    # Remove variables not wanted
    meta_names.remove('profile_type')
    meta_names.remove('geometry_container')

    return meta_names, param_names


def get_profile_mapping_and_conversions(data_obj):

    data_obj = gvm.create_goship_unit_mapping(data_obj)
    data_obj = gvm.create_goship_ref_scale_mapping(data_obj)

    # get c-format (string representation of numbers)
    data_obj = gvm.create_goship_c_format_mapping(data_obj)

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    # Only converting temperature so far
    data_obj = convert_goship_to_argovis_ref_scale(data_obj)

    # Add convert units function
    data_obj = convert_goship_to_argovis_units(data_obj)

    return data_obj
