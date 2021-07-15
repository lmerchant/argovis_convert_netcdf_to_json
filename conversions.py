# Unit and Ref scale conversions

from decimal import Decimal


import get_variable_mappings as gvm


def convert_oxygen(nc, var, var_goship_units, argovis_units):

    # TODO
    # Don't round off. Do that when c_format used on all vars

    if var_goship_units == 'ml/l' and argovis_units == 'micromole/kg':

        # https://www.nodc.noaa.gov/OC5/WOD/wod18-notes.html
        # 1 ml/l of O2 is approximately 43.570 µmol/kg
        # (assumes a molar volume of O2 of 22.392 l/mole and a
        # constant seawater potential density of 1025 kg/m3).

        # Convert to micromole/kg
        oxygen = nc[var].data
        converted_oxygen = oxygen * 43.570

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


def convert_goship_to_argovis_units(nc):

    params = nc.keys()

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

    return nc


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


def convert_goship_to_argovis_ref_scale(nc):

    params = nc.keys()

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

    return nc


def convert_oxygen(nc, var, var_goship_units, argovis_units):

    if var_goship_units == 'ml/l' and argovis_units == 'micromole/kg':

        # https://www.nodc.noaa.gov/OC5/WOD/wod18-notes.html
        # 1 ml/l of O2 is approximately 43.570 µmol/kg
        # (assumes a molar volume of O2 of 22.392 l/mole and a
        # constant seawater potential density of 1025 kg/m3).

        # Convert to micromole/kg
        oxygen = nc[var].data
        converted_oxygen = oxygen * 43.570

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


def convert_goship_to_argovis_units(nc):

    params = nc.keys()

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

    return nc


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


def convert_goship_to_argovis_ref_scale(nc):

    params = nc.keys()

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

    return nc


def apply_equations_and_ref_scale(nc):

    # Rename converted temperature later.
    # Keep 68 in name and show it maps to temp_ctd
    # and ref scale show what scale it was converted to

    # Converting to argovis ref scale if needed
    nc = convert_goship_to_argovis_ref_scale(nc)

    # Apply equations to convert units
    nc = convert_goship_to_argovis_units(nc)

    return nc
