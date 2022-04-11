def get_cchdo_argovis_unit_name_mapping():

    # Special case is mapping of '1' -> 'psu' for salinity
    # Since other vars have unit of '1' and that won't change,
    # need to also check reference scale to be that of salinity

    return {
        'dbar': 'decibar',
        'degC': 'Celsius',
        'umol/kg': 'micromole/kg'
    }


def change_units_to_argovis(units_mapping, ref_scale_mapping):

    # units_mapping => cchdo name: cchdo unit
    # ref_scale_mapping => cchdo name: cchdo ref scale

    # Rename units (no conversion)

    cchdo_param_names = units_mapping.keys()

    unit_name_mapping = get_cchdo_argovis_unit_name_mapping()

    # Get reference scale to determine if salinity because there
    # can be a cchdo unit of '1' that is not salinity
    salinity_ref_scale = 'PSS-78'

    cchdo_name_to_argovis_unit_mapping = {}

    for param in cchdo_param_names:

        # Change salinity  unit
        try:
            cchdo_ref_scale = ref_scale_mapping[param]
            cchdo_unit = units_mapping[param]

            if cchdo_ref_scale == salinity_ref_scale and cchdo_unit == '1':
                new_unit = 'psu'
        except KeyError:
            new_unit = units_mapping[param]

        # Change other units
        try:
            cchdo_unit = units_mapping[param]

            if cchdo_unit != '1':
                new_unit = unit_name_mapping[cchdo_unit]
        except KeyError:
            new_unit = units_mapping[param]

        cchdo_name_to_argovis_unit_mapping[param] = new_unit

    return cchdo_name_to_argovis_unit_mapping
