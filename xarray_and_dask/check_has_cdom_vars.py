def check_has_cdom_vars(file_obj):

    nc = file_obj['nc']

    # get dimensions and check if it has dim CDOM_WAVELENGTHS

    nc_dimensions = list(nc.dims)

    print(f"dimensions are {nc_dimensions}")

    if 'CDOM_WAVELENGTHS' in nc_dimensions:
        return True
    else:
        return False
