import numpy as np
import xarray as xr
import logging


from global_vars import GlobalVars


def explode_cdom_variable(nc):

    # coord holding CDOM column information is
    # CDOM_WAVELENGTHS

    # parameter holding CDOM values is
    # cdom and cdom_qc

    # I want to explode cdom and cdom_qc and name by column number

    cdom_variables = []

    num_cols = len(nc.coords['CDOM_WAVELENGTHS'])

    try:
        cdom_var = nc['cdom']

        for col in range(num_cols):

            name = f"cdom_col_{col}"

            # shape of array is (N_PROF, N_LEVELS, CDOM_WAVELENGTHS)
            nc[name] = cdom_var[:, :, col]

            cdom_variables.append(name)

        nc = nc.drop_vars(['cdom'])

    except KeyError:

        pass

    try:

        cdom_var = nc['cdom_qc']

        for col in range(num_cols):

            name = f"cdom_col_{col}_qc"

            # shape of array is (N_PROF, N_LEVELS, CDOM_WAVELENGTHS)
            nc[name] = cdom_var[:, :, col]

            cdom_variables.append(name)

        nc = nc.drop_vars(['cdom_qc'])

    except KeyError:
        pass

    # Remove CDOM_WAVELENGTHS coord dimension and replace with a coordinate variable

    cdom_wavelengths = nc.coords['CDOM_WAVELENGTHS'].values

    # n_prof_values = nc.coords['N_PROF'].values
    # num_n_prof = len(n_prof_values)

    # nc_xarray = nc.load()

    # for indx, wavelength in enumerate(cdom_wavelengths):

    #     new_var = f"cdom_col{indx}_wavelength_{wavelength}"

    #     new_vals = np.tile(cdom_wavelengths.transpose(), (1, num_n_prof))

    #     new_vals = np.repeat(wavelength, num_n_prof)

    #     array = xr.DataArray(new_vals, dims=['N_PROF'])

    #     nc_xarray[new_var] = array

    # nc_xarray = nc_xarray.drop_dims(['CDOM_WAVELENGTHS'])

    # nc = nc_xarray.chunk(chunks={"N_PROF": GlobalVars.CHUNK_SIZE})

    nc = nc.drop_dims(['CDOM_WAVELENGTHS'])

    return nc, cdom_wavelengths, cdom_variables


def explode_cdom_vars_to_cols(file_obj):

    nc = file_obj['nc']

    # get dimensions and check if it has dim CDOM_WAVELENGTHS

    nc_dimensions = list(nc.dims)

    if 'CDOM_WAVELENGTHS' in nc_dimensions:

        nc, cdom_wavelengths, cdom_variables = explode_cdom_variable(nc)

        file_obj['nc'] = nc

        file_obj['has_extra_dim'] = True

        extra_dim_obj = {}
        cdom_obj = {}
        cdom_obj['wavelengths'] = cdom_wavelengths
        cdom_obj['variables'] = cdom_variables

        extra_dim_obj['cdom'] = cdom_obj

        file_obj['extra_dim_obj'] = extra_dim_obj

        logging.info("Variable with extra dimension found")

        return file_obj
    else:

        file_obj['has_extra_dim'] = False

        return file_obj
