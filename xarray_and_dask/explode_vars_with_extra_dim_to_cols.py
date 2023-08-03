import numpy as np
import logging


def add_coord(nc, coord_length, coord_name, var):
    new_coord_list = [var] * coord_length
    new_coord_np = np.array(new_coord_list, dtype=object)
    nc = nc.assign_coords({coord_name: ("N_PROF", new_coord_np)})

    return nc


def explode_cdom_variable(nc):
    # coord holding CDOM column information is
    # CDOM_WAVELENGTHS

    # parameter holding CDOM values is
    # cdom and cdom_qc

    # explode cdom and cdom_qc out of 2dim array into columns

    cdom_variables = []

    num_cols = len(nc.coords["CDOM_WAVELENGTHS"])

    cdom_wavelengths = nc.coords["CDOM_WAVELENGTHS"].values
    cdom_wavelengths = cdom_wavelengths.tolist()

    coord_length = nc.dims["N_PROF"]

    # Add CDOM wavelengths as a coordinate to be a meta variable later
    cdom_wavelengths_str = ",".join([str(cdom) for cdom in cdom_wavelengths])

    nc = add_coord(nc, coord_length, "cdom_wavelengths", cdom_wavelengths_str)

    try:
        cdom_var = nc["cdom"]

        # for col in range(num_cols):
        for idx, col in enumerate(cdom_wavelengths):
            name = f"cdom_wavelength_{col}"

            # shape of array is (N_PROF, N_LEVELS, CDOM_WAVELENGTHS)
            nc[name] = cdom_var[:, :, idx]

            cdom_variables.append(name)

        nc = nc.drop_vars(["cdom"])

    except KeyError:
        pass

    try:
        cdom_var = nc["cdom_qc"]

        # for col in range(num_cols):
        for idx, col in enumerate(cdom_wavelengths):
            name = f"cdom_wavelength_{col}_qc"

            # shape of array is (N_PROF, N_LEVELS, CDOM_WAVELENGTHS)
            nc[name] = cdom_var[:, :, idx]

            cdom_variables.append(name)

        nc = nc.drop_vars(["cdom_qc"])

    except KeyError:
        pass

    # Remove CDOM_WAVELENGTHS coord dimension so nc only has
    # dims of N_PROF and N_LEVELS
    nc = nc.drop_dims(["CDOM_WAVELENGTHS"])

    return nc, cdom_wavelengths, cdom_variables


def explode_cdom_vars_to_cols(file_obj):
    nc = file_obj["nc"]

    # get dimensions and check if it has dim CDOM_WAVELENGTHS

    nc_dimensions = list(nc.dims)

    if "CDOM_WAVELENGTHS" in nc_dimensions:
        nc, cdom_wavelengths, cdom_variables = explode_cdom_variable(nc)

        file_obj["nc"] = nc

        file_obj["has_extra_dim"] = True

        extra_dim_obj = {}
        cdom_obj = {}
        cdom_obj["wavelengths"] = cdom_wavelengths
        cdom_obj["variables"] = cdom_variables

        extra_dim_obj["cdom"] = cdom_obj

        file_obj["extra_dim_obj"] = extra_dim_obj

        logging.info("Variable with extra dimension found")

        return file_obj
    else:
        file_obj["has_extra_dim"] = False

        return file_obj
