
import logging

import xarray_and_dask.modify_xarray_meta as mod_xr_meta
import xarray_and_dask.modify_xarray_param as mod_xr_param
import xarray_and_dask.get_xarray_mappings as xr_map
import xarray_and_dask.conversions as xr_conv
from xarray_and_dask.rename_to_argovis import rename_to_argovis
from global_vars import GlobalVars


def rearrange_nc(nc):

    # TODO
    # Think of a better way

    # Move all meta data to coords
    # Move all param data to vars

    # metadata is any without N_LEVELS dimension
    meta_names = [
        coord for coord in nc.coords if 'N_LEVELS' not in nc.coords[coord].dims]

    param_names = [
        coord for coord in nc.coords if 'N_LEVELS' in nc.coords[coord].dims]

    meta_names_from_var = [var for var in nc if 'N_LEVELS' not in nc[var].dims]
    param_names_from_var = [var for var in nc if 'N_LEVELS' in nc[var].dims]

    meta_names.extend(meta_names_from_var)
    param_names.extend(param_names_from_var)

    # move params from coords to variables
    # Move if not in nc.coords
    coords_to_move_to_vars = [
        name for name in param_names if name not in list(nc.keys())]
    vars_to_move_to_coords = [
        name for name in meta_names if name not in list(nc.coords)]

    nc = nc.reset_coords(names=coords_to_move_to_vars, drop=False)
    nc = nc.set_coords(names=vars_to_move_to_coords)

    return nc


def modify_xarray_obj(file_obj):

   # ****** Modify nc and create mappings ********

    nc = file_obj['nc']
    data_type = file_obj['data_type']

    # *****************************************
    # Rearrange xarray nc to put meta in coords
    # and put parameters as vars
    #
    # Get mapping after this
    #
    # then add extra ArgoVis meta attributes to coords
    #
    # And if there is a ctd_temperature var
    # but no qc (most bottle files), add a
    # qc variable. Fill with NaN first and
    # later change to be 0. Use NaN first to make
    # dropping null rows easier.
    # *****************************************

    # Move some vars to coordinates and drop some vars
    # Want metadata stored in coordinates and parameter
    # data stored in variables section
    nc = rearrange_nc(nc)

    # Now check so see if there is a 'temp_{data_type}'  column and a corresponding
    # qc col. 'temp_{data_type}_qc'. If not, add a 'temp' qc col. with nan values
    # to make it easier later when remove null rows. Later will set qc = 0
    # when looking for temp_qc all nan. Add it here so it appears in var mappings
    nc = mod_xr_param.add_qc_if_no_temp_qc(nc)

    # Create mapping object of goship names to nc attributes
    # Before add or drop coords
    # mapping obj: names (list), units (obj), ref_scale (obj)
    # c_format (obj), and dtype (obj)
    goship_meta_mapping = xr_map.get_goship_mappings_meta(nc)
    goship_param_mapping = xr_map.get_goship_mappings_param(nc)

    # Add extra coordinates for ArgoVis metadata
    nc = mod_xr_meta.add_extra_coords(nc, file_obj)

    # Drop some coordinates
    nc = mod_xr_meta.drop_coords(nc)

    # *********************************
    # Convert units and reference scale
    # *********************************

    logging.info(f"Apply conversions for {data_type}")

    # Apply units before change unit names

    # Couldn't get apply_ufunc and gsw to work with dask
    # So load it back to pure xarray

    # Load nc from Dask for conversions calc
    nc.load()

    nc = xr_conv.apply_conversions(nc)

    # ****************************
    # Apply C_format print format
    # ****************************

    logging.info(f"Apply c_format for {data_type}")

    chunk_size = GlobalVars.CHUNK_SIZE

    # Convert back to Dask
    nc = nc.chunk({'N_PROF': chunk_size})

    nc = mod_xr_meta.apply_c_format_meta(
        nc, goship_meta_mapping)

    nc = mod_xr_param.apply_c_format_param(
        nc, goship_param_mapping)

    # **********************************
    # Change unit names (no conversions)
    # **********************************

    nc = mod_xr_meta.change_units_to_argovis(nc)
    nc = mod_xr_param.change_units_to_argovis(nc)

    # **************************
    # Rename varables to ArgoVis
    # **************************'

    nc = rename_to_argovis(nc, data_type)

    # **********************
    # Create ArgoVis Mapping
    # **********************

    argovis_meta_mapping = xr_map.get_argovis_mappings_meta(nc)
    argovis_param_mapping = xr_map.get_argovis_mappings_param(nc)

    # ******************

    nc_mappings = {}

    nc_mappings['goship_meta'] = goship_meta_mapping
    nc_mappings['goship_param'] = goship_param_mapping

    nc_mappings['argovis_meta'] = argovis_meta_mapping
    nc_mappings['argovis_param'] = argovis_param_mapping

    return nc, nc_mappings