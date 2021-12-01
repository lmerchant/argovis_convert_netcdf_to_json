import logging

import xarray_and_dask.modify_xarray_meta as mod_xr_meta
import xarray_and_dask.modify_xarray_param as mod_xr_param
import xarray_and_dask.get_xarray_mappings as xr_map
import xarray_and_dask.conversions as xr_conv
#from variable_mapping.rename_to_argovis import rename_to_argovis

from global_vars import GlobalVars


def rearrange_nc(nc):

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


def get_meta_param_names(nc):

    # metadata is any without N_LEVELS dimension
    meta_names_from_coords = [
        coord for coord in nc.coords if 'N_LEVELS' not in nc.coords[coord].dims]

    param_names_from_coords = [
        coord for coord in nc.coords if 'N_LEVELS' in nc.coords[coord].dims]

    meta_names_from_vars = [
        var for var in nc if 'N_LEVELS' not in nc[var].dims]
    param_names_from_vars = [var for var in nc if 'N_LEVELS' in nc[var].dims]

    meta_names = [*meta_names_from_coords, *meta_names_from_vars]
    param_names = [*param_names_from_coords, *param_names_from_vars]

    return meta_names, param_names


def get_meta_param_mapping(nc):

    meta_mapping = {}
    param_mapping = {}

    # metadata is any without N_LEVELS dimension
    meta_names_from_coords = [
        coord for coord in nc.coords if 'N_LEVELS' not in nc.coords[coord].dims]

    param_names_from_coords = [
        coord for coord in nc.coords if 'N_LEVELS' in nc.coords[coord].dims]

    meta_names_from_vars = [
        var for var in nc if 'N_LEVELS' not in nc[var].dims]
    param_names_from_vars = [var for var in nc if 'N_LEVELS' in nc[var].dims]

    meta_mapping['coords'] = meta_names_from_coords
    param_mapping['coords'] = param_names_from_coords

    meta_mapping['vars'] = meta_names_from_vars
    param_mapping['vars'] = param_names_from_vars

    return meta_mapping, param_mapping


def modify_xarray_obj(file_obj):

   # ****** Modify nc and create mappings ********

    # pop off netcdf data and pass on file_obj which
    # contains cchdo meta data
    data_type = file_obj['data_type']
    nc = file_obj['nc']

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
    # Actually, removed this and keep coords and vars as is
    #nc = rearrange_nc(nc)

    # Create meta and param objs that reference coords or vars
    # depending if have both N_PROF and N_LEVELS or not
    #meta_mapping, param_mapping = get_meta_param_mapping(nc)

    # These are the original names and variable attributes
    # before any name changes and conversions
    orig_meta_names, orig_param_names = get_meta_param_names(nc)

    # Create mapping object of cchdo names to nc attributes
    # Before add or drop coords for key in final json representing
    # what variables were included in the netcdf file

    # mapping obj: names (list), units (obj), ref_scale (obj)
    # c_format (obj), and dtype (obj)
    # cchdo_meta_mapping = xr_map.get_nc_variable_mappings(nc, meta_mapping)
    # cchdo_param_mapping = xr_map.get_nc_variable_mappings(nc, param_mapping)
    cchdo_meta_mapping = xr_map.get_nc_variable_mappings(nc, orig_meta_names)
    cchdo_param_mapping = xr_map.get_nc_variable_mappings(
        nc, orig_param_names)

    # Now check so see if there is a 'temp_{data_type}'  column and a corresponding
    # qc col. 'temp_{data_type}_qc'. If not, add a 'temp' qc col. with 0.
    # Add qc col here so it appears in var mappings of cchdo to argovis

    # TODO
    # With updated logic looking for nan rows, skipping looking at qc
    # columns, so can set qc=0 for temp and not nan first
    nc = mod_xr_param.add_qc_if_no_temp_qc(nc)

    # Add station_cast to meta variables
    nc = mod_xr_meta.add_station_cast(nc)

    # Add file and cruise json meta

    # # Add any file meta not in nc meta
    # file_meta = file_obj['cchdo_file_meta']
    # nc = mod_xr_meta.add_file_meta(nc, file_meta)

    # cruise_meta = file_obj['cchdo_cruise_meta']
    # nc = mod_xr_meta.add_cruise_meta(nc, cruise_meta)

    # Get meta and param vars after add temperature qc if needed
    # and station cast
    # Using this for processing
    meta_names, param_names = get_meta_param_names(nc)

    # # TODO
    # # Do this later when rename and converting to Argovis units and names

    # # Ask if this is OK
    # # Donata wants to add pres_qc = 1
    # # First  set to np.nan to make dropping empty rows easy
    # # and then in  modify_dask_obj, set it to 1
    # #nc = mod_xr_param.add_pres_qc(nc)

    # # Add extra coordinates for ArgoVis metadata
    # nc = mod_xr_meta.add_extra_coords(nc, file_obj)

    # # Drop some vars won't be using
    # nc = mod_xr_param.drop_vars(nc)

    # *********************************
    # Convert units and reference scale
    # *********************************

    logging.info(f"Apply conversions for {data_type} if needed")

    # Apply units before change unit names

    # TODO
    # attach info about which units were changed
    # and what vars were converted

    # Call load to convert nc from Dask to pure xarray for conversions calc
    # Couldn't get apply_ufunc and gsw to work with dask
    # So load it back to pure xarray
    nc.load()

    nc, profiles_no_oxy_conversions = xr_conv.apply_conversions(nc)

    # Get mapping to keep track of which var units or scale were converted
    cchdo_meta_mapping_after = xr_map.get_nc_variable_mappings(nc, meta_names)
    cchdo_param_mapping_after = xr_map.get_nc_variable_mappings(
        nc, param_names)

    # look at keys units and reference_scale
    # Find which values have changed for var names
    params_units_changed = {}
    for key, val_before in cchdo_param_mapping['units'].items():
        val_after = cchdo_param_mapping_after['units'][key]

        if val_before != val_after:
            params_units_changed[key] = val_after

    params_ref_scale_changed = {}
    for key, val_before in cchdo_param_mapping['ref_scale'].items():
        val_after = cchdo_param_mapping_after['ref_scale'][key]

        if val_before != val_after:
            params_ref_scale_changed[key] = val_after

    # meta_mapping['names'] = list(nc.coords)
    # meta_mapping['units'] = meta_units
    # meta_mapping['ref_scale'] = meta_ref_scale

    # ****************************
    # Apply C_format print format
    # ****************************

    logging.info(f"Apply c_format for {data_type}")

    chunk_size = GlobalVars.CHUNK_SIZE

    # Convert back to Dask
    nc = nc.chunk({'N_PROF': chunk_size})

    nc = mod_xr_meta.apply_c_format_meta(
        nc, cchdo_meta_mapping)

    nc = mod_xr_param.apply_c_format_param(
        nc, cchdo_param_mapping)

    # # **********************************
    # # Change unit names (no conversions)
    # # **********************************

    # # Do this last
    # nc = mod_xr_meta.change_units_to_argovis(nc, cchdo_meta_mapping)
    # nc = mod_xr_param.change_units_to_argovis(nc, cchdo_param_mapping)

    # # **************************
    # # Rename varables to ArgoVis
    # # **************************'

    # # Do this last
    # nc = rename_to_argovis(nc, data_type)

    # # **********************
    # # Create ArgoVis Mapping
    # # **********************

    # # Create meta and param objs that reference coords or vars
    # # depending if have both N_PROF and N_LEVELS or not
    # meta_mapping, param_mapping = get_meta_param_mapping(nc)

    # # Create mapping object of cchdo names to nc attributes
    # # Before add or drop coords
    # # mapping obj: names (list), units (obj), ref_scale (obj)
    # # c_format (obj), and dtype (obj)
    # argovis_meta_mapping = xr_map.get_nc_variable_mappings(nc, meta_mapping)
    # argovis_param_mapping = xr_map.get_nc_variable_mappings(nc, param_mapping)

    # nc_mappings = {}

    # nc_mappings['argovis_meta'] = argovis_meta_mapping
    # nc_mappings['argovis_param'] = argovis_param_mapping

    # ******************

    nc_mappings = {}

    nc_mappings['cchdo_meta'] = cchdo_meta_mapping
    nc_mappings['cchdo_param'] = cchdo_param_mapping

    nc_mappings['cchdo_units_changed'] = params_units_changed
    nc_mappings['cchdo_ref_scale_changed'] = params_ref_scale_changed

    nc_mappings['cchdo_oxy_not_converted'] = profiles_no_oxy_conversions

    # Save current meta and param names
    meta_param_names = {}
    meta_param_names['meta'] = meta_names
    meta_param_names['params'] = param_names

    return nc, nc_mappings, meta_param_names


# def modify_xarray_obj_orig(file_obj):

#    # ****** Modify nc and create mappings ********

#     nc = file_obj['nc']
#     data_type = file_obj['data_type']

#     # *****************************************
#     # Rearrange xarray nc to put meta in coords
#     # and put parameters as vars
#     #
#     # Get mapping after this
#     #
#     # then add extra ArgoVis meta attributes to coords
#     #
#     # And if there is a ctd_temperature var
#     # but no qc (most bottle files), add a
#     # qc variable. Fill with NaN first and
#     # later change to be 0. Use NaN first to make
#     # dropping null rows easier.
#     # *****************************************

#     # Move some vars to coordinates and drop some vars
#     # Want metadata stored in coordinates and parameter
#     # data stored in variables section
#     #nc = rearrange_nc(nc)

#     # Now check so see if there is a 'temp_{data_type}'  column and a corresponding
#     # qc col. 'temp_{data_type}_qc'. If not, add a 'temp' qc col. with nan values
#     # to make it easier later when remove null rows. Later will set qc = 0
#     # when looking for temp_qc all nan. Add it here so it appears in var mappings
#     nc = mod_xr_param.add_qc_if_no_temp_qc(nc)

#     # TODO
#     # Ask if this is OK
#     # Donata wants to add pres_qc = 1
#     # First  set to np.nan to make dropping empty rows easy
#     # and then in  modify_dask_obj, set it to 1
#     nc = mod_xr_param.add_pres_qc(nc)

#     # Create mapping object of cchdo names to nc attributes
#     # Before add or drop coords
#     # mapping obj: names (list), units (obj), ref_scale (obj)
#     # c_format (obj), and dtype (obj)
#     cchdo_meta_mapping = xr_map.get_nc_variable_mappings_meta(nc)
#     cchdo_param_mapping = xr_map.get_nc_variable_mappings_param(nc)

#     # Add extra coordinates for ArgoVis metadata
#     nc = mod_xr_meta.add_extra_coords(nc, file_obj)

#     # Drop some coordinates
#     nc = mod_xr_meta.drop_coords(nc)

#     # *********************************
#     # Convert units and reference scale
#     # *********************************

#     logging.info(f"Apply conversions for {data_type}")

#     # Apply units before change unit names

#     # Couldn't get apply_ufunc and gsw to work with dask
#     # So load it back to pure xarray

#     # Load nc from Dask for conversions calc
#     nc.load()

#     nc = xr_conv.apply_conversions(nc)

#     # ****************************
#     # Apply C_format print format
#     # ****************************

#     logging.info(f"Apply c_format for {data_type}")

#     chunk_size = GlobalVars.CHUNK_SIZE

#     # Convert back to Dask
#     nc = nc.chunk({'N_PROF': chunk_size})

#     nc = mod_xr_meta.apply_c_format_meta(
#         nc, cchdo_meta_mapping)

#     nc = mod_xr_param.apply_c_format_param(
#         nc, cchdo_param_mapping)

#     # **********************************
#     # Change unit names (no conversions)
#     # **********************************

#     nc = mod_xr_meta.change_units_to_argovis(nc)
#     nc = mod_xr_param.change_units_to_argovis(nc)

#     # **************************
#     # Rename varables to ArgoVis
#     # **************************'

#     nc = rename_to_argovis(nc, data_type)

#     # **********************
#     # Create ArgoVis Mapping
#     # **********************

#     argovis_meta_mapping = xr_map.get_argovis_mappings_meta(nc)
#     argovis_param_mapping = xr_map.get_argovis_mappings_param(nc)

#     # ******************

#     nc_mappings = {}

#     nc_mappings['cchdo_meta'] = cchdo_meta_mapping
#     nc_mappings['cchdo_param'] = cchdo_param_mapping

#     nc_mappings['argovis_meta'] = argovis_meta_mapping
#     nc_mappings['argovis_param'] = argovis_param_mapping

#     return nc, nc_mappings
