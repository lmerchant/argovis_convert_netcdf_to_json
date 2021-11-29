import logging

import xarray_and_dask.modify_xarray_meta as mod_xr_meta
import xarray_and_dask.modify_xarray_param as mod_xr_param
import xarray_and_dask.get_xarray_mappings as xr_map
import xarray_and_dask.conversions as xr_conv
from xarray_and_dask.rename_to_argovis import rename_to_argovis

from global_vars import GlobalVars


def get_meta_param_mapping(nc):

    meta_mapping = {}
    param_mapping = {}

    # metadata is any without N_LEVELS dimension
    meta_names = [
        coord for coord in nc.coords if 'N_LEVELS' not in nc.coords[coord].dims]

    param_names = [
        coord for coord in nc.coords if 'N_LEVELS' in nc.coords[coord].dims]

    meta_mapping['coords'] = meta_names
    param_mapping['coords'] = param_names

    meta_names_from_var = [var for var in nc if 'N_LEVELS' not in nc[var].dims]
    param_names_from_var = [var for var in nc if 'N_LEVELS' in nc[var].dims]

    meta_mapping['vars'] = meta_names_from_var
    param_mapping['vars'] = param_names_from_var

    return meta_mapping, param_mapping


def apply_argovis_names_and_conversions(cruises_profiles_objs):

    # TODO
    # Don't need nc.load if xarray file not Dask format

    # nc probably will be profile object which contains
    # the datatype and goship mappings goship_meta_mapping
    # and goship_param_mapping

    # Ask if this is OK
    # Donata wants to add pres_qc = 1
    # First  set to np.nan to make dropping empty rows easy
    # and then in  modify_dask_obj, set it to 1
    #nc = mod_xr_param.add_pres_qc(nc)

    # Add extra coordinates for ArgoVis metadata
    nc = mod_xr_meta.add_extra_coords(nc, file_obj)

    # Drop some vars won't be using
    nc = mod_xr_param.drop_vars(nc)

    # *********************************
    # Convert units and reference scale
    # *********************************

    logging.info(f"Apply conversions for {data_type} if needed")

    # Apply units before change unit names

    # Call load to convert nc from Dask to pure xarray for conversions calc
    # Couldn't get apply_ufunc and gsw to work with dask
    # So load it back to pure xarray
    nc.load()

    nc = xr_conv.apply_conversions(nc)

    # ****************************
    # Apply C_format print format
    # ****************************

    logging.info(f"Apply c_format for {data_type}")

    # Convert back to Dask
    chunk_size = GlobalVars.CHUNK_SIZE

    nc = nc.chunk({'N_PROF': chunk_size})

    nc = mod_xr_meta.apply_c_format_meta(
        nc, goship_meta_mapping)

    nc = mod_xr_param.apply_c_format_param(
        nc, goship_param_mapping)

    # **********************************
    # Change unit names (no conversions)
    # **********************************

    # Do this last
    nc = mod_xr_meta.change_units_to_argovis(nc, goship_meta_mapping)
    nc = mod_xr_param.change_units_to_argovis(nc, goship_param_mapping)

    # **************************
    # Rename varables to ArgoVis
    # **************************'

    # Do this last
    nc = rename_to_argovis(nc, data_type)

    # **********************
    # Create ArgoVis Mapping
    # **********************

    # TODO
    # take into account goship col temp_qc did not exist if no
    # qc existed for ctd temp. But still need to map it

    # Create meta and param objs that reference coords or vars
    # depending if have both N_PROF and N_LEVELS or not
    # Getting mapping after added extra meta data to nc
    meta_mapping, param_mapping = get_meta_param_mapping(nc)

    # Create mapping object of goship names to nc attributes
    # Before add or drop coords
    # mapping obj: names (list), units (obj), ref_scale (obj)
    # c_format (obj), and dtype (obj)
    argovis_meta_mapping = xr_map.get_nc_variable_mappings(nc, meta_mapping)
    argovis_param_mapping = xr_map.get_nc_variable_mappings(nc, param_mapping)

    nc_mappings = {}

    nc_mappings['argovis_meta'] = argovis_meta_mapping
    nc_mappings['argovis_param'] = argovis_param_mapping

    # determine meas_source_flag ('BTL', 'CTD', 'BTL_CTD')
    # key called measurementsSource

    # {'bottle_salinity': 'ctd_salinity'} renamed psal

    # TODO
    # Get filtered mappping
    # For each profile

    for cruises_profile_obj in cruises_profiles_objs:
        # Filter out from var name mappings any that have no values.
        # This can happen since xarray fills variables if they
        # don't exist in a station cast to create one array of variables
        # for the collection
        all_argovis_param_mapping_list = filter_argovis_mapping(
            nc_mappings, all_name_mapping)

        goship_argovis_mapping_profiles = create_goship_argovis_mappings(
            nc_mappings, all_argovis_param_mapping_list, data_type)

    return cruises_profiles_objs
