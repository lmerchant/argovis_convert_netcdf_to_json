# from variable_naming.meta_param_mapping import get_program_argovis_data_info_mapping
# import logging


# def get_argovis_param_names(profile_dict):
#     argovis_param_names = []

#     profile_keys = profile_dict.keys()

#     # Keep my names and map to latest Argovis names
#     # need this since already renamed individual files

#     # cchdo names are the keys and argovis names are the value
#     key_mapping = get_program_argovis_data_info_mapping()

#     param_names_key = "argovis_param_names"
#     renamed_param_names_key = key_mapping(param_names_key)

#     # For param data
#     argovis_param_names = profile_dict[renamed_param_names_key]

#     return argovis_param_names


# # def find_param_excluded(profile_dict, included_param_names):

# #     # Case for argovis names
# #     excluded_param_names = []

# #     param_names = get_argovis_param_names(
# #         profile_dict)

# #     # For param names
# #     if included_param_names and param_names:
# #         included_param_names_set = set(included_param_names)
# #         param_names_set = set(param_names)
# #         excluded_param_names = param_names_set.difference(
# #             included_param_names_set)

# #     return excluded_param_names


# def add_argovis_vars_one_profile(profile_dict):
#     # TODO
#     # Use my names and map to latest argovis names
#     # So since renamed single types already, need to use the argovis names

#     profile_id = profile_dict["meta"]["_id"]

#     data_type = profile_dict["data_type"]

#     # # Map back to cchdo names
#     # cchdo_meta_keys = rn.convert_argovis_meta_to_cchdo_names(
#     #     argovis_meta_keys)

#     included = []
#     excluded = []

#     # Include qc columns

#     # Get Included Goship Meta and Param names after filtering empty cols
#     # because did mapping after empty columns excluded
#     # name_mapping = profile_dict['cchdoArgovisMetaMapping']
#     # included_meta_argovis = name_mapping.values()

#     # cchdo names are the keys and argovis names are the value
#     # Find what the new key name is
#     cchdo_key = "cchdo_argovis_param_mapping"

#     # key_mapping = get_program_argovis_data_info_mapping()

#     # renamed_key = key_mapping[cchdo_key]

#     # Mapping of all the CCHDO names to argovis names with data_type suffix
#     # TODO
#     # Check these mappings include the data type suffix
#     data_info = profile_dict["meta"]["data_info"]

#     # Any names found in the name mapping have been included in the profile data
#     # because any that were all empty were dropped before here
#     included_param_argovis_names = data_info[0]

#     # Get Param names before filtering out empty cols
#     # These are the names in original cchdo file which includes
#     # empty columns due to starting netcdf file containing all
#     # variable names for all the profiles

#     # cchdo_key = "argovis_param_names"

#     key_mapping = get_program_argovis_data_info_mapping()

#     argovis_param_names = data_info[0]

#     included_argovis_param_names_set = set(included_param_argovis_names)
#     argovis_param_names_set = set(argovis_param_names)

#     excluded_argovis_param_names = argovis_param_names_set.difference(
#         included_argovis_param_names_set
#     )

#     # *******************************
#     # Save included and excluded vars
#     # *******************************

#     # **********************************
#     # for included and excluded argovis names
#     # Add tuple (argovis_name, profile_id, data_type)
#     # ***********************************

#     data_type = profile_dict["data_type"]

#     for name in included_param_argovis_names:
#         included.append((name, profile_id, data_type))

#     for name in excluded_argovis_param_names:
#         excluded.append((name, profile_id, data_type))

#     return (
#         included,
#         excluded,
#         included_param_argovis_names,
#         excluded_argovis_param_names,
#     )


# def add_vars_one_cruise(data_type_profiles_objs):
#     vars_included = []
#     vars_excluded = []

#     included_names = []
#     excluded_names = []

#     # Loop over the profiles for the collection of files that have profiles
#     for data_type_profiles_obj in data_type_profiles_objs:
#         data_type_profiles = data_type_profiles_obj["data_type_profiles_list"]

#         for data_type_profile in data_type_profiles:
#             profile_dict = data_type_profile["profile_dict"]

#             # included and excluded are tuples with profile_id

#             # included_param_cchdo_names and excluded_cchdo_param_names
#             # are the names for one profile

#             (
#                 included,
#                 excluded,
#                 included_param_argovis_names,
#                 excluded_argovis_param_names,
#             ) = add_argovis_vars_one_profile(profile_dict)

#             vars_included.extend(included)
#             vars_excluded.extend(excluded)

#             included_names.extend(included_param_argovis_names)
#             excluded_names.extend(excluded_argovis_param_names)

#     unique_included_names = list(set(included_names))
#     unique_excluded_names = list(set(excluded_names))

#     # Return values are lists of tuples (name, profile_id)
#     return vars_included, vars_excluded, unique_included_names, unique_excluded_names


# def gather_included_excluded_vars(batch_cruises_profiles_objs):
#     cruises_all_included = []
#     cruises_all_excluded = []

#     for cruise_profiles_obj in batch_cruises_profiles_objs:
#         expocode = cruise_profiles_obj["cruise_expocode"]
#         profiles_objs = cruise_profiles_obj["all_data_types_profile_objs"]

#         # included and excluded are lists of tuples (name, profile_id, data_type)

#         # unique_included_names are unique included names for all cruise
#         # unique_excluded_names are unique excluded names for all cruise
#         (
#             included,
#             excluded,
#             unique_included_names,
#             unique_excluded_names,
#         ) = add_vars_one_cruise(profiles_objs)

#         logging.info(f"expocode: {expocode}")
#         logging.info(f"Num of included vars {len(unique_included_names)}")
#         logging.info(f"Num of excluded vars {len(unique_excluded_names)}")

#         cruises_all_included.extend(included)
#         cruises_all_excluded.extend(excluded)

#     return cruises_all_included, cruises_all_excluded


def gather_included_excluded_vars(
    kept_vars_all_cruise_objs, deleted_vars_all_cruise_objs
):
    kept_vars = {}

    for kept_vars_cruise_obj in kept_vars_all_cruise_objs:
        kept_vars_profile_objs = kept_vars_cruise_obj["vars"]

        for kept_vars_profile_obj in kept_vars_profile_objs:
            vars_per_profile = kept_vars_profile_obj["vars_per_profile"]

            for vars in vars_per_profile:
                profile_id = vars["profile_id"]
                param_names = vars["param_names"]

                for param_name in param_names:
                    if param_name not in kept_vars.keys():
                        kept_vars[param_name] = []
                    kept_vars[param_name].append(profile_id)

    deleted_vars = {}

    for deleted_vars_cruise_obj in deleted_vars_all_cruise_objs:
        deleted_vars_profile_objs = deleted_vars_cruise_obj["vars"]

        for deleted_vars_profile_obj in deleted_vars_profile_objs:
            vars_per_profile = deleted_vars_profile_obj["vars_per_profile"]

            for vars in vars_per_profile:
                profile_id = vars["profile_id"]
                param_names = vars["param_names"]

                for param_name in param_names:
                    if param_name not in deleted_vars.keys():
                        deleted_vars[param_name] = []
                    deleted_vars[param_name].append(profile_id)

    return (kept_vars, deleted_vars)
