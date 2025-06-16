from typing import Dict, Optional, Any
from qiimp.src.util import extract_stds_config, deepcopy_dict, \
    METADATA_FIELDS_KEY, STUDY_SPECIFIC_METADATA_KEY, \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, ALIAS_KEY, BASE_TYPE_KEY, \
    DEFAULT_KEY, ALLOWED_KEY, ANYOF_KEY, TYPE_KEY


def combine_stds_and_study_config(
        study_config_dict: Dict[str, Any],
        stds_fp: Optional[str] = None) \
        -> Dict[str, Any]:
    """Combine standards and study-specific-configuration dictionaries.

    Parameters
    ----------
    study_config_dict : Dict[str, Any]
        Study-specific flat-host-type config dictionary.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file.

    Returns
    -------
    Dict[str, Any]
        Nested-host-type config dictionary combining standards and study-specific info.
    """
    stds_nested_dict = extract_stds_config(stds_fp)
    study_flat_dict = study_config_dict.get(STUDY_SPECIFIC_METADATA_KEY, {})
    combined_host_types_dict = _make_combined_stds_and_study_host_type_dicts(
        study_flat_dict, stds_nested_dict)

    stds_plus_study_nested_dict = {
        HOST_TYPE_SPECIFIC_METADATA_KEY: combined_host_types_dict}
    return stds_plus_study_nested_dict


def flatten_nested_stds_dict(
        parent_stds_nested_dict: Dict[str, Any],
        parent_flattened_host_dict: Optional[Dict[str, Any]] = None) \
        -> Dict[str, Any]:
    """Flatten a nested standards dictionary into a flat structure.

    Note: this method is called recursively.
    At each level, this method adds info from the host types dictionary for the
    previous host level's standards nested dictionary (arg 1) into a copy of a growing
    flat-and-complete hosts dictionary for the previous level (arg 2). The result is a
    flat hosts dictionary that (a) contains all hosts and (b) has complete metadata
    definitions for each host.

    Parameters
    ----------
    parent_stds_nested_dict : Dict[str, Any]
        Parent (previous host)-level standards nested dictionary.
    parent_flattened_host_dict : Optional[Dict[str, Any]], default=None
        Parent (previous host)-level flattened host dictionary. If None, a new empty dictionary
        will be created.

    Returns
    -------
    Dict[str, Any]
        Flattened dictionary containing all host types and their complete metadata definitions.
    """
    # if this is the top-level call, set flat parent to new dict.
    # this is what we will be copying to add *TO*
    if parent_flattened_host_dict is None:
        parent_flattened_host_dict = {}

    parent_stds_host_types_dict = \
        parent_stds_nested_dict.get(HOST_TYPE_SPECIFIC_METADATA_KEY, {})
    # define the output dictionary as empty.  This will be overwritten if there
    # are any hosts at this level.
    wip_host_types_dict = {}

    # loop over the host types at this level in parent_stds_nested_dict;
    # these are what we will be adding *FROM*
    for curr_host_type, curr_host_type_stds_nested_dict \
            in parent_stds_host_types_dict.items():

        curr_host_type_wip_flat_dict = \
            _combine_base_and_added_host_type(
                parent_flattened_host_dict,
                curr_host_type_stds_nested_dict)

        # recurse into the next level--depth first search.
        # if this comes back empty, we ignore it.
        curr_host_type_sub_host_dict = flatten_nested_stds_dict(
            curr_host_type_stds_nested_dict, curr_host_type_wip_flat_dict)
        if curr_host_type_sub_host_dict:
            wip_host_types_dict.update(curr_host_type_sub_host_dict)

        # assign the flattened wip dict for the current host type to the result
        # (which now contains flat records for the hosts lower down than
        # this, if there are any)
        wip_host_types_dict[curr_host_type] = \
            curr_host_type_wip_flat_dict
    # next host type

    return wip_host_types_dict


# TODO: Rewrite so this doesn't BOTH modify the wip in place AND return a pointer to it.
# The fact that it returns a dictionary makes it unclear that this returned value is not a copy 
# but is in fact the same dictionary as the one passed in, now with modifications.
# This is confusing and error-prone.
def update_wip_metadata_dict(
        wip_metadata_fields_dict: Dict[str, Any],
        stds_metadata_fields_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Update work-in-progress metadata dictionary *in place* with standards metadata fields.

    Parameters
    ----------
    wip_metadata_fields_dict : Dict[str, Any]
        Current work-in-progress metadata fields dictionary.
    stds_metadata_fields_dict : Dict[str, Any]
        Standards metadata fields dictionary to incorporate.

    Returns
    -------
    Dict[str, Any]
        (Pointer to) updated work-in-progress metadata fields dictionary.
    """
    for curr_metadata_field, curr_stds_metadata_field_dict in stds_metadata_fields_dict.items():
        if curr_metadata_field not in wip_metadata_fields_dict:
            wip_metadata_fields_dict[curr_metadata_field] = {}

        if ALLOWED_KEY in curr_stds_metadata_field_dict:
            # remove the ANYOF_KEY from curr_wip_metadata_fields_dict[curr_metadata_field] if it exists there
            if ANYOF_KEY in wip_metadata_fields_dict[curr_metadata_field]:
                del wip_metadata_fields_dict[curr_metadata_field][ANYOF_KEY]

        if ANYOF_KEY in curr_stds_metadata_field_dict:
            # remove the ALLOWED_KEY from curr_wip_metadata_fields_dict[curr_metadata_field] if it exists there
            if ALLOWED_KEY in wip_metadata_fields_dict[curr_metadata_field]:
                del wip_metadata_fields_dict[curr_metadata_field][ALLOWED_KEY]

            # remove the TYPE_KEY from curr_wip_metadata_fields_dict[curr_metadata_field] if it exists there
            if TYPE_KEY in wip_metadata_fields_dict[curr_metadata_field]:
                del wip_metadata_fields_dict[curr_metadata_field][TYPE_KEY]

        # TODO: Q: is it possible to have a list of allowed with a default
        #  at high level, then lower down have a list of allowed WITHOUT
        #  a default?  If so, how do we handle that?

        # update curr_wip_metadata_fields_dict[curr_metadata_field] with curr_stds_metadata_field_dict
        wip_metadata_fields_dict[curr_metadata_field].update(curr_stds_metadata_field_dict)
    # next metadata field

    return wip_metadata_fields_dict


def _make_combined_stds_and_study_host_type_dicts(
        flat_study_dict: Dict[str, Any],
        parent_host_stds_nested_dict: Dict[str, Any]) \
        -> Dict[str, Any]:
    """Combine standards and study-specific host type dictionaries.

    At each level, this method adds info from a static, flat study-specific
    hosts dictionary (the same at every level; arg 1) into a copy of the host
    types dictionary for the previous host level's standards nested dictionary (arg 2). 
    (Note that the flat study-specific hosts dictionary is NOT expected
    to (a) contains all hosts nor to (b) have complete metadata definitions for
    each host.) The result is an augmented nested hosts dictionary.

    Parameters
    ----------
    flat_study_dict : Dict[str, Any]
        Flat study-specific dictionary. Note that this is the same at every level
        and is NOT the full study-specific config dictionary,
        only the contents of the STUDY_SPECIFIC_METADATA_KEY section thereof.
    parent_host_stds_nested_dict : Dict[str, Any]
        Parent (previous host)-level standards nested dictionary.

    Returns
    -------
    Dict[str, Any]
        Nested dictionary combining standards and study-specific metadata definitions.
    """
    # get all the host type dicts for the study (these are flat);
    # these are what we will be adding *FROM*
    study_host_types_dict = flat_study_dict.get(
        HOST_TYPE_SPECIFIC_METADATA_KEY, {})

    parent_stds_host_types_dict = \
        parent_host_stds_nested_dict.get(HOST_TYPE_SPECIFIC_METADATA_KEY, {})
    # define the output dictionary as a copy of the parent-level standard.
    # This will be augmented if there are any hosts at this level.
    wip_host_types_dict = \
        deepcopy_dict(parent_stds_host_types_dict)

    # loop over the host types at this level in parent_stds_nested_dict;
    # these are what we will be copying to add *TO*
    for curr_host_type, curr_host_type_stds_nested_dict \
            in parent_stds_host_types_dict.items():

        # only need to do work at this level if curr host type is in study dict
        # since otherwise the wip dict is an unchanged copy of the stds dict
        if curr_host_type not in study_host_types_dict:
            # make a copy of the stds for the current host type to add info to
            curr_host_type_wip_nested_dict = \
                deepcopy_dict(curr_host_type_stds_nested_dict)
        else:
            curr_host_type_wip_nested_dict = \
                _combine_base_and_added_host_type(
                    curr_host_type_stds_nested_dict,
                    study_host_types_dict[curr_host_type])
        # endif the host type isn't/is in the study dict

        # recurse into the next level--depth first search.
        # if this comes back empty, we ignore it.
        curr_host_type_sub_host_dict = \
            _make_combined_stds_and_study_host_type_dicts(
                flat_study_dict,
                curr_host_type_stds_nested_dict)
        if curr_host_type_sub_host_dict:
            curr_host_type_wip_nested_dict[HOST_TYPE_SPECIFIC_METADATA_KEY] = \
                curr_host_type_sub_host_dict

        # assign the nested wip dict for the current host type to the result
        # (which now contains nested records for the hosts lower down than
        # this, if there are any)
        wip_host_types_dict[curr_host_type] = \
            curr_host_type_wip_nested_dict
    # next host type in wip dict

    return wip_host_types_dict


def _combine_base_and_added_host_type(
        host_type_base_dict: Dict[str, Any],
        host_type_add_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Combine base and additional host type configurations.

    Parameters
    ----------
    host_type_base_dict : Dict[str, Any]
        Base host type configuration dictionary.
    host_type_add_dict : Dict[str, Any]
        Additional host type configuration to incorporate.

    Returns
    -------
    Dict[str, Any]
        Combined host type configuration dictionary.
    """
    # make a copy of the base for the current host type to add info to
    host_type_wip_nested_dict = \
        deepcopy_dict(host_type_base_dict)

    # look for a default key in the add dict for this host; if
    # it exists, add it to the wip dict (ok to overwrite existing)
    if DEFAULT_KEY in host_type_add_dict:
        host_type_wip_nested_dict[DEFAULT_KEY] = \
            host_type_add_dict.get(DEFAULT_KEY)

    # combine add metadata fields with the wip metadata fields
    # for the current host type and assign to wip if not empty
    host_type_wip_metadata_fields_dict = \
        _combine_base_and_added_metadata_fields(
            host_type_base_dict,
            host_type_add_dict)
    if host_type_wip_metadata_fields_dict:
        host_type_wip_nested_dict[METADATA_FIELDS_KEY] = \
            host_type_wip_metadata_fields_dict
    # endif the host type combination is not empty

    # combine any sample-type specific entries within the current host
    # type and assign to wip if not empty
    curr_host_wip_sample_types_dict = \
        _combine_base_and_added_sample_type_specific_metadata(
            host_type_wip_nested_dict,
            host_type_add_dict)
    # if we got back a non-empty dictionary of sample types,
    # add it to the wip for this host type dict
    if curr_host_wip_sample_types_dict:
        host_type_wip_nested_dict[
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY] = \
            curr_host_wip_sample_types_dict
    # endif the sample types dictionary is not empty

    return host_type_wip_nested_dict


def _combine_base_and_added_metadata_fields(
        host_type_base_dict: Dict[str, Any],
        host_type_add_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Combine just the metadata fields from base and additional host type dictionaries.

    Parameters
    ----------
    host_type_base_dict : Dict[str, Any]
        Base host type configuration dictionary.
    host_type_add_dict : Dict[str, Any]
        Additional configuration to incorporate.

    Returns
    -------
    Dict[str, Any]
        Combined metadata fields dictionary.
    """
    # copy the metadata fields from the base to make the wip metadata fields
    host_type_wip_metadata_fields_dict = deepcopy_dict(
        host_type_base_dict.get(METADATA_FIELDS_KEY, {}))

    # update the wip with the add metadata fields
    host_type_add_metadata_fields_dict = \
        host_type_add_dict.get(METADATA_FIELDS_KEY, {})
    host_type_wip_metadata_fields_dict = \
        update_wip_metadata_dict(
            host_type_wip_metadata_fields_dict,
            host_type_add_metadata_fields_dict)

    return host_type_wip_metadata_fields_dict


def _combine_base_and_added_sample_type_specific_metadata(
        host_type_base_dict: Dict[str, Any],
        host_type_add_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Combine just sample type specific metadata from base and additional host type dictionaries.

    Parameters
    ----------
    host_type_base_dict : Dict[str, Any]
        Base host type configuration dictionary.
    host_type_add_dict : Dict[str, Any]
        Additional configuration to incorporate.

    Returns
    -------
    Dict[str, Any]
        Combined sample type specific metadata dictionary.

    Raises
    ------
    ValueError
        If sample type has both alias and metadata fields, or both alias and base type.
    """
    # copy the dictionary of sample types from the base to make the wip dict
    curr_host_wip_sample_types_dict = deepcopy_dict(
        host_type_base_dict.get(
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY, {}))

    # loop over the sample types in the add dict
    curr_host_add_sample_types_dict = \
        host_type_add_dict.get(
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY, {})
    for curr_sample_type, curr_sample_type_add_dict \
            in curr_host_add_sample_types_dict.items():

        curr_sample_type_wip_dict = deepcopy_dict(
            curr_host_wip_sample_types_dict.get(curr_sample_type, {}))

        curr_sample_type_add_def_type = \
            _id_sample_type_definition(
                curr_sample_type, curr_sample_type_add_dict)
        curr_sample_type_wip_def_type = None
        if curr_sample_type in curr_host_wip_sample_types_dict:
            curr_sample_type_wip_def_type = \
                _id_sample_type_definition(
                    curr_sample_type,
                    curr_sample_type_wip_dict)
        # end if sample type is in wip

        # if the sample type is already in the wip, and it has metadata fields,
        # and it has metadata fields in the add dict, combine metadata fields
        if curr_sample_type_wip_def_type == METADATA_FIELDS_KEY \
                and curr_sample_type_add_def_type == METADATA_FIELDS_KEY:

            # first, add all non-metadata fields from the add dict to the wip;
            # this captures, e.g., base_type
            curr_sample_type_add_dict_wo_metadata = deepcopy_dict(
                curr_sample_type_add_dict)
            del curr_sample_type_add_dict_wo_metadata[METADATA_FIELDS_KEY]
            curr_sample_type_wip_dict.update(
                curr_sample_type_add_dict_wo_metadata)

            curr_sample_type_add_metadata_fields_dict = \
                curr_sample_type_add_dict[METADATA_FIELDS_KEY]
            curr_sample_type_wip_metadata_fields_dict = \
                curr_sample_type_wip_dict[METADATA_FIELDS_KEY]
            curr_sample_type_wip_metadata_fields_dict = (
                update_wip_metadata_dict(
                    curr_sample_type_wip_metadata_fields_dict,
                    curr_sample_type_add_metadata_fields_dict))
            # if the above combination is not of two empties
            if curr_sample_type_wip_metadata_fields_dict:
                curr_sample_type_wip_dict[METADATA_FIELDS_KEY] = \
                    curr_sample_type_wip_metadata_fields_dict
            # end if the metadata fields combination is not empty

            curr_host_wip_sample_types_dict[curr_sample_type] = \
                curr_sample_type_wip_dict
        # end if both wip and add have metadata fields for the sample type

        # otherwise, if a sample type is in the add dict but not in the wip,
        # or it is in both but of different definition types
        # (alias vs metadata) in the two, just set the entry in the wip dict
        # to be the entry in the add dict.
        else:
            curr_host_wip_sample_types_dict[curr_sample_type] = \
                curr_sample_type_add_dict
        # endif sample type is in wip and has metadata fields in both or not
    # next sample type

    return curr_host_wip_sample_types_dict


def _id_sample_type_definition(sample_type_name: str, sample_type_dict: Dict[str, Any]) -> str:
    """Identify the type of sample type definition in the dictionary.

    Parameters
    ----------
    sample_type_name : str
        Name of the sample type.
    sample_type_dict : Dict[str, Any]
        Dictionary containing sample type configuration.

    Returns
    -------
    str
        The type of definition (ALIAS_KEY, METADATA_FIELDS_KEY, or BASE_TYPE_KEY).

    Raises
    ------
    ValueError
        If sample type has both alias and metadata fields, or both alias and base type,
        or neither alias nor metadata fields.
    """
    has_alias = ALIAS_KEY in sample_type_dict
    has_metadata = METADATA_FIELDS_KEY in sample_type_dict
    has_base = BASE_TYPE_KEY in sample_type_dict
    if has_alias and has_metadata:
        raise ValueError(f"Sample type '{sample_type_name}' has both "
                         f"'{ALIAS_KEY}' and '{METADATA_FIELDS_KEY}' keys in "
                         "the same sample type dict")
    elif has_alias and has_base:
        raise ValueError(f"Sample type '{sample_type_name}' has both "
                         f"'{ALIAS_KEY}' and '{BASE_TYPE_KEY}' keys in "
                         "the same sample type dict")
    elif has_alias:
        return ALIAS_KEY
    elif has_metadata:
        return METADATA_FIELDS_KEY
    elif has_base:
        # this implies that it has ONLY a base, not a base and metadata
        return BASE_TYPE_KEY
    else:
        raise ValueError(f"Sample type '{sample_type_name}' has neither "
                         f"'{ALIAS_KEY}' nor '{METADATA_FIELDS_KEY}' keys in "
                         "the same sample type dict")
