from typing import Dict, Optional, Any
from metameq.src.util import METADATA_TRANSFORMERS_KEY, extract_config_dict, extract_stds_config, \
    deepcopy_dict, \
    METADATA_FIELDS_KEY, STUDY_SPECIFIC_METADATA_KEY, \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, ALIAS_KEY, BASE_TYPE_KEY, \
    DEFAULT_KEY, ALLOWED_KEY, ANYOF_KEY, TYPE_KEY, \
    SAMPLE_TYPE_KEY, QIITA_SAMPLE_TYPE, GLOBAL_SETTINGS_KEYS


def combine_stds_and_study_config(
        study_config_dict: Dict[str, Any],
        stds_fp: Optional[str] = None) \
        -> Dict[str, Any]:
    """Combine standards and study-specific configuration dictionaries.

    Merges host type configurations, metadata transformers, and other top-level
    keys from both sources. Study-specific values override standards values
    where they overlap.

    Parameters
    ----------
    study_config_dict : Dict[str, Any]
        Study-specific configuration dictionary. May contain
        STUDY_SPECIFIC_METADATA_KEY with host type overrides,
        METADATA_TRANSFORMERS_KEY with transformer definitions,
        and other top-level configuration keys.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file.

    Returns
    -------
    Dict[str, Any]
        Combined configuration dictionary containing:
        - HOST_TYPE_SPECIFIC_METADATA_KEY: nested host type configurations
        - METADATA_TRANSFORMERS_KEY: merged transformer definitions (if any)
        - Other top-level keys from both standards and study configs
    """
    stds_nested_dict = extract_stds_config(stds_fp)

    # pull the study-specific host type specific metadata out of the (local copy of)
    # the study config; we need it for combining host types but specifically don't
    # want it later when merging the remainder of the study config with the standards config
    study_config_dict_local = deepcopy_dict(study_config_dict)
    study_flat_dict = study_config_dict_local.pop(STUDY_SPECIFIC_METADATA_KEY, {})

    combined_host_types_dict = _make_combined_stds_and_study_host_type_dicts(
        study_flat_dict, stds_nested_dict)

    combined_transformers_dict = _combine_metadata_transformers_dicts(
        stds_nested_dict, study_config_dict_local)

    stds_plus_study_nested_dict = stds_nested_dict | study_config_dict_local
    stds_plus_study_nested_dict[HOST_TYPE_SPECIFIC_METADATA_KEY] = combined_host_types_dict
    if combined_transformers_dict:
        stds_plus_study_nested_dict[METADATA_TRANSFORMERS_KEY] = combined_transformers_dict
    return stds_plus_study_nested_dict


def _combine_metadata_transformers_dicts(
        stds_transformers_dict: Dict[str, Any],
        study_transformers_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Combine metadata transformers from standards and study configurations.

    Merges transformer definitions from both sources. For each transformer type
    (e.g., pre_transformers, post_transformers), transformers from the study
    config override those from standards when they target the same field.

    Parameters
    ----------
    stds_transformers_dict : Dict[str, Any]
        Standards configuration dictionary (may contain METADATA_TRANSFORMERS_KEY).
    study_transformers_dict : Dict[str, Any]
        Study configuration dictionary (may contain METADATA_TRANSFORMERS_KEY).

    Returns
    -------
    Dict[str, Any]
        Combined transformers dictionary with transformer types as keys
        (e.g., pre_transformers, post_transformers) and their merged transformer
        definitions as values. Returns empty dict if neither input has transformers.
    """
    stds_transformers_dict = stds_transformers_dict.get(METADATA_TRANSFORMERS_KEY, {})
    study_transformers_dict = study_transformers_dict.get(METADATA_TRANSFORMERS_KEY, {})

    result = deepcopy_dict(stds_transformers_dict)
    for curr_transformer_type, curr_study_transformers_dict in study_transformers_dict.items():
        curr_stds_transformers_dict = stds_transformers_dict.get(curr_transformer_type, {})
        combined_transformers_dict = curr_stds_transformers_dict | curr_study_transformers_dict
        result[curr_transformer_type] = combined_transformers_dict

    return result


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

        # resolve aliases and base types for this host's sample types
        # This happens AFTER recursion so children inherit unresolved aliases,
        # ensuring correct bottom-up resolution order
        if SAMPLE_TYPE_SPECIFIC_METADATA_KEY in curr_host_type_wip_flat_dict:
            curr_host_type_wip_flat_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY] = \
                _resolve_sample_type_aliases_and_bases(
                    curr_host_type_wip_flat_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY],
                    curr_host_type_wip_flat_dict.get(METADATA_FIELDS_KEY, {}))

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
        add_metadata_fields_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Update work-in-progress metadata dictionary *in place* with additional metadata dictionary.

    Parameters
    ----------
    wip_metadata_fields_dict : Dict[str, Any]
        Current work-in-progress metadata fields dictionary.
    add_metadata_fields_dict : Dict[str, Any]
        Metadata fields dictionary to incorporate.

    Returns
    -------
    Dict[str, Any]
        (Pointer to) updated work-in-progress metadata fields dictionary.
    """
    for curr_add_metadata_field, curr_add_metadata_field_dict in add_metadata_fields_dict.items():
        if curr_add_metadata_field not in wip_metadata_fields_dict:
            wip_metadata_fields_dict[curr_add_metadata_field] = {}

        if ALLOWED_KEY in curr_add_metadata_field_dict:
            # remove the ANYOF_KEY from curr_wip_metadata_fields_dict[curr_metadata_field] if it exists there
            if ANYOF_KEY in wip_metadata_fields_dict[curr_add_metadata_field]:
                del wip_metadata_fields_dict[curr_add_metadata_field][ANYOF_KEY]

        if ANYOF_KEY in curr_add_metadata_field_dict:
            # remove the ALLOWED_KEY from curr_wip_metadata_fields_dict[curr_metadata_field] if it exists there
            if ALLOWED_KEY in wip_metadata_fields_dict[curr_add_metadata_field]:
                del wip_metadata_fields_dict[curr_add_metadata_field][ALLOWED_KEY]

            # remove the TYPE_KEY from curr_wip_metadata_fields_dict[curr_metadata_field] if it exists there
            if TYPE_KEY in wip_metadata_fields_dict[curr_add_metadata_field]:
                del wip_metadata_fields_dict[curr_add_metadata_field][TYPE_KEY]

        # TODO: Q: is it possible to have a list of allowed with a default
        #  at high level, then lower down have a list of allowed WITHOUT
        #  a default?  If so, how do we handle that?

        # update curr_wip_metadata_fields_dict[curr_metadata_field] with curr_add_metadata_field_dict
        wip_metadata_fields_dict[curr_add_metadata_field].update(curr_add_metadata_field_dict)
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

    # look for global settings in the add dict for this host; if
    # any exists, add it to the wip dict (ok to overwrite existing)
    for curr_global_setting_key in GLOBAL_SETTINGS_KEYS:
        if curr_global_setting_key in host_type_add_dict:
            host_type_wip_nested_dict[curr_global_setting_key] = \
                host_type_add_dict.get(curr_global_setting_key)

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
    # Note: resolution of aliases/base types happens in flatten_nested_stds_dict
    # AFTER recursion, to ensure correct bottom-up resolution order
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


def _construct_sample_type_metadata_fields_dict(
        sample_type: str,
        host_sample_types_config_dict: Dict[str, Any],
        a_host_type_metadata_fields_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Construct metadata fields dictionary for a specific host+sample type, resolving aliases and base types.

    Parameters
    ----------
    sample_type : str
        The sample type to process.
    host_sample_types_config_dict : Dict[str, Any]
        Dictionary containing config for *all* sample types in
        the host type in question.
    a_host_type_metadata_fields_dict : Dict[str, Any]
        Dictionary containing metadata fields for the host type in question.

    Returns
    -------
    Dict[str, Any]
        The constructed metadata fields dictionary for this host-and-sample-type combination.

    Raises
    ------
    ValueError
        If there are invalid alias chains or base type configurations.
    """
    sample_type_for_metadata = sample_type

    # get dict associated with the naive sample type
    sample_type_specific_dict = \
        host_sample_types_config_dict[sample_type]

    # if naive sample type contains an alias
    sample_type_alias = sample_type_specific_dict.get(ALIAS_KEY)
    if sample_type_alias:
        # change the sample type to the alias sample type
        # and use the alias's sample type dict
        sample_type_for_metadata = sample_type_alias
        sample_type_specific_dict = \
            host_sample_types_config_dict[sample_type_alias]
        # note that sample_type_specific_dict is now the dict for
        # the alias sample type, not the original naive sample type;
        # if IT has an alias, that would mean we are trying
        # to chain aliases, which is not allowed.
        if ALIAS_KEY in sample_type_specific_dict:
            raise ValueError(f"May not chain aliases "
                             f"('{sample_type}' to '{sample_type_alias}')")
    # endif sample type is an alias

    # if the sample type has a base type
    sample_type_base = sample_type_specific_dict.get(BASE_TYPE_KEY)
    if sample_type_base:
        # if the base type is the same as the current sample type,
        # that is circular and not allowed
        if sample_type_base == sample_type:
            raise ValueError(f"Sample type '{sample_type}' has itself as its base type.")

        # get the base's sample type dict
        base_sample_dict = host_sample_types_config_dict[sample_type_base]

        # if the base sample type dict ALSO has a (different) base type,
        # that is not allowed
        base_sample_base = base_sample_dict.get(BASE_TYPE_KEY)
        if base_sample_base:
            raise ValueError(f"Sample type '{sample_type}' has base type "
                             f"'{sample_type_base}', which has base type "
                             f"'{base_sample_base}'."
                             f" Chained base types are not allowed.")

        # add this sample type's info on top of the base type's info
        sample_type_specific_dict_metadata = update_wip_metadata_dict(
            deepcopy_dict(base_sample_dict[METADATA_FIELDS_KEY]),
            sample_type_specific_dict.get(METADATA_FIELDS_KEY, {}))
        sample_type_specific_dict = deepcopy_dict(sample_type_specific_dict)
        sample_type_specific_dict[METADATA_FIELDS_KEY] = \
            sample_type_specific_dict_metadata
    # endif sample type has a base type

    # add the sample-type-specific info generated above on top of the host info
    sample_type_metadata_dict = update_wip_metadata_dict(
        deepcopy_dict(a_host_type_metadata_fields_dict),
        sample_type_specific_dict.get(METADATA_FIELDS_KEY, {}))

    # set sample_type, and qiita_sample_type if it is not already set
    sample_type_definition = {
        ALLOWED_KEY: [sample_type_for_metadata],
        DEFAULT_KEY: sample_type_for_metadata,
        TYPE_KEY: "string"
    }
    sample_type_metadata_dict = update_wip_metadata_dict(
        sample_type_metadata_dict, {SAMPLE_TYPE_KEY: sample_type_definition})
    if QIITA_SAMPLE_TYPE not in sample_type_metadata_dict:
        sample_type_metadata_dict = update_wip_metadata_dict(
            sample_type_metadata_dict, {QIITA_SAMPLE_TYPE: sample_type_definition})
    # end if qiita_sample_type not already set

    return sample_type_metadata_dict


def _resolve_sample_type_aliases_and_bases(
        sample_types_dict: Dict[str, Any],
        host_metadata_fields_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve aliases and base types in sample type definitions.

    For each sample type in the input dictionary:
    1. If it's an alias, follow the alias and resolve the target's metadata
    2. If it has a base_type, inherit metadata fields from the base
    3. Merge sample-type metadata fields with host-level metadata fields
    4. Add sample_type and qiita_sample_type fields

    Parameters
    ----------
    sample_types_dict : Dict[str, Any]
        Dictionary of sample type configurations (from sample_type_specific_metadata).
    host_metadata_fields_dict : Dict[str, Any]
        Host-level metadata fields to merge into each sample type.

    Returns
    -------
    Dict[str, Any]
        Dictionary with all sample types resolved.

    Raises
    ------
    ValueError
        If chained aliases are detected or base type has invalid structure.
    """
    result = {}

    for sample_type_name in sample_types_dict.keys():
        resolved_metadata = _construct_sample_type_metadata_fields_dict(
            sample_type_name, sample_types_dict, host_metadata_fields_dict)

        result[sample_type_name] = {
            METADATA_FIELDS_KEY: resolved_metadata
        }

    return result


def build_full_flat_config_dict(
        study_specific_config_dict: Optional[Dict[str, Any]] = None,
        software_config_dict: Optional[Dict[str, Any]] = None,
        stds_fp: Optional[str] = None,
        exclude_internals: bool = False
) -> Dict[str, Any]:
    """Build a complete flattened configuration dictionary.

    Merges software configuration, study-specific configuration, and standards
    configuration into a single flat dictionary with fully resolved host type
    specific metadata.

    Parameters
    ----------
    study_specific_config_dict : Optional[Dict[str, Any]], default=None
        Study-specific configuration dictionary. If provided, these settings
        override the software config defaults. May contain host type overrides
        under STUDY_SPECIFIC_METADATA_KEY and transformer definitions under
        METADATA_TRANSFORMERS_KEY.
    software_config_dict : Optional[Dict[str, Any]], default=None
        Software configuration dictionary with default settings. If None,
        the default software config from config.yml will be used.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file. If None, the default standards
        config pulled from the standards.yml file will be used.
    exclude_internals : bool, default=False
        If True, remove host types whose names begin with an underscore,
        and from remaining hosts remove sample types whose names begin
        with an underscore.

    Returns
    -------
    Dict[str, Any]
        A complete flat configuration dictionary containing:
        - HOST_TYPE_SPECIFIC_METADATA_KEY: flattened and merged host type configs
        - METADATA_TRANSFORMERS_KEY: merged transformer definitions (if any)
        - Other top-level configuration keys (default, leave_requireds_blank, etc.)
    """
    if software_config_dict is None:
        software_config_dict = extract_config_dict(None)

    if study_specific_config_dict is None:
        study_specific_config_dict = {}

    # overwrite default settings in software config with study-specific ones (if any)
    software_plus_study_flat_config_dict = deepcopy_dict(study_specific_config_dict)
    software_plus_study_flat_config_dict = \
        software_config_dict | software_plus_study_flat_config_dict

    # combine the software+study flat-host-type config's host type specific info
    # with the standards nested-host-type config's host type specific info
    # to get a full combined, nested dictionary starting from HOST_TYPE_SPECIFIC_METADATA_KEY
    full_nested_hosts_dict = combine_stds_and_study_config(
        software_plus_study_flat_config_dict, stds_fp)

    full_nested_hosts_dict = _push_global_settings_into_top_host(
            full_nested_hosts_dict)

    full_flat_hosts_dict = flatten_nested_stds_dict(
        full_nested_hosts_dict, None)
    full_flat_config_dict = deepcopy_dict(full_nested_hosts_dict)
    full_flat_config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY] = full_flat_hosts_dict

    if exclude_internals:
        # identify all host types whose names begin with an underscore
        internal_host_types = [
            host_type for host_type in
            full_flat_config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY]
            if host_type.startswith("_")]
        # now del all those hosts out of the full flat config dict
        for host_type in internal_host_types:
            del full_flat_config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY][host_type]

        # for each, identify and del any sample types whose names begin with an underscore
        for host_type, host_type_dict in \
                full_flat_config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY].items():
            if SAMPLE_TYPE_SPECIFIC_METADATA_KEY in host_type_dict:
                internal_sample_types = [
                    sample_type for sample_type in
                    host_type_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY].keys()
                    if sample_type.startswith("_")]
                for sample_type in internal_sample_types:
                    del full_flat_config_dict[
                        HOST_TYPE_SPECIFIC_METADATA_KEY][host_type][
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY][sample_type]

    return full_flat_config_dict


def _push_global_settings_into_top_host(
        a_config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Push global settings into the top-level host within the same dictionary.

    Copies any global settings (e.g. default, leave_requireds_blank,
    overwrite_non_nans) found at the top level of a_config_dict into the
    single top-level host entry under HOST_TYPE_SPECIFIC_METADATA_KEY.

    Parameters
    ----------
    a_config_dict : Dict[str, Any]
        Dictionary containing both HOST_TYPE_SPECIFIC_METADATA_KEY (with
        exactly one top-level host) and any global settings keys.

    Returns
    -------
    Dict[str, Any]
        Updated dictionary with global settings added to top-level host.

    Raises
    ------
    ValueError
        If there is not exactly one top-level host in the nested hosts dictionary.
    """
    result = deepcopy_dict(a_config_dict)

    # get the top level host(s) in the dict
    # (should be only one because it is nested)
    top_level_host_keys = list(result[HOST_TYPE_SPECIFIC_METADATA_KEY].keys())
    if len(top_level_host_keys) != 1:
        raise ValueError(f"Expected exactly one top-level key in "
                         f"full_nested_hosts_dict but found: {top_level_host_keys}")
    top_level_host_key = top_level_host_keys[0]

    # check for each top-level setting and copy it under the top level host key
    for curr_setting_key in GLOBAL_SETTINGS_KEYS:
        if curr_setting_key in result:
            result[HOST_TYPE_SPECIFIC_METADATA_KEY][top_level_host_key][curr_setting_key] = \
                result[curr_setting_key]

    return result
