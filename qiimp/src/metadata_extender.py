import cerberus
import copy
import os
import pandas
from pathlib import Path
from typing import Dict, Optional
import yaml
from datetime import datetime
import qiimp.src.metadata_transformers as transformers

# Load in the config file
# Load in the user-supplied metadata file into a pandas dataframe
# For each host type:
#  Filter the metadata file to only include samples from that host type
#  Find the host type in the config dictionary and copy it
#  For each sample type:
#    Filter the host metadata file to only include samples from that sample type
#    Find the sample type in the host config dictionary and copy it
#    Set the metadata for each sample to be the metadata for the host-sample-type dict

# internal code keys
HOSTTYPE_SHORTHAND_KEY = "hosttype_shorthand"
SAMPLETYPE_SHORTHAND_KEY = "sampletype_shorthand"
QC_NOTE_KEY = "qc_note"

# config keys
METADATA_FIELDS_KEY = "metadata_fields"
STUDY_SPECIFIC_METADATA_KEY = "study_specific_metadata"
HOST_TYPE_SPECIFIC_METADATA_KEY = "host_type_specific_metadata"
SAMPLE_TYPE_KEY = "sample_type"
SAMPLE_TYPE_SPECIFIC_METADATA_KEY = "sample_type_specific_metadata"
ALIAS_KEY = "alias"
DEFAULT_KEY = "default"
REQUIRED_KEY = "required"
STUDY_START_DATE_KEY = "study_start_date"

# metadata keys
SAMPLE_NAME_KEY = "sample_name"
COLLECTION_TIMESTAMP_KEY = "collection_timestamp"
HOST_SUBJECT_ID_KEY = "host_subject_id"

# constant field values
NOT_PROVIDED_VAL = "not provided"
BLANK_VAL = "blank"
LEAVE_BLANK_VAL = "leaveblank"
DO_NOT_USE_VAL = "donotuse"

# TODO: add check that these are in the input xlsx file
REQUIRED_RAW_METADATA_FIELDS = [SAMPLE_NAME_KEY,
                                HOST_SUBJECT_ID_KEY,
                                HOSTTYPE_SHORTHAND_KEY,
                                SAMPLETYPE_SHORTHAND_KEY,
                                COLLECTION_TIMESTAMP_KEY]

# columns added to the metadata that are not actually part of it
INTERNAL_COL_KEYS = [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY,
                     QC_NOTE_KEY]


pandas.set_option("future.no_silent_downcasting", True)


def extract_config_dict(config_fp, starting_fp=None):
    if config_fp is None:
        grandparent_dir = _get_grandparent_dir(starting_fp)
        config_fp = os.path.join(grandparent_dir, "config.yml")

    # read in config file
    config_dict = extract_yaml_dict(config_fp)
    return config_dict


def _get_grandparent_dir(starting_fp=None):
    if starting_fp is None:
        starting_fp = __file__
    curr_dir = os.path.dirname(os.path.abspath(starting_fp))
    grandparent_dir = os.path.join(curr_dir, os.pardir, os.pardir)
    return grandparent_dir


def extract_yaml_dict(yaml_fp):
    with open(yaml_fp, "r") as f:
        yaml_dict = yaml.safe_load(f)
    return yaml_dict


def generate_extended_metadata_file_from_raw_metadata_file(
        raw_metadata_fp, study_specific_config, out_dir, out_base):

    # TODO: add sheet name handling?
    raw_metadata_df = pandas.read_excel(raw_metadata_fp)

    return generate_extended_metadata_file_from_raw_metadata_df(
        raw_metadata_df, study_specific_config, out_dir, out_base)


def generate_extended_metadata_file_from_raw_metadata_df(
        raw_metadata_df, study_specific_config, out_dir, out_base,
        study_specific_transformers_dict=None):

    if study_specific_config:
        nested_stds_plus_dict = _combine_stds_and_study_config(
            study_specific_config)
    else:
        study_specific_config = extract_config_dict(None)
        nested_stds_plus_dict = _extract_stds_config(None)

    flattened_hosts_dict = _flatten_nested_stds_dict(
        nested_stds_plus_dict, None)
    study_specific_config[HOST_TYPE_SPECIFIC_METADATA_KEY] = flattened_hosts_dict

    metadata_df = _populate_metadata_df(
        raw_metadata_df, study_specific_transformers_dict,
        study_specific_config)

    _output_to_df(metadata_df, out_dir, out_base,
                  INTERNAL_COL_KEYS, remove_internals=True)
    return metadata_df


def _combine_stds_and_study_config(
        study_config_dict: Dict,
        stds_fp: Optional[str] = None) \
        -> Dict:

    stds_nested_dict = _extract_stds_config(stds_fp)
    study_flat_dict = study_config_dict.get(STUDY_SPECIFIC_METADATA_KEY, {})

    combined_host_types_dict = _make_combined_stds_and_study_host_type_dicts(
        study_flat_dict, stds_nested_dict)

    stds_plus_study_nested_dict = {
        HOST_TYPE_SPECIFIC_METADATA_KEY: combined_host_types_dict}
    return stds_plus_study_nested_dict


# At each level, this method adds info from a static, flat study-specific
# hosts dictionary (the same at every level; arg 1) into a copy of the host
# types dictionary for the previous host level's standards nested dictionary
# (arg 2). (Note that the flat study-specific hosts dictionary is NOT expected
# to (a) contains all hosts nor to (b) have complete metadata definitions for
# each host.) The result is an augmented nested hosts dictionary.
def _make_combined_stds_and_study_host_type_dicts(
        flat_study_dict: Dict,
        parent_host_stds_nested_dict: Dict) \
        -> Dict:

    # get all the host type dicts for the study (these are flat);
    # these are what we will be adding *FROM*
    study_host_types_dict = flat_study_dict.get(
        HOST_TYPE_SPECIFIC_METADATA_KEY, {})

    parent_stds_host_types_dict = \
        parent_host_stds_nested_dict.get(HOST_TYPE_SPECIFIC_METADATA_KEY, {})
    # define the output dictionary as a copy of the parent-level standard.
    # This will be augmented if there are any hosts at this level.
    wip_host_types_dict = \
        _deepcopy_dict(parent_stds_host_types_dict)

    # loop over the host types at this level in parent_stds_nested_dict;
    # these are what we will be copying to add *TO*
    for curr_host_type, curr_host_type_stds_nested_dict \
            in parent_stds_host_types_dict.items():

        # only need to do work at this level if curr host type is in study dict
        # since otherwise the wip dict is an unchanged copy of the stds dict
        if curr_host_type not in study_host_types_dict:
            # make a copy of the stds for the current host type to add info to
            curr_host_type_wip_nested_dict = \
                _deepcopy_dict(curr_host_type_stds_nested_dict)
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
        curr_host_type_base_dict, curr_host_type_add_dict):

    # make a copy of the base for the current host type to add info to
    curr_host_type_wip_nested_dict = \
        _deepcopy_dict(curr_host_type_base_dict)

    # look for a default key in the add dict for this host; if
    # it exists, add it to the wip dict (ok to overwrite existing)
    if DEFAULT_KEY in curr_host_type_add_dict:
        curr_host_type_wip_nested_dict[DEFAULT_KEY] = \
            curr_host_type_add_dict.get(DEFAULT_KEY)

    # combine add metadata fields with the wip metadata fields
    # for the current host type and assign to wip if not empty
    curr_host_type_wip_metadata_fields_dict = \
        _combine_base_and_added_metadata_fields(
            curr_host_type_base_dict,
            curr_host_type_add_dict)
    if curr_host_type_wip_metadata_fields_dict:
        curr_host_type_wip_nested_dict[METADATA_FIELDS_KEY] = \
            curr_host_type_wip_metadata_fields_dict
    # endif the host type combination is not empty

    # combine any sample-type specific entries within the current host
    # type and assign to wip if not empty
    curr_host_wip_sample_types_dict = \
        _combine_base_and_added_sample_type_specific_metadata(
            curr_host_type_wip_nested_dict,
            curr_host_type_add_dict)
    # if we got back a non-empty dictionary of sample types,
    # add it to the wip for this host type dict
    if curr_host_wip_sample_types_dict:
        curr_host_type_wip_nested_dict[
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY] = \
            curr_host_wip_sample_types_dict
    # endif the sample types dictionary is not empty

    return curr_host_type_wip_nested_dict


def _combine_base_and_added_metadata_fields(
        curr_host_type_base_dict, curr_host_type_add_dict):

    # copy the metadata fields from the base to make the wip metadata fields
    curr_host_type_wip_metadata_fields_dict = _deepcopy_dict(
        curr_host_type_base_dict.get(METADATA_FIELDS_KEY, {}))

    # update the wip with the add metadata fields
    curr_host_type_add_metadata_fields_dict = \
        curr_host_type_add_dict.get(METADATA_FIELDS_KEY, {})
    curr_host_type_wip_metadata_fields_dict = \
        _update_wip_metadata_dict(
            curr_host_type_wip_metadata_fields_dict,
            curr_host_type_add_metadata_fields_dict)

    return curr_host_type_wip_metadata_fields_dict


def _combine_base_and_added_sample_type_specific_metadata(
        curr_host_type_base_dict, curr_host_type_add_dict):

    # copy the dictionary of sample types from the base to make the wip dict
    curr_host_wip_sample_types_dict = _deepcopy_dict(
        curr_host_type_base_dict.get(
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY, {}))

    # loop over the sample types in the add dict
    curr_host_add_sample_types_dict = \
        curr_host_type_add_dict.get(
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY, {})
    for curr_sample_type, curr_sample_type_add_dict \
            in curr_host_add_sample_types_dict.items():

        # if the sample type is already in the wip, and it has metadata fields,
        # and it has metadata fields in the add dict, combine metadata fields
        curr_sample_type_add_def_type = \
            _id_sample_type_definition(
                curr_sample_type, curr_sample_type_add_dict)
        curr_sample_type_wip_def_type = None
        if curr_sample_type in curr_host_wip_sample_types_dict:
            curr_sample_type_wip_def_type = \
                _id_sample_type_definition(
                    curr_sample_type,
                    curr_host_wip_sample_types_dict[curr_sample_type])
        # end if sample type is in wip
        if curr_sample_type_wip_def_type == METADATA_FIELDS_KEY \
                and curr_sample_type_add_def_type == METADATA_FIELDS_KEY:
            curr_sample_type_add_metadata_fields_dict = \
                curr_sample_type_add_dict[METADATA_FIELDS_KEY]
            curr_sample_type_wip_metadata_fields_dict = \
                curr_host_wip_sample_types_dict[curr_sample_type][
                    METADATA_FIELDS_KEY]
            curr_sample_type_wip_metadata_fields_dict = (
                _update_wip_metadata_dict(
                    curr_sample_type_wip_metadata_fields_dict,
                    curr_sample_type_add_metadata_fields_dict))
            # if the above combination is not of two empties
            if curr_sample_type_wip_metadata_fields_dict:
                curr_host_wip_sample_types_dict[curr_sample_type] = \
                    {
                        METADATA_FIELDS_KEY:
                            curr_sample_type_wip_metadata_fields_dict
                    }
            # endif the sample type combination is not empty

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


def _extract_stds_config(stds_fp):
    if not stds_fp:
        stds_fp = os.path.join(_get_grandparent_dir(), "standards.yml")
    return extract_config_dict(stds_fp)


def _deepcopy_dict(input_dict):
    output_dict = {}
    for curr_key, curr_val in input_dict.items():
        if isinstance(curr_val, dict):
            output_dict[curr_key] = _deepcopy_dict(curr_val)
        else:
            output_dict[curr_key] = copy.deepcopy(curr_val)
    return output_dict


def _update_wip_metadata_dict(
        curr_wip_metadata_fields_dict, curr_stds_metadata_fields_dict):
    ALLOWED_KEY = "allowed"
    ANYOF_KEY = "anyof"
    TYPE_KEY = "type"

    for curr_metadata_field, curr_stds_metadata_field_dict in curr_stds_metadata_fields_dict.items():
        if curr_metadata_field not in curr_wip_metadata_fields_dict:
            curr_wip_metadata_fields_dict[curr_metadata_field] = {}

        if ALLOWED_KEY in curr_stds_metadata_field_dict:
            # remove the ANYOF_KEY from curr_wip_metadata_fields_dict[curr_metadata_field] if it exists there
            if ANYOF_KEY in curr_wip_metadata_fields_dict[curr_metadata_field]:
                del curr_wip_metadata_fields_dict[curr_metadata_field][ANYOF_KEY]

        if ANYOF_KEY in curr_stds_metadata_field_dict:
            # remove the ALLOWED_KEY from curr_wip_metadata_fields_dict[curr_metadata_field] if it exists there
            if ALLOWED_KEY in curr_wip_metadata_fields_dict[curr_metadata_field]:
                del curr_wip_metadata_fields_dict[curr_metadata_field][ALLOWED_KEY]

            # remove the TYPE_KEY from curr_wip_metadata_fields_dict[curr_metadata_field] if it exists there
            if TYPE_KEY in curr_wip_metadata_fields_dict[curr_metadata_field]:
                del curr_wip_metadata_fields_dict[curr_metadata_field][TYPE_KEY]

        # TODO: Q: is it possible to have a list of allowed with a default
        #  at high level, then lower down have a list of allowed WITHOUT
        #  a default?  If so, how do we handle that?

        # update curr_wip_metadata_fields_dict[curr_metadata_field] with curr_stds_metadata_field_dict
        curr_wip_metadata_fields_dict[curr_metadata_field].update(curr_stds_metadata_field_dict)
    # next metadata field

    return curr_wip_metadata_fields_dict


def _id_sample_type_definition(sample_type_name, sample_type_dict):
    has_alias = ALIAS_KEY in sample_type_dict
    has_metadata = METADATA_FIELDS_KEY in sample_type_dict
    if has_alias and has_metadata:
        raise ValueError(f"Sample type '{sample_type_name}' has both "
                         f"'{ALIAS_KEY}' and '{METADATA_FIELDS_KEY}' keys in "
                         "the same sample type dict")
    elif has_alias:
        return ALIAS_KEY
    elif has_metadata:
        return METADATA_FIELDS_KEY
    else:
        raise ValueError(f"Sample type '{sample_type_name}' has neither "
                         f"'{ALIAS_KEY}' nor '{METADATA_FIELDS_KEY}' keys in "
                         "the same sample type dict")


# At each level, this method adds info from the host types dictionary for the
# previous host level's standards nested dictionary (arg 1) into a copy of a
# growing flat-and-complete hosts dictionary for the previous level (arg 2).
# The result is a flat hosts dictionary that (a) contains all hosts and (b)
# has complete metadata definitions for each host.
def _flatten_nested_stds_dict(
        parent_stds_nested_dict: Dict,
        parent_flattened_host_dict: Dict = None) \
        -> Dict:

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
        # is ok if this comes back as an empty dict; we are adding to it below.
        wip_host_types_dict = _flatten_nested_stds_dict(
            curr_host_type_stds_nested_dict, curr_host_type_wip_flat_dict)

        # assign the flattened wip dict for the current host type to the result
        # (which now contains flat records for the hosts lower down than
        # this, if there are any)
        wip_host_types_dict[curr_host_type] = \
            curr_host_type_wip_flat_dict
    # next host type

    return wip_host_types_dict


def _populate_metadata_df(
        raw_metadata_df, transformer_funcs_dict, main_config_dict):
    metadata_df = raw_metadata_df.copy()
    _update_metadata_df_field(metadata_df, QC_NOTE_KEY, LEAVE_BLANK_VAL)
    # metadata_df[QC_NOTE_KEY] = LEAVE_BLANK_VAL

    # TODO: this assumes that none of the columns we're adding already exist;
    #  that may be an unsafe assumption.  We should check for that and handle
    #  it if it's the case.

    metadata_df = transform_pre_metadata(
        metadata_df, transformer_funcs_dict, main_config_dict)

    # first, add the metadata for the host types
    metadata_df = _generate_metadata_for_host_types(
        metadata_df, main_config_dict)

    # TODO: this maybe could actually be done, at least for studies that
    #  provide participant-level data, but we'd have to suck that in and
    #  transform it into a dict in the format to be added to the config.
    #  I have no idea what the scope of that is, so for now I'm ignoring it.
    # next, add the metadata for the subjects
    # metadata_df = _generate_metadata_for_known_subjects(
    #     metadata_df, curr_settings_dict, config)

    return metadata_df


def transform_pre_metadata(
        pre_metadata_df, transformer_funcs_dict, config_dict):
    if transformer_funcs_dict is None:
        transformer_funcs_dict = {}

    metadata_transformers = config_dict.get("metadata_transformers", None)
    if metadata_transformers:
        pre_transformers = metadata_transformers.get("pre_population", None)
        for curr_target_field, curr_transformer_dict in pre_transformers.items():
            curr_source_field = curr_transformer_dict["sources"]
            curr_func_name = curr_transformer_dict["function"]

            try:
                curr_func = transformer_funcs_dict[curr_func_name]
            except KeyError:
                try:
                    curr_func = getattr(transformers, curr_func_name)
                except AttributeError:
                    raise ValueError(
                        f"Unable to find transformer '{curr_func_name}'")
                # end try to find in qiimp transformers
            # end try to find in input (study-specific) transformers

            # apply the function named curr_func_name to the column of the
            # metadata_df named curr_source_field to fill curr_target_field
            _update_metadata_df_field(pre_metadata_df, curr_target_field,
                                      curr_func, curr_source_field,
                                      overwrite_nans=False)

    return pre_metadata_df


def _update_metadata_df_field(
        metadata_df, field_name, field_val_or_func,
        source_fields=None, overwrite_nans=True):

    # Note: function doesn't return anything.  Work is done in-place on the
    #  metadata_df passed in.

    if source_fields:
        if overwrite_nans or (field_name not in metadata_df.columns):
            metadata_df[field_name] = \
                metadata_df.apply(
                    lambda row: field_val_or_func(row, source_fields),
                    axis=1)
        else:
            # TODO: not yet tested; from StackOverflow
            metadata_df.loc[metadata_df[field_name].isnull(), field_name] = \
                metadata_df.apply(
                    lambda row: field_val_or_func(row, source_fields),
                    axis=1)
        # endif overwrite nans for function call
    else:
        if overwrite_nans or (field_name not in metadata_df.columns):
            metadata_df[field_name] = field_val_or_func
        else:
            metadata_df[field_name] = \
                metadata_df[field_name].fillna(field_val_or_func)
            # metadata_df[field_name].fillna(field_val_or_func, inplace=True)
        # endif overwrite nans for constant value
    # endif using a function/a constant value


def _generate_metadata_for_host_types(
        metadata_df, config):
    # gather global settings
    settings_dict = {DEFAULT_KEY: config.get(DEFAULT_KEY),
                     STUDY_START_DATE_KEY: config.get(STUDY_START_DATE_KEY)}

    host_type_dfs = []
    host_type_shorthands = pandas.unique(metadata_df[HOSTTYPE_SHORTHAND_KEY])
    for curr_host_type_shorthand in host_type_shorthands:
        concatted_dfs = _generate_metadata_for_host_type(
                metadata_df, curr_host_type_shorthand, settings_dict, config)

        host_type_dfs.append(concatted_dfs)
    # next host type

    output_df = pandas.concat(host_type_dfs, ignore_index=True)

    # concatting dfs from different hosts can create large numbers of NAs--
    # for example, if concatting a host-associated df with a control df, where
    # the control df doesn't have values for any of the host-related columns.
    # Fill those NAs with whatever the general default is.
    # NB: passing in the same dict twice here is not a mistake, just a
    # convenience since we don't have a more specific dict at this point.
    output_df = _fill_na_if_default(
        output_df, settings_dict, settings_dict)

    # TODO: this is setting a value in the output; should it be centralized
    #  so it is easy to find?
    output_df.replace(LEAVE_BLANK_VAL, "", inplace=True)
    return output_df


def _generate_metadata_for_host_type(
        metadata_df, curr_host_type, settings_dict, config):

    host_type_mask = \
        metadata_df[HOSTTYPE_SHORTHAND_KEY] == curr_host_type
    host_type_df = metadata_df.loc[host_type_mask, :].copy()

    known_host_shorthands = config[HOST_TYPE_SPECIFIC_METADATA_KEY].keys()
    if curr_host_type not in known_host_shorthands:
        _update_metadata_df_field(
            host_type_df, QC_NOTE_KEY, "invalid host_type")
        # host_type_df[QC_NOTE_KEY] = "invalid host_type"
        concatted_df = host_type_df
    else:
        # # gather host-type-specific settings and apply them to the metadata
        host_type_dict = \
            config[HOST_TYPE_SPECIFIC_METADATA_KEY][curr_host_type]
        curr_settings_dict = _deepcopy_dict(settings_dict)
        curr_settings_dict[DEFAULT_KEY] = host_type_dict.get(
            DEFAULT_KEY, curr_settings_dict[DEFAULT_KEY])

        # for each sample type in metadata for this host type
        dfs_to_concat = []
        found_host_sample_types = \
            pandas.unique(host_type_df[SAMPLETYPE_SHORTHAND_KEY])
        for curr_sample_type in found_host_sample_types:
            curr_sample_type_df = \
                _generate_metadata_for_sample_type_in_host(
                    host_type_df, curr_sample_type, curr_settings_dict,
                    host_type_dict, config)

            dfs_to_concat.append(curr_sample_type_df)
        # next sample type in metadata for this host type

        concatted_df = pandas.concat(dfs_to_concat, ignore_index=True)
    # endif host_type is valid

    return concatted_df


def _update_metadata_from_config_dict(
        metadata_df, config_section_dict, dict_is_metadata_fields=False):

    if not dict_is_metadata_fields:
        metadata_fields_dict = config_section_dict.get(METADATA_FIELDS_KEY)
    else:
        metadata_fields_dict = config_section_dict

    if metadata_fields_dict:
        metadata_df = _update_metadata_from_dict(
            metadata_df, metadata_fields_dict)
    return metadata_df


def _update_metadata_from_dict(metadata_df, metadata_fields_dict):
    output_df = metadata_df.copy()
    for curr_field_name, curr_field_vals_dict in metadata_fields_dict.items():
        if DEFAULT_KEY in curr_field_vals_dict:
            curr_default_val = curr_field_vals_dict[DEFAULT_KEY]
            _update_metadata_df_field(
                output_df, curr_field_name, curr_default_val,
                overwrite_nans=False)
            # output_df[curr_field_name] = curr_default_val
        elif REQUIRED_KEY in curr_field_vals_dict:
            curr_required_val = curr_field_vals_dict[REQUIRED_KEY]
            if curr_required_val and curr_field_name not in output_df:
                _update_metadata_df_field(
                    output_df, curr_field_name, LEAVE_BLANK_VAL)
                # output_df[curr_field_name] = LEAVE_BLANK_VAL
            # note that if the field is (a) required, (b) does not have a
            # default value, and (c) IS already in the metadata, it will
            # be left alone, with no changes made to it!
    return output_df


def _generate_metadata_for_sample_type_in_host(
        host_type_df, sample_type, curr_settings_dict,
        host_type_dict, config):

    host_sample_types_dict = \
        host_type_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY]
    wip_metadata_dict = _deepcopy_dict(
        host_type_dict.get(METADATA_FIELDS_KEY, {}))

    # get df of records for this sample type in this host type
    sample_type_mask = \
        host_type_df[SAMPLETYPE_SHORTHAND_KEY] == sample_type
    sample_type_df = host_type_df.loc[sample_type_mask, :].copy()

    known_sample_types = host_sample_types_dict.keys()
    if sample_type not in known_sample_types:
        _update_metadata_df_field(
            sample_type_df, QC_NOTE_KEY, "invalid sample_type")
        # sample_type_df[QC_NOTE_KEY] = "invalid sample_type"
    else:
        sample_type_for_metadata = sample_type

        # get sample-type-specific metadata dict
        sample_type_specific_dict = \
            host_sample_types_dict[sample_type]
        sample_type_alias = sample_type_specific_dict.get(ALIAS_KEY)
        if sample_type_alias:
            sample_type_for_metadata = sample_type_alias
            sample_type_specific_dict = \
                host_sample_types_dict[sample_type_alias]

        wip_metadata_dict = _update_wip_metadata_dict(
            wip_metadata_dict,
            {
                SAMPLE_TYPE_KEY: {DEFAULT_KEY: sample_type_for_metadata}})
        # _update_metadata_df_field(
        #     sample_type_df, SAMPLE_TYPE_KEY, sample_type_for_metadata)
        # sample_type_df[SAMPLE_TYPE_KEY] = sample_type_for_metadata

        wip_metadata_dict = _update_wip_metadata_dict(
            wip_metadata_dict,
            sample_type_specific_dict.get(METADATA_FIELDS_KEY, {}))

        sample_type_df = _update_metadata_from_config_dict(
            sample_type_df, wip_metadata_dict, True)

        # This code adds all sorts of metadata fields used by ABTX.
        # based on digging the sample collection date out of the ABTX
        # sample name.  It isn't directly applicable to non-ABTX samples.
#        sample_type_df = _update_metadata_for_indiv_samples_of_type(
#            sample_type_df, sample_type_specific_dict, curr_settings_dict)

        # fill NAs with default value if any is set
        sample_type_df = _fill_na_if_default(
            sample_type_df, sample_type_specific_dict, curr_settings_dict)

        # # make complete host_sample_type config dict
        # host_fields_config = copy.deepcopy(host_type_dict[METADATA_FIELDS_KEY])
        # host_sample_config = copy.deepcopy(sample_type_specific_dict[METADATA_FIELDS_KEY])
        # host_fields_config.update(host_sample_config)
        # _validate_raw_metadata_df(
        #    sample_type_df, host_type_dict, sample_type_specific_dict)

    return sample_type_df


# fill NAs with default value if any is set
def _fill_na_if_default(metadata_df, specific_dict, settings_dict):
    default_val = specific_dict.get(DEFAULT_KEY, settings_dict[DEFAULT_KEY])
    if default_val:
        # TODO: this is setting a value in the output; should it be
        #  centralized so it is easy to find?
        metadata_df = \
            metadata_df.astype("string").fillna(default_val)

    return metadata_df


def _validate_raw_metadata_df(
        raw_metadata_df, host_type_dict, sample_type_specific_dict):

    config = _make_cerberus_schema(host_type_dict, sample_type_specific_dict)

    # use python Cerberus validator on all the fields that already exist in the
    # metadata file?
    raw_metadata_dict = raw_metadata_df.to_dict(orient="records")
    v = cerberus.Validator()
    # is_valid = v.validate(raw_metadata_dict, config)
    # if not is_valid:
    for curr_idx, curr_row in enumerate(raw_metadata_dict):
        if not v.validate(curr_row, config):
            print(f"Row {curr_idx} failed validation:")
            print(v.errors)


def _make_cerberus_schema(host_type_dict, sample_type_specific_dict):
    # make complete host_sample_type config dict
    host_fields_config = copy.deepcopy(host_type_dict[METADATA_FIELDS_KEY])
    host_sample_config = copy.deepcopy(sample_type_specific_dict[METADATA_FIELDS_KEY])
    host_fields_config.update(host_sample_config)

    unrecognized_keys = ['is_phi', 'field_desc', 'units',
                         'min_exclusive', 'unique']
    # traverse the host_fields_config dict and remove any keys that are not
    # recognized by cerberus
    host_fields_config = _remove_keys_from_dict(
        host_fields_config, unrecognized_keys)

    return host_fields_config


def _remove_keys_from_dict(input_dict, keys_to_remove):
    output_dict = {}
    for curr_key, curr_val in input_dict.items():
        if isinstance(curr_val, dict):
            output_dict[curr_key] = \
                _remove_keys_from_dict(curr_val, keys_to_remove)
        elif isinstance(curr_val, list):
            output_dict[curr_key] = \
                _remove_keys_from_dict_in_list(curr_val, keys_to_remove)
        else:
            if curr_key not in keys_to_remove:
                output_dict[curr_key] = copy.deepcopy(curr_val)
    return output_dict


def _remove_keys_from_dict_in_list(input_list, keys_to_remove):
    output_list = []
    for curr_val in input_list:
        if isinstance(curr_val, dict):
            output_list.append(
                _remove_keys_from_dict(curr_val, keys_to_remove))
        elif isinstance(curr_val, list):
            output_list.append(
                _remove_keys_from_dict_in_list(curr_val, keys_to_remove))
        else:
            output_list.append(curr_val)
    return output_list


def _output_to_df(a_df, out_dir, out_base, internal_col_names,
                  sep="\t", remove_internals=False):

    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    # sort columns alphabetically
    a_df = a_df.reindex(sorted(a_df.columns), axis=1)

    if remove_internals:
        # output a file of any qc failures; include no contents
        # (not even header line) if there are no failures--bc it is easy to
        # eyeball "zero bytes"
        fails_qc_mask = a_df[QC_NOTE_KEY] != ""
        qc_fails_df = a_df.loc[fails_qc_mask, INTERNAL_COL_KEYS].copy()
        qc_fails_fp = os.path.join(
            out_dir, f"{timestamp_str}_{out_base}_fails.txt")
        if qc_fails_df.empty:
            Path(qc_fails_fp).touch()
        else:
            qc_fails_df.to_csv(qc_fails_fp, sep=sep, index=False)

        a_df = a_df.drop(columns=internal_col_names)
        col_names = list(a_df)
    else:
        # move the internal columns to the end of the list of cols to output
        col_names = list(a_df)
        for curr_internal_col_name in internal_col_names:
            col_names.pop(col_names.index(curr_internal_col_name))
            col_names.append(curr_internal_col_name)

    # move sample name to the first column
    col_names.insert(0, col_names.pop(col_names.index(SAMPLE_NAME_KEY)))
    output_df = a_df.loc[:, col_names].copy()

    out_fp = os.path.join(out_dir, f"{timestamp_str}_{out_base}.txt")
    output_df.to_csv(out_fp, sep=sep, index=False)


# import numpy as np
# from dateutil import parser
# from dateutil.relativedelta import relativedelta
#
# internal code keys
# SUBJECT_SHORTHAND_KEY = "subject_shorthand"
# PLATE_SAMPLE_ID_KEY = "plate_sample_id"
# PLATE_ROW_ID_KEY = "plate_row_id"
# PLATE_COL_ID_KEY = "plate_col_id"
# PLATE_ID_KEY = "plate_id"
# PLATING_NOTES_KEY = "plating_notes"
# PLATING_DATE_KEY = "plating_date"
#
# config fields
# DESIRED_PLATES_KEY = "desired_plates"
# HOST_SPECIFIC_METADATA_KEY = "host_specific_metadata"
# DOB_KEY = "date_of_birth"
# TIMESPANS_KEY = "timespans"
# FIRST_DATE_KEY = "first_date"
# LAST_DATE_KEY = "last_date"
# LOCATION_BREAK_KEY = "location_break"
# BEFORE_LOCATION_KEY = "before_location"
# AFTER_LOCATION_KEY = "after_location"
# AFTER_START_DATE_KEY = "after_start_date"
# LOCATIONS_KEY = "locations"
# LOCATION_KEY = "location"
# ASSUME_DATES_PRESENT_KEY = "assume_dates_present"
#
# metadata fields
# IS_COLLECTION_TIMESTAMP_VALID_KEY = "is_collection_timestamp_valid"
# ORDINAL_TIMESTAMP_KEY = "ordinal_timestamp"
# ORIGINAL_COLLECTION_TIMESTAMP_KEY = "original_collection_timestamp"
# DESCRIPTION_KEY = "description"
# HOST_AGE_KEY = "host_age"
# M03_AGE_YEARS_KEY = "m03_age_years"
# DAYS_SINCE_FIRST_DAY_KEY = "days_since_first_day"
#
#
# def _update_metadata_for_indiv_samples_of_type(
#         sample_type_df, sample_type_specific_dict, curr_settings_dict):
#     # add date-dependent metadata if collection timestamp is not pre-specified
#     if COLLECTION_TIMESTAMP_KEY not in \
#             sample_type_specific_dict[METADATA_FIELDS_KEY]:
#         study_start_str = curr_settings_dict[STUDY_START_DATE_KEY]
#         sample_type_df = _add_collection_dates(sample_type_df, study_start_str)
#         sample_type_df = _add_days_since_start(sample_type_df, study_start_str)
#
#     return sample_type_df
#
#
# def _add_collection_dates(plates_df, study_start_date_str):
#     output_df = plates_df.copy()
#     collection_dates = _get_collection_dates(plates_df, study_start_date_str)
#     output_df[COLLECTION_TIMESTAMP_KEY] = collection_dates
#     output_df[ORDINAL_TIMESTAMP_KEY] = collection_dates.dt.strftime('%Y%m%d')
#     none_date_mask = output_df[COLLECTION_TIMESTAMP_KEY].isna()
#     output_df[IS_COLLECTION_TIMESTAMP_VALID_KEY] = ~none_date_mask
#     output_df.loc[none_date_mask, QC_NOTE_KEY] = \
#         "invalid/unparseable date"
#
#     return output_df
#
#
# def _get_collection_dates(plates_df, start_date_str):
#     start_date = parser.parse(start_date_str, dayfirst=False)
#
#     def _get_date_from_sample_id_if_empty(row):
#         sample_date = None
#         putative_date_str = None
#
#         existing_date_str = row[COLLECTION_TIMESTAMP_KEY]
#         if existing_date_str:
#             putative_date_str = existing_date_str
#         else:
#             sample_id = row[SAMPLE_NAME_KEY]
#             fname_pieces = sample_id.split(".")
#             if len(fname_pieces) >= 3:
#                 putative_date_str = "/".join(fname_pieces[:3])
#
#         if putative_date_str:
#             try:
#                 sample_date = parser.parse(
#                     putative_date_str, fuzzy=True, dayfirst=False)
#             except:
#                 pass
#
#         # sanity check: can't be before study start date
#         if sample_date and sample_date < start_date:
#             sample_date = None
#
#         return sample_date
#
#     collection_dates = \
#         plates_df.apply(_get_date_from_sample_id_if_empty, axis=1)
#     return collection_dates
#
#
# def _add_days_since_start(plates_df, study_start_str):
#     output_df = plates_df.copy()
#     days_since = _get_duration_since_date(
#         plates_df, study_start_str, return_years=False)
#     output_df[DAYS_SINCE_FIRST_DAY_KEY] = days_since
#     return output_df
#
#
# def _get_duration_since_date(plates_df, start_date_str, return_years=True):
#     start_date = parser.parse(start_date_str, dayfirst=False)
#
#     def _get_diff_in_years(x):
#         result = None
#         input_date = pandas.to_datetime(x)
#         try:
#             if return_years:
#                 a_relativedelta = relativedelta(input_date, start_date)
#                 result = a_relativedelta.years
#             else:
#                 a_timedelta = input_date - start_date
#                 result = a_timedelta.days
#             result = int(result)
#         except:
#             pass
#         return result
#
#     age_in_years = plates_df[COLLECTION_TIMESTAMP_KEY].apply(_get_diff_in_years)
#     return age_in_years
#
#
# def _generate_metadata_for_known_subjects(
#         metadata_df, curr_settings_dict, config):
#     # NB: this one goes the other way than the others: it loops over the
#     # subjects defined in the config instead of over those found in the
#     # metadata file.  This is because there may not BE any subjects in the
#     # config, in which case there's nothing to add to the metadata file.
#
#     known_subjects = config[HOST_SUBJECT_ID_KEY].keys()
#     subject_dfs = []
#     for curr_subject in known_subjects:
#         concatted_dfs = _generate_metadata_for_subject(
#             metadata_df, curr_subject, curr_settings_dict, config)
#
#         subject_dfs.append(concatted_dfs)
#     # next subject
#
#     output_df = pandas.concat(subject_dfs, ignore_index=True)
#     # TODO: this is setting a value in the output; should it be centralized
#     #  so it is easy to find?
#     output_df.replace(LEAVE_BLANK_VAL, "", inplace=True)
#     return output_df
#
#
# def _generate_metadata_for_subject(
#         metadata_df, curr_subject, curr_settings_dict, config):
#
#     # get the df of records for this subject
#     subject_mask = \
#         metadata_df[HOST_SUBJECT_ID_KEY] == curr_subject
#     subject_df = metadata_df.loc[subject_mask, :].copy()
#
#     # NB: no need to check if curr_subject is in the config, because we
#     # already did that in the calling function
#
#     # get subject-specific metadata dict
#     subject_specific_dict = curr_settings_dict[curr_subject]
#     subject_df = _update_metadata_from_config_dict(
#         subject_df, subject_specific_dict)
#
#     subject_df = _update_metadata_for_indiv_samples_of_subject(
#         subject_df, subject_specific_dict, curr_settings_dict)
#
#     return subject_df
#
#
# def _update_metadata_for_indiv_samples_of_subject(
#         subject_df, subject_specific_dict, config):
#
#     if subject_specific_dict[DOB_KEY]:
#         subject_df = \
#             _add_host_age(subject_df, subject_specific_dict[DOB_KEY])
#
#     # add time-dependent location metadata if a location-date break exists
#     if subject_specific_dict[LOCATION_BREAK_KEY]:
#         subject_df = _add_location_info_from_break(
#             subject_df, subject_specific_dict[LOCATION_BREAK_KEY], config)
#
#     return subject_df
#
#
# def _add_host_age(plates_df, dob_str):
#     output_df = plates_df.copy()
#     age_in_years = _get_duration_since_date(plates_df, dob_str)
#     output_df[HOST_AGE_KEY] = age_in_years
#     output_df[M03_AGE_YEARS_KEY] = age_in_years
#     return output_df
#
#
# def _add_location_info_from_break(plates_df, location_break_dict, config):
#     output_df = plates_df.copy()
#     before_location_name = location_break_dict[BEFORE_LOCATION_KEY]
#     after_location_name = location_break_dict[AFTER_LOCATION_KEY]
#     first_after_date_str = location_break_dict[AFTER_START_DATE_KEY]
#     first_after_date = parser.parse(first_after_date_str, dayfirst=False)
#
#     before_location_fields = \
#         config[LOCATIONS_KEY][before_location_name][METADATA_FIELDS_KEY]
#     before_mask = plates_df[COLLECTION_TIMESTAMP_KEY] < first_after_date
#     for a_key, a_val in before_location_fields.items():
#         output_df.loc[before_mask, a_key] = a_val
#
#     after_location_fields = \
#         config[LOCATIONS_KEY][after_location_name][METADATA_FIELDS_KEY]
#     for a_key, a_val in after_location_fields.items():
#         output_df.loc[~before_mask, a_key] = a_val
#     return output_df
