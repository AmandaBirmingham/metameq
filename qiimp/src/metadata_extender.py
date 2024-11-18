import logging
import numpy as np
import os
import pandas
from pathlib import Path
from datetime import datetime
from typing import List
import yaml
from qiimp.src.util import extract_config_dict, extract_stds_config, \
    deepcopy_dict, validate_required_columns_exist, get_extension, \
    load_df_with_best_fit_encoding, update_metadata_df_field, \
    HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY, \
    QC_NOTE_KEY, METADATA_FIELDS_KEY, HOST_TYPE_SPECIFIC_METADATA_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, SAMPLE_TYPE_KEY, QIITA_SAMPLE_TYPE, \
    DEFAULT_KEY, REQUIRED_KEY, ALIAS_KEY, BASE_TYPE_KEY, \
    LEAVE_BLANK_VAL, SAMPLE_NAME_KEY, \
    ALLOWED_KEY, TYPE_KEY, LEAVE_REQUIREDS_BLANK_KEY, OVERWRITE_NON_NANS_KEY, \
    METADATA_TRANSFORMERS_KEY, PRE_TRANSFORMERS_KEY, POST_TRANSFORMERS_KEY, \
    SOURCES_KEY, FUNCTION_KEY, REQUIRED_RAW_METADATA_FIELDS
from qiimp.src.metadata_configurator import combine_stds_and_study_config, \
    flatten_nested_stds_dict, update_wip_metadata_dict
from qiimp.src.metadata_validator import validate_metadata_df, \
    output_validation_msgs
import qiimp.src.metadata_transformers as transformers


# columns added to the metadata that are not actually part of it
INTERNAL_COL_KEYS = [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY,
                     QC_NOTE_KEY]

REQ_PLACEHOLDER = "_QIIMP2_REQUIRED"

# Define a logger for this module
logger = logging.getLogger(__name__)

pandas.set_option("future.no_silent_downcasting", True)

# TODO: find a way to inform user that they *MAY NOT* have a 'sample_id' column
#  (Per Antonio 10/28/24, this is a reserved name for Qiita and may not be
#  in the metadata).

def get_reserved_cols(raw_metadata_df, study_specific_config_dict,
                      study_specific_transformers_dict=None):
    validate_required_columns_exist(
        raw_metadata_df, [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY],
        "metadata missing required columns")

    # get unique HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY combinations
    temp_df = raw_metadata_df[
        [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY]].copy()
    temp_df.drop_duplicates(inplace=True)

    # add a SAMPLE_NAME_KEY column to the df that holds sequential integers
    temp_df[SAMPLE_NAME_KEY] = range(1, len(temp_df) + 1)

    temp_df = _catch_nan_required_fields(temp_df)

    # extend the metadata_df
    metadata_df, _ = _extend_metadata_df(
        temp_df, study_specific_config_dict,
        study_specific_transformers_dict)

    return sorted(metadata_df.columns.to_list())


def id_missing_cols(a_df: pandas.DataFrame) -> List[str]:
    # if any of the required columns are missing, return the names of those
    # columns
    missing_cols = set(REQUIRED_RAW_METADATA_FIELDS) - set(a_df.columns)
    return list(missing_cols)


def find_standard_cols(
        a_df: pandas.DataFrame, study_specific_config_dict: dict,
        study_specific_transformers_dict: dict = None,
        suppress_missing_name_err=False) -> List[str]:

    err_msg = "metadata missing required columns"
    if suppress_missing_name_err:
        # get a copy of the required columns list and remove the sample name
        required_cols = REQUIRED_RAW_METADATA_FIELDS.copy()
        required_cols.remove(SAMPLE_NAME_KEY)
        validate_required_columns_exist(a_df, required_cols, err_msg)
    else:
        validate_required_columns_exist(
            a_df, REQUIRED_RAW_METADATA_FIELDS, err_msg)

    # get the intersection of the standard columns and the columns in the
    # input dataframe
    standard_cols = get_reserved_cols(
        a_df, study_specific_config_dict,
        study_specific_transformers_dict=study_specific_transformers_dict)

    standard_cols_set = (set(standard_cols) - set(INTERNAL_COL_KEYS))

    return list(standard_cols_set & set(a_df.columns))


def find_nonstandard_cols(
        a_df: pandas.DataFrame, study_specific_config_dict: dict,
        study_specific_transformers_dict: dict = None) -> List[str]:

    validate_required_columns_exist(a_df, REQUIRED_RAW_METADATA_FIELDS,
                                    "metadata missing required columns")

    # get the columns in
    standard_cols = get_reserved_cols(
        a_df, study_specific_config_dict,
        study_specific_transformers_dict=study_specific_transformers_dict)

    return list(set(a_df.columns) - set(standard_cols))


def get_extended_metadata_from_df_and_yaml(
        raw_metadata_df, study_specific_config_fp):

    study_specific_config_dict = \
        _get_study_specific_config(study_specific_config_fp)

    metadata_df, validation_msgs_df = \
        _extend_metadata_df(raw_metadata_df, study_specific_config_dict)

    return metadata_df, validation_msgs_df


def get_qc_failures(a_df):
    fails_qc_mask = a_df[QC_NOTE_KEY] != ""
    qc_fails_df = \
        a_df.loc[fails_qc_mask, :].copy()
    return qc_fails_df


def write_metadata_results(
        metadata_df, validation_msgs_df, out_dir, out_name_base,
        sep="\t", remove_internals=True, suppress_empty_fails=False,
        internal_col_names=None):

    if internal_col_names is None:
        internal_col_names = INTERNAL_COL_KEYS

    _output_to_df(metadata_df, out_dir, out_name_base,
                  internal_col_names,
                  remove_internals_and_fails=remove_internals, sep=sep,
                  suppress_empty_fails=suppress_empty_fails)
    output_validation_msgs(validation_msgs_df, out_dir, out_name_base, sep=",",
                           suppress_empty_fails=suppress_empty_fails)


def write_extended_metadata_from_df(
        raw_metadata_df, study_specific_config_dict, out_dir, out_name_base,
        study_specific_transformers_dict=None, sep="\t",
        suppress_empty_fails=False, internal_col_names=None):

    metadata_df, validation_msgs_df = _extend_metadata_df(
        raw_metadata_df, study_specific_config_dict,
        study_specific_transformers_dict)

    write_metadata_results(
        metadata_df, validation_msgs_df, out_dir, out_name_base,
        sep, suppress_empty_fails, internal_col_names)

    return metadata_df


def write_extended_metadata(
        raw_metadata_fp, study_specific_config_fp,
        out_dir, out_name_base, sep="\t", suppress_empty_fails=False):

    # extract the extension from the raw_metadata_fp file path
    extension = os.path.splitext(raw_metadata_fp)[1]
    if extension == ".csv":
        raw_metadata_df = load_df_with_best_fit_encoding(raw_metadata_fp, ",")
    elif extension == ".txt":
        raw_metadata_df = load_df_with_best_fit_encoding(raw_metadata_fp, "\t")
    elif extension == ".xlsx":
        # NB: this loads (only) the first sheet of the input excel file.
        # If needed, can expand with pandas.read_excel sheet_name parameter.
        raw_metadata_df = pandas.read_excel(raw_metadata_fp)
    else:
        raise ValueError("Unrecognized input file extension; "
                         "must be .csv, .txt, or .xlsx")

    study_specific_config_dict = \
        _get_study_specific_config(study_specific_config_fp)

    extended_df = write_extended_metadata_from_df(
        raw_metadata_df, study_specific_config_dict,
        out_dir, out_name_base, sep=sep,
        suppress_empty_fails=suppress_empty_fails)

    return extended_df


def _get_study_specific_config(study_specific_config_fp):
    if study_specific_config_fp:
        study_specific_config_dict = \
            extract_config_dict(study_specific_config_fp)
    else:
        study_specific_config_dict = None

    return study_specific_config_dict


def _extend_metadata_df(raw_metadata_df, study_specific_config_dict,
                        study_specific_transformers_dict=None):
    validate_required_columns_exist(
        raw_metadata_df, REQUIRED_RAW_METADATA_FIELDS,
        "metadata missing required columns")

    software_config = extract_config_dict(None)

    if study_specific_config_dict:
        # overwrite default settings in software config with study-specific
        study_specific_config_dict = \
            software_config | study_specific_config_dict
        nested_stds_plus_dict = combine_stds_and_study_config(
            study_specific_config_dict)
    else:
        study_specific_config_dict = software_config
        nested_stds_plus_dict = extract_stds_config(None)

    flattened_hosts_dict = flatten_nested_stds_dict(
        nested_stds_plus_dict, None)
    study_specific_config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY] = \
        flattened_hosts_dict

    metadata_df, validation_msgs_df = _populate_metadata_df(
        raw_metadata_df, study_specific_transformers_dict,
        study_specific_config_dict)

    return metadata_df, validation_msgs_df


def _populate_metadata_df(
        raw_metadata_df, transformer_funcs_dict, main_config_dict):
    metadata_df = raw_metadata_df.copy()
    update_metadata_df_field(metadata_df, QC_NOTE_KEY, LEAVE_BLANK_VAL)

    metadata_df = _catch_nan_required_fields(metadata_df)

    metadata_df = _transform_metadata(
        metadata_df, transformer_funcs_dict, main_config_dict,
        PRE_TRANSFORMERS_KEY)

    # first, add the metadata for the host types
    metadata_df, validation_msgs = _generate_metadata_for_host_types(
        metadata_df, main_config_dict)

    metadata_df = _transform_metadata(
        metadata_df, transformer_funcs_dict, main_config_dict,
        POST_TRANSFORMERS_KEY)

    metadata_df = _reorder_df(metadata_df, INTERNAL_COL_KEYS)
    validation_msgs_df  = pandas.DataFrame(validation_msgs)

    return metadata_df, validation_msgs_df


def _catch_nan_required_fields(metadata_df):
    # if there are any sample_name fields that are NaN, raise an error
    nan_sample_name_mask = metadata_df[SAMPLE_NAME_KEY].isna()
    if nan_sample_name_mask.any():
        raise ValueError(
            f"Metadata contains NaN sample names")

    # if there are any hosttype_shorthand or sampletype_shorthand fields
    # that are NaN, set them to "empty" and raise a warning
    for curr_key in [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY]:
        nan_mask = metadata_df[curr_key].isna()
        if nan_mask.any():
            metadata_df.loc[nan_mask, curr_key] = "empty"
            logging.warning(f"Metadata contains NaN {curr_key}s; "
                            f"these have been set to 'empty'")

    return metadata_df


# transformer runner function
def _transform_metadata(
        metadata_df, transformer_funcs_dict, config_dict, stage_key):
    if transformer_funcs_dict is None:
        transformer_funcs_dict = {}

    overwrite_non_nans = config_dict.get(OVERWRITE_NON_NANS_KEY, False)
    metadata_transformers = config_dict.get(METADATA_TRANSFORMERS_KEY, None)
    if metadata_transformers:
        stage_transformers = metadata_transformers.get(stage_key, None)
        if stage_transformers:
            for curr_target_field, curr_transformer_dict in \
                    stage_transformers.items():
                curr_source_field = curr_transformer_dict[SOURCES_KEY]
                curr_func_name = curr_transformer_dict[FUNCTION_KEY]

                try:
                    curr_func = transformer_funcs_dict[curr_func_name]
                except KeyError:
                    try:
                        # looking into the qiimp transformers module
                        curr_func = getattr(transformers, curr_func_name)
                    except AttributeError:
                        raise ValueError(
                            f"Unable to find transformer '{curr_func_name}'")
                    # end try to find in qiimp transformers
                # end try to find in input (study-specific) transformers

                # apply the function named curr_func_name to the column of the
                # metadata_df named curr_source_field to fill curr_target_field
                update_metadata_df_field(metadata_df, curr_target_field,
                                         curr_func, curr_source_field,
                                         overwrite_non_nans=overwrite_non_nans)
            # next stage transformer
        # end if there are stage transformers for this stage
    # end if there are any metadata transformers

    return metadata_df


def _generate_metadata_for_host_types(
        metadata_df, config):
    # gather global settings
    settings_dict = {DEFAULT_KEY: config.get(DEFAULT_KEY),
                     LEAVE_REQUIREDS_BLANK_KEY:
                         config.get(LEAVE_REQUIREDS_BLANK_KEY),
                     OVERWRITE_NON_NANS_KEY:
                         config.get(OVERWRITE_NON_NANS_KEY)}

    validation_msgs = []
    host_type_dfs = []
    host_type_shorthands = pandas.unique(metadata_df[HOSTTYPE_SHORTHAND_KEY])
    for curr_host_type_shorthand in host_type_shorthands:
        concatted_dfs, curr_validation_msgs = _generate_metadata_for_host_type(
                metadata_df, curr_host_type_shorthand, settings_dict, config)

        host_type_dfs.append(concatted_dfs)
        validation_msgs.extend(curr_validation_msgs)
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
    return output_df, validation_msgs


def _generate_metadata_for_host_type(
        metadata_df, curr_host_type, settings_dict, config):

    host_type_mask = \
        metadata_df[HOSTTYPE_SHORTHAND_KEY] == curr_host_type
    host_type_df = metadata_df.loc[host_type_mask, :].copy()

    validation_msgs = []
    known_host_shorthands = config[HOST_TYPE_SPECIFIC_METADATA_KEY].keys()
    if curr_host_type not in known_host_shorthands:
        update_metadata_df_field(
            host_type_df, QC_NOTE_KEY, "invalid host_type")
        # host_type_df[QC_NOTE_KEY] = "invalid host_type"
        concatted_df = host_type_df
    else:
        # # gather host-type-specific settings and apply them to the metadata
        host_type_dict = \
            config[HOST_TYPE_SPECIFIC_METADATA_KEY][curr_host_type]
        curr_settings_dict = deepcopy_dict(settings_dict)
        curr_settings_dict[DEFAULT_KEY] = host_type_dict.get(
            DEFAULT_KEY, curr_settings_dict[DEFAULT_KEY])

        # for each sample type in metadata for this host type
        dfs_to_concat = []
        found_host_sample_types = \
            pandas.unique(host_type_df[SAMPLETYPE_SHORTHAND_KEY])
        for curr_sample_type in found_host_sample_types:
            curr_sample_type_df, curr_validation_msgs = \
                _generate_metadata_for_sample_type_in_host(
                    host_type_df, curr_sample_type, curr_settings_dict,
                    host_type_dict, config)

            dfs_to_concat.append(curr_sample_type_df)
            validation_msgs.extend(curr_validation_msgs)
        # next sample type in metadata for this host type

        concatted_df = pandas.concat(dfs_to_concat, ignore_index=True)
    # endif host_type is valid

    return concatted_df, validation_msgs


def _update_metadata_from_config_dict(
        metadata_df, config_section_dict, dict_is_metadata_fields=False,
        overwrite_non_nans=False):

    if not dict_is_metadata_fields:
        metadata_fields_dict = config_section_dict.get(METADATA_FIELDS_KEY)
    else:
        metadata_fields_dict = config_section_dict

    if metadata_fields_dict:
        metadata_df = _update_metadata_from_dict(
            metadata_df, metadata_fields_dict,
            overwrite_non_nans=overwrite_non_nans )
    return metadata_df


def _update_metadata_from_dict(
        metadata_df, metadata_fields_dict, overwrite_non_nans):
    output_df = metadata_df.copy()
    for curr_field_name, curr_field_vals_dict in metadata_fields_dict.items():
        if DEFAULT_KEY in curr_field_vals_dict:
            curr_default_val = curr_field_vals_dict[DEFAULT_KEY]
            update_metadata_df_field(
                output_df, curr_field_name, curr_default_val,
                overwrite_non_nans=overwrite_non_nans)
            # output_df[curr_field_name] = curr_default_val
        elif REQUIRED_KEY in curr_field_vals_dict:
            curr_required_val = curr_field_vals_dict[REQUIRED_KEY]
            if curr_required_val and curr_field_name not in output_df:
                update_metadata_df_field(
                    output_df, curr_field_name, REQ_PLACEHOLDER,
                    overwrite_non_nans=overwrite_non_nans)
            # note that if the field is (a) required, (b) does not have a
            # default value, and (c) IS already in the metadata, it will
            # be left alone, with no changes made to it!
    return output_df


def _generate_metadata_for_sample_type_in_host(
        host_type_df, sample_type, curr_settings_dict,
        host_type_dict, config):

    host_sample_types_dict = \
        host_type_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY]
    wip_metadata_dict = deepcopy_dict(
        host_type_dict.get(METADATA_FIELDS_KEY, {}))

    # get df of records for this sample type in this host type
    sample_type_mask = \
        host_type_df[SAMPLETYPE_SHORTHAND_KEY] == sample_type
    sample_type_df = host_type_df.loc[sample_type_mask, :].copy()

    validation_msgs = []
    known_sample_types = host_sample_types_dict.keys()
    if sample_type not in known_sample_types:
        update_metadata_df_field(
            sample_type_df, QC_NOTE_KEY, "invalid sample_type")
        # sample_type_df[QC_NOTE_KEY] = "invalid sample_type"
    else:
        sample_type_metadata_dict = \
            _construct_sample_type_metadata_dict(
                sample_type, host_sample_types_dict, wip_metadata_dict)

        sample_type_df = _update_metadata_from_config_dict(
            sample_type_df, wip_metadata_dict, True,
            curr_settings_dict[OVERWRITE_NON_NANS_KEY])

        # for fields that are required but not yet filled, either leave blank
        # or fill with NA (later replaced with default) based on config setting
        leave_reqs_blank = curr_settings_dict[LEAVE_REQUIREDS_BLANK_KEY]
        reqs_val = LEAVE_BLANK_VAL if leave_reqs_blank else np.nan
        sample_type_df.replace(
            to_replace=REQ_PLACEHOLDER, value=reqs_val, inplace=True)

        # fill NAs with default value if any is set
        sample_type_df = _fill_na_if_default(
            sample_type_df, sample_type_metadata_dict, curr_settings_dict)

        validation_msgs = validate_metadata_df(
            sample_type_df, sample_type_metadata_dict)

    return sample_type_df, validation_msgs


def _construct_sample_type_metadata_dict(
        sample_type, host_sample_types_dict, host_metadata_dict):
    sample_type_for_metadata = sample_type

    # get dict associated with the naive sample type
    sample_type_specific_dict = \
        host_sample_types_dict[sample_type]

    # if naive sample type contains an alias
    sample_type_alias = sample_type_specific_dict.get(ALIAS_KEY)
    if sample_type_alias:
        # change the sample type to the alias sample type
        # and use the alias's sample type dict
        sample_type_for_metadata = sample_type_alias
        sample_type_specific_dict = \
            host_sample_types_dict[sample_type_alias]
        if METADATA_FIELDS_KEY not in sample_type_specific_dict:
            raise ValueError(f"May not chain aliases "
                             f"('{sample_type}' to '{sample_type_alias}')")
    # endif sample type is an alias

    # if the sample type has a base type
    sample_type_base = sample_type_specific_dict.get(BASE_TYPE_KEY)
    if sample_type_base:
        # get the base's sample type dict and add this sample type's
        # info on top of it
        base_sample_dict = host_sample_types_dict[sample_type_base]
        if list(base_sample_dict.keys()) != [METADATA_FIELDS_KEY]:
            raise ValueError(f"Base sample type '{sample_type_base}' "
                             f"must only have metadata fields")
        sample_type_specific_dict_metadata = update_wip_metadata_dict(
            sample_type_specific_dict.get(METADATA_FIELDS_KEY, {}),
            base_sample_dict[METADATA_FIELDS_KEY])
        sample_type_specific_dict[METADATA_FIELDS_KEY] = \
            sample_type_specific_dict_metadata
    # endif sample type has a base type

    # add the sample-type-specific info generated above on top of the host info
    sample_type_metadata_dict = update_wip_metadata_dict(
        host_metadata_dict,
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


# fill NAs with default value if any is set
def _fill_na_if_default(metadata_df, specific_dict, settings_dict):
    default_val = specific_dict.get(DEFAULT_KEY, settings_dict[DEFAULT_KEY])
    if default_val:
        # TODO: this is setting a value in the output; should it be
        #  centralized so it is easy to find?
        metadata_df = \
            metadata_df.fillna(default_val)
#             metadata_df.astype("string").fillna(default_val)

    return metadata_df


def _output_to_df(a_df, out_dir, out_base, internal_col_names, sep="\t",
                  remove_internals_and_fails=False,
                  suppress_empty_fails=False):

    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    extension = get_extension(sep)

    if remove_internals_and_fails:
        # output a file of any qc failures; include no contents
        # (not even header line) if there are no failures--bc it is easy to
        # eyeball "zero bytes"
        qc_fails_df = get_qc_failures(a_df)
        qc_fails_fp = os.path.join(
            out_dir, f"{timestamp_str}_{out_base}_fails.csv")
        if qc_fails_df.empty:
            if not suppress_empty_fails:
                Path(qc_fails_fp).touch()
            # else, just do nothing
        else:
            qc_fails_df.to_csv(qc_fails_fp, sep=",", index=False)

        # remove the qc fails and the internal columns from the metadata
        # TODO: I'd like to avoid repeating this mask here + in get_qc_failures
        fails_qc_mask = a_df[QC_NOTE_KEY] != ""
        a_df = a_df.loc[~fails_qc_mask, :].copy()
        a_df = a_df.drop(columns=internal_col_names)

    out_fp = os.path.join(out_dir, f"{timestamp_str}_{out_base}.{extension}")
    a_df.to_csv(out_fp, sep=sep, index=False)


def _reorder_df(a_df, internal_col_names):
    # sort columns alphabetically
    a_df = a_df.reindex(sorted(a_df.columns), axis=1)

    # move the internal columns to the end of the list of cols to output
    col_names = list(a_df)
    for curr_internal_col_name in internal_col_names:
        col_names.pop(col_names.index(curr_internal_col_name))
        col_names.append(curr_internal_col_name)

    # move sample name to the first column
    col_names.insert(0, col_names.pop(col_names.index(SAMPLE_NAME_KEY)))
    output_df = a_df.loc[:, col_names].copy()
    return output_df


# if __name__ == "__main__":
#     raw_df = pandas.read_csv("~/Desktop/temp_df.csv")
#     study_dict = extract_config_dict("/Users/abirmingham/Desktop/trpca/trpca_study.yml")
#     x = get_reserved_cols(raw_df, study_dict)
#
#     # load the existing metadata file
#     raw_metadata_fp = "/Users/abirmingham/Desktop/temp_15612.csv"
#     raw_metadata_df = pandas.read_csv(
#         raw_metadata_fp, sep="\t", dtype=str)
#
#     # load the study-specific config file
#     study_specific_config_fp = "/Users/abirmingham/Desktop/trpca/trpca_study.yml"
#     with open(study_specific_config_fp, 'r') as file:
#         study_dict = yaml.load(file, Loader=yaml.FullLoader)
#
#     # extend the metadata
#     out_dir = "/Users/abirmingham/Desktop/"
#     out_name_base = "temp_merged_df_extended"
#     write_extended_metadata(
#         raw_metadata_fp, study_specific_config_fp, out_dir, out_name_base)
