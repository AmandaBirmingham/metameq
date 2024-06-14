import cerberus
import copy
import os

import numpy as np
import pandas
from pathlib import Path
from datetime import datetime
from dateutil import parser
from qiimp.src.util import extract_config_dict, extract_stds_config, \
    deepcopy_dict, HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY, \
    QC_NOTE_KEY, METADATA_FIELDS_KEY, HOST_TYPE_SPECIFIC_METADATA_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, SAMPLE_TYPE_KEY, QIITA_SAMPLE_TYPE, \
    DEFAULT_KEY, REQUIRED_KEY, ALIAS_KEY, BASE_TYPE_KEY, \
    LEAVE_BLANK_VAL, SAMPLE_NAME_KEY, COLLECTION_TIMESTAMP_KEY, \
    HOST_SUBJECT_ID_KEY, ALLOWED_KEY, TYPE_KEY, LEAVE_REQUIREDS_BLANK_KEY
from qiimp.src.metadata_configurator import combine_stds_and_study_config, \
    flatten_nested_stds_dict, update_wip_metadata_dict
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


# TODO: add check that these are in the input xlsx file
REQUIRED_RAW_METADATA_FIELDS = [SAMPLE_NAME_KEY,
                                HOST_SUBJECT_ID_KEY,
                                HOSTTYPE_SHORTHAND_KEY,
                                SAMPLETYPE_SHORTHAND_KEY,
                                COLLECTION_TIMESTAMP_KEY]

# columns added to the metadata that are not actually part of it
INTERNAL_COL_KEYS = [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY,
                     QC_NOTE_KEY]

REQ_PLACEHOLDER = "_QIIMP2_REQUIRED"


pandas.set_option("future.no_silent_downcasting", True)


def merge_sample_and_subject_metadata(
        sample_metadata_df, subject_metadata_df,
        merge_col_sample, merge_col_subject=None):

    if merge_col_subject is None:
        merge_col_subject = merge_col_sample

    error_msgs = []

    # check for nans in the merge columns
    error_msgs.extend(_check_for_nans(
        sample_metadata_df, "sample", merge_col_sample))
    error_msgs.extend(_check_for_nans(
        subject_metadata_df, "subject", merge_col_subject))

    # check for duplicates in subject merge column
    # (duplicates in the sample merge column are expected, as we expect
    # there to possibly multiple samples for the same subject)
    error_msgs.extend(_check_for_duplicate_field_vals(
        subject_metadata_df, "subject", merge_col_subject))

    if error_msgs:
        joined_msgs = "\n".join(error_msgs)
        raise ValueError(f"Errors in metadata to merge:\n{joined_msgs}")

    # merge the sample and host dfs on the selected columns
    merge_df = pandas.merge(sample_metadata_df, subject_metadata_df,
                            how="left", validate="many_to_one",
                            left_on=merge_col_sample,
                            right_on=merge_col_subject)

    return merge_df


def _check_for_duplicate_field_vals(metadata_df, df_name, col_name):
    error_msgs = []
    duplicates_mask = metadata_df.duplicated(subset=col_name)
    if duplicates_mask.any():
        # generate an error message including the duplicate values
        error_msgs.append(
            f"{df_name} metadata has duplicate values in column {col_name}: "
            f"{metadata_df.loc[duplicates_mask, col_name].unique()}")
    return error_msgs


def _check_for_nans(metadata_df, df_name, col_name):
    error_msgs = []
    nans_mask = metadata_df[col_name].isna()
    if nans_mask.any():
        error_msgs.append(
            f"{df_name} metadata has NaNs in column {col_name}")
    return error_msgs


def generate_extended_metadata_file_from_raw_metadata_file(
        raw_metadata_fp, study_specific_config, out_dir, out_base):

    # TODO: add sheet name handling?
    raw_metadata_df = pandas.read_excel(raw_metadata_fp)

    return generate_extended_metadata_file_from_raw_metadata_df(
        raw_metadata_df, study_specific_config, out_dir, out_base)


def generate_extended_metadata_file_from_raw_metadata_df(
        raw_metadata_df, study_specific_config, out_dir, out_base,
        study_specific_transformers_dict=None):

    software_config = extract_config_dict(None)

    if study_specific_config:
        study_specific_config.update(software_config)
        nested_stds_plus_dict = combine_stds_and_study_config(
            study_specific_config)
    else:
        study_specific_config = software_config
        nested_stds_plus_dict = extract_stds_config(None)

    flattened_hosts_dict = flatten_nested_stds_dict(
        nested_stds_plus_dict, None)
    study_specific_config[HOST_TYPE_SPECIFIC_METADATA_KEY] = flattened_hosts_dict

    metadata_df, validation_msgs = _populate_metadata_df(
        raw_metadata_df, study_specific_transformers_dict,
        study_specific_config)

    _output_to_df(metadata_df, out_dir, out_base,
                  INTERNAL_COL_KEYS, remove_internals=True)
    _output_validation_msgs(validation_msgs, out_dir, out_base, sep=",")
    return metadata_df


def _populate_metadata_df(
        raw_metadata_df, transformer_funcs_dict, main_config_dict):
    metadata_df = raw_metadata_df.copy()
    _update_metadata_df_field(metadata_df, QC_NOTE_KEY, LEAVE_BLANK_VAL)

    metadata_df = transform_pre_metadata(
        metadata_df, transformer_funcs_dict, main_config_dict)

    # first, add the metadata for the host types
    metadata_df, validation_msgs = _generate_metadata_for_host_types(
        metadata_df, main_config_dict)

    return metadata_df, validation_msgs


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
                                      overwrite_non_nans=False)

    return pre_metadata_df


def _update_metadata_df_field(
        metadata_df, field_name, field_val_or_func,
        source_fields=None, overwrite_non_nans=True):

    # Note: function doesn't return anything.  Work is done in-place on the
    #  metadata_df passed in.

    if source_fields:
        if overwrite_non_nans or (field_name not in metadata_df.columns):
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
        # endif overwrite_non_nans for function call
    else:
        if overwrite_non_nans or (field_name not in metadata_df.columns):
            metadata_df[field_name] = field_val_or_func
        else:
            metadata_df[field_name] = \
                metadata_df[field_name].fillna(field_val_or_func)
            # metadata_df[field_name].fillna(field_val_or_func, inplace=True)
        # endif overwrite_non_nans for constant value
    # endif using a function/a constant value


def _generate_metadata_for_host_types(
        metadata_df, config):
    # gather global settings
    settings_dict = {DEFAULT_KEY: config.get(DEFAULT_KEY),
                     LEAVE_REQUIREDS_BLANK_KEY:
                         config.get(LEAVE_REQUIREDS_BLANK_KEY)}

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
        _update_metadata_df_field(
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
                overwrite_non_nans=False)
            # output_df[curr_field_name] = curr_default_val
        elif REQUIRED_KEY in curr_field_vals_dict:
            curr_required_val = curr_field_vals_dict[REQUIRED_KEY]
            if curr_required_val and curr_field_name not in output_df:
                _update_metadata_df_field(
                    output_df, curr_field_name, REQ_PLACEHOLDER)
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
        _update_metadata_df_field(
            sample_type_df, QC_NOTE_KEY, "invalid sample_type")
        # sample_type_df[QC_NOTE_KEY] = "invalid sample_type"
    else:
        sample_type_metadata_dict = \
            _construct_sample_type_metadata_dict(
                sample_type, host_sample_types_dict, wip_metadata_dict)

        sample_type_df = _update_metadata_from_config_dict(
            sample_type_df, wip_metadata_dict, True)

        # for fields that are required but not yet filled, either leave blank
        # or fill with NA (later replaced with default) based on config setting
        leave_reqs_blank = curr_settings_dict[LEAVE_REQUIREDS_BLANK_KEY]
        reqs_val = LEAVE_BLANK_VAL if leave_reqs_blank else np.nan
        sample_type_df.replace(
            to_replace=REQ_PLACEHOLDER, value=reqs_val, inplace=True)

        # fill NAs with default value if any is set
        sample_type_df = _fill_na_if_default(
            sample_type_df, sample_type_metadata_dict, curr_settings_dict)

        validation_msgs = _validate_raw_metadata_df(
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
        if not METADATA_FIELDS_KEY in sample_type_specific_dict:
            raise ValueError(f"May not chain aliases "
                             f"('{sample_type}' to '{sample_type}')")
    # endif sample type is an alias

    # if the sample type has a base type
    sample_type_base = sample_type_specific_dict.get(BASE_TYPE_KEY)
    if sample_type_base:
        # get the base's sample type dict and add this sample type's
        # info on top of it
        base_sample_dict = host_sample_types_dict[sample_type_base]
        if base_sample_dict.keys().to_list() != [METADATA_FIELDS_KEY]:
            raise ValueError(f"Base sample type '{sample_type_base}' "
                             f"must only have metadata fields")
        sample_type_specific_dict_metadata = update_wip_metadata_dict(
            sample_type_specific_dict[METADATA_FIELDS_KEY],
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


def _validate_raw_metadata_df(metadata_df, sample_type_metadata_dict):
    config = _make_cerberus_schema(sample_type_metadata_dict)

    # cerberus_to_pandas_types = {
    #     "string": "string",
    #     "integer": "int64",
    #     "float": "float64",
    #     "number": "float64",
    #     "bool": "bool",
    #     "datetime": "datetime64"}
    typed_metadata_df = metadata_df.copy()
    # for curr_field, curr_definition in sample_type_metadata_dict.items():
    #     if curr_field not in typed_metadata_df.columns:
    #         # TODO: decide whether to make this a warning or take out
    #         print(f"Field {curr_field} not in metadata file")
    #         continue
    #     curr_cerberus_type = curr_definition.get(TYPE_KEY)
    #     # TODO: add handling for more complicated anyof cases :-|
    #     if curr_cerberus_type:
    #         curr_pandas_type = cerberus_to_pandas_types[curr_cerberus_type]
    #         typed_metadata_df[curr_field] = \
    #             typed_metadata_df[curr_field].astype(curr_pandas_type)
    # # next field in config

    # use python Cerberus validator on all the fields that already exist in the
    # metadata file?
    raw_metadata_dict = typed_metadata_df.to_dict(orient="records")
    v = QiimpValidator()
    v.allow_unknown = True
    # is_valid = v.validate(raw_metadata_dict, config)
    # if not is_valid:
    validation_msgs = []
    for curr_idx, curr_row in enumerate(raw_metadata_dict):
        if not v.validate(curr_row, config):
            curr_sample_name = curr_row[SAMPLE_NAME_KEY]
            for curr_field_name, curr_err_msg in v.errors.items():
                validation_msgs.append({
                    SAMPLE_NAME_KEY: curr_sample_name,
                    "field_name": curr_field_name,
                    "error_message": curr_err_msg})
            # next error for curr row
        # endif row is not valid
    # next row

    return validation_msgs

def _make_cerberus_schema(sample_type_metadata_dict):
    unrecognized_keys = ['is_phi', 'field_desc', 'units',
                         'min_exclusive', 'unique']
    # traverse the host_fields_config dict and remove any keys that are not
    # recognized by cerberus
    cerberus_config = copy.deepcopy(sample_type_metadata_dict)
    cerberus_config = _remove_keys_from_dict(
        cerberus_config, unrecognized_keys)

    return cerberus_config


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
    extension = _get_extension(sep)

    # sort columns alphabetically
    a_df = a_df.reindex(sorted(a_df.columns), axis=1)

    if remove_internals:
        # output a file of any qc failures; include no contents
        # (not even header line) if there are no failures--bc it is easy to
        # eyeball "zero bytes"
        fails_qc_mask = a_df[QC_NOTE_KEY] != ""
        qc_fails_df = a_df.loc[fails_qc_mask, INTERNAL_COL_KEYS].copy()
        qc_fails_fp = os.path.join(
            out_dir, f"{timestamp_str}_{out_base}_fails.{extension}")
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

    out_fp = os.path.join(out_dir, f"{timestamp_str}_{out_base}.{extension}")
    output_df.to_csv(out_fp, sep=sep, index=False)


def _output_validation_msgs(validation_msgs, out_dir, out_base, sep="\t"):
    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    extension = _get_extension(sep)
    out_fp = os.path.join(
        out_dir, f"{timestamp_str}_{out_base}_validation_errors.{extension}")
    msgs_df = pandas.DataFrame(validation_msgs)
    msgs_df.to_csv(out_fp, sep=sep, index=False)


def _get_extension(sep):
    return "csv" if sep == "," else "txt"



class QiimpValidator(cerberus.Validator):
    def _check_with_date_not_in_future(self, field, value):
        # convert the field string to a date
        try:
            putative_date = parser.parse(value, fuzzy=True, dayfirst=False)
        except:
            self._error(field, "Must be a valid date")
            return

        if putative_date > datetime.now():
            self._error(field, "Date cannot be in the future")