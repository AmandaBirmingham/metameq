import numpy as np
import os
import pandas
from pathlib import Path
from datetime import datetime
from qiimp.src.util import extract_config_dict, extract_stds_config, \
    deepcopy_dict, validate_required_columns_exist, get_extension, \
    load_df_with_best_fit_encoding, update_metadata_df_field, \
    HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY, \
    QC_NOTE_KEY, METADATA_FIELDS_KEY, HOST_TYPE_SPECIFIC_METADATA_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, SAMPLE_TYPE_KEY, QIITA_SAMPLE_TYPE, \
    DEFAULT_KEY, REQUIRED_KEY, ALIAS_KEY, BASE_TYPE_KEY, \
    LEAVE_BLANK_VAL, SAMPLE_NAME_KEY, \
    ALLOWED_KEY, TYPE_KEY, LEAVE_REQUIREDS_BLANK_KEY, \
    PRE_TRANSFORMERS_KEY, POST_TRANSFORMERS_KEY
from qiimp.src.metadata_configurator import combine_stds_and_study_config, \
    flatten_nested_stds_dict, update_wip_metadata_dict
from qiimp.src.metadata_validator import validate_metadata_df, \
    output_validation_msgs
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


REQUIRED_RAW_METADATA_FIELDS = [SAMPLE_NAME_KEY,
                                HOSTTYPE_SHORTHAND_KEY,
                                SAMPLETYPE_SHORTHAND_KEY]

# columns added to the metadata that are not actually part of it
INTERNAL_COL_KEYS = [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY,
                     QC_NOTE_KEY]

REQ_PLACEHOLDER = "_QIIMP2_REQUIRED"


pandas.set_option("future.no_silent_downcasting", True)


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

    if study_specific_config_fp:
        study_specific_config_dict = \
            extract_config_dict(study_specific_config_fp)
    else:
        study_specific_config_dict = None

    return write_extended_metadata_from_df(
        raw_metadata_df, study_specific_config_dict,
        out_dir, out_name_base, sep=sep,
        suppress_empty_fails=suppress_empty_fails)


def write_extended_metadata_from_df(
        raw_metadata_df, study_specific_config_dict, out_dir, out_name_base,
        study_specific_transformers_dict=None, sep="\t",
        suppress_empty_fails=False, internal_col_names=None):

    if internal_col_names is None:
        internal_col_names = INTERNAL_COL_KEYS

    validate_required_columns_exist(
        raw_metadata_df, REQUIRED_RAW_METADATA_FIELDS,
        "metadata missing required columns")

    software_config = extract_config_dict(None)

    if study_specific_config_dict:
        study_specific_config_dict.update(software_config)
        nested_stds_plus_dict = combine_stds_and_study_config(
            study_specific_config_dict)
    else:
        study_specific_config_dict = software_config
        nested_stds_plus_dict = extract_stds_config(None)

    flattened_hosts_dict = flatten_nested_stds_dict(
        nested_stds_plus_dict, None)
    study_specific_config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY] = \
        flattened_hosts_dict

    metadata_df, validation_msgs = _populate_metadata_df(
        raw_metadata_df, study_specific_transformers_dict,
        study_specific_config_dict)

    _output_to_df(metadata_df, out_dir, out_name_base,
                  internal_col_names, remove_internals=True, sep=sep,
                  suppress_empty_fails=suppress_empty_fails)
    output_validation_msgs(validation_msgs, out_dir, out_name_base, sep=",",
                           suppress_empty_fails=suppress_empty_fails)
    return metadata_df


def _populate_metadata_df(
        raw_metadata_df, transformer_funcs_dict, main_config_dict):
    metadata_df = raw_metadata_df.copy()
    update_metadata_df_field(metadata_df, QC_NOTE_KEY, LEAVE_BLANK_VAL)

    metadata_df = transformers.transform_metadata(
        metadata_df, transformer_funcs_dict, main_config_dict,
        PRE_TRANSFORMERS_KEY)

    # first, add the metadata for the host types
    metadata_df, validation_msgs = _generate_metadata_for_host_types(
        metadata_df, main_config_dict)

    metadata_df = transformers.transform_metadata(
        metadata_df, transformer_funcs_dict, main_config_dict,
        POST_TRANSFORMERS_KEY)

    return metadata_df, validation_msgs


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
            update_metadata_df_field(
                output_df, curr_field_name, curr_default_val,
                overwrite_non_nans=False)
            # output_df[curr_field_name] = curr_default_val
        elif REQUIRED_KEY in curr_field_vals_dict:
            curr_required_val = curr_field_vals_dict[REQUIRED_KEY]
            if curr_required_val and curr_field_name not in output_df:
                update_metadata_df_field(
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
        update_metadata_df_field(
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
        if list(base_sample_dict.keys()) != [METADATA_FIELDS_KEY]:
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


def _output_to_df(a_df, out_dir, out_base, internal_col_names,
                  sep="\t", remove_internals=False,
                  suppress_empty_fails=False):

    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    extension = get_extension(sep)

    # sort columns alphabetically
    a_df = a_df.reindex(sorted(a_df.columns), axis=1)

    if remove_internals:
        # output a file of any qc failures; include no contents
        # (not even header line) if there are no failures--bc it is easy to
        # eyeball "zero bytes"
        fails_qc_mask = a_df[QC_NOTE_KEY] != ""
        qc_fails_df = a_df.loc[fails_qc_mask, internal_col_names].copy()
        qc_fails_fp = os.path.join(
            out_dir, f"{timestamp_str}_{out_base}_fails.csv")
        if qc_fails_df.empty:
            if not suppress_empty_fails:
                Path(qc_fails_fp).touch()
            # else, just do nothing
        else:
            qc_fails_df.to_csv(qc_fails_fp, sep=",", index=False)

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


if __name__ == '__main__':
    write_extended_metadata(
        "/Users/abirmingham/Desktop/extended_abtx_metadata_w_faked_host_height_not_applicable.csv",
        "/Users/abirmingham/Work/Repositories/custom_abtx_metadata_generator/config.yml",
        "/Users/abirmingham/Desktop",
        "test_qiimp2_cli2")