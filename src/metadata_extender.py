import cerberus
import copy
import os

import numpy as np
import pandas
import yaml
from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta

# Load in the config file
# Load in the user-supplied metadata file into a pandas dataframe
# Ensure everything in the metadata file has the following fields:
#   - sample_name
#   - subject_id
#   - host_type_id
#   - sample_type_id
#   - collection_datetime
# For each host type:
#  Filter the metadata file to only include samples from that host type
#  Find the host type in the config dictionary and copy it
#  For each sample type:
#    Filter the host metadata file to only include samples from that sample type
#    Find the sample type in the host config dictionary and copy it
#    Set the metadata for each sample to be the metadata for the host-sample-type dict
# For each subject:
#  Filter the metadata file to only include samples from that subject
#  Find the subject in the config dictionary and copy it
#  Set the metadata for each subject to be the metadata for the subject dict
# For each sample:
#  Calculate the per-sample metadata using the metadata in the dataframe

# internal code keys
PLATE_SAMPLE_ID_KEY = "plate_sample_id"
PLATE_ROW_ID_KEY = "plate_row_id"
PLATE_COL_ID_KEY = "plate_col_id"
PLATE_ID_KEY = "plate_id"
PLATING_NOTES_KEY = "plating_notes"
PLATING_DATE_KEY = "plating_date"
SUBJECT_SHORTHAND_KEY = "subject_shorthand"
HOSTTYPE_SHORTHAND_KEY = "hosttype_shorthand"
SAMPLETYPE_SHORTHAND_KEY = "sampletype_shorthand"
#GLOBAL_DEFAULT_KEY = "global_default"
QC_NOTE_KEY = "qc_note"

# config keys
DESIRED_PLATES_KEY = "desired_plates"
METADATA_FIELDS_KEY = "metadata_fields"
HOST_SPECIFIC_METADATA_KEY = "host_specific_metadata"
HOST_TYPE_SPECIFIC_METADATA_KEY = "host_type_specific_metadata"
#HOST_TYPE_KEY = "host_type"
SAMPLE_TYPE_KEY = "sample_type"
SAMPLE_TYPE_SPECIFIC_METADATA_KEY = "sample_type_specific_metadata"
ALIAS_KEY = "alias"
#DEFAULT_KEY = "default_value"
DEFAULT_KEY = "default"
REQUIRED_KEY = "required"
DOB_KEY = "date_of_birth"
STUDY_START_DATE_KEY = "study_start_date"
TIMESPANS_KEY = "timespans"
FIRST_DATE_KEY = "first_date"
LAST_DATE_KEY = "last_date"
LOCATION_BREAK_KEY = "location_break"
BEFORE_LOCATION_KEY = "before_location"
BEFORE_LOCATION_KEY = "before_location"
AFTER_LOCATION_KEY = "after_location"
AFTER_START_DATE_KEY = "after_start_date"
LOCATIONS_KEY = "locations"
LOCATION_KEY = "location"
SAMPLE_NAME_KEY = "sample_name"
ASSUME_DATES_PRESENT_KEY = "assume_dates_present"

# metadata keys
COLLECTION_TIMESTAMP_KEY = "collection_timestamp"
IS_COLLECTION_TIMESTAMP_VALID_KEY = "is_collection_timestamp_valid"
ORDINAL_TIMESTAMP_KEY = "ordinal_timestamp"
ORIGINAL_COLLECTION_TIMESTAMP_KEY = "original_collection_timestamp"
HOST_SUBJECT_ID_KEY = "host_subject_id"
DESCRIPTION_KEY = "description"
HOST_AGE_KEY = "host_age"
M03_AGE_YEARS_KEY = "m03_age_years"
DAYS_SINCE_FIRST_DAY_KEY = "days_since_first_day"
NOTES_KEY = "notes"

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


def _read_package_config(
        package_config_dict, prev_level_full_host_dict, flattened_hosts_dict):

    if prev_level_full_host_dict is None:
        prev_level_full_host_dict = {}

    if flattened_hosts_dict is None:
        flattened_hosts_dict = {}

    # look for the HOST_TYPE_SPECIFIC_METADATA_KEY in package_config_dict
    host_type_specific_metadata_dict = \
        package_config_dict.get(HOST_TYPE_SPECIFIC_METADATA_KEY, {})
    for curr_host_type, curr_host_type_dict in host_type_specific_metadata_dict.items():
        curr_host_full_dict = _deepcopy_dict(prev_level_full_host_dict)
        curr_host_full_metadata_dict = _deepcopy_dict(curr_host_full_dict.get(METADATA_FIELDS_KEY, {}))
        # guess it was bound to happen eventually: the "vertebrate" host class
        # has sample-type specific metadata, but no metadata fields of its own ...
        # TODO: is this going to encourage bad yaml design where ppl forget
        #  the metadata_fields key when it really SHOULD be there?
        if METADATA_FIELDS_KEY in curr_host_type_dict:
            curr_host_metadata_fields = curr_host_type_dict[METADATA_FIELDS_KEY]
            curr_host_full_metadata_dict = _update_package_config_dict(curr_host_metadata_fields, curr_host_full_metadata_dict)
            curr_host_full_dict[METADATA_FIELDS_KEY] = curr_host_full_metadata_dict

        curr_host_sample_types_dict = _deepcopy_dict(curr_host_full_dict.get(SAMPLE_TYPE_SPECIFIC_METADATA_KEY, {}))
        sample_type_specific_metadata_dict = \
            curr_host_type_dict.get(SAMPLE_TYPE_SPECIFIC_METADATA_KEY, {})
        for curr_sample_type, curr_sample_type_dict in sample_type_specific_metadata_dict.items():
            if curr_sample_type in curr_host_sample_types_dict:
                curr_sample_type_copy = _deepcopy_dict(curr_host_sample_types_dict[curr_sample_type])
                curr_sample_type_full_metadata_dict = _deepcopy_dict(curr_sample_type_copy.get(METADATA_FIELDS_KEY, {}))
            else:
                curr_sample_type_full_metadata_dict = {}

            # TODO: not sure I should do this; it gets redefined in all the
            #  legitimate paths, and if it *isn't* redefined, that means
            #  something is wrong, so maybe it should be handled better ...
            curr_sample_type_full_dict = {}

            has_alias = ALIAS_KEY in curr_sample_type_dict
            if has_alias:
                curr_sample_type_alias = curr_sample_type_dict[ALIAS_KEY]
                curr_sample_type_full_dict = {curr_sample_type: {ALIAS_KEY: curr_sample_type_alias}}

            has_metadata = METADATA_FIELDS_KEY in curr_sample_type_dict
            if has_alias and has_metadata:
                raise ValueError("Cannot have both an alias and metadata "
                                 "fields in the same sample type dict")

            if has_metadata:
                curr_sample_type_metadata_fields = curr_sample_type_dict[METADATA_FIELDS_KEY]
                curr_sample_type_full_metadata_dict = _update_package_config_dict(curr_sample_type_metadata_fields, curr_sample_type_full_metadata_dict)
                curr_sample_type_full_dict = {curr_sample_type: {METADATA_FIELDS_KEY: curr_sample_type_full_metadata_dict}}

            if not SAMPLE_TYPE_SPECIFIC_METADATA_KEY in curr_host_full_dict:
                curr_host_full_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY] = {}
            curr_host_full_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY].update(curr_sample_type_full_dict)

        flattened_hosts_dict[curr_host_type] = curr_host_full_dict

        # recurse into the next level of the config--depth first search
        flattened_hosts_dict = _read_package_config(
            curr_host_type_dict, curr_host_full_dict, flattened_hosts_dict)
    # next host type

    return flattened_hosts_dict


def _deepcopy_dict(input_dict):
    output_dict = {}
    for curr_key, curr_val in input_dict.items():
        if isinstance(curr_val, dict):
            output_dict[curr_key] = _deepcopy_dict(curr_val)
        else:
            output_dict[curr_key] = copy.deepcopy(curr_val)
    return output_dict


def _update_package_config_dict(curr_type_metadata_fields, curr_full_dict):
    ALLOWED_KEY = "allowed"
    ANYOF_KEY = "anyof"
    TYPE_KEY = "type"

    for curr_metadata_field, curr_metadata_field_dict in curr_type_metadata_fields.items():
        if curr_metadata_field not in curr_full_dict:
            curr_full_dict[curr_metadata_field] = {}

        if ALLOWED_KEY in curr_metadata_field_dict:
            # remove the ANYOF_KEY from curr_full_dict[curr_metadata_field] if it exists there
            if ANYOF_KEY in curr_full_dict[curr_metadata_field]:
                del curr_full_dict[curr_metadata_field][ANYOF_KEY]

        if ANYOF_KEY in curr_metadata_field_dict:
            # remove the ALLOWED_KEY from curr_full_dict[curr_metadata_field] if it exists there
            if ALLOWED_KEY in curr_full_dict[curr_metadata_field]:
                del curr_full_dict[curr_metadata_field][ALLOWED_KEY]

            # remove the TYPE_KEY from curr_full_dict[curr_metadata_field] if it exists there
            if TYPE_KEY in curr_full_dict[curr_metadata_field]:
                del curr_full_dict[curr_metadata_field][TYPE_KEY]

        # TODO: Q: is it possible to have a list of allowed with a default
        #  at high level, then lower down have a list of allowed WITHOUT
        #  a default?  If so, how do we handle that?

        # update curr_full_dict[curr_metadata_field] with curr_metadata_field_dict
        curr_full_dict[curr_metadata_field].update(curr_metadata_field_dict)
    # next metadata field

    return curr_full_dict


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


def _validate_raw_metadata_df(
        raw_metadata_df, host_type_dict, sample_type_specific_dict):

    config = _make_cerberus_schema(host_type_dict, sample_type_specific_dict)

    # use python Cerberus validator on all the fields that already exist in the
    # metadata file?
    raw_metadata_dict = raw_metadata_df.to_dict(orient="records")
    v = cerberus.Validator()
    #is_valid = v.validate(raw_metadata_dict, config)
    #if not is_valid:
    for curr_idx, curr_row in enumerate(raw_metadata_dict):
        if not v.validate(curr_row, config):
            print(f"Row {curr_idx} failed validation:")
            print(v.errors)


def _populate_metadata_df(raw_metadata_df, main_config_dict):
    metadata_df = raw_metadata_df.copy()
    metadata_df[QC_NOTE_KEY] = LEAVE_BLANK_VAL

    # TODO: this assumes that none of the columns we're adding already exist;
    #  that may be an unsafe assumption.  We should check for that and handle
    #  it if it's the case.

    metadata_transformers = main_config_dict.get("metadata_transformers", None)
    if metadata_transformers:
        pre_transformers = metadata_transformers.get("pre_population", None)
        for curr_target_field, curr_transformer_dict in pre_transformers.items():
            curr_source_field = curr_transformer_dict["source"]
            curr_func_name = curr_transformer_dict["function"]
            curr_func = globals()[curr_func_name]

            # apply the function named curr_func_name to the column of the
            # metadata_df named curr_source_field to fill curr_target_field
            metadata_df[curr_target_field] = \
                metadata_df[curr_source_field].apply(curr_func)

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
    output_df.replace(LEAVE_BLANK_VAL, "", inplace=True)
    return output_df


def _generate_metadata_for_host_type(
        metadata_df, curr_host_type, settings_dict, config):

    host_type_mask = \
        metadata_df[HOSTTYPE_SHORTHAND_KEY] == curr_host_type
    host_type_df = metadata_df.loc[host_type_mask, :].copy()

    # TODO: this will have to change
    known_host_shorthands = config[HOST_TYPE_SPECIFIC_METADATA_KEY].keys()
    if curr_host_type not in known_host_shorthands:
        host_type_df[QC_NOTE_KEY] = "invalid host_type"
        concatted_df = host_type_df
    else:
        # gather host-type-specific settings and apply them to the metadata
        # TODO: this will have to change
        host_type_dict = \
            config[HOST_TYPE_SPECIFIC_METADATA_KEY][curr_host_type]
        host_type_df = _update_metadata_from_config_dict(
            host_type_df, host_type_dict)

        # for each sample type in metadata for this host type
        dfs_to_concat = []
        found_host_sample_types = \
            pandas.unique(host_type_df[SAMPLETYPE_SHORTHAND_KEY])
        for curr_sample_type in found_host_sample_types:
            curr_sample_type_df = \
                _generate_metadata_for_sample_type_in_host(
                    host_type_df, curr_sample_type, settings_dict,
                    host_type_dict, config)

            dfs_to_concat.append(curr_sample_type_df)
        # next sample type in metadata for this host type

        concatted_df = pandas.concat(dfs_to_concat, ignore_index=True)
    # endif host_type is valid

    return concatted_df


def _update_metadata_from_config_dict(metadata_df, config_section_dict):
    metadata_fields_dict = config_section_dict.get(METADATA_FIELDS_KEY)
    if metadata_fields_dict:
        metadata_df = _update_metadata_from_dict(
            metadata_df, metadata_fields_dict)
    return metadata_df


def _update_metadata_from_dict(metadata_df, metadata_fields_dict):
    output_df = metadata_df.copy()
    for curr_field_name, curr_field_vals_dict in metadata_fields_dict.items():
        if DEFAULT_KEY in curr_field_vals_dict:
            curr_default_val = curr_field_vals_dict[DEFAULT_KEY]
            output_df[curr_field_name] = curr_default_val
        elif REQUIRED_KEY in curr_field_vals_dict:
            curr_required_val = curr_field_vals_dict[REQUIRED_KEY]
            if curr_required_val and curr_field_name not in output_df:
                output_df[curr_field_name] = LEAVE_BLANK_VAL
            # note that if the field is (a) required, (b) does not have a
            # default value, and (c) IS already in the metadata, it will
            # be left alone, with no changes made to it!
    return output_df


def _generate_metadata_for_sample_type_in_host(
        host_type_df, sample_type, curr_settings_dict,
        host_type_dict, config):

    # gather sample type settings and apply their default to the dict
    host_sample_types_dict = \
        host_type_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY]
    curr_settings_dict[DEFAULT_KEY] = host_sample_types_dict.get(
        DEFAULT_KEY, curr_settings_dict[DEFAULT_KEY])

    # get df of records for this sample type in this host type
    sample_type_mask = \
        host_type_df[SAMPLETYPE_SHORTHAND_KEY] == sample_type
    sample_type_df = host_type_df.loc[sample_type_mask, :].copy()

    # TODO: this will have to change
    known_sample_types = host_sample_types_dict.keys()
    if sample_type not in known_sample_types:
        sample_type_df[QC_NOTE_KEY] = "invalid sample_type"
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

        sample_type_df[SAMPLE_TYPE_KEY] = sample_type_for_metadata

        sample_type_df = _update_metadata_from_config_dict(
            sample_type_df, sample_type_specific_dict)

        # This code adds all sorts of metadata fields used by ABTX.
        # based on digging the sample collection date out of the ABTX
        # sample name.  It isn't directly applicable to non-ABTX samples.
#        sample_type_df = _update_metadata_for_indiv_samples_of_type(
#            sample_type_df, sample_type_specific_dict, curr_settings_dict)

        # fill NAs with default value if any is set
        default_val = sample_type_specific_dict.get(
            DEFAULT_KEY, curr_settings_dict[DEFAULT_KEY])
        if default_val:
            sample_type_df.astype("string").fillna(default_val, inplace=True)

        # # make complete host_sample_type config dict
        # host_fields_config = copy.deepcopy(host_type_dict[METADATA_FIELDS_KEY])
        # host_sample_config = copy.deepcopy(sample_type_specific_dict[METADATA_FIELDS_KEY])
        # host_fields_config.update(host_sample_config)
        #_validate_raw_metadata_df(
        #    sample_type_df, host_type_dict, sample_type_specific_dict)

    return sample_type_df


def _update_metadata_for_indiv_samples_of_type(
        sample_type_df, sample_type_specific_dict, curr_settings_dict):
    # add date-dependent metadata if collection timestamp is not pre-specified
    if COLLECTION_TIMESTAMP_KEY not in \
            sample_type_specific_dict[METADATA_FIELDS_KEY]:
        study_start_str = curr_settings_dict[STUDY_START_DATE_KEY]
        sample_type_df = _add_collection_dates(sample_type_df, study_start_str)
        sample_type_df = _add_days_since_start(sample_type_df, study_start_str)

    return sample_type_df


def _add_collection_dates(plates_df, study_start_date_str):
    output_df = plates_df.copy()
    collection_dates = _get_collection_dates(plates_df, study_start_date_str)
    output_df[COLLECTION_TIMESTAMP_KEY] = collection_dates
    output_df[ORDINAL_TIMESTAMP_KEY] = collection_dates.dt.strftime('%Y%m%d')
    none_date_mask = output_df[COLLECTION_TIMESTAMP_KEY].isna()
    output_df[IS_COLLECTION_TIMESTAMP_VALID_KEY] = ~none_date_mask
    output_df.loc[none_date_mask, QC_NOTE_KEY] = \
        "invalid/unparseable date"

    return output_df


def _get_collection_dates(plates_df, start_date_str):
    start_date = parser.parse(start_date_str, dayfirst=False)

    def _get_date_from_sample_id_if_empty(row):
        sample_date = None
        putative_date_str = None

        existing_date_str = row[COLLECTION_TIMESTAMP_KEY]
        if existing_date_str:
            putative_date_str = existing_date_str
        else:
            sample_id = row[SAMPLE_NAME_KEY]
            fname_pieces = sample_id.split(".")
            if len(fname_pieces) >= 3:
                putative_date_str = "/".join(fname_pieces[:3])

        if putative_date_str:
            try:
                sample_date = parser.parse(
                    putative_date_str, fuzzy=True, dayfirst=False)
            except:
                pass

        # sanity check: can't be before study start date
        if sample_date and sample_date < start_date:
            sample_date = None

        return sample_date

    collection_dates = \
        plates_df.apply(_get_date_from_sample_id_if_empty, axis=1)
    return collection_dates


def _add_days_since_start(plates_df, study_start_str):
    output_df = plates_df.copy()
    days_since = _get_duration_since_date(
        plates_df, study_start_str, return_years=False)
    output_df[DAYS_SINCE_FIRST_DAY_KEY] = days_since
    return output_df


def _get_duration_since_date(plates_df, start_date_str, return_years=True):
    start_date = parser.parse(start_date_str, dayfirst=False)

    def _get_diff_in_years(x):
        result = None
        input_date = pandas.to_datetime(x)
        try:
            if return_years:
                a_relativedelta = relativedelta(input_date, start_date)
                result = a_relativedelta.years
            else:
                a_timedelta = input_date - start_date
                result = a_timedelta.days
            result = int(result)
        except:
            pass
        return result

    age_in_years = plates_df[COLLECTION_TIMESTAMP_KEY].apply(_get_diff_in_years)
    return age_in_years


def _generate_metadata_for_known_subjects(
        metadata_df, curr_settings_dict, config):
    # NB: this one goes the other way than the others: it loops over the
    # subjects defined in the config instead of over those found in the
    # metadata file.  This is because there may not BE any subjects in the
    # config, in which case there's nothing to add to the metadata file.

    # TODO: this will have to change
    known_subjects = config[HOST_SUBJECT_ID_KEY].keys()
    subject_dfs = []
    for curr_subject in known_subjects:
        concatted_dfs = _generate_metadata_for_subject(
            metadata_df, curr_subject, curr_settings_dict, config)

        subject_dfs.append(concatted_dfs)
    # next subject

    output_df = pandas.concat(subject_dfs, ignore_index=True)
    output_df.replace(LEAVE_BLANK_VAL, "", inplace=True)
    return output_df


def _generate_metadata_for_subject(
        metadata_df, curr_subject, curr_settings_dict, config):

    # get the df of records for this subject
    subject_mask = \
        metadata_df[HOST_SUBJECT_ID_KEY] == curr_subject
    subject_df = metadata_df.loc[subject_mask, :].copy()

    # NB: no need to check if curr_subject is in the config, because we
    # already did that in the calling function

    # get subject-specific metadata dict
    subject_specific_dict = curr_settings_dict[curr_subject]
    subject_df = _update_metadata_from_config_dict(
        subject_df, subject_specific_dict)

    subject_df = _update_metadata_for_indiv_samples_of_subject(
        subject_df, subject_specific_dict, curr_settings_dict)

    return subject_df


def _update_metadata_for_indiv_samples_of_subject(
        subject_df, subject_specific_dict, config):

    if subject_specific_dict[DOB_KEY]:
        subject_df = \
            _add_host_age(subject_df, subject_specific_dict[DOB_KEY])

    # add time-dependent location metadata if a location-date break exists
    if subject_specific_dict[LOCATION_BREAK_KEY]:
        subject_df = _add_location_info_from_break(
            subject_df, subject_specific_dict[LOCATION_BREAK_KEY], config)

    return subject_df


def _add_host_age(plates_df, dob_str):
    output_df = plates_df.copy()
    age_in_years = _get_duration_since_date(plates_df, dob_str)
    output_df[HOST_AGE_KEY] = age_in_years
    output_df[M03_AGE_YEARS_KEY] = age_in_years
    return output_df


def _add_location_info_from_break(plates_df, location_break_dict, config):
    output_df = plates_df.copy()
    before_location_name = location_break_dict[BEFORE_LOCATION_KEY]
    after_location_name = location_break_dict[AFTER_LOCATION_KEY]
    first_after_date_str = location_break_dict[AFTER_START_DATE_KEY]
    first_after_date = parser.parse(first_after_date_str, dayfirst=False)

    before_location_fields = \
        config[LOCATIONS_KEY][before_location_name][METADATA_FIELDS_KEY]
    before_mask = plates_df[COLLECTION_TIMESTAMP_KEY] < first_after_date
    for a_key, a_val in before_location_fields.items():
        output_df.loc[before_mask, a_key] = a_val

    after_location_fields = \
        config[LOCATIONS_KEY][after_location_name][METADATA_FIELDS_KEY]
    for a_key, a_val in after_location_fields.items():
        output_df.loc[~before_mask, a_key] = a_val
    return output_df


def _output_to_df(a_df, out_dir, out_base, internal_col_names):
    # # output a file of any qc failures
    # fails_qc_mask = a_df[QC_NOTE_KEY] != ""
    # qc_fails_df = a_df.loc[fails_qc_mask, INTERNAL_COL_KEYS].copy()
    # qc_fails_df.to_csv(f"{out_fp_base}_qc_fails.csv", index=False)

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

    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    out_fp = os.path.join(out_dir, f"{timestamp_str}_{out_base}.csv")
    output_df.to_csv(out_fp, index=False)


def extract_config_dict(config_fp):
    if config_fp is None:
        parent_dir = get_parent_dir()
        config_fp = os.path.join(parent_dir, "config.yml")

    # read in config file
    config_dict = extract_yaml_dict(config_fp)
    return config_dict


def get_parent_dir():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.join(curr_dir, os.pardir)
    return parent_dir


def extract_yaml_dict(yaml_fp):
    with open(yaml_fp, "r") as f:
        yaml_dict = yaml.safe_load(f)
    return yaml_dict


def generate_extended_metadata_file_from_raw_metadata_file(
        raw_metadata_fp, config, out_dir, out_base):

    # TODO: add sheet name handling?
    raw_metadata_df = pandas.read_excel(raw_metadata_fp)

    metadata_df = _populate_metadata_df(raw_metadata_df, config)

    _output_to_df(metadata_df, out_dir, out_base, INTERNAL_COL_KEYS)
    return metadata_df


def pass_through(x):
    return x


if __name__ == "__main__":
    # TODO: remove hardcoded arguments
    # raw_metadata_fp = "/Users/abirmingham/Desktop/metadata/test_raw_metadata_short.xlsx"
    raw_metadata_fp = "/Users/abirmingham/Desktop/metadata/test_nph_metadata_short.xlsx"
    included_sheet_names = []
    output_dir = "/Users/abirmingham/Desktop/"
    output_base = "test_extended_metadata"

    main_config_dict = extract_config_dict(None)

    # TODO: remove hardcoding of path
    #package_config_dict = extract_config_dict("/Users/abirmingham/Work/Repositories/qiimp2/temp.yaml")
    package_config_dict = extract_config_dict("/Users/abirmingham/Library/Application Support/JetBrains/PyCharm2023.3/scratches/temp2.yaml")
    flattened_hosts_dict = _read_package_config(package_config_dict, None, None)
    main_config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY] = flattened_hosts_dict


    generate_extended_metadata_file_from_raw_metadata_file(
        raw_metadata_fp, main_config_dict, output_dir, output_base)
