import logging
import numpy as np
import os
import pandas
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from metameq.src.util import extract_config_dict, \
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
from metameq.src.metadata_configurator import update_wip_metadata_dict, \
    build_full_flat_config_dict
from metameq.src.metadata_validator import validate_metadata_df, \
    output_validation_msgs
import metameq.src.metadata_transformers as transformers


# columns added to the metadata that are not actually part of it
INTERNAL_COL_KEYS = [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY,
                     QC_NOTE_KEY]

REQ_PLACEHOLDER = "_METAMEQ_REQUIRED"

# Define a logger for this module
logger = logging.getLogger(__name__)

pandas.set_option("future.no_silent_downcasting", True)

# TODO: find a way to inform user that they *are not allowed* to have a 'sample_id' column
#  (Per Antonio 10/28/24, this is a reserved name for Qiita and may not be
#  in the metadata).


def get_reserved_cols(
        raw_metadata_df: pandas.DataFrame,
        study_specific_config_dict: Dict[str, Any],
        stds_fp: Optional[str] = None) -> List[str]:
    """Get a list of all reserved column names for all host+sample type combinations in the metadata.

    Note that 'reserved' is not the same as 'required'.  Some column names (e.g.,
        irb_institute for human host types) are not *required*, but are *reserved*, so they can
        only be used to name columns that hold standardized info, not for arbitrary metadata.

    Parameters
    ----------
    raw_metadata_df : pandas.DataFrame
        The input metadata DataFrame.
    study_specific_config_dict : Dict[str, Any]
        Study-specific flat-host-type config dictionary.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file. If None, the default standards
        config pulled from the standards.yml file will be used.

    Returns
    -------
    List[str]
        Sorted list of all reserved column names.
        Empty if there are no reserved columns.

    Raises
    ------
    ValueError
        If required columns are missing from the metadata.
    """
    validate_required_columns_exist(
        raw_metadata_df, [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY],
        "metadata missing required columns")

    # Essentially, mock a minimal metadata valid metadata dataframe and then
    # use extend_metadata_df to add all the required columns to it (either empty
    # or with default values but we don't care about the actual values), then
    # return the list of column names from that extended df.

    # get unique HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY combinations
    temp_df = raw_metadata_df[
        [HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY]].copy()
    temp_df.drop_duplicates(inplace=True)

    # add a bogus SAMPLE_NAME_KEY column to the df that just holds sequential integers
    temp_df[SAMPLE_NAME_KEY] = range(1, len(temp_df) + 1)

    temp_df = _catch_nan_required_fields(temp_df)

    # extend the metadata_df to get all the required columns for all host+sample type combinations;
    # we don't really care about the contents of these columns, just their names.
    # (Likewise, it is not necessary to pass the actual study_specific_transformers_dict so
    # just use None)
    metadata_df, _ = extend_metadata_df(
        temp_df, study_specific_config_dict, None, None, stds_fp)

    return sorted(metadata_df.columns.to_list())


def id_missing_cols(a_df: pandas.DataFrame) -> List[str]:
    """Identify required columns that are missing from the DataFrame.

    Parameters
    ----------
    a_df : pandas.DataFrame
        The metadata DataFrame to check for missing columns.

    Returns
    -------
    List[str]
        Sorted list of required column names that are missing from the DataFrame.
        Empty if there are no missing columns.
    """
    missing_cols = set(REQUIRED_RAW_METADATA_FIELDS) - set(a_df.columns)
    return sorted(list(missing_cols))


def find_standard_cols(
        a_df: pandas.DataFrame,
        study_specific_config_dict: Dict[str, Any],
        stds_fp: Optional[str] = None,
        suppress_missing_name_err: bool = False) -> List[str]:
    """Find all the standard columns in the metadata DataFrame.

    Parameters
    ----------
    a_df : pandas.DataFrame
        The metadata DataFrame to analyze.
    study_specific_config_dict : Dict[str, Any]
        Study-specific flat-host-type config dictionary.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file. If None, the default standards
        config pulled from the standards.yml file will be used.
    suppress_missing_name_err : bool, default=False
        Whether to suppress errors about missing sample name.

    Returns
    -------
    List[str]
        List of standard column names found in the DataFrame.
        Empty if there are no standard columns.

    Raises
    ------
    ValueError
        If required columns are missing from the metadata.
    """
    err_msg = "metadata missing required columns"
    required_cols = REQUIRED_RAW_METADATA_FIELDS.copy()
    if suppress_missing_name_err:
        # remove the sample name from the required columns list
        required_cols.remove(SAMPLE_NAME_KEY)
    # endif
    validate_required_columns_exist(a_df, required_cols, err_msg)

    # get the intersection of the reserved standard columns and
    # the columns in the input dataframe
    standard_cols = get_reserved_cols(
        a_df, study_specific_config_dict, stds_fp)

    standard_cols_set = (set(standard_cols) - set(INTERNAL_COL_KEYS))

    return list(standard_cols_set & set(a_df.columns))


def find_nonstandard_cols(
        a_df: pandas.DataFrame,
        study_specific_config_dict: Dict[str, Any],
        stds_fp: Optional[str] = None) -> List[str]:
    """Find any non-standard columns in the metadata DataFrame.

    Parameters
    ----------
    a_df : pandas.DataFrame
        The metadata DataFrame to analyze.
    study_specific_config_dict : Dict[str, Any]
        Study-specific flat-host-type config dictionary.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file. If None, the default standards
        config pulled from the standards.yml file will be used.

    Returns
    -------
    List[str]
        List of non-standard column names found in the DataFrame.
        Empty if there are no non-standard columns.

    Raises
    ------
    ValueError
        If required columns are missing from the metadata.
    """
    validate_required_columns_exist(a_df, REQUIRED_RAW_METADATA_FIELDS,
                                    "metadata missing required columns")

    # get the columns in
    standard_cols = get_reserved_cols(
        a_df, study_specific_config_dict, stds_fp)

    return list(set(a_df.columns) - set(standard_cols))


def get_extended_metadata_from_df_and_yaml(
        raw_metadata_df: pandas.DataFrame,
        study_specific_config_fp: Optional[str],
        stds_fp: Optional[str] = None) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
    """Extend metadata using configuration from a study-specific YAML config file.

    Parameters
    ----------
    raw_metadata_df : pandas.DataFrame
        The raw metadata DataFrame to extend.
    study_specific_config_fp : Optional[str]
        Path to the study-specific configuration YAML file.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file. If None, the default standards
        config pulled from the standards.yml file will be used.

    Returns
    -------
    Tuple[pandas.DataFrame, pandas.DataFrame]
        A tuple containing:
            - The extended metadata DataFrame
            - A DataFrame containing validation messages
    """
    # get the study-specific flat-host-type config dictionary from the input yaml file
    study_specific_config_dict = \
        _get_study_specific_config(study_specific_config_fp)

    # extend the metadata DataFrame using the study-specific flat-host-type config dictionary
    metadata_df, validation_msgs_df = \
        extend_metadata_df(raw_metadata_df, study_specific_config_dict,
                           None, None, stds_fp)

    return metadata_df, validation_msgs_df


def get_qc_failures(a_df: pandas.DataFrame) -> pandas.DataFrame:
    """Get rows from the extended metadata DataFrame that have QC failures.

    Parameters
    ----------
    a_df : pandas.DataFrame
        The extended metadata DataFrame to check for QC failures.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame containing only the rows that failed QC checks.
    """
    fails_qc_mask = a_df[QC_NOTE_KEY] != ""
    qc_fails_df = \
        a_df.loc[fails_qc_mask, :].copy()
    return qc_fails_df


def write_extended_metadata(
        raw_metadata_fp: str,
        study_specific_config_fp: str,
        out_dir: str,
        out_name_base: str,
        sep: str = "\t",
        remove_internals: bool = True,
        suppress_empty_fails: bool = False,
        stds_fp: Optional[str] = None) -> pandas.DataFrame:
    """Write extended metadata to files starting from input file paths to metadata and config.

    Parameters
    ----------
    raw_metadata_fp : str
        Path to the raw metadata file (.csv, .txt, or .xlsx).
    study_specific_config_fp : str
        Path to the study-specific configuration YAML file.
    out_dir : str
        Directory where output files will be written.
    out_name_base : str
        Base name for output files.
    sep : str, default="\t"
        Separator to use in output files.
    remove_internals : bool, default=True
        Whether to remove internal columns.
    suppress_empty_fails : bool, default=False
        Whether to suppress empty failure files.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file. If None, the default standards
        config pulled from the standards.yml file will be used.

    Returns
    -------
    pandas.DataFrame
        The extended metadata DataFrame.

    Raises
    ------
    ValueError
        If the input file extension is not recognized.
    """
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

    # get the study-specific flat-host-type config dictionary from the input yaml file
    study_specific_config_dict = \
        _get_study_specific_config(study_specific_config_fp)

    # write the extended metadata to files
    extended_df = write_extended_metadata_from_df(
        raw_metadata_df, study_specific_config_dict,
        out_dir, out_name_base, sep=sep,
        remove_internals=remove_internals,
        suppress_empty_fails=suppress_empty_fails,
        stds_fp=stds_fp)

    # for good measure, return the extended metadata DataFrame
    return extended_df


def _get_study_specific_config(study_specific_config_fp: Optional[str]) -> Optional[Dict[str, Any]]:
    """Load study-specific flat-host-type configuration from a YAML file.

    Parameters
    ----------
    study_specific_config_fp : Optional[str]
        Path to the study-specific configuration YAML file.
        This file should contain study-specific values for top-level settings (e.g., default
        value) and, if necessary, a HOST_TYPE_SPECIFIC_METADATA_KEY holding a *flat*
        dictionary of host types, defining only their study-specific host and sample type
        metadata fields.

    Returns
    -------
    Optional[Dict[str, Any]]
        The loaded flat-host-type configuration dictionary, or None if no file path provided.
    """
    if study_specific_config_fp:
        study_specific_config_dict = \
            extract_config_dict(study_specific_config_fp)
    else:
        study_specific_config_dict = None

    return study_specific_config_dict


def write_extended_metadata_from_df(
        raw_metadata_df: pandas.DataFrame,
        study_specific_config_dict: Dict[str, Any],
        out_dir: str,
        out_name_base: str,
        study_specific_transformers_dict: Optional[Dict[str, Any]] = None,
        sep: str = "\t",
        remove_internals: bool = True,
        suppress_empty_fails: bool = False,
        internal_col_names: Optional[List[str]] = None,
        stds_fp: Optional[str] = None) -> pandas.DataFrame:
    """Write extended metadata to files starting from a metadata DataFrame and config dictionary.

    Parameters
    ----------
    raw_metadata_df : pandas.DataFrame
        The raw metadata DataFrame to extend.
    study_specific_config_dict : Dict[str, Any]
        Study-specific configuration dictionary.
    out_dir : str
        Directory where output files will be written.
    out_name_base : str
        Base name for output files.
    study_specific_transformers_dict : Optional[Dict[str, Any]], default=None
        Dictionary of custom transformers.
    sep : str, default="\t"
        Separator to use in output files.
    remove_internals : bool, default=True
        Whether to remove internal columns.
    suppress_empty_fails : bool, default=False
        Whether to suppress empty failure files.
    internal_col_names : Optional[List[str]], default=None
        List of internal column names.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file. If None, the default standards
        config pulled from the standards.yml file will be used.

    Returns
    -------
    pandas.DataFrame
        The extended metadata DataFrame.
    """
    # extend the metadata DataFrame using the study-specific flat-host-type config dictionary
    metadata_df, validation_msgs_df = extend_metadata_df(
        raw_metadata_df, study_specific_config_dict,
        study_specific_transformers_dict, None, stds_fp)

    # write the metadata and validation results to files
    write_metadata_results(
        metadata_df, validation_msgs_df, out_dir, out_name_base,
        sep=sep, remove_internals=remove_internals,
        suppress_empty_fails=suppress_empty_fails,
        internal_col_names=internal_col_names)

    # for good measure, return the extended metadata DataFrame
    return metadata_df


def extend_metadata_df(
        raw_metadata_df: pandas.DataFrame,
        study_specific_config_dict: Optional[Dict[str, Any]],
        study_specific_transformers_dict: Optional[Dict[str, Any]] = None,
        software_config_dict: Optional[Dict[str, Any]] = None,
        stds_fp: Optional[str] = None
) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
    """Extend a metadata DataFrame based on metadata standards and study-specific configurations.

    Parameters
    ----------
    raw_metadata_df : pandas.DataFrame
        The raw metadata DataFrame to extend.
    study_specific_config_dict : Optional[Dict[str, Any]]
        Study-specific flat-host-type config dictionary.
    study_specific_transformers_dict : Optional[Dict[str, Any]], default=None
        Dictionary of custom transformers for this study (only).
    software_config_dict : Optional[Dict[str, Any]], default=None
        Software configuration dictionary. If None, the default software
        config pulled from the config.yml file will be used.
    stds_fp : Optional[str], default=None
        Path to standards dictionary file. If None, the default standards
        config pulled from the standards.yml file will be used.

    Returns
    -------
    Tuple[pandas.DataFrame, pandas.DataFrame]
        A tuple containing:
            - The extended metadata DataFrame
            - A DataFrame containing validation messages

    Raises
    ------
    ValueError
        If required columns are missing from the metadata.
    """
    validate_required_columns_exist(
        raw_metadata_df, REQUIRED_RAW_METADATA_FIELDS,
        "metadata missing required columns")

    full_flat_config_dict = build_full_flat_config_dict(
        study_specific_config_dict, software_config_dict, stds_fp)

    metadata_df, validation_msgs_df = _populate_metadata_df(
        raw_metadata_df, full_flat_config_dict,
        study_specific_transformers_dict)

    return metadata_df, validation_msgs_df


def write_metadata_results(
        metadata_df: pandas.DataFrame,
        validation_msgs_df: pandas.DataFrame,
        out_dir: str,
        out_name_base: str,
        sep: str = "\t",
        remove_internals: bool = True,
        suppress_empty_fails: bool = False,
        internal_col_names: Optional[List[str]] = None) -> None:
    """Write metadata and validation results to files.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The metadata DataFrame to write.
    validation_msgs_df : pandas.DataFrame
        DataFrame containing validation messages.
    out_dir : str
        Directory where output files will be written.
    out_name_base : str
        Base name for output files.
    sep : str, default="\t"
        Separator to use in output files.
    remove_internals : bool, default=True
        Whether to remove internal columns.
    suppress_empty_fails : bool, default=False
        Whether to suppress empty failure files.
    internal_col_names : Optional[List[str]], default=None
        List of internal column names.
    """
    if internal_col_names is None:
        internal_col_names = INTERNAL_COL_KEYS

    _output_metadata_df_to_files(
        metadata_df, out_dir, out_name_base, internal_col_names,
        remove_internals_and_fails=remove_internals, sep=sep,
        suppress_empty_fails=suppress_empty_fails)

    output_validation_msgs(validation_msgs_df, out_dir, out_name_base, sep=",",
                           suppress_empty_fails=suppress_empty_fails)


def _populate_metadata_df(
        raw_metadata_df: pandas.DataFrame,
        full_flat_config_dict: Dict[str, Any],
        transformer_funcs_dict: Optional[Dict[str, Any]]) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
    """Populate columns and fields in a metadata DataFrame.

    Parameters
    ----------
    raw_metadata_df : pandas.DataFrame
        The raw metadata DataFrame to populate, which must contain at least
        the columns in REQUIRED_RAW_METADATA_FIELDS.
    full_flat_config_dict : Dict[str, Any]
        Fully combined flat-host-type config dictionary.
    transformer_funcs_dict : Optional[Dict[str, Any]]
        Dictionary of transformer functions, keyed by field name,
        with each value being a dict with keys SOURCES_KEY and FUNCTION_KEY,
        which map to lists of source field names for the transformer to use
        and an existing transformer function name, respectively.

    Returns
    -------
    Tuple[pandas.DataFrame, pandas.DataFrame]
        A tuple containing:
            - The populated metadata DataFrame
            - A DataFrame containing validation messages
    """
    metadata_df = raw_metadata_df.copy()
    # Don't try to populate the QC_NOTE_KEY field, since it is an internal field
    update_metadata_df_field(metadata_df, QC_NOTE_KEY, LEAVE_BLANK_VAL)

    # Error for NaNs in sample name, warn for NaNs in host- and sample-type- shorthand fields.
    metadata_df = _catch_nan_required_fields(metadata_df)

    # Apply pre-transformers to the metadata. Pre-transformers run BEFORE host- and sample-type
    # specific generation (which also includes validation), so they can transform raw input fields
    # into values that the config validation expects (for example, converting a study's custom sex
    # format like "M"/"F" into standardized values like "male"/"female" before validation occurs.
    metadata_df = _transform_metadata(
        metadata_df, full_flat_config_dict,
        PRE_TRANSFORMERS_KEY, transformer_funcs_dict)

    # Add specific metadata based on each host type present in the metadata.
    # This step also validates the metadata against the config requirements.
    metadata_df, validation_msgs = _generate_metadata_for_host_types(
        metadata_df, full_flat_config_dict)

    # Apply post-transformers to the metadata. Post-transformers run AFTER host- and sample-type
    # specific generation, so they can use fields that only exist or were only filled in
    # after that step, such as passing through a value filled in by the defaults to another field.
    metadata_df = _transform_metadata(
        metadata_df, full_flat_config_dict,
        POST_TRANSFORMERS_KEY, transformer_funcs_dict)

    # Reorder the metadata columns for better readability.
    metadata_df = _reorder_df(metadata_df, INTERNAL_COL_KEYS)

    # Turn the validation messages into a DataFrame of validation messages for easier use downstream.
    validation_msgs_df = pandas.DataFrame(validation_msgs)

    return metadata_df, validation_msgs_df


def _catch_nan_required_fields(metadata_df: pandas.DataFrame) -> pandas.DataFrame:
    """Error for NaNs in sample name, warn for NaNs in host- and sample-type- shorthand fields.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The metadata DataFrame to process.

    Returns
    -------
    pandas.DataFrame
        The processed DataFrame. NaNs in host- and sample-type-shorthand fields are set to "empty".

    Raises
    ------
    ValueError
        If any sample names are NaN.
    """
    # if there are any sample_name fields that are NaN, raise an error
    nan_sample_name_mask = metadata_df[SAMPLE_NAME_KEY].isna()
    if nan_sample_name_mask.any():
        raise ValueError("Metadata contains NaN sample names")

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
        metadata_df: pandas.DataFrame,
        full_flat_config_dict: Dict[str, Any],
        stage_key: str,
        transformer_funcs_dict: Optional[Dict[str, Any]]) -> pandas.DataFrame:
    """Apply transformations defined in full_flat_config_dict to metadata fields using dict of transformer functions.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The metadata DataFrame to transform, which must contain at least
        the columns in REQUIRED_RAW_METADATA_FIELDS.
    full_flat_config_dict : Dict[str, Any]
        Fully combined flat-host-type config dictionary.
    stage_key : str
        Key indicating the transformation stage (pre or post).
    transformer_funcs_dict : Optional[Dict[str, Any]]
        Dictionary of transformer functions, keyed by field name,
        with each value being a dict with keys SOURCES_KEY and FUNCTION_KEY,
        which map to lists of source field names for the transformer to use
        and an existing transformer function name, respectively.

    Returns
    -------
    pandas.DataFrame
        The transformed metadata DataFrame.

    Raises
    ------
    ValueError
        If a specified transformer function cannot be found.
    """
    if transformer_funcs_dict is None:
        transformer_funcs_dict = {}
    # If the necessary keys aren't already in the config, set them to do-nothing defaults
    overwrite_non_nans = full_flat_config_dict.get(OVERWRITE_NON_NANS_KEY, False)
    metadata_transformers = full_flat_config_dict.get(METADATA_TRANSFORMERS_KEY, None)
    if metadata_transformers:
        stage_transformers = metadata_transformers.get(stage_key, None)
        # If there are transformers for the stage we're at, apply them
        if stage_transformers:
            for curr_target_field, curr_transformer_dict in \
                    stage_transformers.items():
                curr_source_fields = curr_transformer_dict[SOURCES_KEY]
                curr_func_name = curr_transformer_dict[FUNCTION_KEY]

                try:
                    curr_func = transformer_funcs_dict[curr_func_name]
                except KeyError:
                    try:
                        # if the transformer function isn't in the dictionary
                        # that was passed in, probably it is a built-in one,
                        # so look for it in the metameq transformers module
                        # looking into the metameq transformers module
                        curr_func = getattr(transformers, curr_func_name)
                    except AttributeError:
                        raise ValueError(
                            f"Unable to find transformer '{curr_func_name}'")
                    # end try to find in metameq transformers
                # end try to find in input (study-specific) transformers

                # apply the function named curr_func_name to the column(s) of the
                # metadata_df named curr_source_fields to fill curr_target_field
                update_metadata_df_field(metadata_df, curr_target_field,
                                         curr_func, curr_source_fields,
                                         overwrite_non_nans=overwrite_non_nans)
            # next stage transformer
        # end if there are stage transformers for this stage
    # end if there are any metadata transformers

    return metadata_df


def _generate_metadata_for_host_types(
        metadata_df: pandas.DataFrame,
        full_flat_config_dict: Dict[str, Any]) -> Tuple[pandas.DataFrame, List[str]]:
    """Generate metadata for samples of all host types in the DataFrame.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The metadata DataFrame to process, which must contain at least
        the columns in REQUIRED_RAW_METADATA_FIELDS.
    full_flat_config_dict : Dict[str, Any]
        Fully combined flat-host-type config dictionary.

    Returns
    -------
    Tuple[pandas.DataFrame, List[str]]
        A tuple containing:
            - The processed DataFrame with specific metadata added to each sample of each host type
            - A list of validation messages
    """
    # gather global settings
    settings_dict = {DEFAULT_KEY: full_flat_config_dict.get(DEFAULT_KEY),
                     LEAVE_REQUIREDS_BLANK_KEY:
                         full_flat_config_dict.get(LEAVE_REQUIREDS_BLANK_KEY),
                     OVERWRITE_NON_NANS_KEY:
                         full_flat_config_dict.get(OVERWRITE_NON_NANS_KEY)}

    validation_msgs = []
    host_type_dfs = []
    # For all the host types present in the metadata, generate the specific metadata
    host_type_shorthands = pandas.unique(metadata_df[HOSTTYPE_SHORTHAND_KEY])
    for curr_host_type_shorthand in host_type_shorthands:
        concatted_dfs, curr_validation_msgs = _generate_metadata_for_a_host_type(
                metadata_df, curr_host_type_shorthand, settings_dict, full_flat_config_dict)

        host_type_dfs.append(concatted_dfs)
        validation_msgs.extend(curr_validation_msgs)
    # next host type

    # Concatenate the processed host-type-specific metadata DataFrames into a single output DataFrame
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
    # Replace the LEAVE_BLANK_VAL with an empty string in the output DataFrame
    output_df.replace(LEAVE_BLANK_VAL, "", inplace=True)
    return output_df, validation_msgs


def _generate_metadata_for_a_host_type(
        metadata_df: pandas.DataFrame,
        a_host_type: str,
        settings_dict: Dict[str, Any],
        full_flat_config_dict: Dict[str, Any]) -> Tuple[pandas.DataFrame, List[str]]:
    """Generate metadata df for samples with a specific host type.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The metadata DataFrame to process, which must contain at least
        the columns in REQUIRED_RAW_METADATA_FIELDS.
    a_host_type : str
        The specific host type for which to process samples.
    settings_dict : Dict[str, Any]
        Dictionary containing global settings for default/nan/etc.
    full_flat_config_dict : Dict[str, Any]
        Fully combined flat-host-type config dictionary.

    Returns
    -------
    Tuple[pandas.DataFrame, List[str]]
        A tuple containing:
            - The processed DataFrame with specific metadata added to each sample of the input host type
            - A list of validation messages
    """
    # get the subset of the metadata DataFrame that contains samples of the input host type
    host_type_mask = \
        metadata_df[HOSTTYPE_SHORTHAND_KEY] == a_host_type
    host_type_df = metadata_df.loc[host_type_mask, :].copy()

    validation_msgs = []
    known_host_shorthands = full_flat_config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY].keys()
    if a_host_type not in known_host_shorthands:
        # if the input host type is not in the config, add a QC note to the metadata
        # for these samples but do not error out; move on to the next host type
        update_metadata_df_field(
            host_type_df, QC_NOTE_KEY, "invalid host_type")
        # host_type_df[QC_NOTE_KEY] = "invalid host_type"
        concatted_df = host_type_df
    else:
        # gather host-type-specific settings and overwrite the global settings with them, if any
        a_host_type_config_dict = \
            full_flat_config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY][a_host_type]
        global_plus_host_settings_dict = deepcopy_dict(settings_dict)
        # if this host type has a default value for empty fields, use it; otherwise, use the global default
        global_plus_host_settings_dict[DEFAULT_KEY] = a_host_type_config_dict.get(
            DEFAULT_KEY, global_plus_host_settings_dict[DEFAULT_KEY])

        dfs_to_concat = []
        # loop through each sample type in the metadata for this host type
        found_host_sample_types = \
            pandas.unique(host_type_df[SAMPLETYPE_SHORTHAND_KEY])
        for curr_sample_type in found_host_sample_types:
            # generate the specific metadata for this sample type *in this host type*
            curr_sample_type_df, curr_validation_msgs = \
                _generate_metadata_for_a_sample_type_in_a_host_type(
                    host_type_df, curr_sample_type, global_plus_host_settings_dict,
                    a_host_type_config_dict)

            dfs_to_concat.append(curr_sample_type_df)
            validation_msgs.extend(curr_validation_msgs)
        # next sample type in metadata for this host type

        # Concatenate the processed sample-type-specific metadata DataFrames
        # for the host type into a single output DataFrame
        concatted_df = pandas.concat(dfs_to_concat, ignore_index=True)
    # endif host_type is valid

    return concatted_df, validation_msgs


def _generate_metadata_for_a_sample_type_in_a_host_type(
        host_type_metadata_df: pandas.DataFrame,
        a_sample_type: str,
        global_plus_host_settings_dict: Dict[str, Any],
        a_host_type_config_dict: Dict[str, Any]) -> Tuple[pandas.DataFrame, List[str]]:
    """Generate metadata df for samples with a specific sample type within a specific host type.

    Parameters
    ----------
    host_type_metadata_df : pandas.DataFrame
        DataFrame containing metadata samples for a specific host type.
    a_sample_type : str
        The sample type to process.
    global_plus_host_settings_dict : Dict[str, Any]
        Dictionary containing default/nan/etc settings for current context.
    a_host_type_config_dict : Dict[str, Any]
        Dictionary containing config for this host type.

    Returns
    -------
    Tuple[pandas.DataFrame, List[str]]
        A tuple containing:
            - The updated metadata DataFrame with sample-type-specific elements added
            - A list of validation messages
    """
    # get the config section for *all* sample types within this host type
    host_sample_types_config_dict = \
        a_host_type_config_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY]

    # get df of records for this sample type in this host type
    sample_type_mask = \
        host_type_metadata_df[SAMPLETYPE_SHORTHAND_KEY] == a_sample_type
    sample_type_df = host_type_metadata_df.loc[sample_type_mask, :].copy()

    validation_msgs = []
    known_sample_types = host_sample_types_config_dict.keys()
    if a_sample_type not in known_sample_types:
        # if the input sample type is not in the config, add a QC note to the metadata
        # for these samples but do not error out; move on to the next sample type
        update_metadata_df_field(
            sample_type_df, QC_NOTE_KEY, "invalid sample_type")
    else:
        # Get the already-resolved metadata fields dict for this sample type.
        # The config is pre-resolved: aliases/base types are merged and
        # host metadata is combined.
        sample_type_config = host_sample_types_config_dict[a_sample_type]
        full_sample_type_metadata_fields_dict = \
            sample_type_config.get(METADATA_FIELDS_KEY, {})

        # update the metadata df with the sample type specific metadata fields
        sample_type_df = _update_metadata_from_dict(
            sample_type_df, full_sample_type_metadata_fields_dict,
            dict_is_metadata_fields=True,
            overwrite_non_nans=global_plus_host_settings_dict[OVERWRITE_NON_NANS_KEY])

        # for fields that are required but not yet filled, replace the placeholder with
        # either an indicator that it should be blank or else
        # fill with NA (replaced with default just below), based on config setting
        leave_reqs_blank = global_plus_host_settings_dict[LEAVE_REQUIREDS_BLANK_KEY]
        reqs_val = LEAVE_BLANK_VAL if leave_reqs_blank else np.nan
        sample_type_df.replace(
            to_replace=REQ_PLACEHOLDER, value=reqs_val, inplace=True)

        # fill NAs with appropriate default value if any is set
        sample_type_df = _fill_na_if_default(
            sample_type_df, full_sample_type_metadata_fields_dict, global_plus_host_settings_dict)

        # validate the metadata df based on the specific requirements
        # for this host+sample type
        validation_msgs = validate_metadata_df(
            sample_type_df, full_sample_type_metadata_fields_dict)

    return sample_type_df, validation_msgs


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
        if METADATA_FIELDS_KEY not in sample_type_specific_dict:
            raise ValueError(f"May not chain aliases "
                             f"('{sample_type}' to '{sample_type_alias}')")
    # endif sample type is an alias

    # if the sample type has a base type
    sample_type_base = sample_type_specific_dict.get(BASE_TYPE_KEY)
    if sample_type_base:
        # get the base's sample type dict and add this sample type's
        # info on top of it
        base_sample_dict = host_sample_types_config_dict[sample_type_base]
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
        a_host_type_metadata_fields_dict,
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


def _update_metadata_from_dict(
        metadata_df: pandas.DataFrame,
        config_section_dict: Dict[str, Any],
        dict_is_metadata_fields: bool = False,
        overwrite_non_nans: bool = False) -> pandas.DataFrame:
    """Create an updated copy of the metadata DataFrame based on an input dictionary.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The metadata DataFrame to update.
    config_section_dict : Dict[str, Any]
        The relevant section of a config dictionary to use.
    dict_is_metadata_fields : bool, default=False
        Whether the config dict contains a METADATA_FIELDS_KEY
        (in which case False) or is itself the contents of
        a METADATA_FIELDS_KEY (in which case True).
    overwrite_non_nans : bool, default=False
        Whether to overwrite non-NaN values with default values.

    Returns
    -------
    pandas.DataFrame
        An updated copy of the metadata DataFrame.
    """
    if not dict_is_metadata_fields:
        metadata_fields_dict = config_section_dict.get(METADATA_FIELDS_KEY)
    else:
        metadata_fields_dict = config_section_dict

    output_df = _update_metadata_from_metadata_fields_dict(
        metadata_df, metadata_fields_dict,
        overwrite_non_nans=overwrite_non_nans)
    return output_df


def _update_metadata_from_metadata_fields_dict(
        metadata_df: pandas.DataFrame,
        metadata_fields_dict: Dict[str, Any],
        overwrite_non_nans: bool) -> pandas.DataFrame:
    """Create an updated copy of the metadata DataFrame based on a metadata fields dictionary.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The metadata DataFrame to update.
    metadata_fields_dict : Dict[str, Any]
        Dictionary containing metadata field definitions and required values.
    overwrite_non_nans : bool
        Whether to overwrite non-NaN values with default values.

    Returns
    -------
    pandas.DataFrame
        An updated copy of the metadata DataFrame.
    """
    output_df = metadata_df.copy()

    # loop through each metadata field in the metadata fields dict
    for curr_field_name, curr_field_vals_dict in metadata_fields_dict.items():
        # if the field has a default value (regardless of whether it is
        # required), update the metadata df with it (this includes adding the
        # field if it does not already exist). For existing fields, what exactly
        # will beupdated depends on the value of overwrite_non_nans:
        # if overwrite_non_nans is True, then all values will be updated;
        # if overwrite_non_nans is False, then only NA values will be updated
        # if the field already exists in the metadata; otherwise, the field
        # will be added to the metadata with the default value throughout.
        if DEFAULT_KEY in curr_field_vals_dict:
            curr_default_val = curr_field_vals_dict[DEFAULT_KEY]
            update_metadata_df_field(
                output_df, curr_field_name, curr_default_val,
                overwrite_non_nans=overwrite_non_nans)
        # if the field is required BUT has no default value, then if the field does not
        # already exist in the metadata, add the field to the metadata with a placeholder value.
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


# fill NAs with default value if any is set
def _fill_na_if_default(
        metadata_df: pandas.DataFrame,
        specific_dict: Dict[str, Any],
        settings_dict: Dict[str, Any]) -> pandas.DataFrame:
    """Fill NaN values in metadata df with default values if available.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The metadata DataFrame to process.
    specific_dict : Dict[str, Any]
        Dictionary containing context-specific settings. Will be used first as a source of default values.
    settings_dict : Dict[str, Any]
        Dictionary containing global settings. Will be used as a
          source of default values if specific_dict does not contain a DEFAULT_KEY.

    Returns
    -------
    pandas.DataFrame
        The updated DataFrame with NaN values filled. Unchanged if no default values are set.
    """
    default_val = specific_dict.get(DEFAULT_KEY, settings_dict[DEFAULT_KEY])
    if default_val:
        # TODO: this is setting a value in the output; should it be
        #  centralized so it is easy to find?
        metadata_df = \
            metadata_df.fillna(default_val)
#             metadata_df.astype("string").fillna(default_val)

    return metadata_df


def _output_metadata_df_to_files(
        a_df: pandas.DataFrame,
        out_dir: str,
        out_base: str,
        internal_col_names: List[str],
        sep: str = "\t",
        remove_internals_and_fails: bool = False,
        suppress_empty_fails: bool = False) -> None:
    """Output DataFrame to files, optionally removing internal columns and failures.

    Parameters
    ----------
    a_df : pandas.DataFrame
        The metadata DataFrame to output.
    out_dir : str
        Directory where output files will be written.
    out_base : str
        Base name for output files.
    internal_col_names : List[str]
        List of internal column names that will be moved
        to the end of the DataFrame.
    sep : str, default="tab"
        Separator to use in output files.
    remove_internals_and_fails : bool, default=False
        Whether to remove internal columns and failures.
    suppress_empty_fails : bool, default=False
        Whether to suppress empty failure files.
    """
    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    extension = get_extension(sep)

    # if we've been told to remove the qc fails and the internal columns
    if remove_internals_and_fails:
        # output a file of any qc failures
        qc_fails_df = get_qc_failures(a_df)
        qc_fails_fp = os.path.join(
            out_dir, f"{timestamp_str}_{out_base}_fails.csv")
        if qc_fails_df.empty:
            # unless we've been told to suppress empty files
            if not suppress_empty_fails:
                # if there are no failures, create an empty file
                # (not even header line) if there are no failures--bc it is easy to
                # eyeball "zero bytes"
                Path(qc_fails_fp).touch()
            # else, just do nothing
        else:
            qc_fails_df.to_csv(qc_fails_fp, sep=",", index=False)

        # then remove the qc fails and the internal columns from the metadata
        # TODO: I'd like to avoid repeating this mask here + in get_qc_failures
        fails_qc_mask = a_df[QC_NOTE_KEY] != ""
        a_df = a_df.loc[~fails_qc_mask, :].copy()
        a_df = a_df.drop(columns=internal_col_names)

    # output the metadata
    out_fp = os.path.join(out_dir, f"{timestamp_str}_{out_base}.{extension}")
    a_df.to_csv(out_fp, sep=sep, index=False)


def _reorder_df(a_df: pandas.DataFrame, internal_col_names: List[str]) -> pandas.DataFrame:
    """Reorder DataFrame columns according to standard rules.

    Parameters
    ----------
    a_df : pandas.DataFrame
        The DataFrame to reorder.
    internal_col_names : List[str]
        List of internal column names that will be moved to the end of the DataFrame.

    Returns
    -------
    pandas.DataFrame
        A reordered copy of the input DataFrame with:
            - sample_name as the first column
            - remaining columns except for internal columns in alphabetical order
            - internal columns at the end in the order they were provided
    """
    # sort columns alphabetically
    working_df = a_df.copy().reindex(sorted(a_df.columns), axis=1)

    # move the internal columns to the end of the list of cols to output
    col_names = list(working_df)
    for curr_internal_col_name in internal_col_names:
        # TODO: throw an error if the internal col name is not present
        col_names.pop(col_names.index(curr_internal_col_name))
        col_names.append(curr_internal_col_name)

    # move sample name to the first column
    col_names.insert(0, col_names.pop(col_names.index(SAMPLE_NAME_KEY)))
    output_df = working_df.loc[:, col_names].copy()
    return output_df
