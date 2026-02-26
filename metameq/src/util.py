import copy
from importlib.resources import files
import pandas
from typing import List, Optional, Union, Callable, Any
import yaml

CONFIG_MODULE_PATH = "metameq.config"

# config keys
METADATA_FIELDS_KEY = "metadata_fields"
STUDY_SPECIFIC_METADATA_KEY = "study_specific_metadata"
HOST_TYPE_SPECIFIC_METADATA_KEY = "host_type_specific_metadata"
SAMPLE_TYPE_KEY = "sample_type"
QIITA_SAMPLE_TYPE = "qiita_sample_type"
SAMPLE_TYPE_SPECIFIC_METADATA_KEY = "sample_type_specific_metadata"
METADATA_TRANSFORMERS_KEY = "metadata_transformers"
PRE_TRANSFORMERS_KEY = "pre_transformers"
POST_TRANSFORMERS_KEY = "post_transformers"
ALIAS_KEY = "alias"
BASE_TYPE_KEY = "base_type"
DEFAULT_KEY = "default"
REQUIRED_KEY = "required"
ALLOWED_KEY = "allowed"
ANYOF_KEY = "anyof"
TYPE_KEY = "type"
SOURCES_KEY = "sources"
FUNCTION_KEY = "function"
LEAVE_REQUIREDS_BLANK_KEY = "leave_requireds_blank"
OVERWRITE_NON_NANS_KEY = "overwrite_non_nans"
HOST_OVERRIDES_ANCESTOR_SAMPLE_TYPE_KEY = "host_overrides_ancestor_sample_type"
HOSTTYPE_COL_OPTIONS_KEY = "hosttype_column_options"
SAMPLETYPE_COL_OPTIONS_KEY = "sampletype_column_options"
REUSABLE_DEFINITIONS_KEY = "_reusable_definitions"

# internal code keys
HOSTTYPE_SHORTHAND_KEY = "hosttype_shorthand"
SAMPLETYPE_SHORTHAND_KEY = "sampletype_shorthand"
QC_NOTE_KEY = "qc_note"

# metadata keys
SAMPLE_NAME_KEY = "sample_name"
COLLECTION_TIMESTAMP_KEY = "collection_timestamp"
HOST_SUBJECT_ID_KEY = "host_subject_id"

# constant field values
NOT_PROVIDED_VAL = "not provided"
LEAVE_BLANK_VAL = "leaveblank"
DO_NOT_USE_VAL = "donotuse"

# required raw metadata fields
REQUIRED_RAW_METADATA_FIELDS = [SAMPLE_NAME_KEY,
                                HOSTTYPE_SHORTHAND_KEY,
                                SAMPLETYPE_SHORTHAND_KEY]


GLOBAL_SETTINGS_KEYS = [
    DEFAULT_KEY,
    LEAVE_REQUIREDS_BLANK_KEY,
    OVERWRITE_NON_NANS_KEY
]


def extract_config_dict(
        config_fp: Union[str, None],
        keys_to_remove: Optional[List[str]] = None) -> dict:
    """Extract configuration dictionary from a YAML file.

    If no config file path is provided, looks for config.yml in the grandparent
    directory of the starting file path or current file.

    Parameters
    ----------
    config_fp : Union[str, None]
        Path to the configuration YAML file. If None, will look for config.yml
        in the "config" module of the package.
    keys_to_remove : Optional[List[str]]
        Top-level keys to remove from the loaded dictionary. Keys that
        do not exist in the dictionary are silently ignored.

    Returns
    -------
    dict
        Configuration dictionary loaded from the YAML file.

    Raises
    ------
    FileNotFoundError
        If the config file cannot be found.
    yaml.YAMLError
        If the YAML file is invalid.
    """
    if config_fp is None:
        config_dir = files(CONFIG_MODULE_PATH)
        config_fp = config_dir.joinpath("config.yml")

    # read in config file
    config_dict = extract_yaml_dict(config_fp)
    if keys_to_remove:
        for key in keys_to_remove:
            config_dict.pop(key, None)
    return config_dict


def extract_yaml_dict(yaml_fp: str) -> dict:
    """Extract dictionary from a YAML file.

    Parameters
    ----------
    yaml_fp : str
        Path to the YAML file.

    Returns
    -------
    dict
        Dictionary loaded from the YAML file.

    Raises
    ------
    FileNotFoundError
        If the YAML file cannot be found.
    yaml.YAMLError
        If the YAML file is invalid.
    """
    with open(yaml_fp, "r") as f:
        yaml_dict = yaml.safe_load(f)
    return yaml_dict


def extract_stds_config(stds_fp: Union[str, None]) -> dict:
    """Extract standards dictionary from a YAML file.

    If no standards file path is provided, looks for standards.yml in the
    "config" module of the package.

    Parameters
    ----------
    stds_fp : Union[str, None]
        Path to the standards YAML file. If None, will look for
        standards.yml in the "config" module.

    Returns
    -------
    dict
        Standards dictionary loaded from the YAML file.

    Raises
    ------
    FileNotFoundError
        If the standards file cannot be found.
    yaml.YAMLError
        If the YAML file is invalid.
    """
    if not stds_fp:
        config_dir = files(CONFIG_MODULE_PATH)
        stds_fp = config_dir.joinpath("standards.yml")
    return extract_config_dict(
        stds_fp, keys_to_remove=[REUSABLE_DEFINITIONS_KEY])


def deepcopy_dict(input_dict: dict) -> dict:
    """Create a deep copy of a dictionary, including nested dictionaries.

    Parameters
    ----------
    input_dict : dict
        Dictionary to be copied.

    Returns
    -------
    dict
        Deep copy of the input dictionary.
    """
    output_dict = {}
    for curr_key, curr_val in input_dict.items():
        if isinstance(curr_val, dict):
            output_dict[curr_key] = deepcopy_dict(curr_val)
        else:
            output_dict[curr_key] = copy.deepcopy(curr_val)
    return output_dict


def load_df_with_best_fit_encoding(
        an_fp: str, a_file_separator: str, dtype: Optional[str] = None) -> \
        pandas.DataFrame:
    """Load a DataFrame from a file, trying multiple encodings.

    Attempts to load the file using various common encodings (utf-8, utf-8-sig,
    iso-8859-1, latin1, cp1252) until successful.

    Parameters
    ----------
    an_fp : str
        Path to the file to load.
    a_file_separator : str
        Separator character used in the file (e.g., ',' for CSV).
    dtype : Optional[str]
        Data type to use for the DataFrame. If None, pandas will infer types.

    Returns
    -------
    pandas.DataFrame
        DataFrame loaded from the file.

    Raises
    ------
    ValueError
        If the file cannot be decoded with any of the available encodings.
    """
    result = None

    # from https://stackoverflow.com/a/76366653
    encodings = ["utf-8", "utf-8-sig", "iso-8859-1", "latin1", "cp1252"]
    for encoding in encodings:
        # noinspection PyBroadException
        try:
            result = pandas.read_csv(
                an_fp, sep=a_file_separator, encoding=encoding, dtype=dtype)
            break
        except Exception:  # noqa: E722
            pass

    if result is None:
        raise ValueError(f"Unable to decode {an_fp} "
                         f"with any available encoder")

    return result


def validate_required_columns_exist(
        input_df: pandas.DataFrame, required_cols_list: List[str],
        error_msg: str) -> None:
    """Validate that a DataFrame contains all required columns.

    Parameters
    ----------
    input_df : pandas.DataFrame
        DataFrame to validate.
    required_cols_list : List[str]
        List of column names that must be present in the DataFrame.
    error_msg : str
        Error message to be raised if any required columns are missing.

    Raises
    ------
    ValueError
        If any of the required columns are missing from the DataFrame.
    """
    missing_cols = set(required_cols_list) - set(input_df.columns)
    if len(missing_cols) > 0:
        missing_cols = sorted(missing_cols)
        raise ValueError(
            f"{error_msg}: {missing_cols}")


def get_extension(sep: str) -> str:
    """Get the appropriate file extension based on the separator character.

    Parameters
    ----------
    sep : str
        Separator character used in the file.

    Returns
    -------
    str
        File extension: 'csv' for comma-separated files, 'txt' for others.
    """
    return "csv" if sep == "," else "txt"


def update_metadata_df_field(
        metadata_df: pandas.DataFrame, field_name: str,
        field_val_or_func: Union[
            str, Callable[[pandas.Series, List[str]], str]],
        source_fields: Optional[List[str]] = None,
        overwrite_non_nans: bool = True) -> None:
    """Update or add a field in an existing metadata DataFrame.

    Can update an existing field or add a new one, using either a constant
    value or a function to compute values based on other fields.


    Parameters
    ----------
    metadata_df : pandas.DataFrame
        DataFrame to update. Modified in place.
    field_name : str
        Name of the field to update or add.
    field_val_or_func : Union[str, Callable]
        Either a constant value to set, or a function that takes a row and
        source fields as input and returns a value.
    source_fields : Optional[List[str]]
        List of field names to use as input for the function. Required if
        field_val_or_func is a function.
    overwrite_non_nans : bool
        If True, overwrites all values in the field. If False, only updates
        NaN values.
    """
    # Note: function doesn't return anything.  Work is done in-place on the
    #  metadata_df passed in.

    TEMP_COL_SUFFIX = "___metameq___temp"

    # pandas has hard-to-predict behavior when setting values in a DataFrame
    # (such as turning a int input value into a float column even when setting
    # for all values in df so there are no NaNs).  To avoid this, we convert
    # all non-NaN values to strings before setting them.  The validator code
    # casts values to the expected type before validating them so this won't
    # impede validation. We leave NaNs as-is so they can be caught by the
    # downstream default-filling logic.
    def turn_non_nans_to_str(val: Any) -> Any:
        """Convert non-NaN values to strings."""
        return str(val) if pandas.notna(val) else val

    # if the field already exists in the metadata, make a temporary copy of it
    # with a different name; we will set values on this rather than on the
    # original in case the original uses itself as a source field
    field_to_set = field_name
    if field_name in metadata_df.columns:
        field_to_set = f"{field_name}{TEMP_COL_SUFFIX}"
        metadata_df[field_to_set] = metadata_df[field_name]

    try:
        # If the field does not already exist in the metadata OR if we have
        # been told to overwrite existing (i.e., non-NaN) values, we will set its
        # value in all rows; otherwise, will only set it where it is currently NaN
        set_all = overwrite_non_nans or (field_name not in metadata_df.columns)
        row_mask = \
            metadata_df.index if set_all else metadata_df[field_name].isnull()

        # If source fields were passed in, the field_val_or_func must be a function
        if source_fields:
            # Apply only to masked rows to avoid overhead of running func
            # on rows that won't be updated; pandas aligns the result back
            # to the correct rows by matching on the index
            metadata_df.loc[row_mask, field_to_set] = \
                metadata_df.loc[row_mask].apply(
                    lambda row: turn_non_nans_to_str(
                        field_val_or_func(row, source_fields)),
                    axis=1)
        else:
            # Otherwise, it is a constant value
            metadata_df.loc[row_mask, field_to_set] = \
                turn_non_nans_to_str(field_val_or_func)
        # endif using a function/a constant value

        # if field already existed and we set values in a temporary column,
        # copy the set values back to the original column and drop the temp column
        if field_to_set != field_name:
            metadata_df[field_name] = metadata_df[field_to_set]
    finally:
        # if we created a temporary column, drop it, even if an error was raised during setting
        if field_to_set != field_name:
            metadata_df.drop(columns=[field_to_set], inplace=True)


def _try_cast_to_int(raw_field_val):
    """Attempt to cast a value to integer without losing information.

    Converts values to int only if the conversion is lossless:
    - Float-formatted strings like "42.0" are accepted
    - Floats with non-zero decimals like "42.7" are rejected
    - Actual integers pass through

    Parameters
    ----------
    raw_field_val : any
        The value to attempt to cast to integer.

    Returns
    -------
    int or None
        The integer value if casting succeeds, None otherwise.
    """
    # If already an int (but not a bool, which is a subclass of int),
    # return it directly to avoid precision loss from float conversion
    if isinstance(raw_field_val, int) and not isinstance(raw_field_val, bool):
        return raw_field_val

    # If it's a string, try int() directly first to avoid precision loss
    # from float conversion for large integers beyond 2^53
    if isinstance(raw_field_val, str):
        try:
            return int(raw_field_val)
        except (ValueError, TypeError):
            pass

    # Fall back to float path for "42.0"-style strings and actual floats
    try:
        float_val = float(raw_field_val)
        if isinstance(float_val, float) and float_val.is_integer():
            return int(float_val)
    except Exception:  # noqa: E722
        pass
    return None


def _try_cast_to_bool(raw_field_val):
    """Attempt to cast a value to boolean with strict validation.

    Only accepts values that clearly represent boolean intent:
    - Actual bool values pass through
    - Numeric 0 or 1 (int or float) convert to False/True
    - String representations: 'true', 't', 'yes', 'y', '1' for True;
      'false', 'f', 'no', 'n', '0' for False (case-insensitive)

    Parameters
    ----------
    raw_field_val : any
        The value to attempt to cast to boolean.

    Returns
    -------
    bool or None
        The boolean value if casting succeeds, None otherwise.
    """
    if isinstance(raw_field_val, bool):
        return raw_field_val

    if isinstance(raw_field_val, (int, float)) and \
            (raw_field_val == 0 or raw_field_val == 1):
        return bool(raw_field_val)

    if isinstance(raw_field_val, str):
        stripped = raw_field_val.strip().lower()
        if stripped in ('true', 't', 'yes', 'y', '1'):
            return True
        if stripped in ('false', 'f', 'no', 'n', '0'):
            return False

    return None


def cast_field_to_type(raw_field_val, allowed_pandas_types):
    """Cast a field value to one of the allowed Python types.

    Attempts to cast the raw field value to each type in allowed_pandas_types
    in order, returning the first successful cast. This allows flexible type
    coercion where a value might be validly interpreted as multiple types.

    Parameters
    ----------
    raw_field_val : any
        The raw value to cast.
    allowed_pandas_types : list
        A list of Python type callables (e.g., str, int, float) to attempt
        casting to, in order of preference.

    Returns
    -------
    any
        The field value cast to the first successfully matched type.

    Raises
    ------
    ValueError
        If the value cannot be cast to any of the allowed types.
    """
    typed_field_val = None
    for curr_type in allowed_pandas_types:
        if curr_type is int:
            typed_field_val = _try_cast_to_int(raw_field_val)
            if typed_field_val is not None:
                break
        elif curr_type is bool:
            typed_field_val = _try_cast_to_bool(raw_field_val)
            if typed_field_val is not None:
                break
        else:
            # noinspection PyBroadException
            try:
                typed_field_val = curr_type(raw_field_val)
                break
            except Exception:  # noqa: E722
                pass
    # next allowed type

    if typed_field_val is None:
        raise ValueError(
            f"Unable to cast '{raw_field_val}' to any of the allowed "
            f"types: {allowed_pandas_types}")

    return typed_field_val
