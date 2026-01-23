import copy
from importlib.resources import files
import pandas
from typing import List, Optional, Union, Callable
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


def extract_config_dict(
        config_fp: Union[str, None]) -> dict:
    """Extract configuration dictionary from a YAML file.

    If no config file path is provided, looks for config.yml in the grandparent
    directory of the starting file path or current file.

    Parameters
    ----------
    config_fp : Union[str, None]
        Path to the configuration YAML file. If None, will look for config.yml
        in the "config" module of the package.

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
    return extract_config_dict(stds_fp)


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

    Can update an existing field or add a new one, using either a constant value
    or a function to compute values based on other fields.


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

    # If the field does not already exist in the metadata OR if we have
    # been told to overwrite existing (i.e., non-NaN) values, we will set its
    # value in all rows; otherwise, will only set it where it is currently NaN
    set_all = overwrite_non_nans or (field_name not in metadata_df.columns)
    row_mask = \
        metadata_df.index if set_all else metadata_df[field_name].isnull()

    # If source fields were passed in, the field_val_or_func must be a function
    if source_fields:
        metadata_df.loc[row_mask, field_name] = \
            metadata_df.apply(
                lambda row: field_val_or_func(row, source_fields),
                axis=1)
    else:
        # Otherwise, it is a constant value
        metadata_df.loc[row_mask, field_name] = field_val_or_func
    # endif using a function/a constant value
