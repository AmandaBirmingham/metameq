import cerberus
import copy
from datetime import datetime
from dateutil import parser
import logging
import os
from pathlib import Path
from metameq.src.util import SAMPLE_NAME_KEY, get_extension

_TYPE_KEY = "type"
_ANYOF_KEY = "anyof"

# Define a logger for this module
logger = logging.getLogger(__name__)


class MetameqValidator(cerberus.Validator):
    """Custom cerberus Validator with metameq-specific validation rules.

    Extends the cerberus Validator class to add custom check_with rules
    for validating metadata fields according to metameq requirements.
    Custom rules are invoked by including "check_with" in a field's
    cerberus schema definition.

    See Also
    --------
    https://docs.python-cerberus.org/customize.html
    """

    def _check_with_date_not_in_future(self, field, value):
        """Validate that a date field value is not in the future.

        This method is automatically invoked by cerberus when a field's schema
        includes "check_with": "date_not_in_future". It parses the value as a
        date and validates that it is not after the current date/time.

        Parameters
        ----------
        field : str
            The name of the field being validated.
        value : str
            The date string to validate.

        Notes
        -----
        Adds a validation error if:
        - The value cannot be parsed as a valid date
        - The parsed date is in the future
        """
        # convert the field string to a date
        try:
            putative_date = parser.parse(value, fuzzy=True, dayfirst=False)
        except Exception:  # noqa: E722
            self._error(field, "Must be a valid date")
            return

        if putative_date > datetime.now():
            self._error(field, "Date cannot be in the future")


def validate_metadata_df(metadata_df, sample_type_full_metadata_fields_dict):
    """Validate a metadata DataFrame against a field definition schema.

    Converts the metadata fields dictionary into a cerberus schema, casts
    each field in the DataFrame to its expected type, and validates all rows
    against the schema. Fields defined in the schema but missing from the
    DataFrame are logged and skipped.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        The metadata DataFrame to validate. Must contain a SAMPLE_NAME_KEY
        columnfor identifying samples in validation error messages.
    sample_type_full_metadata_fields_dict : dict
        A dictionary defining metadata fields and their validation rules.
        May contain metameq-specific keys (is_phi, field_desc, units,
        min_exclusive, unique) which will be stripped before cerberus
        validation, as well as standard cerberus keys (type, required,
        allowed, regex, etc.).

    Returns
    -------
    list
        A list of dictionaries containing validation errors. Each dictionary
        contains SAMPLE_NAME_KEY, "field_name", and "error_message" keys.
        Returns an empty list if all rows pass validation.
    """
    config = _make_cerberus_schema(sample_type_full_metadata_fields_dict)

    # NB: typed_metadata_df (the type-cast version of metadata_df) is only
    # used for generating validation messages, after which it is discarded.
    typed_metadata_df = metadata_df.copy()
    for curr_field, curr_definition in \
            sample_type_full_metadata_fields_dict.items():

        if curr_field not in typed_metadata_df.columns:
            logging.info(
                f"Standard field {curr_field} not in metadata file")
            continue

        curr_allowed_types = _get_allowed_pandas_types(
            curr_field, curr_definition)
        typed_metadata_df[curr_field] = typed_metadata_df[curr_field].apply(
            lambda x: _cast_field_to_type(x, curr_allowed_types))
    # next field in config

    validation_msgs = _generate_validation_msg(typed_metadata_df, config)
    return validation_msgs


def output_validation_msgs(validation_msgs_df, out_dir, out_base, sep="\t",
                           suppress_empty_fails=False):
    """Write validation messages to a timestamped file.

    Outputs the validation messages DataFrame to a file with a timestamp prefix.
    If the DataFrame is empty and suppress_empty_fails is False, creates an empty
    file. If suppress_empty_fails is True and the DataFrame is empty, no file is
    created.

    Parameters
    ----------
    validation_msgs_df : pandas.DataFrame
        DataFrame containing validation error messages.
    out_dir : str
        Directory where the output file will be written.
    out_base : str
        Base name for the output file. The full filename will be
        "{timestamp}_{out_base}_validation_errors.{extension}".
    sep : str, default="\t"
        Separator to use in the output file. Determines file extension
        (tab -> .txt, comma -> .csv).
    suppress_empty_fails : bool, default=False
        If True, no file is created when validation_msgs_df is empty.
        If False, an empty file is created when there are no validation errors.
    """
    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    extension = get_extension(sep)
    out_fp = os.path.join(
        out_dir, f"{timestamp_str}_{out_base}_validation_errors.{extension}")

    if validation_msgs_df.empty:
        if not suppress_empty_fails:
            Path(out_fp).touch()
        # else, just do nothing
    else:
        validation_msgs_df.to_csv(out_fp, sep=sep, index=False)


def _make_cerberus_schema(sample_type_metadata_dict):
    """Convert a metadata fields dictionary into a cerberus-compatible validation schema.

    Creates a deep copy of the input dictionary and removes keys that are not
    recognized by the cerberus validation library (is_phi, field_desc, units,
    min_exclusive, unique). The resulting dictionary can be used directly with
    cerberus for validation.

    Parameters
    ----------
    sample_type_metadata_dict : dict
        A dictionary containing metadata field definitions, potentially including
        keys that are not recognized by cerberus.

    Returns
    -------
    dict
        A cerberus-compatible schema with unrecognized keys removed.
    """
    unrecognized_keys = ['is_phi', 'field_desc', 'units',
                         'min_exclusive', 'unique']
    # traverse the host_fields_config dict and remove any keys that are not
    # recognized by cerberus
    cerberus_config = copy.deepcopy(sample_type_metadata_dict)
    cerberus_config = _remove_leaf_keys_from_dict(
        cerberus_config, unrecognized_keys)

    return cerberus_config


def _remove_leaf_keys_from_dict(input_dict, keys_to_remove):
    """Remove specified leaf keys from a dictionary, recursively processing nested structures.

    Traverses the input dictionary and removes any keys with non-dict, non-list (leaf) values
    that are in the keys_to_remove list. Keys with dict or list values are always
    preserved (even if they match one of the keys_to_remove), with their contents recursively
    processed. For lists, delegates to _remove_leaf_keys_from_dict_in_list.
    Non-dict, non-list values are deep-copied if their key is not being removed.

    Parameters
    ----------
    input_dict : dict
        The dictionary to process.
    keys_to_remove : list
        List of key names to remove from the dictionary and any nested dicts.
        Only keys with non-dict, non-list values will be removed.

    Returns
    -------
    dict
        A new dictionary with the specified leaf keys removed at all nesting levels.
    """
    output_dict = {}
    for curr_key, curr_val in input_dict.items():
        if isinstance(curr_val, dict):
            output_dict[curr_key] = \
                _remove_leaf_keys_from_dict(curr_val, keys_to_remove)
        elif isinstance(curr_val, list):
            output_dict[curr_key] = \
                _remove_leaf_keys_from_dict_in_list(curr_val, keys_to_remove)
        else:
            if curr_key not in keys_to_remove:
                output_dict[curr_key] = copy.deepcopy(curr_val)
    return output_dict


def _remove_leaf_keys_from_dict_in_list(input_list, keys_to_remove):
    """Remove specified leaf keys from all dictionaries within a list.

    Recursively processes the input list and removes any keys with non-dict, non-list (leaf)
    values that are in the keys_to_remove list from any dictionaries found using
    _remove_leaf_keys_from_dict. Handles nested lists and dictionaries at any depth.
    Non-dict, non-list items are preserved unchanged.

    Parameters
    ----------
    input_list : list
        The list to process. May contain dicts, nested lists, or other values.
    keys_to_remove : list
        List of key names to remove from any dictionaries found.
        Only keys with non-dict, non-list values will be removed.

    Returns
    -------
    list
        A new list with the specified leaf keys removed from all contained dicts.
    """
    output_list = []
    for curr_val in input_list:
        if isinstance(curr_val, dict):
            output_list.append(
                _remove_leaf_keys_from_dict(curr_val, keys_to_remove))
        elif isinstance(curr_val, list):
            output_list.append(
                _remove_leaf_keys_from_dict_in_list(curr_val, keys_to_remove))
        else:
            output_list.append(curr_val)
    return output_list


def _cast_field_to_type(raw_field_val, allowed_pandas_types):
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


def _get_allowed_pandas_types(field_name, field_definition):
    """Extract allowed Python types from a cerberus field definition.

    Reads the type specification from a cerberus field definition and converts
    the cerberus type names to their corresponding Python types. Handles both
    single-type definitions (using "type" key) and multiple-type definitions
    (using "anyof" key with a list of type options).

    Parameters
    ----------
    field_name : str
        The name of the field being processed. Used only for error messages.
    field_definition : dict
        A cerberus field definition dictionary containing either a "type" key
        with a single type name, or an "anyof" key with a list of type options.

    Returns
    -------
    list
        A list of Python type callables (str, int, float, bool, or datetime.date)
        corresponding to the allowed cerberus types for this field.

    Raises
    ------
    ValueError
        If the field definition contains neither a "type" nor an "anyof" key.
    """
    cerberus_to_python_types = {
        "string": str,
        "integer": int,
        "float": float,
        "number": float,
        "bool": bool,
        "datetime": datetime.date}

    allowed_cerberus_types = []
    if _TYPE_KEY in field_definition:
        allowed_cerberus_types.append(field_definition.get(_TYPE_KEY))
    elif _ANYOF_KEY in field_definition:
        for curr_allowed_type_entry in field_definition[_ANYOF_KEY]:
            allowed_cerberus_types.append(
                curr_allowed_type_entry[_TYPE_KEY])
        # next anyof entry
    else:
        raise ValueError(
            f"Unable to find type definition for field '{field_name}'")
    # if type or anyof key in definition

    allowed_pandas_types = \
        [cerberus_to_python_types[x] for x in allowed_cerberus_types]
    return allowed_pandas_types


def _generate_validation_msg(typed_metadata_df, config):
    """Generate validation error messages for a metadata DataFrame.

    Validates each row of the metadata DataFrame against the provided cerberus
    schema configuration and collects any validation errors into a list of
    dictionaries.

    Parameters
    ----------
    typed_metadata_df : pandas.DataFrame
        A metadata DataFrame with values already cast to their expected types.
        Must contain a SAMPLE_NAME_KEY column for identifying samples.
    config : dict
        A cerberus-compatible validation schema dictionary defining the
        validation rules for each metadata field.

    Returns
    -------
    list
        A list of dictionaries, where each dictionary contains:
        - SAMPLE_NAME_KEY: The sample name for the row with the error
        - "field_name": The name of the field that failed validation
        - "error_message": The validation error message(s) from cerberus as a list of strings
        Returns an empty list if all rows pass validation.
    """
    v = MetameqValidator()
    v.allow_unknown = True

    validation_msgs = []
    raw_metadata_dict = typed_metadata_df.to_dict(orient="records")
    for _, curr_row in enumerate(raw_metadata_dict):
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
