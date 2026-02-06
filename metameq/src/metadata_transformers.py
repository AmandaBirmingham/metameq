import pandas
from dateutil import parser
from typing import Any, Dict, List, Union
from datetime import datetime
from metameq.src.util import cast_field_to_type


# individual transformer functions
def pass_through(row: pandas.Series, source_fields: List[str]) -> Any:
    """Pass through a value from a source field without transformation.

    Parameters
    ----------
    row : pandas.Series
        Row of data containing the source field.
    source_fields : List[str]
        List containing exactly one source field name.

    Returns
    -------
    Any
        The value from the source field.

    Raises
    ------
    ValueError
        If source_fields does not contain exactly one field name.
    """
    return _get_one_source_field(row, source_fields, "pass_through")


def transform_input_sex_to_std_sex(row: pandas.Series, source_fields: List[str]) -> str:
    """Transform input sex value to standardized sex value.

    Parameters
    ----------
    row : pandas.Series
        Row of data containing the source field.
    source_fields : List[str]
        List containing exactly one source field name.

    Returns
    -------
    str
        Standardized sex value: 'female', 'male', 'intersex', or 'not provided'.

    Raises
    ------
    ValueError
        If source_fields does not contain exactly one field name.
        If the input sex value is not recognized.
    """
    x = _get_one_source_field(
        row, source_fields, "standardize_input_sex")

    return standardize_input_sex(x)


def transform_age_to_life_stage(row: pandas.Series, source_fields: List[str]) -> str:
    """Transform age in years to life stage category.

    Note: Input age is assumed to be in years. Because of this, this function
    does NOT attempt to identify neonates--children aged 0-6 *weeks*. All
    ages under 17 are considered "child".

    Parameters
    ----------
    row : pandas.Series
        Row of data containing the source field.
    source_fields : List[str]
        List containing exactly one source field name.

    Returns
    -------
    str
        Life stage category: 'child' for ages < 17, 'adult' for ages >= 17.

    Raises
    ------
    ValueError
        If source_fields does not contain exactly one field name.
        If the age value is not convertable to an integer.
    """
    x = _get_one_source_field(
        row, source_fields, "transform_age_to_life_stage")
    return set_life_stage_from_age_yrs(x, source_fields[0])


def transform_date_to_formatted_date(row: pandas.Series, source_fields: List[str]) -> str:
    """Transform date to standardized format (YYYY-MM-DD HH:MM).

    Parameters
    ----------
    row : pandas.Series
        Row of data containing the source field.
    source_fields : List[str]
        List containing exactly one source field name.

    Returns
    -------
    str
        Formatted date string in YYYY-MM-DD HH:MM format.

    Raises
    ------
    ValueError
        If source_fields does not contain exactly one field name.
        If the source field cannot be parsed as a date.
    """
    x = _get_one_source_field(
        row, source_fields, "transform_date_to_formatted_date")
    return format_a_datetime(x, source_fields[0])


def transform_format_field_as_int(
        row: pandas.Series, source_fields: List[str]) -> str:
    """Transform a field to an integer format.

    Parameters
    ----------
    row : pandas.Series
        Row of data containing the source field.
    source_fields : List[str]
        List containing exactly one source field name.

    Returns
    -------
    str
        The value from the source field formatted as an integer string.

    Raises
    ------
    ValueError
        If source_fields does not contain exactly one field name.
    """
    return _format_field_val(row, source_fields, int, '{0:d}')


def transform_format_field_as_location(row: pandas.Series, source_fields: List[str]) -> str:
    """Transform a field to a float format for a location (latitude, longitude, elevation).

    Parameters
    ----------
    row : pandas.Series
        Row of data containing the source field.
    source_fields : List[str]
        List containing exactly one source field name.

    Returns
    -------
    str
        The value from the source field formatted as a location string.

    Raises
    ------
    ValueError
        If source_fields does not contain exactly one field name.
    """

    # Use a high precision format string to turn the input value into
    # a string without being forced into scientific notation
    result = _format_field_val(row, source_fields, float, None)
    # if the result is a string
    if isinstance(result, str):
        # Strip any trailing zeros and any subsequently unnecessary decimal point
        result = result.rstrip('0').rstrip('.')
    return result


def help_transform_mapping(
        row: pandas.Series,
        source_fields: List[str],
        mapping: Dict[str, Any],
        field_name: str = "help_transform_mapping") -> Any:
    """Transform a value using a provided mapping dictionary.

    Parameters
    ----------
    row : pandas.Series
        Row of data containing the source field.
    source_fields : List[str]
        List containing exactly one source field name.
    mapping : Dict[str, Any]
        Dictionary mapping input values to output values.
    field_name : str, optional
        Name of the field being transformed, used in error messages.
        Defaults to "help_transform_mapping".

    Returns
    -------
    Any
        The mapped value from the mapping dictionary.

    Raises
    ------
    ValueError
        If source_fields does not contain exactly one field name.
        If the input value is not found in the mapping dictionary.
    """
    x = _get_one_source_field(
        row, source_fields, field_name)

    return _help_transform_mapping(x, mapping, field_name)


# helper functions
def standardize_input_sex(input_val: str) -> str:
    """Standardize sex input to Qiita standard values.

    Parameters
    ----------
    input_val : str
        Input sex value to standardize.

    Returns
    -------
    str
        Standardized sex value: 'female', 'male', 'intersex', or 'not provided'.

    Raises
    ------
    ValueError
        If the input sex value is not recognized.
    """
    qiita_standard_female = "female"
    qiita_standard_male = "male"
    qiita_standard_intersex = "intersex"

    sex_mapping = {
        "female": qiita_standard_female,
        "f": qiita_standard_female,
        "male": qiita_standard_male,
        "m": qiita_standard_male,
        "intersex": qiita_standard_intersex,
        "prefernottoanswer": "not provided"
    }

    standardized_sex = _help_transform_mapping(
        input_val, sex_mapping, "sex", make_lower=True)
    return standardized_sex


def set_life_stage_from_age_yrs(age_in_yrs: Union[float, int], source_name: str = "input") -> str:
    """Convert age in years to life stage category.

    Note: Input age is assumed to be in years. Because of this, this function
    does NOT attempt to identify neonates--children aged 0-6 *weeks*. All
    ages under 17 are considered "child".

    Parameters
    ----------
    age_in_yrs : Union[float, int]
        Age in years.
    source_name : str, optional
        Name of the source field, used in error messages.
        Defaults to "input".

    Returns
    -------
    str
        Life stage category: 'child' for ages < 17, 'adult' for ages >= 17.

    Raises
    ------
    ValueError
        If age_in_yrs is not null or convertable to an integer.
    """
    if pandas.isnull(age_in_yrs):
        return age_in_yrs

    try:
        x = int(age_in_yrs)
    except ValueError:
        raise ValueError(f"{source_name} must be an integer")

    if x < 17:
        return "child"
    return "adult"


def format_a_datetime(x: Union[str, datetime, None], source_name: str = "input") -> str:
    """Format a datetime value to YYYY-MM-DD HH:MM string format.

    Parameters
    ----------
    x : Union[str, datetime, None]
        Input datetime value to format.
    source_name : str, optional
        Name of the source field, used in error messages.
        Defaults to "input".

    Returns
    -------
    str
        Formatted datetime string in YYYY-MM-DD HH:MM format.

    Raises
    ------
    ValueError
        If the input cannot be parsed as a datetime.
    """
    if pandas.isnull(x):
        return x
    if hasattr(x, "strftime"):
        strftimeable_x = x
    else:
        try:
            strftimeable_x = parser.parse(x)
        except:  # noqa: E722
            raise ValueError(f"{source_name} cannot be parsed to a date")

    formatted_x = strftimeable_x.strftime('%Y-%m-%d %H:%M')
    return formatted_x


def _get_one_source_field(row: pandas.Series, source_fields: List[str], func_name: str) -> Any:
    """Get a single source field value from a row of data.

    Parameters
    ----------
    row : pandas.Series
        Row of data containing the source field.
    source_fields : List[str]
        List of source field names.
    func_name : str
        Name of the calling function, used in error messages.

    Returns
    -------
    Any
        The value from the source field.

    Raises
    ------
    ValueError
        If source_fields does not contain exactly one field name.
    """
    if len(source_fields) != 1:
        raise ValueError(f"{func_name} requires exactly one source field")
    return row[source_fields[0]]


def _help_transform_mapping(
        input_val: Any,
        mapping: Dict[str, Any],
        field_name: str = "value",
        make_lower: bool = False) -> Any:
    """Transform a value using a mapping dictionary.

    Parameters
    ----------
    input_val : Any
        Input value to transform.
    mapping : Dict[str, Any]
        Dictionary mapping input values to output values.
    field_name : str, optional
        Name of the field being transformed, used in error messages.
        Defaults to "value".
    make_lower : bool, optional
        Whether to convert input to lowercase before mapping.
        Defaults to False.

    Returns
    -------
    Any
        The mapped value from the mapping dictionary.

    Raises
    ------
    ValueError
        If the input value is not found in the mapping dictionary.
    """
    if pandas.isnull(input_val):
        return input_val

    if make_lower:
        input_val = input_val.lower()

    if input_val in mapping:
        return mapping[input_val]
    raise ValueError(f"Unrecognized {field_name}: {input_val}")


def _format_field_val(row, source_fields, field_type, format_string=None):
    """Format a field value by casting to a type and optionally applying a format string.

    Parameters
    ----------
    row : pandas.Series
        Row of data containing the source field.
    source_fields : List[str]
        List containing exactly one source field name.
    field_type : type
        Type to cast the value to (e.g., int, float, bool).
    format_string : str, optional
        Format string to apply (e.g., '{0:d}', '{0:.2f}', '{0:.6g}').
        Defaults to None.

    Returns
    -------
    str
        The formatted value as a string. Returns the original value as a string
        if casting fails, or the cast value as a string if formatting fails.

    Raises
    ------
    ValueError
        If source_fields does not contain exactly one field name.
    """
    x = _get_one_source_field(row, source_fields, "format_field_val")

    if pandas.isnull(x):
        return x

    # TODO: be aware that calling str() on a float that is very small
    # very large or has many decimal places will turn it into as str of
    # scientific notation, which may not be expected.  Should there
    # be some sort of handling for this case?
    result = str(x)
    try:
        result = cast_field_to_type(x, [field_type])
    except ValueError:
        # if it can't be cast to the specified type,
        # just return string version of the original value
        return result

    if format_string is not None:
        try:
            result = format_string.format(result)
        except ValueError:
            # if it CAN be cast but can't be formatted, return the cast value
            # (e.g., if it's a float but the format string is for an int)
            pass

    return str(result)
