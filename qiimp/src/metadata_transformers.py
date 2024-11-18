import pandas
from dateutil import parser


# individual transformer functions
def pass_through(row, source_fields):
    return _get_one_source_field(row, source_fields, "pass_through")


def transform_input_sex_to_std_sex(row, source_fields):
    x = _get_one_source_field(
        row, source_fields, "transform_sex_at_birth_to_sex")

    return standardize_input_sex(x)


def transform_age_to_life_stage(row, source_fields):
    # NB: Input age is assumed to be in years.  Because of this, this function
    # does NOT attempt to identify neonates--children aged 0-6 *weeks*. All
    # ages under 17 are considered "child".
    x = _get_one_source_field(
        row, source_fields, "transform_age_to_life_stage")
    return set_life_stage_from_age_yrs(x, source_fields[0])


def transform_date_to_formatted_date(row, source_fields):
    x = _get_one_source_field(
        row, source_fields, "transform_age_to_life_stage")
    return format_a_datetime(x, source_fields[0])


def help_transform_mapping(row, source_fields, mapping,
                           field_name="help_transform_mapping"):
    x = _get_one_source_field(
        row, source_fields, field_name)

    return _help_transform_mapping(x, mapping, field_name)


# helper functions
def standardize_input_sex(input_val):
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


def set_life_stage_from_age_yrs(age_in_yrs, source_name="input"):
    # NB: Input age is assumed to be in years.  Because of this, this function
    # does NOT attempt to identify neonates--children aged 0-6 *weeks*. All
    # ages under 17 are considered "child".

    if pandas.isnull(age_in_yrs):
        return age_in_yrs

    try:
        x = int(age_in_yrs)
    except ValueError:
        raise ValueError(f"{source_name} must be an integer")

    if x < 17:
        return "child"
    return "adult"


def format_a_datetime(x, source_name="input"):
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


def _get_one_source_field(row, source_fields, func_name):
    if len(source_fields) != 1:
        raise ValueError(f"{func_name} requires exactly one source field")
    return row[source_fields[0]]


def _help_transform_mapping(
        input_val, mapping, field_name="value", make_lower=False):
    if pandas.isnull(input_val):
        return input_val

    if make_lower:
        input_val = input_val.lower()

    if input_val in mapping:
        return mapping[input_val]
    raise ValueError(f"Unrecognized {field_name}: {input_val}")


def _format_field_val(row, source_fields, field_type, format_string):
    x = _get_one_source_field(row, source_fields, "format_field_val")

    result = x
    # format string should be something like '{0:g}' or '{0:.2f}'
    # field type should be something like float or int
    if isinstance(x, field_type):
        result = format_string.format(x)
    return result
