import pandas
from dateutil import parser


def pass_through(row, source_fields):
    return _get_one_source_field(row, source_fields, "pass_through")


def transform_sex_at_birth_to_sex(row, source_fields):
    x = _get_one_source_field(
        row, source_fields, "transform_sex_at_birth_to_sex")

    if pandas.isnull(x):
        return x
    if "Female" in x:
        return "female"
    # NB: gotta test male second so don't get false pos on fe*male*
    if "Male" in x:
        return "male"

    # if we got here, none of our checks recognized the sex at birth value
    raise ValueError(f"Unrecognized sex: {x}")


def transform_age_to_life_stage(row, source_fields):
    x = _get_one_source_field(
        row, source_fields, "transform_age_to_life_stage")

    # NB: Input age is assumed to be in years.  Because of this, this function
    # does NOT attempt to identify neonates--children aged 0-6 *weeks*. All
    # ages under 17 are considered "child".

    if pandas.isnull(x):
        return x

    try:
        x = int(x)
    except ValueError:
        raise ValueError(f"{source_fields[0]} must be an integer")

    if x < 17:
        return "child"
    return "adult"


def format_a_datetime(row, source_fields):
    x = _get_one_source_field(
        row, source_fields, "format_a_datetime")

    if pandas.isnull(x):
        return x
    if hasattr(x, "strftime"):
        strftimeable_x = x
    else:
        try:
            strftimeable_x = parser.parse(x)
        except:
            raise ValueError(f"{source_fields[0]} cannot be parsed to a date")

    formatted_x = strftimeable_x.strftime('%Y-%m-%d %H:%M')
    return formatted_x


def _get_one_source_field(row, source_fields, func_name):
    if len(source_fields) != 1:
        raise ValueError(f"{func_name} requires exactly one source field")
    return row[source_fields[0]]
