import cerberus
import copy
from datetime import datetime
from dateutil import parser
import os
import pandas
from pathlib import Path
from qiimp.src.util import SAMPLE_NAME_KEY, get_extension

_TYPE_KEY = "type"
_ANYOF_KEY = "anyof"


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


def validate_metadata_df(metadata_df, sample_type_full_metadata_fields_dict):
    config = _make_cerberus_schema(sample_type_full_metadata_fields_dict)

    # NB: typed_metadata_df (the type-cast version of metadata_df) is only
    # used for generating validation messages, after which it is discarded.
    typed_metadata_df = metadata_df.copy()
    for curr_field, curr_definition in \
            sample_type_full_metadata_fields_dict.items():

        if curr_field not in typed_metadata_df.columns:
            # TODO: decide whether to make this a warning or take out
            print(f"Field {curr_field} not in metadata file")
            continue

        curr_allowed_types = _get_allowed_pandas_types(
            curr_field, curr_definition)
        typed_metadata_df[curr_field] = typed_metadata_df[curr_field].apply(
            lambda x: _cast_field_to_type(x, curr_allowed_types))
    # next field in config

    validation_msgs = _generate_validation_msg(typed_metadata_df, config)
    return validation_msgs


def output_validation_msgs(validation_msgs, out_dir, out_base, sep="\t",
                           suppress_empty_fails=False):
    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    extension = get_extension(sep)
    out_fp = os.path.join(
        out_dir, f"{timestamp_str}_{out_base}_validation_errors.{extension}")
    msgs_df = pandas.DataFrame(validation_msgs)
    if msgs_df.empty:
        if not suppress_empty_fails:
            Path(out_fp).touch()
        # else, just do nothing
    else:
        msgs_df.to_csv(out_fp, sep=sep, index=False)


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


def _cast_field_to_type(raw_field_val, allowed_pandas_types):
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
    v = QiimpValidator()
    v.allow_unknown = True

    validation_msgs = []
    raw_metadata_dict = typed_metadata_df.to_dict(orient="records")
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
