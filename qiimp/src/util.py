import copy
import os
import pandas
from typing import List
import yaml

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


def extract_config_dict(config_fp, starting_fp=None):
    if config_fp is None:
        grandparent_dir = _get_grandparent_dir(starting_fp)
        config_fp = os.path.join(grandparent_dir, "config.yml")

    # read in config file
    config_dict = extract_yaml_dict(config_fp)
    return config_dict


def _get_grandparent_dir(starting_fp=None):
    if starting_fp is None:
        starting_fp = __file__
    curr_dir = os.path.dirname(os.path.abspath(starting_fp))
    grandparent_dir = os.path.join(curr_dir, os.pardir, os.pardir)
    return grandparent_dir


def extract_yaml_dict(yaml_fp):
    with open(yaml_fp, "r") as f:
        yaml_dict = yaml.safe_load(f)
    return yaml_dict


def extract_stds_config(stds_fp):
    if not stds_fp:
        stds_fp = os.path.join(_get_grandparent_dir(), "standards.yml")
    return extract_config_dict(stds_fp)


def deepcopy_dict(input_dict):
    output_dict = {}
    for curr_key, curr_val in input_dict.items():
        if isinstance(curr_val, dict):
            output_dict[curr_key] = deepcopy_dict(curr_val)
        else:
            output_dict[curr_key] = copy.deepcopy(curr_val)
    return output_dict


def load_df_with_best_fit_encoding(an_fp, a_file_separator, dtype=None):
    result = None

    # from https://stackoverflow.com/a/76366653
    encodings = ["utf-8", "utf-8-sig", "iso-8859-1", "latin1", "cp1252"]
    for encoding in encodings:
        try:
            result = pandas.read_csv(
                an_fp, sep=a_file_separator, encoding=encoding, dtype=dtype)
            break
        except Exception:
            pass

    if result is None:
        raise ValueError(f"Unable to decode {an_fp} "
                         f"with any available encoder")

    return result


def validate_required_columns_exist(
        input_df: pandas.DataFrame,
        required_cols_list: List[str],
        error_msg: str):

    """Checks that the input dataframe has the required columns.

    Parameters
    ----------
    input_df: pd.DataFrame
        A Dataframe to be checked.
    required_cols_list: list[str]
        List of column names that must be present in the dataframe.
    error_msg: str
        Error message to be raised if any of the required columns are missing.
    """

    missing_cols = set(required_cols_list) - set(input_df.columns)
    if len(missing_cols) > 0:
        missing_cols = sorted(missing_cols)
        raise ValueError(
            f"{error_msg}: {missing_cols}")


def get_extension(sep):
    return "csv" if sep == "," else "txt"


def update_metadata_df_field(
        metadata_df, field_name, field_val_or_func,
        source_fields=None, overwrite_non_nans=True):

    # Note: function doesn't return anything.  Work is done in-place on the
    #  metadata_df passed in.

    if source_fields:
        if overwrite_non_nans or (field_name not in metadata_df.columns):
            metadata_df[field_name] = \
                metadata_df.apply(
                    lambda row: field_val_or_func(row, source_fields),
                    axis=1)
        else:
            # TODO: not yet tested; from StackOverflow
            metadata_df.loc[metadata_df[field_name].isnull(), field_name] = \
                metadata_df.apply(
                    lambda row: field_val_or_func(row, source_fields),
                    axis=1)
        # endif overwrite_non_nans for function call
    else:
        if overwrite_non_nans or (field_name not in metadata_df.columns):
            metadata_df[field_name] = field_val_or_func
        else:
            metadata_df[field_name] = \
                metadata_df[field_name].fillna(field_val_or_func)
            # metadata_df[field_name].fillna(field_val_or_func, inplace=True)
        # endif overwrite_non_nans for constant value
    # endif using a function/a constant value