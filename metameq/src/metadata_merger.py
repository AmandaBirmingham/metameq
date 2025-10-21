import pandas
from typing import List, Optional, Literal
from metameq.src.util import validate_required_columns_exist


def merge_sample_and_subject_metadata(
        sample_metadata_df: pandas.DataFrame,
        subject_metadata_df: pandas.DataFrame,
        merge_col_sample: str, merge_col_subject: Optional[str] = None,
        join_type: Literal["left", "right", "inner", "outer"] = "left") -> \
        pandas.DataFrame:
    """Merge sample metadata with subject metadata using a many-to-one relationship.

    This is a convenience wrapper around merge_many_to_one_metadata that uses
    standard naming conventions for sample and subject metadata.

    Parameters
    ----------
    sample_metadata_df : pandas.DataFrame
        DataFrame containing sample metadata (the "many" side of the relationship).
    subject_metadata_df : pandas.DataFrame
        DataFrame containing subject metadata (the "one" side of the relationship).
    merge_col_sample : str
        Column name in sample_metadata_df to merge on.
    merge_col_subject : str, optional
        Column name in subject_metadata_df to merge on. If None, uses merge_col_sample.
        Defaults to None.
    join_type : {"left", "right", "inner", "outer"}, optional
        Type of join to perform. Defaults to "left".

    Returns
    -------
    pandas.DataFrame
        Merged DataFrame containing combined sample and subject metadata.

    Raises
    ------
    ValueError
        If merge columns are missing or contain invalid values.
        If there are duplicate values in the subject merge column.
        If there are non-merge columns with the same name in both DataFrames.
    """
    result = merge_many_to_one_metadata(
        sample_metadata_df, subject_metadata_df,
        merge_col_sample, merge_col_subject,
        "sample", "subject", join_type=join_type)

    return result


def merge_many_to_one_metadata(
        many_metadata_df: pandas.DataFrame, one_metadata_df: pandas.DataFrame,
        merge_col_many: str, merge_col_one: Optional[str] = None,
        set_name_many: str = "many-set", set_name_one: str = "one-set",
        join_type: Literal["left", "right", "inner", "outer"] = "left") -> \
        pandas.DataFrame:
    """Merge two metadata DataFrames with a many-to-one relationship.

    This function merges a DataFrame that may have multiple records per merge key
    (many_metadata_df) with a DataFrame that must have unique merge keys
    (one_metadata_df).

    Parameters
    ----------
    many_metadata_df : pandas.DataFrame
        DataFrame that may have multiple records per merge key.
    one_metadata_df : pandas.DataFrame
        DataFrame that must have unique merge keys.
    merge_col_many : str
        Column name in many_metadata_df to merge on.
    merge_col_one : str, optional
        Column name in one_metadata_df to merge on. If None, uses merge_col_many.
        Defaults to None.
    set_name_many : str, optional
        Name of the many_metadata_df set, used in error messages.
        Defaults to "many-set".
    set_name_one : str, optional
        Name of the one_metadata_df set, used in error messages.
        Defaults to "one-set".
    join_type : {"left", "right", "inner", "outer"}, optional
        Type of join to perform. Defaults to "left".

    Returns
    -------
    pandas.DataFrame
        Merged DataFrame containing combined metadata.

    Raises
    ------
    ValueError
        If merge columns are missing or contain invalid values.
        If there are duplicate values in the one_metadata_df merge column.
        If there are non-merge columns with the same name in both DataFrames.
    """
    merge_col_one = merge_col_many if merge_col_one is None else merge_col_one

    # Note: duplicates in the many-set merge column are expected, as we expect
    # there to possibly multiple records for the same one-set record
    _validate_merge(many_metadata_df, one_metadata_df, merge_col_many,
                    merge_col_one, set_name_many, set_name_one,
                    check_left_for_dups=False)

    # merge the sample and host dfs on the selected columns
    merge_df = pandas.merge(many_metadata_df, one_metadata_df,
                            how=join_type, validate="many_to_one",
                            left_on=merge_col_many,
                            right_on=merge_col_one)

    return merge_df


def merge_one_to_one_metadata(
        left_metadata_df: pandas.DataFrame,
        right_metadata_df: pandas.DataFrame,
        merge_col_left: str, merge_col_right: Optional[str] = None,
        set_name_left: str = "left", set_name_right: str = "right",
        join_type: Literal["left", "right", "inner", "outer"] = "left") -> \
        pandas.DataFrame:
    """Merge two metadata DataFrames with a one-to-one relationship.

    This function merges two DataFrames where each DataFrame's merge key must be unique in
    that DataFrame.

    Parameters
    ----------
    left_metadata_df : pandas.DataFrame
        Left DataFrame to merge.
    right_metadata_df : pandas.DataFrame
        Right DataFrame to merge.
    merge_col_left : str
        Column name in left_metadata_df to merge on.
    merge_col_right : str, optional
        Column name in right_metadata_df to merge on. If None, uses merge_col_left.
        Defaults to None.
    set_name_left : str, optional
        Name of the left_metadata_df set, used in error messages.
        Defaults to "left".
    set_name_right : str, optional
        Name of the right_metadata_df set, used in error messages.
        Defaults to "right".
    join_type : {"left", "right", "inner", "outer"}, optional
        Type of join to perform. Defaults to "left".

    Returns
    -------
    pandas.DataFrame
        Merged DataFrame containing combined metadata.

    Raises
    ------
    ValueError
        If merge columns are missing or contain invalid values.
        If there are duplicate values in either merge column.
        If there are non-merge columns with the same name in both DataFrames.
    """
    merge_col_right = \
        merge_col_left if merge_col_right is None else merge_col_right

    _validate_merge(left_metadata_df, right_metadata_df, merge_col_left,
                    merge_col_right, set_name_left, set_name_right)

    # merge the sample and host dfs on the selected columns
    merge_df = pandas.merge(left_metadata_df, right_metadata_df,
                            how=join_type, validate="one_to_one",
                            left_on=merge_col_left,
                            right_on=merge_col_right)

    return merge_df


def find_common_df_cols(left_df: pandas.DataFrame,
                        right_df: pandas.DataFrame) -> List[str]:
    """Find column names that exist in both DataFrames.

    Parameters
    ----------
    left_df : pandas.DataFrame
        First DataFrame to compare.
    right_df : pandas.DataFrame
        Second DataFrame to compare.

    Returns
    -------
    List[str]
        List of column names that exist in both DataFrames, sorted alphabetically.
    """
    left_non_merge_cols = set(left_df.columns)
    right_non_merge_cols = set(right_df.columns)
    common_cols = left_non_merge_cols.intersection(right_non_merge_cols)
    return sorted(list(common_cols))


def find_common_col_names(left_cols, right_cols,
                          left_exclude_list: List[str] = None,
                          right_exclude_list: List[str] = None) -> List[str]:
    """Find column names that exist in both lists, excluding specified columns.

    Parameters
    ----------
    left_cols : List[str]
        First list of column names to compare.
    right_cols : List[str]
        Second list of column names to compare.
    left_exclude_list : List[str], optional
        List of column names to exclude from left_cols.
        Defaults to None.
    right_exclude_list : List[str], optional
        List of column names to exclude from right_cols.
        Defaults to None.

    Returns
    -------
    List[str]
        List of column names that exist in both lists (after exclusions),
        sorted alphabetically.
    """
    if left_exclude_list is None:
        left_exclude_list = []
    if right_exclude_list is None:
        right_exclude_list = []

    left_non_merge_cols = set(left_cols) - set(left_exclude_list)
    right_non_merge_cols = set(right_cols) - set(right_exclude_list)
    common_cols = left_non_merge_cols.intersection(right_non_merge_cols)
    return sorted(list(common_cols))


def _validate_merge(
        left_df: pandas.DataFrame, right_df: pandas.DataFrame,
        left_on: str, right_on: str, set_name_left: Optional[str] = "left",
        set_name_right: Optional[str] = "right",
        check_left_for_dups: bool = True, check_right_for_dups: bool = True) \
        -> None:
    """Validate that two DataFrames can be merged.

    Checks that:
    1. Required merge columns exist
    2. No NaN values are in merge columns
    3. No duplicate values are in merge columns (if specified)
    4. No common non-merge column names exist in both DataFrames

    Parameters
    ----------
    left_df : pandas.DataFrame
        Left DataFrame to validate.
    right_df : pandas.DataFrame
        Right DataFrame to validate.
    left_on : str
        Column name in left_df to merge on.
    right_on : str
        Column name in right_df to merge on.
    set_name_left : str, optional
        Name of the left_df set, used in error messages.
        Defaults to "left".
    set_name_right : str, optional
        Name of the right_df set, used in error messages.
        Defaults to "right".
    check_left_for_dups : bool, optional
        Whether to check for duplicates in left_df merge column.
        Defaults to True.
    check_right_for_dups : bool, optional
        Whether to check for duplicates in right_df merge column.
        Defaults to True.

    Raises
    ------
    ValueError
        If any validation checks fail.
    """
    validate_required_columns_exist(
        left_df, [left_on],
        f"{set_name_left} metadata missing merge column")
    validate_required_columns_exist(
        right_df, [right_on],
        f"{set_name_right} metadata missing merge column")

    error_msgs = []
    # check for nans in the merge columns
    error_msgs.extend(_check_for_nans(
        left_df, set_name_left, left_on))
    error_msgs.extend(_check_for_nans(
        right_df, set_name_right, right_on))

    # check for duplicates
    if check_left_for_dups:
        error_msgs.extend(_check_for_duplicate_field_vals(
            left_df, set_name_left, left_on))
    if check_right_for_dups:
        error_msgs.extend(_check_for_duplicate_field_vals(
            right_df, set_name_right, right_on))

    # check for non-merge columns with the same name in both dataframes
    common_cols = find_common_col_names(
        left_df.columns, right_df.columns, [left_on], [right_on])
    if common_cols:
        error_msgs.append(
            f"Both {set_name_left} and {set_name_right} metadata have "
            f"non-merge columns with the following names: {common_cols}")

    if error_msgs:
        joined_msgs = "\n".join(error_msgs)
        raise ValueError(f"Errors in metadata to merge:\n{joined_msgs}")


def _check_for_duplicate_field_vals(
        metadata_df: pandas.DataFrame, df_name: str,
        col_name: str) -> List[str]:
    """Check for duplicate values in a DataFrame column.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        DataFrame to check for duplicates.
    df_name : str
        Name of the DataFrame, used in error messages.
    col_name : str
        Name of the column to check for duplicates.

    Returns
    -------
    List[str]
        List of error messages for any duplicates found.
        Empty list if no duplicates found.
    """
    error_msgs = []
    duplicates_mask = metadata_df.duplicated(subset=col_name)
    if duplicates_mask.any():
        duplicates = metadata_df.loc[duplicates_mask, col_name].unique()
        duplicates.sort()

        # generate an error message including the duplicate values
        error_msgs.append(
            f"'{df_name}' metadata has duplicates of the following values "
            f"in column '{col_name}': {duplicates}")
    return error_msgs


def _check_for_nans(metadata_df: pandas.DataFrame,
                    df_name: str, col_name: str) -> List[str]:
    """Check for NaN values in a DataFrame column.

    Parameters
    ----------
    metadata_df : pandas.DataFrame
        DataFrame to check for NaNs.
    df_name : str
        Name of the DataFrame, used in error messages.
    col_name : str
        Name of the column to check for NaNs.

    Returns
    -------
    List[str]
        List of error messages for any NaNs found.
        Empty list if no NaNs found.
    """
    error_msgs = []
    nans_mask = metadata_df[col_name].isna()
    if nans_mask.any():
        error_msgs.append(
            f"'{df_name}' metadata has NaNs in column '{col_name}'")
    return error_msgs
