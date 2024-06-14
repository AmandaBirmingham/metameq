import pandas
from qiimp.src.util import validate_required_columns_exist


def merge_sample_and_subject_metadata(
        sample_metadata_df, subject_metadata_df,
        merge_col_sample, merge_col_subject=None):

    if merge_col_subject is None:
        merge_col_subject = merge_col_sample

    validate_required_columns_exist(sample_metadata_df, [merge_col_sample],
                                    "sample metadata missing merge column")
    validate_required_columns_exist(subject_metadata_df, [merge_col_subject],
                                    "subject metadata missing merge column")

    error_msgs = []
    # check for nans in the merge columns
    error_msgs.extend(_check_for_nans(
        sample_metadata_df, "sample", merge_col_sample))
    error_msgs.extend(_check_for_nans(
        subject_metadata_df, "subject", merge_col_subject))

    # check for duplicates in subject merge column
    # (duplicates in the sample merge column are expected, as we expect
    # there to possibly multiple samples for the same subject)
    error_msgs.extend(_check_for_duplicate_field_vals(
        subject_metadata_df, "subject", merge_col_subject))

    if error_msgs:
        joined_msgs = "\n".join(error_msgs)
        raise ValueError(f"Errors in metadata to merge:\n{joined_msgs}")

    # merge the sample and host dfs on the selected columns
    merge_df = pandas.merge(sample_metadata_df, subject_metadata_df,
                            how="left", validate="many_to_one",
                            left_on=merge_col_sample,
                            right_on=merge_col_subject)

    return merge_df


def _check_for_duplicate_field_vals(metadata_df, df_name, col_name):
    error_msgs = []
    duplicates_mask = metadata_df.duplicated(subset=col_name)
    if duplicates_mask.any():
        # generate an error message including the duplicate values
        error_msgs.append(
            f"{df_name} metadata has duplicate values in column {col_name}: "
            f"{metadata_df.loc[duplicates_mask, col_name].unique()}")
    return error_msgs


def _check_for_nans(metadata_df, df_name, col_name):
    error_msgs = []
    nans_mask = metadata_df[col_name].isna()
    if nans_mask.any():
        error_msgs.append(
            f"{df_name} metadata has NaNs in column {col_name}")
    return error_msgs
