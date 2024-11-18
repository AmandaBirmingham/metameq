import pandas
from typing import List, Optional, Literal, Tuple
from qiimp.src.util import validate_required_columns_exist


def merge_sample_and_subject_metadata(
        sample_metadata_df: pandas.DataFrame,
        subject_metadata_df: pandas.DataFrame,
        merge_col_sample: str, merge_col_subject: Optional[str] = None,
        join_type: Literal["left", "right", "inner", "outer"] = "left") -> \
        pandas.DataFrame:

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
    left_non_merge_cols = set(left_df.columns)
    right_non_merge_cols = set(right_df.columns)
    common_cols = left_non_merge_cols.intersection(right_non_merge_cols)
    return list(common_cols)


def find_common_col_names(left_cols, right_cols,
                          left_exclude_list: List[str] = None,
                          right_exclude_list: List[str] = None) -> List[str]:
    if left_exclude_list is None:
        left_exclude_list = []
    if right_exclude_list is None:
        right_exclude_list = []

    left_non_merge_cols = set(left_cols) - set(left_exclude_list)
    right_non_merge_cols = set(right_cols) - set(right_exclude_list)
    common_cols = left_non_merge_cols.intersection(right_non_merge_cols)
    return list(common_cols)


def _validate_merge(
        left_df: pandas.DataFrame, right_df: pandas.DataFrame,
        left_on: str, right_on: str, set_name_left: Optional[str] = "left",
        set_name_right: Optional[str] = "right",
        check_left_for_dups: bool = True, check_right_for_dups: bool = True) \
        -> None:

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
    error_msgs = []
    nans_mask = metadata_df[col_name].isna()
    if nans_mask.any():
        error_msgs.append(
            f"'{df_name}' metadata has NaNs in column '{col_name}'")
    return error_msgs


if __name__ == "__main__":
    qiita_study_id = 15614
    qiita_metadata_df = pandas.read_csv(
        "/Users/abirmingham/Desktop/15614_20240715-061026.txt", sep="\t")
    researcher_metadata_df = pandas.read_csv(
        "/Users/abirmingham/Desktop/PRJNA_554499NatComm_WGS_pheno.csv")

    # the researcher metadata has column names that contain periods, which
    # qiita doesn't allow.  So, we'll rename the columns to remove the periods.
    researcher_metadata_df.columns = \
        [col.replace(".", "_") for col in researcher_metadata_df.columns]

    # There is no id in the researcher_metadata_df that exactly matches an id
    # in the qiita_metadata_df, so some string munging is necessary.
    # By manual examination, I know that the "sample_name" column in the
    # qiita_metadata_df has the format:
    # <qiita_study_id>.<scilife_id_pt_1>.<scilife_id_pt_2>.hf.1
    # (don't ask me what the hf.1 means).
    qiita_metadata_df["scilife_id"] = \
        qiita_metadata_df["sample_name"].apply(
            lambda x: x.split(".")[1] + "_" + x.split(".")[2])

    result_df = merge_one_to_one_metadata(
        qiita_metadata_df, researcher_metadata_df,
        "scilife_id", "scilife_id",
        "qiita", "researcher")

    result_df.to_csv(
        "/Users/abirmingham/Desktop/qiita_15614_merged_metadata.csv",
        index=False)
