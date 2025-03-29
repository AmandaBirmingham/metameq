import numpy as np
import pandas
from pandas.testing import assert_frame_equal
from os.path import dirname
from unittest import TestCase
from qiimp.src.metadata_merger import _check_for_nans, \
    _check_for_duplicate_field_vals, _validate_merge, \
    merge_many_to_one_metadata, merge_one_to_one_metadata, \
    merge_sample_and_subject_metadata, find_common_col_names, \
    find_common_df_cols


class TestMetadataMerger(TestCase):
    # Tests for _check_for_nans
    def test__check_for_nans_wo_nans(self):
        # test case 1: no nans in selected column
        df = pandas.DataFrame({
            "a": [1, 2, 3],
            "b": [4, np.NaN, 6]
        })

        obs = _check_for_nans(df, "test", "a")
        self.assertEqual([], obs)

    def test__check_for_nans_w_nans(self):
        # test case 2: nan in selected column (and another, but that's ignored)
        df = pandas.DataFrame({
            "a": [1, np.NaN, 3],
            "b": [4, np.nan, 6]
        })

        obs = _check_for_nans(df, "test", "b")
        self.assertEqual(["'test' metadata has NaNs in column 'b'"], obs)

    def test__check_for_nans_with_empty(self):
        """Test that checking for NaNs in an empty DataFrame raises an error"""
        empty_df = pandas.DataFrame()
        with self.assertRaises(Exception):
            _check_for_nans(empty_df, "test", "a")

    # Tests for _check_for_duplicate_field_vals
    def test__check_for_duplicate_field_vals(self):
        # test case 1: no duplicates in selected column
        df = pandas.DataFrame({
            "a": [1, 2, 3],
            "b": [4, 5, 5]
        })

        obs = _check_for_duplicate_field_vals(df, "test", "a")
        self.assertEqual([], obs)

    def test__check_for_duplicate_field_vals_w_duplicates(self):
        # test case 2: duplicates in selected column (and in other, but those
        # are ignored)
        df = pandas.DataFrame({
            "a": [1, 2, 2, 3, 1],
            "b": [4, 5, 6, 6, 4]
        })

        obs = _check_for_duplicate_field_vals(df, "test", "a")
        self.assertEqual(
            ["'test' metadata has duplicates of the following values in "
             "column 'a': [1 2]"], obs)

    def test_check_for_duplicate_field_vals_with_empty(self):
        df = pandas.DataFrame()
        result = _check_for_duplicate_field_vals(df, "test", "col")
        self.assertEqual(result, [])

    # Tests for _validate_merge
    def test__validate_merge(self):
        # test case 1: no errors
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z'],
            "c": [7, 8, 9],
            "d": [10, 11, 12]
        })

        _validate_merge(left_df, right_df, "a", "c")
        self.assertTrue(True)

    def test__validate_merge_err_left_col(self):
        # test case 1: no errors
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z'],
            "c": [7, 8, 9],
            "d": [10, 11, 12]
        })

        with self.assertRaisesRegex(
                ValueError, r"left metadata missing merge column: \['c'\]"):
            _validate_merge(left_df, right_df, "c", "c")

    def test__validate_merge_err_right_col(self):
        # test case 1: no errors
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z'],
            "c": [7, 8, 9],
            "d": [10, 11, 12]
        })

        with self.assertRaisesRegex(
                ValueError, r"right metadata missing merge column: \['a'\]"):
            _validate_merge(left_df, right_df, "a", "a")

    def test__validate_merge_err_msgs(self):
        # test case 1: no errors
        left_df = pandas.DataFrame({
            "id": ['x', np.NaN, 'x'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": [np.NaN, 'y', 'y'],
            "c": [7, 8, 9],
            "d": [10, 11, 12]
        })

        exp_msg = r"""Errors in metadata to merge:
'left' metadata has NaNs in column 'id'
'right' metadata has NaNs in column 'name'
'left' metadata has duplicates of the following values in column 'id': \['x'\]
'right' metadata has duplicates of the following values in column 'name': \['y'\]"""  # noqa E501

        with self.assertRaisesRegex(ValueError, exp_msg):
            _validate_merge(left_df, right_df, "id", "name")

    # I'm not going to test every variation of the merge_one_to_one_metadata
    # join (left, right, inner, outer, etc.) because the pandas library is
    # already well-tested.  I'm just going to test one to show that the
    # function's calling the pandas merge function with the correct parameters.

    # Tests for merge_one_to_one_metadata
    def test_merge_one_to_one_metadata_left(self):
        # test case 1: no errors
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z', 'q'],
            "c": [7, 8, 9, 90],
            "d": [10, 11, 12, 120]
        })

        obs = merge_one_to_one_metadata(
            left_df, right_df, "id", "name")
        exp = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "name": ['x', 'y', 'z'],
            "c": [7, 8, 9],
            "d": [10, 11, 12]
        })

        assert_frame_equal(obs, exp)

    def test_merge_one_to_one_metadata_err(self):
        # this doesn't test ALL the errors, just that errors can be thrown
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', np.NaN, 'z'],
            "c": [7, 8, 9],
            "d": [10, 11, 12]
        })

        with self.assertRaisesRegex(
                ValueError, r"Errors in metadata to merge:\n"
                            r"'second' metadata has NaNs in column 'name'"):
            merge_one_to_one_metadata(
                left_df, right_df, "id", "name",
                set_name_left="first", set_name_right="second")

    # Tests for merge_many_to_one_metadata
    def test_merge_many_to_one_metadata(self):
        # test case 1: no errors
        left_df = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'x'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z'],
            "c": [9, 10, 11],
            "d": [12, 13, 14]
        })

        obs = merge_many_to_one_metadata(
            left_df, right_df, "name", "name")
        exp = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'x'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8],
            "c": [9, 10, 11, 9],
            "d": [12, 13, 14, 12]
        })

        assert_frame_equal(obs, exp)

    def test_merge_many_to_one_metadata_err(self):
        # this doesn't test ALL the errors, just that errors can be thrown
        left_df = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'x'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', np.NaN],
            "c": [9, 10, 11],
            "d": [12, 13, 14]
        })

        with self.assertRaisesRegex(
                ValueError, r"Errors in metadata to merge:\n'uno' metadata "
                            r"has NaNs in column 'name'"):
            merge_many_to_one_metadata(
                left_df, right_df, "name",
                set_name_many="lots", set_name_one="uno")

    # Tests for merge_sample_and_subject_metadata
    def test_merge_sample_and_subject_metadata(self):
        left_df = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'x'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z'],
            "c": [9, 10, 11],
            "d": [12, 13, 14]
        })

        obs = merge_sample_and_subject_metadata(
            left_df, right_df, "name")
        exp = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'x'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8],
            "c": [9, 10, 11, 9],
            "d": [12, 13, 14, 12]
        })

        assert_frame_equal(obs, exp)

    def test_merge_sample_and_subject_metadata_err(self):
        left_df = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'x'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', np.NaN],
            "c": [9, 10, 11],
            "d": [12, 13, 14]
        })

        with self.assertRaisesRegex(
                ValueError, r"Errors in metadata to merge:\n'subject' metadata"
                            r" has NaNs in column 'name'"):
            merge_sample_and_subject_metadata(
                left_df, right_df, "name",)

    # Tests for find_common_col_names
    def test_find_common_col_names(self):
        """Test finding common column names between two lists"""
        list1 = ['col1', 'col2', 'col3']
        list2 = ['col2', 'col3', 'col4']
        result = find_common_col_names(list1, list2)
        self.assertEqual(result, ['col2', 'col3'])
        
    def test_find_common_col_names_empty(self):
        """Test finding common column names with empty lists"""
        result = find_common_col_names([], [])
        self.assertEqual(result, [])

    def test_find_common_col_names_no_common(self):
        """Test finding common column names with no common columns"""
        list1 = ['col1', 'col2']
        list2 = ['col3', 'col4']
        result = find_common_col_names(list1, list2)
        self.assertEqual(result, [])

    def test_find_common_col_names_case_sensitive(self):
        """Test finding common column names with case sensitivity"""
        list1 = ['Col1', 'col2']
        list2 = ['col1', 'Col2']
        result = find_common_col_names(list1, list2)
        self.assertEqual(result, [])

    # Tests for find_common_df_cols
    def test_find_common_df_cols(self):
        """Test finding common columns between two DataFrames"""
        df1 = pandas.DataFrame({
            'col1': [1, 2],
            'col2': [3, 4],
            'col3': [5, 6]
        })
        df2 = pandas.DataFrame({
            'col2': [7, 8],
            'col3': [9, 10],
            'col4': [11, 12]
        })
        
        result = find_common_df_cols(df1, df2)
        self.assertEqual(result, ['col2', 'col3'])

    def test_find_common_df_cols_empty(self):
        """Test finding common columns with empty DataFrames"""
        df1 = pandas.DataFrame()
        df2 = pandas.DataFrame()
        result = find_common_df_cols(df1, df2)
        self.assertEqual(result, [])

    def test_find_common_df_cols_no_common(self):
        """Test finding common columns with no common columns"""
        df1 = pandas.DataFrame({
            'col1': [1, 2],
            'col2': [3, 4]
        })

        df2 = pandas.DataFrame({
            'col3': [5, 6],
            'col4': [7, 8]
        })
        result = find_common_df_cols(df1, df2)
        self.assertEqual(result, [])

    # Tests for merge_one_to_one_metadata
    def test_merge_one_to_one_metadata_right(self):
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z', 'q'],
            "c": [7, 8, 9, 90],
            "d": [10, 11, 12, 120]
        })

        obs = merge_one_to_one_metadata(
            left_df, right_df, "id", "name", join_type='right')
        exp = pandas.DataFrame({
            "id": ['x', 'y', 'z', np.nan],
            "a": [1, 2, 3, np.nan],
            "b": [4, 5, 6, np.nan],
            "name": ['x', 'y', 'z', 'q'],
            "c": [7, 8, 9, 90],
            "d": [10, 11, 12, 120]
        })

        assert_frame_equal(obs, exp)

    def test_merge_one_to_one_metadata_inner(self):
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'q'],
            "c": [7, 8, 90],
            "d": [10, 11, 120]
        })

        obs = merge_one_to_one_metadata(
            left_df, right_df, "id", "name", join_type='inner')
        exp = pandas.DataFrame({
            "id": ['x', 'y'],
            "a": [1, 2],
            "b": [4, 5],
            "name": ['x', 'y'],
            "c": [7, 8],
            "d": [10, 11]
        })

        assert_frame_equal(obs, exp)

    def test_merge_one_to_one_metadata_with_nans(self):
        """Test merging one-to-one metadata with NaN values"""
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "id": ['x', 'y', np.nan],
            "c": [7, 8, 90],
            "d": [10, 11, 120]
        })
        
        with self.assertRaises(ValueError):
            merge_one_to_one_metadata(left_df, right_df, 'id')

    def test_merge_one_to_one_metadata_with_duplicates(self):
        """Test merging one-to-one metadata with duplicate values"""
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z', 'y'],
            "c": [7, 8, 9, 90],
            "d": [10, 11, 12, 120]
        })

        with self.assertRaises(ValueError):
            merge_one_to_one_metadata(left_df, right_df, 'id', 'name')

    # Tests for merge_many_to_one_metadata
    def test_merge_many_to_one_metadata_with_duplicates(self):
        left_df = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'x'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z'],
            "c": [9, 10, 11],
            "d": [12, 13, 14]
        })

        obs = merge_many_to_one_metadata(
            left_df, right_df, "name", "name")
        exp = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'x'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8],
            "c": [9, 10, 11, 9],
            "d": [12, 13, 14, 12]
        })

        assert_frame_equal(obs, exp)

    # Tests for merge_sample_and_subject_metadata
    def test_merge_sample_and_subject_metadata_with_missing(self):
        sample_df = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'w'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8]
        })
        subject_df = pandas.DataFrame({
            "name": ['x', 'y', 'z'],
            "c": [9, 10, 11],
            "d": [12, 13, 14]
        })

        obs = merge_sample_and_subject_metadata(
            sample_df, subject_df, "name")
        exp = pandas.DataFrame({
            "id": [101, 102, 103, 104],
            "name": ['x', 'y', 'z', 'w'],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8],
            "c": [9, 10, 11, np.nan],
            "d": [12, 13, 14, np.nan]
        })

        assert_frame_equal(obs, exp)

    
    def test_validate_merge(self):
        """Test validating merge operation"""
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z'],
            "c": [7, 8, 9],
            # NaN in non-merge column shouldn't matter
            "d": [10, np.nan, 12]
        })

        _validate_merge(left_df, right_df, 'id', 'name')
        # Should not raise any exception

    def test_validate_merge_non_existent_col(self):
        """Test validating merge operation on non-existent column"""
        left_df = pandas.DataFrame({
            "id": ['x', 'y', 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', 'z'],
            "c": [7, 8, 9],
            # NaN in non-merge column shouldn't matter
            "d": [10, np.nan, 12]
        })

        with self.assertRaises(ValueError):
            _validate_merge(left_df, right_df, 'subject_id', 'name')

    def test_validate_merge_with_nans(self):
        left_df = pandas.DataFrame({
            "id": ['x', np.nan, 'z'],
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        right_df = pandas.DataFrame({
            "name": ['x', 'y', np.nan],
            "c": [7, 8, 9],
            "d": [10, 11, 12]
        })

        with self.assertRaisesRegex(ValueError, "Errors in metadata to merge"):
            _validate_merge(left_df, right_df, "id", "name")
