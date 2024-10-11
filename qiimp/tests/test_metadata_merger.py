import numpy as np
import pandas
from pandas.testing import assert_frame_equal
from os.path import dirname
from unittest import TestCase
from qiimp.src.metadata_merger import _check_for_nans, \
    _check_for_duplicate_field_vals, _validate_merge, \
    merge_many_to_one_metadata, merge_one_to_one_metadata, \
    merge_sample_and_subject_metadata


class TestMetadataMerger(TestCase):
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
