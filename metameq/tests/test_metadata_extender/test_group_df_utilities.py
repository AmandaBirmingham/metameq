import numpy as np
import pandas
from pandas.testing import assert_frame_equal
from metameq.src.util import \
    SAMPLE_NAME_KEY, \
    HOSTTYPE_SHORTHAND_KEY, \
    SAMPLETYPE_SHORTHAND_KEY, \
    QC_NOTE_KEY, \
    DEFAULT_KEY
from metameq.src.metadata_extender import \
    _reorder_df, \
    _catch_nan_required_fields, \
    _fill_na_if_default, \
    INTERNAL_COL_KEYS
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestReorderDf(ExtenderTestBase):
    def test__reorder_df_sample_name_first(self):
        """Test that sample_name becomes the first column."""
        input_df = pandas.DataFrame({
            "zebra": ["z"],
            SAMPLE_NAME_KEY: ["sample1"],
            "apple": ["a"],
            QC_NOTE_KEY: [""],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })

        result = _reorder_df(input_df, INTERNAL_COL_KEYS)

        self.assertEqual(SAMPLE_NAME_KEY, result.columns[0])

    def test__reorder_df_alphabetical_order(self):
        """Test that non-internal columns are sorted alphabetically after sample_name."""
        input_df = pandas.DataFrame({
            "zebra": ["z"],
            SAMPLE_NAME_KEY: ["sample1"],
            "apple": ["a"],
            QC_NOTE_KEY: [""],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })

        result = _reorder_df(input_df, INTERNAL_COL_KEYS)

        expected_order = [SAMPLE_NAME_KEY, "apple", "zebra"] + INTERNAL_COL_KEYS
        self.assertEqual(expected_order, list(result.columns))

    def test__reorder_df_internals_at_end(self):
        """Test that internal columns are moved to the end in the provided order."""
        input_df = pandas.DataFrame({
            "field1": ["value1"],
            SAMPLE_NAME_KEY: ["sample1"],
            QC_NOTE_KEY: [""],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })

        result = _reorder_df(input_df, INTERNAL_COL_KEYS)

        expected_order = [SAMPLE_NAME_KEY, "field1"] + INTERNAL_COL_KEYS
        self.assertEqual(expected_order, list(result.columns))

    def test__reorder_df_full_ordering(self):
        """Test complete column ordering: sample_name, alphabetical, internals."""
        input_df = pandas.DataFrame({
            "zebra": ["z"],
            SAMPLE_NAME_KEY: ["sample1"],
            "apple": ["a"],
            QC_NOTE_KEY: [""],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            "banana": ["b"]
        })

        result = _reorder_df(input_df, INTERNAL_COL_KEYS)

        expected_order = [SAMPLE_NAME_KEY, "apple", "banana", "zebra"] + INTERNAL_COL_KEYS
        self.assertEqual(expected_order, list(result.columns))


class TestCatchNanRequiredFields(ExtenderTestBase):
    def test__catch_nan_required_fields_no_nans(self):
        """Test returns unchanged df when no NaNs in required fields."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "control"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "blank"]
        })

        result = _catch_nan_required_fields(input_df)

        assert_frame_equal(input_df, result)

    def test__catch_nan_required_fields_nan_sample_name_raises(self):
        """Test raises ValueError when sample_name contains NaN."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", np.nan],
            HOSTTYPE_SHORTHAND_KEY: ["human", "control"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "blank"]
        })

        with self.assertRaisesRegex(ValueError, "Metadata contains NaN sample names"):
            _catch_nan_required_fields(input_df)

    def test__catch_nan_required_fields_nan_shorthand_fields_become_empty(self):
        """Test that NaN hosttype_shorthand and sampletype_shorthand values are set to 'empty'."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", np.nan],
            SAMPLETYPE_SHORTHAND_KEY: [np.nan, "blank"]
        })

        result = _catch_nan_required_fields(input_df)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "empty"],
            SAMPLETYPE_SHORTHAND_KEY: ["empty", "blank"]
        })
        assert_frame_equal(expected, result)


class TestFillNaIfDefault(ExtenderTestBase):
    def test__fill_na_if_default_has_default_in_settings(self):
        """Test that specific_dict default takes precedence over settings_dict."""
        input_df = pandas.DataFrame({
            "field1": ["value1", np.nan, "value3"],
            "field2": [np.nan, "value2", np.nan]
        })
        settings_dict = {DEFAULT_KEY: "filled"}

        result = _fill_na_if_default(input_df, settings_dict)

        expected = pandas.DataFrame({
            "field1": ["value1", "filled", "value3"],
            "field2": ["filled", "value2", "filled"]
        })
        assert_frame_equal(expected, result)

    def test__fill_na_if_default_no_default_in_settings(self):
        """Test that NaN values are unchanged when no default is in settings."""
        input_df = pandas.DataFrame({
            "field1": ["value1", np.nan, "value3"],
            "field2": [np.nan, "value2", np.nan]
        })
        settings_dict = {}

        result = _fill_na_if_default(input_df, settings_dict)

        expected = pandas.DataFrame({
            "field1": ["value1", np.nan, "value3"],
            "field2": [np.nan, "value2", np.nan]
        })
        assert_frame_equal(expected, result)
