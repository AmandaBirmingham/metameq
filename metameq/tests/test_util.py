import numpy as np
import pandas
from pandas.testing import assert_frame_equal
import os
import os.path as path
from unittest import TestCase
from metameq.src.util import extract_config_dict, \
    extract_yaml_dict, extract_stds_config, deepcopy_dict, \
    validate_required_columns_exist, update_metadata_df_field, get_extension, \
    load_df_with_best_fit_encoding


class TestUtil(TestCase):
    """Test suite for utility functions in metameq.src.util module."""

    # get the parent directory of the current file
    TEST_DIR = path.dirname(__file__)

    TEST_CONFIG_DICT = {
            "host_type_specific_metadata": {
                "base": {
                    "metadata_fields": {
                        "sample_name": {
                            "type": "string",
                            "unique": True
                        },
                        "sample_type": {
                            "empty": False,
                            "is_phi": False
                        }
                    }
                }
            }
        }

    # Tests for extract_config_dict
    def test_extract_config_dict_no_inputs(self):
        """Test extracting config dictionary with no inputs.

        NB: this test is looking at the *real* config, which may change, so
        just checking that a couple of the expected keys (which are not in
        the test config) are present.
        """
        obs = extract_config_dict(None)
        self.assertIn("default", obs)
        self.assertIn("leave_requireds_blank", obs)

    def test_extract_config_dict_w_config_fp(self):
        """Test extracting config dictionary from a valid config file path."""
        config_fp = path.join(self.TEST_DIR, "data/test_config.yml")
        obs = extract_config_dict(config_fp)
        self.assertDictEqual(self.TEST_CONFIG_DICT, obs)

    def test_extract_config_dict_missing_file(self):
        """Test that attempting to extract config from non-existent file raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            extract_config_dict("nonexistent.yml")

    def test_extract_config_dict_invalid_yaml(self):
        """Test that attempting to extract config from invalid YAML raises an exception."""
        # Create a temporary invalid YAML file
        invalid_yaml_path = path.join(self.TEST_DIR, "data/invalid.yml")
        with open(invalid_yaml_path, "w") as f:
            f.write("invalid: yaml: content: - [")

        with self.assertRaises(Exception):
            extract_config_dict(invalid_yaml_path)

    # Tests for extract_yaml_dict
    def test_extract_yaml_dict(self):
        """Test extracting YAML dictionary from a valid YAML file."""
        config_fp = path.join(self.TEST_DIR, "data/test_config.yml")
        obs = extract_yaml_dict(config_fp)
        self.assertDictEqual(self.TEST_CONFIG_DICT, obs)

    # Tests for extract_stds_config
    def test_extract_stds_config(self):
        """Test extracting standards configuration with default settings.

        Verifies that the extracted config contains expected standard keys.
        """
        obs = extract_stds_config(None)
        self.assertIn("ebi_null_vals_all", obs)

    def test_extract_stds_config_default_path(self):
        """Test extracting standards configuration using default path.

        NB: This test assumes the default standards.yml exists. This may change, so
        it's just checking that a couple of the expected keys are present.
        """
        config = extract_stds_config(None)
        self.assertIsInstance(config, dict)
        self.assertIn("host_type_specific_metadata", config)

    def test_extract_stds_config_custom_path(self):
        """Test extracting standards configuration using a custom path."""
        config = extract_stds_config(path.join(self.TEST_DIR, "data/test_config.yml"))
        self.assertDictEqual(config, self.TEST_CONFIG_DICT)

    # Tests for deepcopy_dict
    def test_deepcopy_dict(self):
        """Test deep copying of nested dictionary structure.

        Verifies that modifications to the copy do not affect the original dictionary.
        """
        obs = deepcopy_dict(self.TEST_CONFIG_DICT)
        self.assertDictEqual(self.TEST_CONFIG_DICT, obs)
        self.assertIsNot(self.TEST_CONFIG_DICT, obs)
        obs["host_type_specific_metadata"]["base"]["metadata_fields"].pop(
            "sample_name")
        self.assertFalse(self.TEST_CONFIG_DICT == obs)

    # Tests for load_df_with_best_fit_encoding
    def test_load_df_with_best_fit_encoding_utf8(self):
        """Test loading DataFrame from a file with UTF-8 encoding."""
        test_data = "col1,col2\nval1,val2"
        test_file = path.join(self.TEST_DIR, "data/test_utf8.csv")
        with open(test_file, "w", encoding="utf-8") as f:
            f.write(test_data)

        try:
            df = load_df_with_best_fit_encoding(test_file, ",")
            self.assertEqual(len(df), 1)
            self.assertEqual(df.columns.tolist(), ["col1", "col2"])
            self.assertEqual(df.iloc[0]["col1"], "val1")
            self.assertEqual(df.iloc[0]["col2"], "val2")
        finally:
            if path.exists(test_file):
                os.remove(test_file)

    def test_load_df_with_best_fit_encoding_utf8_sig(self):
        """Test loading DataFrame from a file with UTF-8 with BOM signature encoding."""
        test_data = "col1,col2\nval1,val2"
        test_file = path.join(self.TEST_DIR, "data/test_utf8_sig.csv")
        with open(test_file, "w", encoding="utf-8-sig") as f:
            f.write(test_data)

        try:
            df = load_df_with_best_fit_encoding(test_file, ",")
            self.assertEqual(len(df), 1)
            self.assertEqual(df.columns.tolist(), ["col1", "col2"])
            self.assertEqual(df.iloc[0]["col1"], "val1")
            self.assertEqual(df.iloc[0]["col2"], "val2")
        finally:
            if path.exists(test_file):
                os.remove(test_file)

    def test_load_df_with_best_fit_encoding_invalid_file(self):
        """Test that attempting to load DataFrame from non-existent file raises ValueError."""
        with self.assertRaises(ValueError):
            load_df_with_best_fit_encoding("nonexistent.csv", ",")

    def test_load_df_with_best_fit_encoding_unsupported_encoding(self):
        """Test that attempting to load DataFrame with unsupported encoding raises ValueError."""
        test_file = os.path.join(self.TEST_DIR, "data/test.biom")

        try:
            with self.assertRaisesRegex(ValueError, "Unable to decode .* with any available encoder"):
                load_df_with_best_fit_encoding(test_file, ",")
        finally:
            if path.exists(test_file):
                os.remove(test_file)

    # Tests for validate_required_columns_exist
    def test_validate_required_columns_exist_empty_df(self):
        """Test that validation of required columns in an empty DataFrame raises ValueError."""

        empty_df = pandas.DataFrame()
        with self.assertRaisesRegex(ValueError, "test_df missing columns: \\['sample_name', 'sample_type'\\]"):
            validate_required_columns_exist(
                empty_df, ["sample_name", "sample_type"],
                "test_df missing columns")

    def test_validate_required_columns_exist_no_err(self):
        """Test successful validation of required columns when all required columns exist."""
        test_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        validate_required_columns_exist(
            test_df, ["sample_name", "sample_type"], "test_df missing")
        # if no error at step above, this test passed
        self.assertTrue(True)

    def test_validate_required_columns_exist_err(self):
        """Test that validation of required columns when a required column is missing raises ValueError."""

        test_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_tye": ["st1", "st2"]
        })

        err_msg = r"test_df missing column: \['sample_type'\]"
        with self.assertRaisesRegex(ValueError, err_msg):
            validate_required_columns_exist(
                test_df, ["sample_name", "sample_type"],
                "test_df missing column")

    # Tests for get_extension
    def test_get_extension(self):
        """Test that the correct file extension is returned for different separator types."""

        # Test comma separator
        self.assertEqual(get_extension(","), "csv")

        # Test tab separator
        self.assertEqual(get_extension("\t"), "txt")

        # Test other separators
        self.assertEqual(get_extension(";"), "txt")
        self.assertEqual(get_extension("|"), "txt")

    # Tests for update_metadata_df_field
    def test_update_metadata_df_field_constant_new_field(self):
        """Test that a new field can be added to the DataFrame with a constant value."""

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"],
            "new_field": ["bacon", "bacon"]
        })

        update_metadata_df_field(
            working_df, "new_field", "bacon",
            overwrite_non_nans=True)
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_constant_overwrite(self):
        """Test overwriting existing field in DataFrame with constant value.

        Verifies that an existing field can be overwritten with a constant value
        when overwrite_non_nans is True.
        """
        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["bacon", "bacon"]
        })

        update_metadata_df_field(
            working_df, "sample_type", "bacon",
            overwrite_non_nans=True)
        # with overwrite set to True, the column in question should have
        # every entry set to the input constant value
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_constant_no_overwrite_no_nan(self):
        """Test (not) updating field in DataFrame with constant value when no NaN values exist.

        Verifies that no changes are made when overwrite_non_nans is False
        and there are no NaN values to replace.
        """
        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", "bacon",
            overwrite_non_nans=False)
        # with overwrite set to False, no change should be made because there
        # are no NaN values in the column in question
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_constant_no_overwrite_w_nan(self):
        """Test updating field in DataFrame with constant value when NaN values exist.

        Verifies that only NaN values are replaced when overwrite_non_nans is False
        and there are NaN values to replace.
        """
        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": [np.nan, "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["bacon", "st2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", "bacon",
            overwrite_non_nans=False)
        # with overwrite set to False, only one change should be made because
        # there is only one NaN value in the column in question
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_function_new_field(self):
        """Test updating DataFrame with a new field using a function.

        Verifies that a new field can be added to the DataFrame using a function
        to compute values based on existing fields.
        """
        def test_func(row, source_fields):
            return f"processed_{row[source_fields[0]]}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", np.nan],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", np.nan],
            "sample_type": ["st1", "st2"],
            "processed": ["processed_s1", "processed_nan"]
        })

        update_metadata_df_field(
            working_df, "processed", test_func,
            ["sample_name"], overwrite_non_nans=True)
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_function_overwrite(self):
        """Test overwriting existing field in DataFrame using a function.

        Verifies that an existing field can be overwritten using a function
        to compute values based on existing fields when overwrite_non_nans is True.
        """
        def test_func(row, source_fields):
            source_field = source_fields[0]
            last_char = row[source_field][-1]
            return f"bacon{last_char}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["bacon1", "bacon2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", test_func,
            ["sample_name"], overwrite_non_nans=True)
        # with overwrite set to True, the column in question should have
        # every entry set to result of running the input function on the input
        # source fields in the same row
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_function_no_overwrite_no_nan(self):
        """Test (not) updating field in DataFrame with function when no NaN values exist.

        Verifies that, when using a function, no changes are made when overwrite_non_nans is False
        and there are no NaN values to replace.
        """
        def test_func(row, source_fields):
            source_field = source_fields[0]
            last_char = row[source_field][-1]
            return f"bacon{last_char}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", test_func,
            ["sample_name"], overwrite_non_nans=False)
        # with overwrite set to False, no change should be made because there
        # are no NaN values in the column in question
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_function_no_overwrite_w_nan(self):
        """Test updating field in DataFrame with function when NaN values exist.

        Verifies that, when using a function, only NaN values are replaced when overwrite_non_nans is False
        and there are NaN values to replace.
        """
        def test_func(row, source_fields):
            source_field = source_fields[0]
            last_char = row[source_field][-1]
            return f"bacon{last_char}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": [np.nan, "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["bacon1", "st2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", test_func,
            ["sample_name"], overwrite_non_nans=False)
        # with overwrite set to False, only one change should be made because
        # there is only one NaN value in the column in question
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_function_multiple_sources(self):
        """Test updating field using function with multiple source fields.

        Verifies that a new field can be created using a function that combines
        values from multiple source fields.
        """
        def test_func(row, source_fields):
            return f"{row[source_fields[0]]}_{row[source_fields[1]]}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"],
            "combined": ["s1_st1", "s2_st2"]
        })

        update_metadata_df_field(
            working_df, "combined", test_func,
            ["sample_name", "sample_type"], overwrite_non_nans=True)
        assert_frame_equal(exp_df, working_df)
