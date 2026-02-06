import numpy as np
import pandas
from pandas.testing import assert_frame_equal
import os
import os.path as path
from unittest import TestCase
from metameq.src.util import extract_config_dict, \
    extract_yaml_dict, extract_stds_config, deepcopy_dict, \
    validate_required_columns_exist, update_metadata_df_field, get_extension, \
    load_df_with_best_fit_encoding, cast_field_to_type, \
    _try_cast_to_int, _try_cast_to_bool


class TestUtil(TestCase):
    """Test suite for utility functions in metameq.src.util module."""

    # get the parent directory of the current file
    TEST_DIR = path.dirname(__file__)

    TEST_CONFIG_DICT = {
        "metadata_transformers": {
            "pre_transformers": {
                "collection_date": {
                    "sources": ["collection_timestamp"],
                    "function": "pass_through",
                },
                "days_since_first_day": {
                    "sources": ["days_since_first_day"],
                    "function": "transform_format_field_as_int",
                    "overwrite_non_nans": True
                }
            }
        },
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

    def test_update_metadata_df_field_integer_stored_as_string(self):
        """Test that an integer value is stored as a string in the DataFrame.

        Verifies that when an integer is passed as the field value, it is
        converted to a string representation to avoid pandas float coercion.
        """
        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"],
            "taxon_id": ["539655", "539655"]
        })

        update_metadata_df_field(
            working_df, "taxon_id", 539655,
            overwrite_non_nans=True)

        # Verify the values are strings, not integers or floats
        self.assertIsInstance(working_df["taxon_id"].iloc[0], str)
        self.assertIsInstance(working_df["taxon_id"].iloc[1], str)
        self.assertEqual(working_df["taxon_id"].iloc[0], "539655")
        self.assertEqual(working_df["taxon_id"].iloc[1], "539655")
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_same_source_and_target(self):
        """Test using the same column as both source and target of a transformer.

        Verifies that a field can be transformed in place using itself as the
        source, which requires internal handling to avoid reading modified values.
        """
        def format_lat_to_two_decimals(row, source_fields):
            val = float(row[source_fields[0]])
            return f"{val:.2f}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2", "s3"],
            "latitude": ["32.8812345678", "-117.2345678901", "0.1234567890"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2", "s3"],
            "latitude": ["32.88", "-117.23", "0.12"]
        })

        update_metadata_df_field(
            working_df, "latitude", format_lat_to_two_decimals,
            ["latitude"], overwrite_non_nans=True)
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_same_source_and_target_no_overwrite(self):
        """Test that same-column transformation fails when overwrite_non_nans is False.

        When overwrite_non_nans is False, existing non-NaN values are not modified,
        so the transformation is not applied to the existing latitude values.
        """
        def format_lat_to_two_decimals(row, source_fields):
            val = float(row[source_fields[0]])
            return f"{val:.2f}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2", "s3"],
            "latitude": ["32.8812345678", "-117.2345678901", "0.1234567890"]
        })

        # Expected: values remain unchanged because overwrite_non_nans=False
        # and all values are non-NaN
        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2", "s3"],
            "latitude": ["32.8812345678", "-117.2345678901", "0.1234567890"]
        })

        update_metadata_df_field(
            working_df, "latitude", format_lat_to_two_decimals,
            ["latitude"], overwrite_non_nans=False)
        assert_frame_equal(exp_df, working_df)


class TestCastFieldToType(TestCase):
    """Tests for cast_field_to_type function."""

    def test_cast_field_to_type_string(self):
        """Test casting a value to string."""
        result = cast_field_to_type(123, [str])

        self.assertEqual("123", result)
        self.assertIsInstance(result, str)

    def test_cast_field_to_type_integer(self):
        """Test casting a value to integer."""
        result = cast_field_to_type("42", [int])

        self.assertEqual(42, result)
        self.assertIsInstance(result, int)

    def test_cast_field_to_type_float(self):
        """Test casting a value to float."""
        result = cast_field_to_type("3.14", [float])

        self.assertEqual(3.14, result)
        self.assertIsInstance(result, float)

    def test_cast_field_to_type_bool(self):
        """Test casting a value to bool."""
        result = cast_field_to_type(1, [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_first_type_succeeds(self):
        """Test that first matching type in list is used."""
        result = cast_field_to_type("42", [str, int])

        self.assertEqual("42", result)
        self.assertIsInstance(result, str)

    def test_cast_field_to_type_fallback_to_second_type(self):
        """Test fallback to second type when first fails."""
        result = cast_field_to_type("hello", [int, str])

        self.assertEqual("hello", result)
        self.assertIsInstance(result, str)

    def test_cast_field_to_type_no_valid_type_raises_error(self):
        """Test that ValueError is raised when no type matches."""
        with self.assertRaisesRegex(ValueError, "Unable to cast 'hello' to any of the allowed types"):
            cast_field_to_type("hello", [int, float])

    def test_cast_field_to_type_float_string_to_int_success(self):
        """Test casting a float-formatted string to integer."""
        result = cast_field_to_type(" 447426.0 ", [int])

        self.assertEqual(447426, result)
        self.assertIsInstance(result, int)

    def test_cast_field_to_type_float_string_to_int_fail(self):
        """Test fail of casting a float-formatted string with nonzero decimals to integer."""
        with self.assertRaisesRegex(ValueError, "Unable to cast ' 447426.7 ' to any of the allowed types"):
            cast_field_to_type(" 447426.7 ", [int])

    def test_cast_field_to_type_non_string_to_int_fail(self):
        """Test that non-integer float input fails to cast to int."""
        with self.assertRaisesRegex(ValueError, "Unable to cast '123.8' to any of the allowed types"):
            cast_field_to_type(123.8, [int])

    def test_cast_field_to_type_negative_float_string_to_int(self):
        """Test casting a negative float-formatted string to integer."""
        result = cast_field_to_type("-42.0", [int])

        self.assertEqual(-42, result)
        self.assertIsInstance(result, int)

    def test_cast_field_to_type_zero_float_string_to_int(self):
        """Test casting zero as float-formatted string to integer."""
        result = cast_field_to_type("0.0", [int])

        self.assertEqual(0, result)
        self.assertIsInstance(result, int)

    # Bool passthrough tests
    def test_cast_field_to_type_bool_true_passthrough(self):
        """Test that True bool passes through unchanged."""
        result = cast_field_to_type(True, [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_bool_false_passthrough(self):
        """Test that False bool passes through unchanged."""
        result = cast_field_to_type(False, [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    # Numeric 0/1 to bool tests
    def test_cast_field_to_type_int_zero_to_bool(self):
        """Test casting int 0 to bool False."""
        result = cast_field_to_type(0, [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_int_one_to_bool(self):
        """Test casting int 1 to bool True."""
        result = cast_field_to_type(1, [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_float_zero_to_bool(self):
        """Test casting float 0.0 to bool False."""
        result = cast_field_to_type(0.0, [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_float_one_to_bool(self):
        """Test casting float 1.0 to bool True."""
        result = cast_field_to_type(1.0, [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    # String to bool (truthy) tests
    def test_cast_field_to_type_string_true_lowercase_to_bool(self):
        """Test casting 'true' to bool True."""
        result = cast_field_to_type("true", [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_True_to_bool(self):
        """Test casting 'True' to bool True."""
        result = cast_field_to_type("True", [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_TRUE_to_bool(self):
        """Test casting 'TRUE' to bool True (case insensitive)."""
        result = cast_field_to_type("TRUE", [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_yes_to_bool(self):
        """Test casting 'yes' to bool True."""
        result = cast_field_to_type("yes", [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_y_to_bool(self):
        """Test casting 'y' to bool True."""
        result = cast_field_to_type("y", [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_t_to_bool(self):
        """Test casting 't' to bool True."""
        result = cast_field_to_type("t", [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_1_to_bool(self):
        """Test casting '1' to bool True."""
        result = cast_field_to_type("1", [bool])

        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    # String to bool (falsy) tests
    def test_cast_field_to_type_string_false_lowercase_to_bool(self):
        """Test casting 'false' to bool False."""
        result = cast_field_to_type("false", [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_False_to_bool(self):
        """Test casting 'False' to bool False."""
        result = cast_field_to_type("False", [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_FALSE_to_bool(self):
        """Test casting 'FALSE' to bool False (case insensitive)."""
        result = cast_field_to_type("FALSE", [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_no_to_bool(self):
        """Test casting 'no' to bool False."""
        result = cast_field_to_type("no", [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_n_to_bool(self):
        """Test casting 'n' to bool False."""
        result = cast_field_to_type("n", [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_f_to_bool(self):
        """Test casting 'f' to bool False."""
        result = cast_field_to_type("f", [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_cast_field_to_type_string_0_to_bool(self):
        """Test casting '0' to bool False."""
        result = cast_field_to_type("0", [bool])

        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    # Values that should NOT cast to bool
    def test_cast_field_to_type_int_two_to_bool_fail(self):
        """Test that int 2 fails to cast to bool."""
        with self.assertRaisesRegex(ValueError, "Unable to cast '2' to any of the allowed types"):
            cast_field_to_type(2, [bool])

    def test_cast_field_to_type_float_nonbool_to_bool_fail(self):
        """Test that arbitrary float fails to cast to bool."""
        with self.assertRaisesRegex(ValueError, "Unable to cast '-138.3' to any of the allowed types"):
            cast_field_to_type(-138.3, [bool])

    def test_cast_field_to_type_string_maybe_to_bool_fail(self):
        """Test that non-boolean string fails to cast to bool."""
        with self.assertRaisesRegex(ValueError, "Unable to cast 'maybe' to any of the allowed types"):
            cast_field_to_type("maybe", [bool])

    def test_cast_field_to_type_float_string_to_bool_int_fail(self):
        """Test that '123.9' fails to cast when allowed types are [bool, int]."""
        with self.assertRaisesRegex(ValueError, "Unable to cast '123.9' to any of the allowed types"):
            cast_field_to_type("123.9", [bool, int])


class TestTryCastToInt(TestCase):
    """Tests for _try_cast_to_int helper function."""

    def test_try_cast_to_int_integer_string(self):
        """Test casting an integer string to int."""
        result = _try_cast_to_int("42")
        self.assertEqual(42, result)
        self.assertIsInstance(result, int)

    def test_try_cast_to_int_float_string_whole_number(self):
        """Test casting a float string with zero decimals to int."""
        result = _try_cast_to_int("42.0")
        self.assertEqual(42, result)
        self.assertIsInstance(result, int)

    def test_try_cast_to_int_float_string_with_decimals(self):
        """Test that float string with non-zero decimals returns None."""
        result = _try_cast_to_int("42.7")
        self.assertIsNone(result)

    def test_try_cast_to_int_negative_float_string(self):
        """Test casting a negative float string with zero decimals."""
        result = _try_cast_to_int("-42.0")
        self.assertEqual(-42, result)
        self.assertIsInstance(result, int)

    def test_try_cast_to_int_zero(self):
        """Test casting zero string."""
        result = _try_cast_to_int("0")
        self.assertEqual(0, result)
        self.assertIsInstance(result, int)

    def test_try_cast_to_int_zero_float_string(self):
        """Test casting zero as float string."""
        result = _try_cast_to_int("0.0")
        self.assertEqual(0, result)
        self.assertIsInstance(result, int)

    def test_try_cast_to_int_whitespace_padded(self):
        """Test casting string with whitespace padding."""
        result = _try_cast_to_int(" 42.0 ")
        self.assertEqual(42, result)
        self.assertIsInstance(result, int)

    def test_try_cast_to_int_actual_int(self):
        """Test casting an actual int passes through."""
        result = _try_cast_to_int(42)
        self.assertEqual(42, result)
        self.assertIsInstance(result, int)

    def test_try_cast_to_int_actual_float_whole(self):
        """Test casting an actual float with zero decimals."""
        result = _try_cast_to_int(42.0)
        self.assertEqual(42, result)
        self.assertIsInstance(result, int)

    def test_try_cast_to_int_actual_float_with_decimals(self):
        """Test that actual float with non-zero decimals returns None."""
        result = _try_cast_to_int(42.7)
        self.assertIsNone(result)

    def test_try_cast_to_int_non_numeric_string(self):
        """Test that non-numeric string returns None."""
        result = _try_cast_to_int("hello")
        self.assertIsNone(result)

    def test_try_cast_to_int_empty_string(self):
        """Test that empty string returns None."""
        result = _try_cast_to_int("")
        self.assertIsNone(result)

    def test_try_cast_to_int_none(self):
        """Test that None input returns None."""
        result = _try_cast_to_int(None)
        self.assertIsNone(result)


class TestTryCastToBool(TestCase):
    """Tests for _try_cast_to_bool helper function."""

    # Bool passthrough tests
    def test_try_cast_to_bool_true_passthrough(self):
        """Test that True bool passes through."""
        result = _try_cast_to_bool(True)
        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_try_cast_to_bool_false_passthrough(self):
        """Test that False bool passes through."""
        result = _try_cast_to_bool(False)
        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    # Numeric 0/1 tests
    def test_try_cast_to_bool_int_zero(self):
        """Test casting int 0 to bool False."""
        result = _try_cast_to_bool(0)
        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_try_cast_to_bool_int_one(self):
        """Test casting int 1 to bool True."""
        result = _try_cast_to_bool(1)
        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    def test_try_cast_to_bool_float_zero(self):
        """Test casting float 0.0 to bool False."""
        result = _try_cast_to_bool(0.0)
        self.assertEqual(False, result)
        self.assertIsInstance(result, bool)

    def test_try_cast_to_bool_float_one(self):
        """Test casting float 1.0 to bool True."""
        result = _try_cast_to_bool(1.0)
        self.assertEqual(True, result)
        self.assertIsInstance(result, bool)

    # String truthy tests
    def test_try_cast_to_bool_string_true(self):
        """Test casting 'true' to bool True."""
        result = _try_cast_to_bool("true")
        self.assertEqual(True, result)

    def test_try_cast_to_bool_string_TRUE(self):
        """Test casting 'TRUE' to bool True (case insensitive)."""
        result = _try_cast_to_bool("TRUE")
        self.assertEqual(True, result)

    def test_try_cast_to_bool_string_yes(self):
        """Test casting 'yes' to bool True."""
        result = _try_cast_to_bool("yes")
        self.assertEqual(True, result)

    def test_try_cast_to_bool_string_y(self):
        """Test casting 'y' to bool True."""
        result = _try_cast_to_bool("y")
        self.assertEqual(True, result)

    def test_try_cast_to_bool_string_t(self):
        """Test casting 't' to bool True."""
        result = _try_cast_to_bool("t")
        self.assertEqual(True, result)

    def test_try_cast_to_bool_string_1(self):
        """Test casting '1' to bool True."""
        result = _try_cast_to_bool("1")
        self.assertEqual(True, result)

    # String falsy tests
    def test_try_cast_to_bool_string_false(self):
        """Test casting 'false' to bool False."""
        result = _try_cast_to_bool("false")
        self.assertEqual(False, result)

    def test_try_cast_to_bool_string_FALSE(self):
        """Test casting 'FALSE' to bool False (case insensitive)."""
        result = _try_cast_to_bool("FALSE")
        self.assertEqual(False, result)

    def test_try_cast_to_bool_string_no(self):
        """Test casting 'no' to bool False."""
        result = _try_cast_to_bool("no")
        self.assertEqual(False, result)

    def test_try_cast_to_bool_string_n(self):
        """Test casting 'n' to bool False."""
        result = _try_cast_to_bool("n")
        self.assertEqual(False, result)

    def test_try_cast_to_bool_string_f(self):
        """Test casting 'f' to bool False."""
        result = _try_cast_to_bool("f")
        self.assertEqual(False, result)

    def test_try_cast_to_bool_string_0(self):
        """Test casting '0' to bool False."""
        result = _try_cast_to_bool("0")
        self.assertEqual(False, result)

    # Values that should return None
    def test_try_cast_to_bool_int_two(self):
        """Test that int 2 returns None."""
        result = _try_cast_to_bool(2)
        self.assertIsNone(result)

    def test_try_cast_to_bool_negative_int(self):
        """Test that negative int returns None."""
        result = _try_cast_to_bool(-1)
        self.assertIsNone(result)

    def test_try_cast_to_bool_arbitrary_float(self):
        """Test that arbitrary float returns None."""
        result = _try_cast_to_bool(3.14)
        self.assertIsNone(result)

    def test_try_cast_to_bool_non_boolean_string(self):
        """Test that non-boolean string returns None."""
        result = _try_cast_to_bool("maybe")
        self.assertIsNone(result)

    def test_try_cast_to_bool_empty_string(self):
        """Test that empty string returns None."""
        result = _try_cast_to_bool("")
        self.assertIsNone(result)

    def test_try_cast_to_bool_none(self):
        """Test that None input returns None."""
        result = _try_cast_to_bool(None)
        self.assertIsNone(result)
