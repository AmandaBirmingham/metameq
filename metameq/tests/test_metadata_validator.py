import glob
import os
import pandas as pd
import tempfile
from unittest import TestCase
from datetime import datetime
from datetime import timedelta
from metameq.src.metadata_validator import (
    _flatten_error_message,
    _generate_validation_msg,
    _get_allowed_pandas_types,
    _make_cerberus_schema,
    _remove_leaf_keys_from_dict,
    _remove_leaf_keys_from_dict_in_list,
    format_validation_msgs_as_df,
    MetameqValidator,
    output_validation_msgs,
    validate_metadata_df
)


class TestRemoveLeafKeysFromDictInList(TestCase):
    """Tests for _remove_leaf_keys_from_dict_in_list function."""

    def test__remove_leaf_keys_from_dict_in_list_simple(self):
        """Test removing keys from dicts in a flat list."""
        input_list = [
            {"a": 1, "b": 2, "c": 3},
            {"a": 4, "b": 5, "c": 6}
        ]
        keys_to_remove = ["b"]

        result = _remove_leaf_keys_from_dict_in_list(input_list, keys_to_remove)

        expected = [
            {"a": 1, "c": 3},
            {"a": 4, "c": 6}
        ]
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_in_list_nested_dicts(self):
        """Test removing keys from nested dicts within list items."""
        input_list = [
            {
                "outer": "value",
                "nested": {
                    "keep": "yes",
                    "remove_me": "be gone"
                }
            }
        ]
        keys_to_remove = ["remove_me"]

        result = _remove_leaf_keys_from_dict_in_list(input_list, keys_to_remove)

        expected = [
            {
                "outer": "value",
                "nested": {
                    "keep": "yes"
                }
            }
        ]
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_in_list_nested_lists(self):
        """Test handling nested lists containing dicts."""
        input_list = [
            [
                {"a": 1, "b": 2},
                {"a": 3, "b": 4}
            ],
            {"c": 5, "b": 6}
        ]
        keys_to_remove = ["b"]

        result = _remove_leaf_keys_from_dict_in_list(input_list, keys_to_remove)

        expected = [
            [
                {"a": 1},
                {"a": 3}
            ],
            {"c": 5}
        ]
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_in_list_non_dict_items(self):
        """Test that non-dict items in the list are preserved unchanged."""
        input_list = [
            "string_item",
            "b",  # Note this is a string, not a dict, so should remain
            123,
            {"a": 1, "b": 2},
            None,
            True
        ]
        keys_to_remove = ["b"]

        result = _remove_leaf_keys_from_dict_in_list(input_list, keys_to_remove)

        expected = [
            "string_item",
            "b",  # remains unchanged
            123,
            {"a": 1},
            None,
            True
        ]
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_in_list_empty_list(self):
        """Test that empty list returns empty list."""
        input_list = []
        keys_to_remove = ["a", "b"]

        result = _remove_leaf_keys_from_dict_in_list(input_list, keys_to_remove)

        self.assertEqual([], result)

    def test__remove_leaf_keys_from_dict_in_list_no_matching_keys(self):
        """Test when no keys match those to be removed."""
        input_list = [
            {"a": 1, "b": 2},
            {"c": 3, "d": 4}
        ]
        keys_to_remove = ["x", "y", "z"]

        result = _remove_leaf_keys_from_dict_in_list(input_list, keys_to_remove)

        expected = [
            {"a": 1, "b": 2},
            {"c": 3, "d": 4}
        ]
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_in_list_multiple_keys(self):
        """Test removing multiple keys at once."""
        input_list = [
            {"a": 1, "b": 2, "c": 3, "d": 4},
            {"a": 5, "b": 6, "c": 7, "d": 8}
        ]
        keys_to_remove = ["b", "d"]

        result = _remove_leaf_keys_from_dict_in_list(input_list, keys_to_remove)

        expected = [
            {"a": 1, "c": 3},
            {"a": 5, "c": 7}
        ]
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_in_list_deeply_nested(self):
        """Test removing keys from deeply nested structures."""
        input_list = [
            {
                "level1": {
                    "level2": {
                        "keep": "value",
                        "remove_me": "be gone"
                    },
                    "remove_me": "also be gone"
                }
            }
        ]
        keys_to_remove = ["remove_me"]

        result = _remove_leaf_keys_from_dict_in_list(input_list, keys_to_remove)

        expected = [
            {
                "level1": {
                    "level2": {
                        "keep": "value"
                    }
                }
            }
        ]
        self.assertEqual(expected, result)


class TestRemoveLeafKeysFromDict(TestCase):
    """Tests for _remove_leaf_keys_from_dict function."""

    def test__remove_leaf_keys_from_dict_simple(self):
        """Test removing specified keys from a flat dict."""
        input_dict = {"a": 1, "b": 2, "c": 3}
        keys_to_remove = ["b"]

        result = _remove_leaf_keys_from_dict(input_dict, keys_to_remove)

        expected = {"a": 1, "c": 3}
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_nested(self):
        """Test removing specified keys from nested dicts."""
        input_dict = {
            "outer": "value",
            "nested": {
                "keep": "yes",
                "remove_me": "be gone"
            },
            "remove_me": "top-level be gone"
        }
        keys_to_remove = ["remove_me"]

        result = _remove_leaf_keys_from_dict(input_dict, keys_to_remove)

        expected = {
            "outer": "value",
            "nested": {
                "keep": "yes"
            }
        }
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_with_list(self):
        """Test removing keys from dicts within lists."""
        input_dict = {
            "items": [
                {"a": 1, "b": 2},
                {"a": 3, "b": 4}
            ],
            "b": "top level"
        }
        keys_to_remove = ["b"]

        result = _remove_leaf_keys_from_dict(input_dict, keys_to_remove)

        expected = {
            "items": [
                {"a": 1},
                {"a": 3}
            ]
        }
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_no_matching_keys(self):
        """Test when no keys match those to be removed."""
        input_dict = {"a": 1, "b": 2, "c": 3}
        keys_to_remove = ["x", "y", "z"]

        result = _remove_leaf_keys_from_dict(input_dict, keys_to_remove)

        expected = {"a": 1, "b": 2, "c": 3}
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_empty(self):
        """Test that empty dict returns empty dict."""
        input_dict = {}
        keys_to_remove = ["a", "b"]

        result = _remove_leaf_keys_from_dict(input_dict, keys_to_remove)

        self.assertEqual({}, result)

    def test__remove_leaf_keys_from_dict_multiple_keys(self):
        """Test removing multiple keys at once."""
        input_dict = {"a": 1, "b": 2, "c": 3, "d": 4}
        keys_to_remove = ["b", "d"]

        result = _remove_leaf_keys_from_dict(input_dict, keys_to_remove)

        expected = {"a": 1, "c": 3}
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_deeply_nested(self):
        """Test removing keys from deeply nested structures."""
        input_dict = {
            "level1": {
                "level2": {
                    "level3": {
                        "keep": "value",
                        "remove_me": "be gone"
                    },
                    "remove_me": "level2 be gone"
                },
                "remove_me": "level1 be gone"
            }
        }
        keys_to_remove = ["remove_me"]

        result = _remove_leaf_keys_from_dict(input_dict, keys_to_remove)

        expected = {
            "level1": {
                "level2": {
                    "level3": {
                        "keep": "value"
                    }
                }
            }
        }
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_key_with_dict_value_not_removed(self):
        """Test that keys with dict values are preserved, only their contents processed."""
        input_dict = {
            "remove_me": {
                "nested_key": "value",
                "remove_me": "be gone"
            },
            "keep": "yes"
        }
        keys_to_remove = ["remove_me"]

        result = _remove_leaf_keys_from_dict(input_dict, keys_to_remove)

        # Keys with dict values are NOT removed; only non-dict, non-list-valued keys are removed
        expected = {
            "remove_me": {
                "nested_key": "value"
            },
            "keep": "yes"
        }
        self.assertEqual(expected, result)

    def test__remove_leaf_keys_from_dict_mixed_nested_structures(self):
        """Test with mixed nested dicts and lists."""
        input_dict = {
            "config": {
                "items": [
                    {"name": "item1", "secret": "hidden"},
                    {"name": "item2", "secret": "also hidden"}
                ],
                "secret": "hidden config"
            },
            "secret": "hidden secret"
        }
        keys_to_remove = ["secret"]

        result = _remove_leaf_keys_from_dict(input_dict, keys_to_remove)

        expected = {
            "config": {
                "items": [
                    {"name": "item1"},
                    {"name": "item2"}
                ]
            }
        }
        self.assertEqual(expected, result)


class TestMakeCerberusSchema(TestCase):
    """Tests for _make_cerberus_schema function."""

    def test__make_cerberus_schema_removes_is_phi(self):
        """Test that is_phi key is removed from schema."""
        input_dict = {
            "field1": {
                "type": "string",
                "is_phi": True
            }
        }

        result = _make_cerberus_schema(input_dict)

        expected = {
            "field1": {
                "type": "string"
            }
        }
        self.assertEqual(expected, result)

    def test__make_cerberus_schema_removes_field_desc(self):
        """Test that field_desc key is removed from schema."""
        input_dict = {
            "field1": {
                "type": "string",
                "field_desc": "A description of the field"
            }
        }

        result = _make_cerberus_schema(input_dict)

        expected = {
            "field1": {
                "type": "string"
            }
        }
        self.assertEqual(expected, result)

    def test__make_cerberus_schema_removes_units(self):
        """Test that units key is removed from schema."""
        input_dict = {
            "field1": {
                "type": "float",
                "units": "meters"
            }
        }

        result = _make_cerberus_schema(input_dict)

        expected = {
            "field1": {
                "type": "float"
            }
        }
        self.assertEqual(expected, result)

    def test__make_cerberus_schema_removes_min_exclusive(self):
        """Test that min_exclusive key is removed from schema."""
        input_dict = {
            "field1": {
                "type": "integer",
                "min_exclusive": 0
            }
        }

        result = _make_cerberus_schema(input_dict)

        expected = {
            "field1": {
                "type": "integer"
            }
        }
        self.assertEqual(expected, result)

    def test__make_cerberus_schema_removes_unique(self):
        """Test that unique key is removed from schema."""
        input_dict = {
            "field1": {
                "type": "string",
                "unique": True
            }
        }

        result = _make_cerberus_schema(input_dict)

        expected = {
            "field1": {
                "type": "string"
            }
        }
        self.assertEqual(expected, result)

    def test__make_cerberus_schema_preserves_cerberus_keys(self):
        """Test that valid cerberus keys are preserved."""
        input_dict = {
            "field1": {
                "type": "string",
                "required": True,
                "allowed": ["a", "b", "c"],
                "default": "a"
            }
        }

        result = _make_cerberus_schema(input_dict)

        expected = {
            "field1": {
                "type": "string",
                "required": True,
                "allowed": ["a", "b", "c"],
                "default": "a"
            }
        }
        self.assertEqual(expected, result)

    def test__make_cerberus_schema_removes_multiple_unrecognized_keys(self):
        """Test removing multiple unrecognized keys at once."""
        input_dict = {
            "field1": {
                "type": "string",
                "is_phi": False,
                "field_desc": "description",
                "units": "none",
                "min_exclusive": 0,
                "unique": True,
                "required": True
            }
        }

        result = _make_cerberus_schema(input_dict)

        expected = {
            "field1": {
                "type": "string",
                "required": True
            }
        }
        self.assertEqual(expected, result)

    def test__make_cerberus_schema_nested_fields(self):
        """Test that unrecognized keys are removed from nested structures."""
        input_dict = {
            "field1": {
                "type": "string",
                "is_phi": True,
                "anyof": [
                    {"type": "string", "field_desc": "string option"},
                    {"type": "integer", "units": "count"}
                ]
            }
        }

        result = _make_cerberus_schema(input_dict)

        expected = {
            "field1": {
                "type": "string",
                "anyof": [
                    {"type": "string"},
                    {"type": "integer"}
                ]
            }
        }
        self.assertEqual(expected, result)

    def test__make_cerberus_schema_empty_dict(self):
        """Test that empty dict returns empty dict."""
        input_dict = {}

        result = _make_cerberus_schema(input_dict)

        self.assertEqual({}, result)

    def test__make_cerberus_schema_does_not_modify_original(self):
        """Test that the original dictionary is not modified."""
        input_dict = {
            "field1": {
                "type": "string",
                "is_phi": True
            }
        }

        _make_cerberus_schema(input_dict)

        # Original should still have is_phi
        self.assertEqual(True, input_dict["field1"]["is_phi"])


class TestOutputValidationMsgs(TestCase):
    """Tests for output_validation_msgs function."""

    def test_output_validation_msgs_non_empty_df_tab_separator(self):
        """Test writing non-empty DataFrame with tab separator creates .txt file."""
        validation_msgs_df = pd.DataFrame({
            "sample_name": ["sample1", "sample2"],
            "field_name": ["field1", "field2"],
            "error_message": ["error1", "error2"]
        })

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_validation_msgs(validation_msgs_df, tmp_dir, "test", sep="\t")

            output_files = glob.glob(os.path.join(tmp_dir, "*_test_validation_errors.txt"))
            self.assertEqual(1, len(output_files))

            result_df = pd.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
            pd.testing.assert_frame_equal(validation_msgs_df, result_df)

    def test_output_validation_msgs_non_empty_df_comma_separator(self):
        """Test writing non-empty DataFrame with comma separator creates .csv file."""
        validation_msgs_df = pd.DataFrame({
            "sample_name": ["sample1", "sample2"],
            "field_name": ["field1", "field2"],
            "error_message": ["error1", "error2"]
        })

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_validation_msgs(validation_msgs_df, tmp_dir, "test", sep=",")

            output_files = glob.glob(os.path.join(tmp_dir, "*_test_validation_errors.csv"))
            self.assertEqual(1, len(output_files))

            result_df = pd.read_csv(output_files[0], sep=",", dtype=str, keep_default_na=False)
            pd.testing.assert_frame_equal(validation_msgs_df, result_df)

    def test_output_validation_msgs_empty_df_creates_empty_file(self):
        """Test that empty DataFrame creates empty file when suppress_empty_fails=False."""
        validation_msgs_df = pd.DataFrame()

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_validation_msgs(
                validation_msgs_df, tmp_dir, "test", sep="\t",
                suppress_empty_fails=False)

            output_files = glob.glob(os.path.join(tmp_dir, "*_test_validation_errors.txt"))
            self.assertEqual(1, len(output_files))

            # Verify file is empty
            self.assertEqual(0, os.path.getsize(output_files[0]))

    def test_output_validation_msgs_empty_df_suppressed_no_file(self):
        """Test that empty DataFrame creates no file when suppress_empty_fails=True."""
        validation_msgs_df = pd.DataFrame()

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_validation_msgs(
                validation_msgs_df, tmp_dir, "test", sep="\t",
                suppress_empty_fails=True)

            # Verify no file was created
            output_files = glob.glob(os.path.join(tmp_dir, "*_test_validation_errors.*"))
            self.assertEqual(0, len(output_files))

    def test_output_validation_msgs_filename_contains_timestamp(self):
        """Test that output filename contains a timestamp prefix."""
        validation_msgs_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["field1"],
            "error_message": ["error1"]
        })

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_validation_msgs(validation_msgs_df, tmp_dir, "mybase", sep="\t")

            output_files = glob.glob(os.path.join(tmp_dir, "*_mybase_validation_errors.txt"))
            self.assertEqual(1, len(output_files))

            # Verify filename has timestamp pattern (YYYY-MM-DD_HH-MM-SS)
            filename = os.path.basename(output_files[0])
            # Format: YYYY-MM-DD_HH-MM-SS_mybase_validation_errors.txt
            parts = filename.split("_")
            # Should have date part (YYYY-MM-DD) and time part (HH-MM-SS)
            self.assertEqual(3, len(parts[0].split("-")))  # date has 3 parts
            self.assertEqual(3, len(parts[1].split("-")))  # time has 3 parts

    def test_output_validation_msgs_default_separator_is_tab(self):
        """Test that default separator is tab, producing .txt file."""
        validation_msgs_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["field1"],
            "error_message": ["error1"]
        })

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Call without specifying sep parameter
            output_validation_msgs(validation_msgs_df, tmp_dir, "test")

            # Should create .txt file (tab separator default)
            txt_files = glob.glob(os.path.join(tmp_dir, "*_test_validation_errors.txt"))
            csv_files = glob.glob(os.path.join(tmp_dir, "*_test_validation_errors.csv"))
            self.assertEqual(1, len(txt_files))
            self.assertEqual(0, len(csv_files))


class TestGetAllowedPandasTypes(TestCase):
    """Tests for _get_allowed_pandas_types function."""

    def test__get_allowed_pandas_types_string(self):
        """Test that cerberus 'string' type maps to Python str."""
        field_definition = {"type": "string"}

        result = _get_allowed_pandas_types("test_field", field_definition)

        self.assertEqual([str], result)

    def test__get_allowed_pandas_types_integer(self):
        """Test that cerberus 'integer' type maps to Python int."""
        field_definition = {"type": "integer"}

        result = _get_allowed_pandas_types("test_field", field_definition)

        self.assertEqual([int], result)

    def test__get_allowed_pandas_types_float(self):
        """Test that cerberus 'float' type maps to Python float."""
        field_definition = {"type": "float"}

        result = _get_allowed_pandas_types("test_field", field_definition)

        self.assertEqual([float], result)

    def test__get_allowed_pandas_types_number(self):
        """Test that cerberus 'number' type maps to Python float."""
        field_definition = {"type": "number"}

        result = _get_allowed_pandas_types("test_field", field_definition)

        self.assertEqual([float], result)

    def test__get_allowed_pandas_types_bool(self):
        """Test that cerberus 'bool' type maps to Python bool."""
        field_definition = {"type": "bool"}

        result = _get_allowed_pandas_types("test_field", field_definition)

        self.assertEqual([bool], result)

    def test__get_allowed_pandas_types_datetime(self):
        """Test that cerberus 'datetime' type maps to datetime.date."""
        field_definition = {"type": "datetime"}

        result = _get_allowed_pandas_types("test_field", field_definition)

        self.assertEqual([datetime.date], result)

    def test__get_allowed_pandas_types_anyof_single(self):
        """Test anyof with single type option."""
        field_definition = {
            "anyof": [
                {"type": "string"}
            ]
        }

        result = _get_allowed_pandas_types("test_field", field_definition)

        self.assertEqual([str], result)

    def test__get_allowed_pandas_types_anyof_multiple(self):
        """Test anyof with multiple type options."""
        field_definition = {
            "anyof": [
                {"type": "string"},
                {"type": "integer"},
                {"type": "float"}
            ]
        }

        result = _get_allowed_pandas_types("test_field", field_definition)

        self.assertEqual([str, int, float], result)

    def test__get_allowed_pandas_types_no_type_raises_error(self):
        """Test that missing type definition raises ValueError."""
        field_definition = {"required": True}

        self.assertRaisesRegex(
            ValueError,
            "Unable to find type definition for field 'my_field'",
            _get_allowed_pandas_types,
            "my_field",
            field_definition)


class TestMetameqValidatorCheckWithDateNotInFuture(TestCase):
    """Tests for MetameqValidator._check_with_date_not_in_future method."""

    def test_check_with_date_not_in_future_valid_past_date(self):
        """Test that a past date passes validation."""
        validator = MetameqValidator()
        schema = {"date_field": {"type": "string", "check_with": "date_not_in_future"}}

        result = validator.validate({"date_field": "2020-01-15"}, schema)

        self.assertTrue(result)
        self.assertEqual({}, validator.errors)

    def test_check_with_date_not_in_future_valid_today(self):
        """Test that today's date passes validation."""
        validator = MetameqValidator()
        schema = {"date_field": {"type": "string", "check_with": "date_not_in_future"}}
        today_str = datetime.now().strftime("%Y-%m-%d")

        result = validator.validate({"date_field": today_str}, schema)

        self.assertTrue(result)
        self.assertEqual({}, validator.errors)

    def test_check_with_date_not_in_future_future_date_fails(self):
        """Test that a future date fails validation."""
        validator = MetameqValidator()
        schema = {"date_field": {"type": "string", "check_with": "date_not_in_future"}}
        future_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

        result = validator.validate({"date_field": future_date}, schema)

        self.assertFalse(result)
        self.assertIn("date_field", validator.errors)
        self.assertIn("Date cannot be in the future", validator.errors["date_field"])

    def test_check_with_date_not_in_future_invalid_date_string_fails(self):
        """Test that an invalid date string fails validation."""
        validator = MetameqValidator()
        schema = {"date_field": {"type": "string", "check_with": "date_not_in_future"}}

        result = validator.validate({"date_field": "not a date"}, schema)

        self.assertFalse(result)
        self.assertIn("date_field", validator.errors)
        self.assertIn("Must be a valid date", validator.errors["date_field"])

    def test_check_with_date_not_in_future_various_date_formats(self):
        """Test that various date formats are accepted."""
        validator = MetameqValidator()
        schema = {"date_field": {"type": "string", "check_with": "date_not_in_future"}}

        date_formats = [
            "2020-01-15",
            "01/15/2020",
            "January 15, 2020",
            "15 Jan 2020"
        ]

        for date_str in date_formats:
            result = validator.validate({"date_field": date_str}, schema)
            self.assertTrue(result, f"Date format '{date_str}' should be valid")


class TestGenerateValidationMsg(TestCase):
    """Tests for _generate_validation_msg function."""

    def test__generate_validation_msg_all_valid(self):
        """Test that valid rows return empty list."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1", "sample2"],
            "field1": ["value1", "value2"]
        })
        config = {
            "sample_name": {"type": "string"},
            "field1": {"type": "string"}
        }

        result = _generate_validation_msg(metadata_df, config)

        self.assertEqual([], result)

    def test__generate_validation_msg_single_error(self):
        """Test that a single validation error is captured."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "age": ["not_an_integer"]
        })
        config = {
            "sample_name": {"type": "string"},
            "age": {"type": "integer"}
        }

        result = _generate_validation_msg(metadata_df, config)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["age"],
            "error_message": [["must be of integer type"]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)

    def test__generate_validation_msg_multiple_errors_single_row(self):
        """Test that multiple errors in one row are all captured."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "age": ["not_an_integer"],
            "count": ["also_not_an_integer"]
        })
        config = {
            "sample_name": {"type": "string"},
            "age": {"type": "integer"},
            "count": {"type": "integer"}
        }

        result = _generate_validation_msg(metadata_df, config)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1", "sample1"],
            "field_name": ["age", "count"],
            "error_message": [["must be of integer type"], ["must be of integer type"]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)

    def test__generate_validation_msg_errors_across_multiple_rows(self):
        """Test that errors across multiple rows are all captured."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1", "sample2"],
            "age": ["not_an_integer", "also_not_an_integer"]
        })
        config = {
            "sample_name": {"type": "string"},
            "age": {"type": "integer"}
        }

        result = _generate_validation_msg(metadata_df, config)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1", "sample2"],
            "field_name": ["age", "age"],
            "error_message": [["must be of integer type"], ["must be of integer type"]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)

    def test__generate_validation_msg_allows_unknown_fields(self):
        """Test that unknown fields are allowed and don't cause errors."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "known_field": [42],
            "unknown_field": ["extra_value"]
        })
        config = {
            "sample_name": {"type": "string"},
            "known_field": {"type": "integer"}
        }

        result = _generate_validation_msg(metadata_df, config)

        self.assertEqual([], result)

    def test__generate_validation_msg_required_field_missing(self):
        """Test that missing required fields are caught."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "optional_field": ["value"]
        })
        config = {
            "sample_name": {"type": "string"},
            "required_field": {"type": "string", "required": True}
        }

        result = _generate_validation_msg(metadata_df, config)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["required_field"],
            "error_message": [["required field"]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)

    def test__generate_validation_msg_multiple_errors_same_field(self):
        """Test that multiple errors for the same field are returned as a list."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "date_field": ["not a date"]
        })
        config = {
            "sample_name": {"type": "string"},
            "date_field": {
                "type": "string",
                "regex": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
                "check_with": "date_not_in_future"
            }
        }

        result = _generate_validation_msg(metadata_df, config)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["date_field"],
            "error_message": [[
                "Must be a valid date",
                "value does not match regex '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'"
            ]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)

    def test__generate_validation_msg_anyof_violation(self):
        """Test that an anyof schema violation is captured with all
        sub-definition errors."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "status": ["invalid"]
        })
        config = {
            "sample_name": {"type": "string"},
            "status": {
                "anyof": [
                    {"type": "string", "allowed": ["active", "inactive"]},
                    {"type": "integer", "allowed": [0, 1]}
                ]
            }
        }

        result = _generate_validation_msg(metadata_df, config)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["status"],
            "error_message": [[
                "no definitions validate",
                {"anyof definition 0": ["unallowed value invalid"],
                 "anyof definition 1": ["must be of integer type"]}
            ]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)


class TestValidateMetadataDf(TestCase):
    """Tests for validate_metadata_df function."""

    def test_validate_metadata_df_all_valid(self):
        """Test that valid metadata returns empty list."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1", "sample2"],
            "age": [25, 30]
        })
        fields_dict = {
            "sample_name": {"type": "string"},
            "age": {"type": "integer"}
        }

        result = validate_metadata_df(metadata_df, fields_dict)

        self.assertEqual([], result)

    def test_validate_metadata_df_uncastable_value_raises_error(self):
        """Test that values that cannot be cast to expected type raise ValueError."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "age": ["not_an_integer"]
        })
        fields_dict = {
            "sample_name": {"type": "string"},
            "age": {"type": "integer"}
        }

        self.assertRaisesRegex(
            ValueError,
            "Unable to cast 'not_an_integer' to any of the allowed types",
            validate_metadata_df,
            metadata_df,
            fields_dict)

    def test_validate_metadata_df_strips_metameq_keys(self):
        """Test that metameq-specific keys are stripped before validation."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field1": ["12"]
        })
        # Include metameq-specific keys that should be stripped
        fields_dict = {
            "sample_name": {"type": "string", "unique": True},
            "field1": {
                "type": "integer",
                "is_phi": True,
                "field_desc": "A test field",
                "units": "none",
                "min_exclusive": 0
            }
        }

        # Should not raise an error about unknown schema keys
        result = validate_metadata_df(metadata_df, fields_dict)

        self.assertEqual([], result)

    def test_validate_metadata_df_missing_field_in_df_skipped(self):
        """Test that fields defined in schema but missing from DataFrame are skipped."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "existing_field": ["value"]
        })
        fields_dict = {
            "sample_name": {"type": "string"},
            "existing_field": {"type": "string"},
            "missing_field": {"type": "integer"}
        }

        # Should not raise an error; missing_field is simply skipped
        result = validate_metadata_df(metadata_df, fields_dict)

        self.assertEqual([], result)

    def test_validate_metadata_df_casts_to_expected_type(self):
        """Test that fields are cast to their expected types before validation."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "count": ["42"]  # String that can be cast to int
        })
        fields_dict = {
            "sample_name": {"type": "string"},
            "count": {"type": "integer"}
        }

        result = validate_metadata_df(metadata_df, fields_dict)

        # After casting "42" to int, it should be valid
        self.assertEqual([], result)

    def test_validate_metadata_df_anyof_type_validation(self):
        """Test validation with anyof type definitions."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1", "sample2"],
            "flexible_field": ["text", "123"]
        })
        fields_dict = {
            "sample_name": {"type": "string"},
            "flexible_field": {
                "anyof": [
                    {"type": "string"},
                    {"type": "integer"}
                ]
            }
        }

        result = validate_metadata_df(metadata_df, fields_dict)

        self.assertEqual([], result)

    def test_validate_metadata_df_multiple_rows(self):
        """Test validation across multiple rows."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1", "sample2"],
            "status": ["invalid_status", "active"]
        })
        fields_dict = {
            "sample_name": {"type": "string"},
            "status": {"type": "string", "allowed": ["active", "inactive"]}
        }

        result = validate_metadata_df(metadata_df, fields_dict)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["status"],
            "error_message": [["unallowed value invalid_status"]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)

    def test_validate_metadata_df_allowed_values_validation(self):
        """Test validation of allowed values constraint."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "status": ["invalid_status"]
        })
        fields_dict = {
            "sample_name": {"type": "string"},
            "status": {"type": "string", "allowed": ["active", "inactive"]}
        }

        result = validate_metadata_df(metadata_df, fields_dict)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["status"],
            "error_message": [["unallowed value invalid_status"]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)

    def test_validate_metadata_df_regex_validation(self):
        """Test validation of regex constraint."""
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "code": ["abc"]
        })
        fields_dict = {
            "sample_name": {"type": "string"},
            "code": {"type": "string", "regex": "^[0-9]+$"}
        }

        result = validate_metadata_df(metadata_df, fields_dict)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["code"],
            "error_message": [["value does not match regex '^[0-9]+$'"]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)

    def test_validate_metadata_df_custom_check_with_validation(self):
        """Test validation with custom check_with rule."""
        future_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
        metadata_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "collection_date": [future_date]
        })
        fields_dict = {
            "sample_name": {"type": "string"},
            "collection_date": {"type": "string", "check_with": "date_not_in_future"}
        }

        result = validate_metadata_df(metadata_df, fields_dict)
        result_df = pd.DataFrame(result)

        expected_df = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["collection_date"],
            "error_message": [["Date cannot be in the future"]]
        })
        pd.testing.assert_frame_equal(expected_df, result_df)


class TestFlattenErrorMessage(TestCase):
    """Tests for _flatten_error_message function."""

    def test__flatten_error_message_plain_strings(self):
        """Test that a list of plain strings is returned as-is."""
        error_message = ["must be of integer type"]

        result = _flatten_error_message(error_message)

        self.assertEqual(["must be of integer type"], result)

    def test__flatten_error_message_multiple_plain_strings(self):
        """Test that multiple plain strings are returned as-is."""
        error_message = ["error one", "error two"]

        result = _flatten_error_message(error_message)

        self.assertEqual(["error one", "error two"], result)

    def test__flatten_error_message_anyof_pattern(self):
        """Test that the anyof pattern (string + dict) produces two
        separate string items."""
        error_message = [
            "no definitions validate",
            {"anyof definition 0": ["unallowed value invalid"],
             "anyof definition 1": ["must be of integer type"]}
        ]

        result = _flatten_error_message(error_message)

        self.assertEqual(
            ["no definitions validate",
             "anyof definition 0: unallowed value invalid; "
             "anyof definition 1: must be of integer type"],
            result)

    def test__flatten_error_message_dict_only(self):
        """Test that a dict with no preceding string produces a detail
        string without parentheses."""
        error_message = [
            {"anyof definition 0": ["error a"],
             "anyof definition 1": ["error b"]}
        ]

        result = _flatten_error_message(error_message)

        self.assertEqual(
            ["anyof definition 0: error a; anyof definition 1: error b"],
            result)

    def test__flatten_error_message_empty_list(self):
        """Test that an empty list returns an empty list."""
        result = _flatten_error_message([])

        self.assertEqual([], result)

    def test__flatten_error_message_multiple_errors_in_definition(self):
        """Test that multiple errors within a single anyof definition
        are comma-separated."""
        error_message = [
            "no definitions validate",
            {"anyof definition 0": ["error a", "error b"]}
        ]

        result = _flatten_error_message(error_message)

        self.assertEqual(
            ["no definitions validate",
             "anyof definition 0: error a, error b"],
            result)

    def test__flatten_error_message_strings_and_dict_each_independent(self):
        """Test that strings and dicts are each independently converted,
        with no merging between items."""
        error_message = [
            "first error",
            "no definitions validate",
            {"anyof definition 0": ["error a"]}
        ]

        result = _flatten_error_message(error_message)

        self.assertEqual(
            ["first error",
             "no definitions validate",
             "anyof definition 0: error a"],
            result)

    def test__flatten_error_message_unexpected_type(self):
        """Test that unexpected types are converted to strings via str()."""
        error_message = ["a string error", 42, ["a", "list"]]

        result = _flatten_error_message(error_message)

        self.assertEqual(
            ["a string error", "42", "['a', 'list']"],
            result)


class TestFormatValidationMsgsAsDf(TestCase):
    """Tests for format_validation_msgs_as_df function."""

    def test_format_validation_msgs_as_df_empty_list(self):
        """Test that empty input returns an empty DataFrame with correct columns."""
        result = format_validation_msgs_as_df([])

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(
            ["sample_name", "field_name", "error_message"],
            list(result.columns))
        self.assertEqual(0, len(result))

    def test_format_validation_msgs_as_df_single_error(self):
        """Test formatting a single validation message with one error."""
        validation_msgs = [
            {
                "sample_name": "sample1",
                "field_name": "age",
                "error_message": ["must be of integer type"]
            }
        ]

        result = format_validation_msgs_as_df(validation_msgs)

        expected = pd.DataFrame({
            "sample_name": ["sample1"],
            "field_name": ["age"],
            "error_message": ["must be of integer type"]
        })
        pd.testing.assert_frame_equal(expected, result)

    def test_format_validation_msgs_as_df_multiple_errors_same_field(self):
        """Test that multiple errors for one field are flattened to separate rows."""
        validation_msgs = [
            {
                "sample_name": "sample1",
                "field_name": "date_field",
                "error_message": [
                    "Must be a valid date",
                    "value does not match regex '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'"
                ]
            }
        ]

        result = format_validation_msgs_as_df(validation_msgs)

        expected = pd.DataFrame({
            "sample_name": ["sample1", "sample1"],
            "field_name": ["date_field", "date_field"],
            "error_message": [
                "Must be a valid date",
                "value does not match regex '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'"
            ]
        })
        pd.testing.assert_frame_equal(expected, result)

    def test_format_validation_msgs_as_df_multiple_fields_same_sample(self):
        """Test multiple fields with errors for the same sample."""
        validation_msgs = [
            {
                "sample_name": "sample1",
                "field_name": "age",
                "error_message": ["must be of integer type"]
            },
            {
                "sample_name": "sample1",
                "field_name": "count",
                "error_message": ["must be of integer type"]
            }
        ]

        result = format_validation_msgs_as_df(validation_msgs)

        expected = pd.DataFrame({
            "sample_name": ["sample1", "sample1"],
            "field_name": ["age", "count"],
            "error_message": [
                "must be of integer type",
                "must be of integer type"
            ]
        })
        pd.testing.assert_frame_equal(expected, result)

    def test_format_validation_msgs_as_df_multiple_samples(self):
        """Test errors across multiple samples."""
        validation_msgs = [
            {
                "sample_name": "sample1",
                "field_name": "age",
                "error_message": ["must be of integer type"]
            },
            {
                "sample_name": "sample2",
                "field_name": "age",
                "error_message": ["must be of integer type"]
            }
        ]

        result = format_validation_msgs_as_df(validation_msgs)

        expected = pd.DataFrame({
            "sample_name": ["sample1", "sample2"],
            "field_name": ["age", "age"],
            "error_message": [
                "must be of integer type",
                "must be of integer type"
            ]
        })
        pd.testing.assert_frame_equal(expected, result)

    def test_format_validation_msgs_as_df_sorted_by_sample_then_field(self):
        """Test that output is sorted by sample_name then field_name."""
        validation_msgs = [
            {
                "sample_name": "sample_z",
                "field_name": "beta_field",
                "error_message": ["error z-beta"]
            },
            {
                "sample_name": "sample_a",
                "field_name": "gamma_field",
                "error_message": ["error a-gamma"]
            },
            {
                "sample_name": "sample_a",
                "field_name": "alpha_field",
                "error_message": ["error a-alpha"]
            },
            {
                "sample_name": "sample_z",
                "field_name": "alpha_field",
                "error_message": ["error z-alpha"]
            }
        ]

        result = format_validation_msgs_as_df(validation_msgs)

        expected = pd.DataFrame({
            "sample_name": [
                "sample_a", "sample_a", "sample_z", "sample_z"],
            "field_name": [
                "alpha_field", "gamma_field", "alpha_field", "beta_field"],
            "error_message": [
                "error a-alpha", "error a-gamma",
                "error z-alpha", "error z-beta"]
        })
        pd.testing.assert_frame_equal(expected, result)

    def test_format_validation_msgs_as_df_flattening_and_sorting_combined(self):
        """Test that flattening and sorting work correctly together."""
        validation_msgs = [
            {
                "sample_name": "sample_b",
                "field_name": "field_x",
                "error_message": ["error 2", "error 1"]
            },
            {
                "sample_name": "sample_a",
                "field_name": "field_y",
                "error_message": ["error 5", "error 3"]
            },
            {
                "sample_name": "sample_a",
                "field_name": "field_x",
                "error_message": ["error 4"]
            }
        ]

        result = format_validation_msgs_as_df(validation_msgs)

        expected = pd.DataFrame({
            "sample_name": [
                "sample_a", "sample_a", "sample_a",
                "sample_b", "sample_b"],
            "field_name": [
                "field_x", "field_y", "field_y",
                "field_x", "field_x"],
            "error_message": [
                "error 4", "error 3", "error 5",
                "error 1", "error 2"]
        })
        pd.testing.assert_frame_equal(expected, result)

    def test_format_validation_msgs_as_df_anyof_error(self):
        """Test that anyof error messages are flattened to separate string
        rows."""
        validation_msgs = [
            {
                "sample_name": "sample1",
                "field_name": "status",
                "error_message": [
                    "no definitions validate",
                    {"anyof definition 0": ["unallowed value invalid"],
                     "anyof definition 1": ["must be of integer type"]}
                ]
            }
        ]

        result = format_validation_msgs_as_df(validation_msgs)

        expected = pd.DataFrame({
            "sample_name": ["sample1", "sample1"],
            "field_name": ["status", "status"],
            "error_message": [
                "anyof definition 0: unallowed value invalid; "
                "anyof definition 1: must be of integer type",
                "no definitions validate"
            ]
        })
        pd.testing.assert_frame_equal(expected, result)

    def test_format_validation_msgs_as_df_anyof_and_other_error(self):
        """Test that a field failing both anyof and another constraint
        produces three separate rows."""
        validation_msgs = [
            {
                "sample_name": "sample1",
                "field_name": "status",
                "error_message": [
                    "no definitions validate",
                    "value does not match regex '^[0-9]+'",
                    {"anyof definition 0": ["unallowed value invalid"],
                     "anyof definition 1": ["must be of integer type"]}
                ]
            }
        ]

        result = format_validation_msgs_as_df(validation_msgs)

        expected = pd.DataFrame({
            "sample_name": ["sample1", "sample1", "sample1"],
            "field_name": ["status", "status", "status"],
            "error_message": [
                "anyof definition 0: unallowed value invalid; "
                "anyof definition 1: must be of integer type",
                "no definitions validate",
                "value does not match regex '^[0-9]+'"
            ]
        })
        pd.testing.assert_frame_equal(expected, result)

    def test_format_validation_msgs_as_df_unexpected_error_type(self):
        """Test that an unexpected non-string, non-dict error item is
        converted to a string via str()."""
        validation_msgs = [
            {
                "sample_name": "sample1",
                "field_name": "age",
                "error_message": ["a string error", 42]
            }
        ]

        result = format_validation_msgs_as_df(validation_msgs)

        expected = pd.DataFrame({
            "sample_name": ["sample1", "sample1"],
            "field_name": ["age", "age"],
            "error_message": ["42", "a string error"]
        })
        pd.testing.assert_frame_equal(expected, result)
