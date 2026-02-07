import numpy as np
import pandas
from pandas.testing import assert_frame_equal
from metameq.src.util import \
    SAMPLE_NAME_KEY, \
    DEFAULT_KEY, \
    REQUIRED_KEY, \
    METADATA_FIELDS_KEY
from metameq.src.metadata_extender import \
    _update_metadata_from_metadata_fields_dict, \
    _update_metadata_from_dict, \
    REQ_PLACEHOLDER
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestUpdateMetadataFromMetadataFieldsDict(ExtenderTestBase):
    def test__update_metadata_from_metadata_fields_dict_adds_new_column_with_default(self):
        """Test that a new column is added with the default value when field has default."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"]
        })
        metadata_fields_dict = {
            "new_field": {
                DEFAULT_KEY: "default_value"
            }
        }

        result = _update_metadata_from_metadata_fields_dict(
            input_df, metadata_fields_dict, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "new_field": ["default_value", "default_value"]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_metadata_fields_dict_fills_nans_with_default(self):
        """Test that NaN values in existing column are filled with default."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["value1", np.nan]
        })
        metadata_fields_dict = {
            "existing_field": {
                DEFAULT_KEY: "default_value"
            }
        }

        result = _update_metadata_from_metadata_fields_dict(
            input_df, metadata_fields_dict, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["value1", "default_value"]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_metadata_fields_dict_overwrite_non_nans_false(self):
        """Test that existing non-NaN values are preserved when overwrite_non_nans is False."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["original", np.nan]
        })
        metadata_fields_dict = {
            "existing_field": {
                DEFAULT_KEY: "default_value"
            }
        }

        result = _update_metadata_from_metadata_fields_dict(
            input_df, metadata_fields_dict, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["original", "default_value"]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_metadata_fields_dict_overwrite_non_nans_true(self):
        """Test that existing values are overwritten when overwrite_non_nans is True."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["original", "also_original"]
        })
        metadata_fields_dict = {
            "existing_field": {
                DEFAULT_KEY: "default_value"
            }
        }

        result = _update_metadata_from_metadata_fields_dict(
            input_df, metadata_fields_dict, overwrite_non_nans=True)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["default_value", "default_value"]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_metadata_fields_dict_adds_required_placeholder(self):
        """Test that required field without default gets placeholder when column doesn't exist."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"]
        })
        metadata_fields_dict = {
            "required_field": {
                REQUIRED_KEY: True
            }
        }

        result = _update_metadata_from_metadata_fields_dict(
            input_df, metadata_fields_dict, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "required_field": [REQ_PLACEHOLDER, REQ_PLACEHOLDER]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_metadata_fields_dict_preserves_existing_required(self):
        """Test that existing values in required, no-default field are preserved (no placeholder)."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "required_field": ["existing1", "existing2"]
        })
        metadata_fields_dict = {
            "required_field": {
                REQUIRED_KEY: True
            }
        }

        result = _update_metadata_from_metadata_fields_dict(
            input_df, metadata_fields_dict, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "required_field": ["existing1", "existing2"]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_metadata_fields_dict_required_false_no_placeholder(self):
        """Test that field with required=False and no default doesn't get added."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"]
        })
        metadata_fields_dict = {
            "optional_field": {
                REQUIRED_KEY: False
            }
        }

        result = _update_metadata_from_metadata_fields_dict(
            input_df, metadata_fields_dict, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_metadata_fields_dict_default_takes_precedence(self):
        """Test that default value is used even when field is also marked required."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"]
        })
        metadata_fields_dict = {
            "field_with_both": {
                DEFAULT_KEY: "the_default",
                REQUIRED_KEY: True
            }
        }

        result = _update_metadata_from_metadata_fields_dict(
            input_df, metadata_fields_dict, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_with_both": ["the_default", "the_default"]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_metadata_fields_dict_multiple_fields(self):
        """Test updating multiple fields at once."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing": ["val1", np.nan]
        })
        metadata_fields_dict = {
            "existing": {
                DEFAULT_KEY: "filled"
            },
            "new_default": {
                DEFAULT_KEY: "new_val"
            },
            "new_required": {
                REQUIRED_KEY: True
            }
        }

        result = _update_metadata_from_metadata_fields_dict(
            input_df, metadata_fields_dict, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing": ["val1", "filled"],
            "new_default": ["new_val", "new_val"],
            "new_required": [REQ_PLACEHOLDER, REQ_PLACEHOLDER]
        })
        assert_frame_equal(expected, result)


class TestUpdateMetadataFromDict(ExtenderTestBase):
    def test__update_metadata_from_dict_extracts_metadata_fields(self):
        """Test that METADATA_FIELDS_KEY is extracted when dict_is_metadata_fields=False."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"]
        })
        config_section_dict = {
            METADATA_FIELDS_KEY: {
                "new_field": {
                    DEFAULT_KEY: "default_value"
                }
            },
            "other_key": "ignored"
        }

        result = _update_metadata_from_dict(
            input_df, config_section_dict,
            dict_is_metadata_fields=False, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "new_field": ["default_value", "default_value"]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_dict_uses_dict_directly(self):
        """Test that dict is used directly when dict_is_metadata_fields=True."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"]
        })
        config_section_dict = {
            "new_field": {
                DEFAULT_KEY: "default_value"
            }
        }

        result = _update_metadata_from_dict(
            input_df, config_section_dict,
            dict_is_metadata_fields=True, overwrite_non_nans=False)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "new_field": ["default_value", "default_value"]
        })
        assert_frame_equal(expected, result)

    def test__update_metadata_from_dict_passes_overwrite_non_nans(self):
        """Test that overwrite_non_nans parameter is passed through correctly."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["original", "also_original"]
        })
        config_section_dict = {
            "existing_field": {
                DEFAULT_KEY: "new_value"
            }
        }

        result = _update_metadata_from_dict(
            input_df, config_section_dict,
            dict_is_metadata_fields=True, overwrite_non_nans=True)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["new_value", "new_value"]
        })
        assert_frame_equal(expected, result)
