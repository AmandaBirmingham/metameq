import numpy as np
import pandas
from pandas.testing import assert_frame_equal
from metameq.src.util import \
    SAMPLE_NAME_KEY, \
    HOSTTYPE_SHORTHAND_KEY, \
    SAMPLETYPE_SHORTHAND_KEY, \
    OVERWRITE_NON_NANS_KEY, \
    METADATA_TRANSFORMERS_KEY, \
    SOURCES_KEY, \
    FUNCTION_KEY, \
    PRE_TRANSFORMERS_KEY
from metameq.src.metadata_extender import \
    _transform_metadata
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestTransformMetadata(ExtenderTestBase):
    def test__transform_metadata_no_transformers(self):
        """Test that df is returned unchanged when no transformers are configured."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field1": ["value1", "value2"]
        })
        full_flat_config_dict = {}

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, "pre", None)

        expected_df = input_df

        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_no_stage_transformers(self):
        """Test that df is returned unchanged when stage has no transformers."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field1": ["value1", "value2"]
        })
        full_flat_config_dict = {
            METADATA_TRANSFORMERS_KEY: {
                "post": {
                    "target_field": {
                        SOURCES_KEY: ["field1"],
                        FUNCTION_KEY: "pass_through"
                    }
                }
            }
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, "pre", None)

        expected_df = input_df

        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_builtin_pass_through(self):
        """Test using built-in pass_through transformer."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["value1", "value2"]
        })
        full_flat_config_dict = {
            METADATA_TRANSFORMERS_KEY: {
                "pre": {
                    "target_field": {
                        SOURCES_KEY: ["source_field"],
                        FUNCTION_KEY: "pass_through"
                    }
                }
            }
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, "pre", None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["value1", "value2"],
            "target_field": ["value1", "value2"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_builtin_sex_transformer(self):
        """Test using built-in transform_input_sex_to_std_sex transformer."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            "input_sex": ["F", "Male", "female"]
        })
        full_flat_config_dict = {
            METADATA_TRANSFORMERS_KEY: {
                "pre": {
                    "sex": {
                        SOURCES_KEY: ["input_sex"],
                        FUNCTION_KEY: "transform_input_sex_to_std_sex"
                    }
                }
            }
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, "pre", None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            "input_sex": ["F", "Male", "female"],
            "sex": ["female", "male", "female"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_builtin_age_to_life_stage(self):
        """Test using built-in transform_age_to_life_stage transformer."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            "age_years": [10, 17, 45]
        })
        full_flat_config_dict = {
            METADATA_TRANSFORMERS_KEY: {
                "pre": {
                    "life_stage": {
                        SOURCES_KEY: ["age_years"],
                        FUNCTION_KEY: "transform_age_to_life_stage"
                    }
                }
            }
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, "pre", None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            "age_years": [10, 17, 45],
            "life_stage": ["child", "adult", "adult"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_custom_transformer(self):
        """Test using a custom transformer function passed in transformer_funcs_dict."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["hello", "world"]
        })
        full_flat_config_dict = {
            METADATA_TRANSFORMERS_KEY: {
                "pre": {
                    "target_field": {
                        SOURCES_KEY: ["source_field"],
                        FUNCTION_KEY: "custom_upper"
                    }
                }
            }
        }

        def custom_upper(row, source_fields):
            return row[source_fields[0]].upper()

        transformer_funcs_dict = {
            "custom_upper": custom_upper
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, "pre", transformer_funcs_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["hello", "world"],
            "target_field": ["HELLO", "WORLD"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_unknown_transformer_raises(self):
        """Test that unknown transformer function raises ValueError."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            "source_field": ["value1"]
        })
        full_flat_config_dict = {
            METADATA_TRANSFORMERS_KEY: {
                "pre": {
                    "target_field": {
                        SOURCES_KEY: ["source_field"],
                        FUNCTION_KEY: "nonexistent_function"
                    }
                }
            }
        }

        with self.assertRaisesRegex(ValueError, "Unable to find transformer 'nonexistent_function'"):
            _transform_metadata(input_df, full_flat_config_dict, "pre", None)

    def test__transform_metadata_overwrite_non_nans_false(self):
        """Test that existing values are preserved when overwrite_non_nans is False."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["value1", "value2"],
            "target_field": ["existing", np.nan]
        })
        full_flat_config_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            METADATA_TRANSFORMERS_KEY: {
                "pre": {
                    "target_field": {
                        SOURCES_KEY: ["source_field"],
                        FUNCTION_KEY: "pass_through"
                    }
                }
            }
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, "pre", None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["value1", "value2"],
            "target_field": ["existing", "value2"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_overwrite_non_nans_true(self):
        """Test that existing values are overwritten when overwrite_non_nans is True."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["value1", "value2"],
            "target_field": ["existing", "also_existing"]
        })
        full_flat_config_dict = {
            OVERWRITE_NON_NANS_KEY: True,
            METADATA_TRANSFORMERS_KEY: {
                "pre": {
                    "target_field": {
                        SOURCES_KEY: ["source_field"],
                        FUNCTION_KEY: "pass_through"
                    }
                }
            }
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, "pre", None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["value1", "value2"],
            "target_field": ["value1", "value2"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_multiple_transformers(self):
        """Test applying multiple transformers in a single stage."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"],
            "field_b": ["b1", "b2"]
        })
        full_flat_config_dict = {
            METADATA_TRANSFORMERS_KEY: {
                "pre": {
                    "target_a": {
                        SOURCES_KEY: ["field_a"],
                        FUNCTION_KEY: "pass_through"
                    },
                    "target_b": {
                        SOURCES_KEY: ["field_b"],
                        FUNCTION_KEY: "pass_through"
                    }
                }
            }
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, "pre", None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"],
            "field_b": ["b1", "b2"],
            "target_a": ["a1", "a2"],
            "target_b": ["b1", "b2"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_same_source_and_target_with_nan(self):
        """Test overriding false non-nan rewrite for individual transformer.

        Uses the same column (latitude) as both source and target of a
        pre-transformer that formats values to two decimal places.
        Non-NaN values should be formatted, NaN values should remain NaN.
        """
        def format_lat_to_two_decimals(row, source_fields):
            val = row[source_fields[0]]
            if pandas.isna(val):
                return val
            return f"{float(val):.2f}"

        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3", "sample4"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "stool", "stool"],
            "latitude": ["32.8812345678", "-117.2345678901", "0.1234567890", np.nan]
        })

        full_flat_config_dict = {
            OVERWRITE_NON_NANS_KEY: True,
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "latitude": {
                        SOURCES_KEY: ["latitude"],
                        FUNCTION_KEY: "format_lat_to_two_decimals",
                        OVERWRITE_NON_NANS_KEY: True
                    }
                }
            }
        }

        transformer_funcs_dict = {
            "format_lat_to_two_decimals": format_lat_to_two_decimals
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, PRE_TRANSFORMERS_KEY,
            transformer_funcs_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3", "sample4"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "stool", "stool"],
            "latitude": ["32.88", "-117.23", "0.12", np.nan]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_per_transformer_overwrite_true_overrides_global_false(self):
        """Test per-transformer overwrite_non_nans=True overrides global False."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["new_value1", "new_value2"],
            "target_field": ["existing1", "existing2"]
        })
        full_flat_config_dict = {
            OVERWRITE_NON_NANS_KEY: False,  # Global setting: don't overwrite
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "target_field": {
                        SOURCES_KEY: ["source_field"],
                        FUNCTION_KEY: "pass_through",
                        OVERWRITE_NON_NANS_KEY: True  # Per-transformer: do overwrite
                    }
                }
            }
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, PRE_TRANSFORMERS_KEY, None)

        # Should overwrite because per-transformer setting takes precedence
        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["new_value1", "new_value2"],
            "target_field": ["new_value1", "new_value2"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_per_transformer_overwrite_false_overrides_global_true(self):
        """Test per-transformer overwrite_non_nans=False overrides global True."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["new_value1", "new_value2"],
            "target_field": ["existing1", np.nan]
        })
        full_flat_config_dict = {
            OVERWRITE_NON_NANS_KEY: True,  # Global setting: do overwrite
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "target_field": {
                        SOURCES_KEY: ["source_field"],
                        FUNCTION_KEY: "pass_through",
                        OVERWRITE_NON_NANS_KEY: False  # Per-transformer: don't overwrite
                    }
                }
            }
        }

        result_df = _transform_metadata(
            input_df, full_flat_config_dict, PRE_TRANSFORMERS_KEY, None)

        # Should NOT overwrite existing1 because per-transformer setting takes precedence
        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "source_field": ["new_value1", "new_value2"],
            "target_field": ["existing1", "new_value2"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__transform_metadata_missing_source_field_skips_with_warning(self):
        """Test that missing source fields cause transformer to be skipped with warning."""
        import logging

        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["value1", "value2"]
        })
        full_flat_config_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "target_field": {
                        SOURCES_KEY: ["missing_field"],  # This field doesn't exist
                        FUNCTION_KEY: "pass_through"
                    }
                }
            }
        }

        # Should not raise an error, just skip the transformer and log warning
        with self.assertLogs(level=logging.WARNING) as log_context:
            result_df = _transform_metadata(
                input_df, full_flat_config_dict, PRE_TRANSFORMERS_KEY, None)

        # DataFrame should be unchanged (no target_field added)
        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "existing_field": ["value1", "value2"]
        })
        assert_frame_equal(expected_df, result_df)

        # Check that warning was logged with expected content
        self.assertEqual(1, len(log_context.output))
        self.assertIn("pass_through", log_context.output[0])
        self.assertIn("target_field", log_context.output[0])
        self.assertIn("missing_field", log_context.output[0])

    def test__transform_metadata_partial_missing_source_skips_transformer(self):
        """Test transformer is skipped when only some source fields are missing."""
        import logging

        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"]
            # field_b is missing
        })
        full_flat_config_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "target_field": {
                        SOURCES_KEY: ["field_a", "field_b"],  # field_b doesn't exist
                        FUNCTION_KEY: "pass_through"
                    }
                }
            }
        }

        with self.assertLogs(level=logging.WARNING) as log_context:
            result_df = _transform_metadata(
                input_df, full_flat_config_dict, PRE_TRANSFORMERS_KEY, None)

        # DataFrame should be unchanged (transformer skipped)
        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"]
        })
        assert_frame_equal(expected_df, result_df)

        # Check warning mentions only the missing field, not the present one
        self.assertEqual(1, len(log_context.output))
        self.assertIn("field_b", log_context.output[0])
        self.assertNotIn("field_a", log_context.output[0])
