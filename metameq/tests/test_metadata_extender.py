import glob
import numpy as np
import os
import os.path as path
import pandas
import tempfile
from pandas.testing import assert_frame_equal
from unittest import TestCase
from metameq.src.util import \
    SAMPLE_NAME_KEY, HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY, \
    QC_NOTE_KEY, DEFAULT_KEY, REQUIRED_RAW_METADATA_FIELDS, REQUIRED_KEY, \
    METADATA_FIELDS_KEY, ALIAS_KEY, BASE_TYPE_KEY, ALLOWED_KEY, TYPE_KEY, \
    SAMPLE_TYPE_KEY, QIITA_SAMPLE_TYPE, SAMPLE_TYPE_SPECIFIC_METADATA_KEY, \
    OVERWRITE_NON_NANS_KEY, LEAVE_REQUIREDS_BLANK_KEY, LEAVE_BLANK_VAL, \
    HOST_TYPE_SPECIFIC_METADATA_KEY, METADATA_TRANSFORMERS_KEY, \
    SOURCES_KEY, FUNCTION_KEY, PRE_TRANSFORMERS_KEY, POST_TRANSFORMERS_KEY, \
    STUDY_SPECIFIC_METADATA_KEY
from metameq.src.metadata_extender import \
    id_missing_cols, get_qc_failures, get_reserved_cols, find_standard_cols, \
    find_nonstandard_cols, write_metadata_results, \
    get_extended_metadata_from_df_and_yaml, write_extended_metadata_from_df, \
    write_extended_metadata, _reorder_df, _catch_nan_required_fields, \
    _fill_na_if_default, _update_metadata_from_metadata_fields_dict, \
    _update_metadata_from_dict, _construct_sample_type_metadata_fields_dict, \
    _generate_metadata_for_a_sample_type_in_a_host_type, \
    _generate_metadata_for_a_host_type, _generate_metadata_for_host_types, \
    _transform_metadata, _populate_metadata_df, extend_metadata_df, \
    _get_study_specific_config, _output_metadata_df_to_files, \
    INTERNAL_COL_KEYS, REQ_PLACEHOLDER


class TestMetadataExtender(TestCase):
    """Test suite for metadata_extender module."""

    # Tests for id_missing_cols

    def test_id_missing_cols_all_present(self):
        """Test returns empty list when all required columns exist."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })

        result = id_missing_cols(input_df)

        expected = []
        self.assertEqual(expected, result)

    def test_id_missing_cols_some_missing(self):
        """Test returns sorted list of missing required columns."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"]
        })

        result = id_missing_cols(input_df)

        expected = sorted([HOSTTYPE_SHORTHAND_KEY, SAMPLETYPE_SHORTHAND_KEY])
        self.assertEqual(expected, result)

    def test_id_missing_cols_all_missing(self):
        """Test returns all required columns when df has none of them."""
        input_df = pandas.DataFrame({
            "other_col": ["value1"]
        })

        result = id_missing_cols(input_df)

        expected = sorted(REQUIRED_RAW_METADATA_FIELDS)
        self.assertEqual(expected, result)

    # Tests for get_reserved_cols

    def test_get_reserved_cols_single_host_sample_type(self):
        """Test returns sorted list of reserved column names for a single host/sample type."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {
                            "host_common_name": {
                                DEFAULT_KEY: "human",
                                TYPE_KEY: "string"
                            }
                        },
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {
                                    "body_site": {
                                        DEFAULT_KEY: "gut",
                                        TYPE_KEY: "string"
                                    },
                                    "stool_consistency": {
                                        DEFAULT_KEY: "normal",
                                        TYPE_KEY: "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        result = get_reserved_cols(input_df, study_config, self.TEST_STDS_FP)

        # Expected columns are union of study_config fields and test_standards.yml fields
        # From standards: sample_name, sample_type (base), description (human overrides host_associated),
        # body_site (host_associated stool), body_product (human stool), host_common_name (human)
        expected = [
            "body_product",  # from human stool in test_standards.yml
            "body_site",
            "description",  # from human in test_standards.yml (overrides host_associated)
            "host_common_name",
            HOSTTYPE_SHORTHAND_KEY,
            QC_NOTE_KEY,
            QIITA_SAMPLE_TYPE,
            SAMPLE_NAME_KEY,
            SAMPLE_TYPE_KEY,
            SAMPLETYPE_SHORTHAND_KEY,
            "stool_consistency"
        ]
        self.assertEqual(expected, result)

    def test_get_reserved_cols_missing_hosttype_shorthand_raises(self):
        """Test raises ValueError when hosttype_shorthand column is missing."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        study_config = {}

        with self.assertRaisesRegex(ValueError, HOSTTYPE_SHORTHAND_KEY):
            get_reserved_cols(input_df, study_config)

    def test_get_reserved_cols_missing_sampletype_shorthand_raises(self):
        """Test raises ValueError when sampletype_shorthand column is missing."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"]
        })
        study_config = {}

        with self.assertRaisesRegex(ValueError, SAMPLETYPE_SHORTHAND_KEY):
            get_reserved_cols(input_df, study_config)

    def test_get_reserved_cols_multiple_host_sample_types(self):
        """Test returns deduped union of reserved columns for multiple host/sample type combinations."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "mouse"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "blood", "stool"]
        })
        # Both human and mouse define host_common_name and body_site - should appear only once each
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {
                            "host_common_name": {
                                DEFAULT_KEY: "human",
                                TYPE_KEY: "string"
                            },
                            "human_field": {
                                DEFAULT_KEY: "human_value",
                                TYPE_KEY: "string"
                            }
                        },
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {
                                    "body_site": {
                                        DEFAULT_KEY: "gut",
                                        TYPE_KEY: "string"
                                    },
                                    "stool_consistency": {
                                        DEFAULT_KEY: "normal",
                                        TYPE_KEY: "string"
                                    }
                                }
                            },
                            "blood": {
                                METADATA_FIELDS_KEY: {
                                    "body_site": {
                                        DEFAULT_KEY: "blood",
                                        TYPE_KEY: "string"
                                    },
                                    "blood_type": {
                                        DEFAULT_KEY: "unknown",
                                        TYPE_KEY: "string"
                                    }
                                }
                            }
                        }
                    },
                    "mouse": {
                        METADATA_FIELDS_KEY: {
                            "host_common_name": {
                                DEFAULT_KEY: "mouse",
                                TYPE_KEY: "string"
                            },
                            "mouse_field": {
                                DEFAULT_KEY: "mouse_value",
                                TYPE_KEY: "string"
                            }
                        },
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {
                                    "body_site": {
                                        DEFAULT_KEY: "gut",
                                        TYPE_KEY: "string"
                                    },
                                    "mouse_stool_field": {
                                        DEFAULT_KEY: "mouse_stool_value",
                                        TYPE_KEY: "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        result = get_reserved_cols(input_df, study_config, self.TEST_STDS_FP)

        # Expected columns are union of study_config fields and test_standards.yml fields
        # From standards for human/stool: sample_name, sample_type (base), description (human),
        #   body_site (host_associated stool), body_product (human stool), host_common_name (human)
        # From standards for human/blood: body_site (human blood), body_product (human blood),
        #   description (human), host_common_name (human)
        # From standards for mouse/stool: sample_name, sample_type (base), description (host_associated),
        #   body_site (host_associated stool), host_common_name (mouse)
        # TODO: cage_id from mouse stool in test_standards.yml SHOULD be included here
        # but is currently excluded because it has required: false and no default.
        # The function under test needs to be changed to include fields even when
        # they have required: false and no default.
        expected = [
            "blood_type",
            "body_product",  # from human stool and human blood in test_standards.yml
            "body_site",
            "description",  # from human (overrides host_associated) and host_associated (mouse inherits)
            "host_common_name",
            HOSTTYPE_SHORTHAND_KEY,
            "human_field",
            "mouse_field",
            "mouse_stool_field",
            QC_NOTE_KEY,
            QIITA_SAMPLE_TYPE,
            SAMPLE_NAME_KEY,
            SAMPLE_TYPE_KEY,
            SAMPLETYPE_SHORTHAND_KEY,
            "stool_consistency"
        ]
        self.assertEqual(expected, result)

    # Tests for find_standard_cols

    def test_find_standard_cols_returns_standard_cols_in_df(self):
        """Test returns standard columns that exist in the input DataFrame, excluding internals."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            "body_site": ["gut"],
            "host_common_name": ["human"],
            "my_custom_column": ["custom_value"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {
                            "host_common_name": {
                                DEFAULT_KEY: "human",
                                TYPE_KEY: "string"
                            }
                        },
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {
                                    "body_site": {
                                        DEFAULT_KEY: "gut",
                                        TYPE_KEY: "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        result = find_standard_cols(input_df, study_config, self.TEST_STDS_FP)

        # Returns intersection of reserved cols (minus internals) with df columns.
        # body_site, host_common_name, sample_name are standard and in df
        # hosttype_shorthand, sampletype_shorthand are internal (excluded)
        # my_custom_column is nonstandard (excluded)
        expected = ["body_site", "host_common_name", SAMPLE_NAME_KEY]
        self.assertEqual(sorted(expected), sorted(result))

    def test_find_standard_cols_missing_hosttype_shorthand_raises(self):
        """Test raises ValueError when hosttype_shorthand column is missing."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        study_config = {}

        with self.assertRaisesRegex(ValueError, HOSTTYPE_SHORTHAND_KEY):
            find_standard_cols(input_df, study_config, self.TEST_STDS_FP)

    def test_find_standard_cols_missing_sampletype_shorthand_raises(self):
        """Test raises ValueError when sampletype_shorthand column is missing."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"]
        })
        study_config = {}

        with self.assertRaisesRegex(ValueError, SAMPLETYPE_SHORTHAND_KEY):
            find_standard_cols(input_df, study_config, self.TEST_STDS_FP)

    def test_find_standard_cols_missing_sample_name_raises(self):
        """Test raises ValueError when sample_name column is missing."""
        input_df = pandas.DataFrame({
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        study_config = {}

        with self.assertRaisesRegex(ValueError, SAMPLE_NAME_KEY):
            find_standard_cols(input_df, study_config, self.TEST_STDS_FP)

    def test_find_standard_cols_suppress_missing_name_err(self):
        """Test that suppress_missing_name_err=True allows missing sample_name."""
        input_df = pandas.DataFrame({
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            "body_site": ["gut"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {},
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {
                                    "body_site": {
                                        DEFAULT_KEY: "gut",
                                        TYPE_KEY: "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        result = find_standard_cols(
            input_df, study_config, self.TEST_STDS_FP,
            suppress_missing_name_err=True)

        # Only body_site is a standard col in df (sample_name is missing but allowed)
        expected = ["body_site"]
        self.assertEqual(expected, sorted(result))

    # Tests for find_nonstandard_cols

    def test_find_nonstandard_cols_returns_nonstandard_cols(self):
        """Test returns columns in df that are not in the reserved columns list."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            "body_site": ["gut"],
            "host_common_name": ["human"],
            "my_custom_column": ["custom_value"],
            "another_nonstandard": ["value"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {
                            "host_common_name": {
                                DEFAULT_KEY: "human",
                                TYPE_KEY: "string"
                            }
                        },
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {
                                    "body_site": {
                                        DEFAULT_KEY: "gut",
                                        TYPE_KEY: "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        result = find_nonstandard_cols(input_df, study_config, self.TEST_STDS_FP)

        # Only my_custom_column and another_nonstandard are not in the reserved list
        # sample_name, body_site, host_common_name, hosttype_shorthand,
        # sampletype_shorthand are all reserved
        expected = ["another_nonstandard", "my_custom_column"]
        self.assertEqual(sorted(expected), sorted(result))

    def test_find_nonstandard_cols_missing_required_col_raises(self):
        """Test raises ValueError when a required column is missing."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
            # missing HOSTTYPE_SHORTHAND_KEY
        })
        study_config = {}

        with self.assertRaisesRegex(ValueError, HOSTTYPE_SHORTHAND_KEY):
            find_nonstandard_cols(input_df, study_config, self.TEST_STDS_FP)

    # Tests for write_metadata_results

    def test_write_metadata_results_creates_all_files(self):
        """Test creates metadata file and validation errors file, includes failed rows."""
        metadata_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            "field_a": ["a1", "a2", "a3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "stool"],
            QC_NOTE_KEY: ["", "invalid host_type", ""]
        })
        validation_msgs_df = pandas.DataFrame({
            "field": ["field_a"],
            "error": ["some validation error"]
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            write_metadata_results(
                metadata_df, validation_msgs_df, tmpdir, "test_output",
                sep="\t", remove_internals=False)

            # Find the main metadata file
            metadata_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(metadata_files))

            # Verify metadata file contents - includes failed row when remove_internals=False
            result_df = pandas.read_csv(
                metadata_files[0], sep="\t", keep_default_na=False)
            assert_frame_equal(metadata_df, result_df)

            # Find the validation errors file (uses comma separator)
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))

            # Verify validation errors file contents
            result_validation_df = pandas.read_csv(validation_files[0], sep=",")
            assert_frame_equal(validation_msgs_df, result_validation_df)

            # No fails file should be created when remove_internals=False
            fails_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(0, len(fails_files))

    def test_write_metadata_results_remove_internals_creates_fails_file(self):
        """Test with remove_internals=True creates fails file and removes internal cols."""
        metadata_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            "field_a": ["a1", "a2", "a3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "stool"],
            QC_NOTE_KEY: ["", "invalid host_type", ""]
        })
        validation_msgs_df = pandas.DataFrame()

        with tempfile.TemporaryDirectory() as tmpdir:
            write_metadata_results(
                metadata_df, validation_msgs_df, tmpdir, "test_output",
                sep="\t", remove_internals=True)

            # Find the main metadata file
            metadata_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(metadata_files))

            # Verify metadata has internal cols removed and no failures
            result_df = pandas.read_csv(metadata_files[0], sep="\t")
            expected_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample3"],
                "field_a": ["a1", "a3"]
            })
            assert_frame_equal(expected_df, result_df)

            # Find the fails file
            fails_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))

            # Verify fails file contains the failed row
            fails_df = pandas.read_csv(fails_files[0], sep=",")
            expected_fails_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample2"],
                "field_a": ["a2"],
                HOSTTYPE_SHORTHAND_KEY: ["human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool"],
                QC_NOTE_KEY: ["invalid host_type"]
            })
            assert_frame_equal(expected_fails_df, fails_df)

            # Validation errors file should be empty (touched)
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self.assertEqual(0, os.path.getsize(validation_files[0]))

    def test_write_metadata_results_suppress_empty_fails(self):
        """Test with suppress_empty_fails=True does not create empty files."""
        metadata_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        validation_msgs_df = pandas.DataFrame()

        with tempfile.TemporaryDirectory() as tmpdir:
            write_metadata_results(
                metadata_df, validation_msgs_df, tmpdir, "test_output",
                sep="\t", remove_internals=True, suppress_empty_fails=True)

            # Main metadata file should exist
            metadata_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(metadata_files))

            # Fails file should NOT exist (no failures, suppressed)
            fails_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(0, len(fails_files))

            # Validation errors file should NOT exist (empty, suppressed)
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(0, len(validation_files))

    def test_write_metadata_results_custom_internal_col_names(self):
        """Test with custom internal_col_names parameter."""
        metadata_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"],
            "custom_internal": ["x", "y"],
            QC_NOTE_KEY: ["", ""]
        })
        validation_msgs_df = pandas.DataFrame()

        with tempfile.TemporaryDirectory() as tmpdir:
            write_metadata_results(
                metadata_df, validation_msgs_df, tmpdir, "test_output",
                sep="\t", remove_internals=True, suppress_empty_fails=True,
                internal_col_names=["custom_internal", QC_NOTE_KEY])

            # Find the main metadata file
            metadata_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(metadata_files))

            # Verify custom internal cols are removed
            result_df = pandas.read_csv(metadata_files[0], sep="\t")
            expected_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "field_a": ["a1", "a2"]
            })
            assert_frame_equal(expected_df, result_df)

    # Tests for get_qc_failures

    def test_get_qc_failures_no_failures(self):
        """Test returns empty df when QC_NOTE_KEY is all empty strings."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            QC_NOTE_KEY: ["", ""]
        })

        result = get_qc_failures(input_df)

        self.assertTrue(result.empty)

    def test_get_qc_failures_some_failures(self):
        """Test returns only rows where QC_NOTE_KEY is not empty."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            QC_NOTE_KEY: ["", "invalid host_type", ""]
        })

        result = get_qc_failures(input_df)

        expected = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample2"],
            QC_NOTE_KEY: ["invalid host_type"]
        }, index=[1])
        assert_frame_equal(expected, result)

    def test_get_qc_failures_all_failures(self):
        """Test returns all rows when all have QC notes."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            QC_NOTE_KEY: ["invalid host_type", "invalid sample_type"]
        })

        result = get_qc_failures(input_df)

        assert_frame_equal(input_df, result)

    # Tests for _reorder_df

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

    # Tests for _catch_nan_required_fields

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

    # Tests for _fill_na_if_default

    def test__fill_na_if_default_specific_overrides_settings(self):
        """Test that specific_dict default takes precedence over settings_dict."""
        input_df = pandas.DataFrame({
            "field1": ["value1", np.nan, "value3"],
            "field2": [np.nan, "value2", np.nan]
        })
        specific_dict = {DEFAULT_KEY: "filled"}
        settings_dict = {DEFAULT_KEY: "unused"}

        result = _fill_na_if_default(input_df, specific_dict, settings_dict)

        expected = pandas.DataFrame({
            "field1": ["value1", "filled", "value3"],
            "field2": ["filled", "value2", "filled"]
        })
        assert_frame_equal(expected, result)

    def test__fill_na_if_default_uses_settings_when_specific_missing(self):
        """Test that settings_dict default is used when specific_dict has no default."""
        input_df = pandas.DataFrame({
            "field1": [np.nan]
        })
        specific_dict = {}
        settings_dict = {DEFAULT_KEY: "settings_default"}

        result = _fill_na_if_default(input_df, specific_dict, settings_dict)

        expected = pandas.DataFrame({
            "field1": ["settings_default"]
        })
        assert_frame_equal(expected, result)

    # Tests for _update_metadata_from_metadata_fields_dict

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

    # Tests for _update_metadata_from_dict

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

    # Tests for _construct_sample_type_metadata_fields_dict

    def test__construct_sample_type_metadata_fields_dict_simple(self):
        """Test combining host and sample type fields for a simple sample type."""
        host_sample_types_config_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "sample_field": {
                        DEFAULT_KEY: "sample_default"
                    }
                }
            }
        }
        host_metadata_fields_dict = {
            "host_field": {
                DEFAULT_KEY: "host_default"
            }
        }

        result = _construct_sample_type_metadata_fields_dict(
            "stool", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
            "host_field": {
                DEFAULT_KEY: "host_default"
            },
            "sample_field": {
                DEFAULT_KEY: "sample_default"
            },
            SAMPLE_TYPE_KEY: {
                ALLOWED_KEY: ["stool"],
                DEFAULT_KEY: "stool",
                TYPE_KEY: "string"
            },
            QIITA_SAMPLE_TYPE: {
                ALLOWED_KEY: ["stool"],
                DEFAULT_KEY: "stool",
                TYPE_KEY: "string"
            }
        }
        self.assertDictEqual(expected, result)

    def test__construct_sample_type_metadata_fields_dict_with_alias(self):
        """Test that alias resolves to target sample type."""
        host_sample_types_config_dict = {
            "feces": {
                ALIAS_KEY: "stool"
            },
            "stool": {
                METADATA_FIELDS_KEY: {
                    "stool_field": {
                        DEFAULT_KEY: "stool_value"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _construct_sample_type_metadata_fields_dict(
            "feces", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
            "stool_field": {
                DEFAULT_KEY: "stool_value"
            },
            SAMPLE_TYPE_KEY: {
                ALLOWED_KEY: ["stool"],
                DEFAULT_KEY: "stool",
                TYPE_KEY: "string"
            },
            QIITA_SAMPLE_TYPE: {
                ALLOWED_KEY: ["stool"],
                DEFAULT_KEY: "stool",
                TYPE_KEY: "string"
            }
        }
        self.assertDictEqual(expected, result)

    def test__construct_sample_type_metadata_fields_dict_chained_alias_raises(self):
        """Test that chained aliases raise ValueError."""
        host_sample_types_config_dict = {
            "feces": {
                ALIAS_KEY: "stool"
            },
            "stool": {
                ALIAS_KEY: "poop"
            },
            "poop": {
                METADATA_FIELDS_KEY: {}
            }
        }
        host_metadata_fields_dict = {}

        with self.assertRaisesRegex(ValueError, "May not chain aliases"):
            _construct_sample_type_metadata_fields_dict(
                "feces", host_sample_types_config_dict, host_metadata_fields_dict)

    def test__construct_sample_type_metadata_fields_dict_with_base_type(self):
        """Test that base type fields are inherited and overlaid."""
        host_sample_types_config_dict = {
            "base_sample": {
                METADATA_FIELDS_KEY: {
                    "base_field": {
                        DEFAULT_KEY: "base_value"
                    }
                }
            },
            "derived_sample": {
                BASE_TYPE_KEY: "base_sample",
                METADATA_FIELDS_KEY: {
                    "derived_field": {
                        DEFAULT_KEY: "derived_value"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _construct_sample_type_metadata_fields_dict(
            "derived_sample", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
            "base_field": {
                DEFAULT_KEY: "base_value"
            },
            "derived_field": {
                DEFAULT_KEY: "derived_value"
            },
            SAMPLE_TYPE_KEY: {
                ALLOWED_KEY: ["derived_sample"],
                DEFAULT_KEY: "derived_sample",
                TYPE_KEY: "string"
            },
            QIITA_SAMPLE_TYPE: {
                ALLOWED_KEY: ["derived_sample"],
                DEFAULT_KEY: "derived_sample",
                TYPE_KEY: "string"
            }
        }
        self.assertDictEqual(expected, result)

    def test__construct_sample_type_metadata_fields_dict_base_type_invalid_raises(self):
        """Test that base type with non-metadata-fields keys raises ValueError."""
        host_sample_types_config_dict = {
            "base_sample": {
                METADATA_FIELDS_KEY: {
                    "base_field": {DEFAULT_KEY: "value"}
                },
                "extra_key": "not_allowed"
            },
            "derived_sample": {
                BASE_TYPE_KEY: "base_sample",
                METADATA_FIELDS_KEY: {}
            }
        }
        host_metadata_fields_dict = {}

        with self.assertRaisesRegex(ValueError, "must only have metadata fields"):
            _construct_sample_type_metadata_fields_dict(
                "derived_sample", host_sample_types_config_dict, host_metadata_fields_dict)

    def test__construct_sample_type_metadata_fields_dict_sets_sample_type(self):
        """Test that sample_type field is set with correct allowed/default values."""
        host_sample_types_config_dict = {
            "blood": {
                METADATA_FIELDS_KEY: {}
            }
        }
        host_metadata_fields_dict = {}

        result = _construct_sample_type_metadata_fields_dict(
            "blood", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
            SAMPLE_TYPE_KEY: {
                ALLOWED_KEY: ["blood"],
                DEFAULT_KEY: "blood",
                TYPE_KEY: "string"
            },
            QIITA_SAMPLE_TYPE: {
                ALLOWED_KEY: ["blood"],
                DEFAULT_KEY: "blood",
                TYPE_KEY: "string"
            }
        }
        self.assertDictEqual(expected, result)

    def test__construct_sample_type_metadata_fields_dict_preserves_existing_qiita_sample_type(self):
        """Test that existing qiita_sample_type is not overwritten."""
        host_sample_types_config_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    QIITA_SAMPLE_TYPE: {
                        ALLOWED_KEY: ["custom_type"],
                        DEFAULT_KEY: "custom_type",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _construct_sample_type_metadata_fields_dict(
            "stool", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
            SAMPLE_TYPE_KEY: {
                ALLOWED_KEY: ["stool"],
                DEFAULT_KEY: "stool",
                TYPE_KEY: "string"
            },
            QIITA_SAMPLE_TYPE: {
                ALLOWED_KEY: ["custom_type"],
                DEFAULT_KEY: "custom_type",
                TYPE_KEY: "string"
            }
        }
        self.assertDictEqual(expected, result)

    # Tests for _generate_metadata_for_a_sample_type_in_a_host_type

    def test__generate_metadata_for_a_sample_type_in_a_host_type_basic(self):
        """Test basic metadata generation for a known sample type."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        global_plus_host_settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided"
        }
        # Config is pre-resolved: sample type's metadata_fields already includes
        # host fields merged in, plus sample_type and qiita_sample_type
        host_type_config_dict = {
            METADATA_FIELDS_KEY: {
                "host_field": {
                    DEFAULT_KEY: "host_default",
                    TYPE_KEY: "string"
                }
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            DEFAULT_KEY: "host_default",
                            TYPE_KEY: "string"
                        },
                        "stool_field": {
                            DEFAULT_KEY: "stool_default",
                            TYPE_KEY: "string"
                        },
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"
                        },
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_sample_type_in_a_host_type(
            input_df, "stool", global_plus_host_settings_dict, host_type_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""],
            "host_field": ["host_default", "host_default"],
            "stool_field": ["stool_default", "stool_default"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual([], validation_msgs)

    def test__generate_metadata_for_a_sample_type_in_a_host_type_unknown_sample_type(self):
        """Test that unknown sample type adds QC note."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["unknown_type"],
            QC_NOTE_KEY: [""]
        })
        global_plus_host_settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided"
        }
        host_type_config_dict = {
            METADATA_FIELDS_KEY: {},
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {}
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_sample_type_in_a_host_type(
            input_df, "unknown_type", global_plus_host_settings_dict, host_type_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["unknown_type"],
            QC_NOTE_KEY: ["invalid sample_type"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual([], validation_msgs)

    def test__generate_metadata_for_a_sample_type_in_a_host_type_filters_by_sample_type(self):
        """Test that only rows matching the sample type are processed."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "blood", "stool"],
            QC_NOTE_KEY: ["", "", ""]
        })
        global_plus_host_settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided"
        }
        host_type_config_dict = {
            METADATA_FIELDS_KEY: {},
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "stool_field": {
                            DEFAULT_KEY: "stool_value",
                            TYPE_KEY: "string"
                        }
                    }
                },
                "blood": {
                    METADATA_FIELDS_KEY: {}
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_sample_type_in_a_host_type(
            input_df, "stool", global_plus_host_settings_dict, host_type_config_dict)

        # Should only have the two stool samples
        self.assertEqual(2, len(result_df))
        self.assertEqual(["sample1", "sample3"], result_df[SAMPLE_NAME_KEY].tolist())
        self.assertEqual(["stool_value", "stool_value"], result_df["stool_field"].tolist())

    def test__generate_metadata_for_a_sample_type_in_a_host_type_leave_requireds_blank_true(self):
        """Test that required fields get LEAVE_BLANK_VAL when leave_requireds_blank is True."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""]
        })
        global_plus_host_settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: True,
            DEFAULT_KEY: "not provided"
        }
        host_type_config_dict = {
            METADATA_FIELDS_KEY: {},
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "required_field": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_sample_type_in_a_host_type(
            input_df, "stool", global_plus_host_settings_dict, host_type_config_dict)

        self.assertEqual(LEAVE_BLANK_VAL, result_df["required_field"].iloc[0])

    def test__generate_metadata_for_a_sample_type_in_a_host_type_leave_requireds_blank_false(self):
        """Test that required fields get default when leave_requireds_blank is False."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""]
        })
        global_plus_host_settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "global_default"
        }
        host_type_config_dict = {
            METADATA_FIELDS_KEY: {},
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "required_field": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_sample_type_in_a_host_type(
            input_df, "stool", global_plus_host_settings_dict, host_type_config_dict)

        # When leave_requireds_blank is False, NaN values get filled with global default
        self.assertEqual("global_default", result_df["required_field"].iloc[0])

    def test__generate_metadata_for_a_sample_type_in_a_host_type_overwrite_non_nans_true(self):
        """Test that existing values are overwritten when overwrite_non_nans is True."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""],
            "existing_field": ["original_value"]
        })
        global_plus_host_settings_dict = {
            OVERWRITE_NON_NANS_KEY: True,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided"
        }
        host_type_config_dict = {
            METADATA_FIELDS_KEY: {},
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "existing_field": {
                            DEFAULT_KEY: "new_value",
                            TYPE_KEY: "string"
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_sample_type_in_a_host_type(
            input_df, "stool", global_plus_host_settings_dict, host_type_config_dict)

        self.assertEqual("new_value", result_df["existing_field"].iloc[0])

    def test__generate_metadata_for_a_sample_type_in_a_host_type_overwrite_non_nans_false(self):
        """Test that existing values are preserved when overwrite_non_nans is False."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""],
            "existing_field": ["original_value"]
        })
        global_plus_host_settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided"
        }
        host_type_config_dict = {
            METADATA_FIELDS_KEY: {},
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "existing_field": {
                            DEFAULT_KEY: "new_value",
                            TYPE_KEY: "string"
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_sample_type_in_a_host_type(
            input_df, "stool", global_plus_host_settings_dict, host_type_config_dict)

        self.assertEqual("original_value", result_df["existing_field"].iloc[0])

    def test__generate_metadata_for_a_sample_type_in_a_host_type_with_alias(self):
        """Test that sample type aliases are resolved correctly."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["feces"],
            QC_NOTE_KEY: [""]
        })
        global_plus_host_settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided"
        }
        # Config is pre-resolved: alias "feces" has its own metadata_fields
        # that is a copy of "stool"'s resolved fields with sample_type="stool"
        host_type_config_dict = {
            METADATA_FIELDS_KEY: {},
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "feces": {
                    METADATA_FIELDS_KEY: {
                        "stool_field": {
                            DEFAULT_KEY: "stool_value",
                            TYPE_KEY: "string"
                        },
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"
                        },
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"
                        }
                    }
                },
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "stool_field": {
                            DEFAULT_KEY: "stool_value",
                            TYPE_KEY: "string"
                        },
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"
                        },
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_sample_type_in_a_host_type(
            input_df, "feces", global_plus_host_settings_dict, host_type_config_dict)

        self.assertEqual("stool_value", result_df["stool_field"].iloc[0])
        # sample_type should be set to the resolved type "stool"
        self.assertEqual("stool", result_df[SAMPLE_TYPE_KEY].iloc[0])

    # Tests for _generate_metadata_for_a_host_type

    def test__generate_metadata_for_a_host_type_basic(self):
        """Test basic metadata generation for a known host type."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "global_default"
        }
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    DEFAULT_KEY: "human_default",
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            DEFAULT_KEY: "host_value",
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "host_field": {
                                    DEFAULT_KEY: "host_value",
                                    TYPE_KEY: "string"
                                },
                                "stool_field": {
                                    DEFAULT_KEY: "stool_value",
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_host_type(
            input_df, "human", settings_dict, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""],
            "host_field": ["host_value", "host_value"],
            "stool_field": ["stool_value", "stool_value"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual([], validation_msgs)

    def test__generate_metadata_for_a_host_type_unknown_host_type(self):
        """Test that unknown host type adds QC note."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""]
        })
        settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "global_default"
        }
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {}
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_host_type(
            input_df, "unknown_host", settings_dict, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: ["invalid host_type"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual([], validation_msgs)

    def test__generate_metadata_for_a_host_type_unknown_sample_type(self):
        """Test that unknown sample type within known host type adds QC note."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["unknown_sample"],
            QC_NOTE_KEY: [""]
        })
        settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "global_default"
        }
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {}
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_host_type(
            input_df, "human", settings_dict, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["unknown_sample"],
            QC_NOTE_KEY: ["invalid sample_type"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual([], validation_msgs)

    def test__generate_metadata_for_a_host_type_filters_by_host_type(self):
        """Test that only rows matching the host type are processed."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "mouse", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "stool"],
            QC_NOTE_KEY: ["", "", ""]
        })
        settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "global_default"
        }
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {
                        "human_field": {
                            DEFAULT_KEY: "human_value",
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "human_field": {
                                    DEFAULT_KEY: "human_value",
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "mouse": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {}
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_host_type(
            input_df, "human", settings_dict, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""],
            "human_field": ["human_value", "human_value"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__generate_metadata_for_a_host_type_uses_host_default(self):
        """Test that host-type-specific default overrides global default."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""]
        })
        settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "global_default"
        }
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    DEFAULT_KEY: "human_specific_default",
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "required_field": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_host_type(
            input_df, "human", settings_dict, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""],
            "required_field": ["human_specific_default"],
            SAMPLE_TYPE_KEY: ["stool"],
            QIITA_SAMPLE_TYPE: ["stool"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__generate_metadata_for_a_host_type_uses_global_default_when_no_host_default(self):
        """Test that global default is used when host type has no specific default."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""]
        })
        settings_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "global_default"
        }
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    # No DEFAULT_KEY here
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "required_field": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_host_type(
            input_df, "human", settings_dict, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""],
            "required_field": ["global_default"],
            SAMPLE_TYPE_KEY: ["stool"],
            QIITA_SAMPLE_TYPE: ["stool"]
        })
        assert_frame_equal(expected_df, result_df)

    # Tests for _generate_metadata_for_host_types

    def test__generate_metadata_for_host_types_single_host_type(self):
        """Test metadata generation for a single host type."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            DEFAULT_KEY: "global_default",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            DEFAULT_KEY: "host_value",
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "host_field": {
                                    DEFAULT_KEY: "host_value",
                                    TYPE_KEY: "string"
                                },
                                "stool_field": {
                                    DEFAULT_KEY: "stool_value",
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_host_types(
            input_df, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""],
            "host_field": ["host_value", "host_value"],
            "stool_field": ["stool_value", "stool_value"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual([], validation_msgs)

    def test__generate_metadata_for_host_types_multiple_host_types(self):
        """Test metadata generation for multiple host types with NA filling."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "mouse", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "blood"],
            QC_NOTE_KEY: ["", "", ""]
        })
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            DEFAULT_KEY: "global_default",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {
                        "human_field": {
                            DEFAULT_KEY: "human_value",
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "human_field": {
                                    DEFAULT_KEY: "human_value",
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        },
                        "blood": {
                            METADATA_FIELDS_KEY: {
                                "human_field": {
                                    DEFAULT_KEY: "human_value",
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "mouse": {
                    METADATA_FIELDS_KEY: {
                        "mouse_field": {
                            DEFAULT_KEY: "mouse_value",
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "mouse_field": {
                                    DEFAULT_KEY: "mouse_value",
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_host_types(
            input_df, full_flat_config_dict)

        # After concat, columns from different host types will have NaNs filled with global_default
        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample3", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "mouse"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "blood", "stool"],
            QC_NOTE_KEY: ["", "", ""],
            "human_field": ["human_value", "human_value", "global_default"],
            SAMPLE_TYPE_KEY: ["stool", "blood", "stool"],
            QIITA_SAMPLE_TYPE: ["stool", "blood", "stool"],
            "mouse_field": ["global_default", "global_default", "mouse_value"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual([], validation_msgs)

    def test__generate_metadata_for_host_types_unknown_host_type(self):
        """Test that unknown host type adds QC note."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""]
        })
        full_flat_config_dict = {
            DEFAULT_KEY: "global_default",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {}
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_host_types(
            input_df, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: ["invalid host_type"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual([], validation_msgs)

    def test__generate_metadata_for_host_types_unknown_sample_type(self):
        """Test that unknown sample type within known host type adds QC note."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["unknown_sample"],
            QC_NOTE_KEY: [""]
        })
        full_flat_config_dict = {
            DEFAULT_KEY: "global_default",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {}
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_host_types(
            input_df, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["unknown_sample"],
            QC_NOTE_KEY: ["invalid sample_type"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual([], validation_msgs)

    def test__generate_metadata_for_host_types_replaces_leave_blank_val(self):
        """Test that LEAVE_BLANK_VAL is replaced with empty string."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""]
        })
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            DEFAULT_KEY: "global_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,  # This causes required fields to get LEAVE_BLANK_VAL
            OVERWRITE_NON_NANS_KEY: False,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "required_field": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_host_types(
            input_df, full_flat_config_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""],
            "required_field": [""],  # LEAVE_BLANK_VAL replaced with empty string
            SAMPLE_TYPE_KEY: ["stool"],
            QIITA_SAMPLE_TYPE: ["stool"]
        })
        assert_frame_equal(expected_df, result_df)

    # Tests for _transform_metadata

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

    # Tests for _populate_metadata_df

    def test__populate_metadata_df_basic(self):
        """Test basic metadata population with a simple config."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            DEFAULT_KEY: "host_value",
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "host_field": {
                                    DEFAULT_KEY: "host_value",
                                    TYPE_KEY: "string"
                                },
                                "stool_field": {
                                    DEFAULT_KEY: "stool_value",
                                    TYPE_KEY: "string"
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs_df = _populate_metadata_df(
            input_df, full_flat_config_dict, None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_field": ["host_value", "host_value"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            "stool_field": ["stool_value", "stool_value"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)

    def test__populate_metadata_df_with_pre_transformer(self):
        """Test metadata population with pre-transformer."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            "input_sex": ["F", "Male"]
        })
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "sex": {
                        SOURCES_KEY: ["input_sex"],
                        FUNCTION_KEY: "transform_input_sex_to_std_sex"
                    }
                }
            },
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs_df = _populate_metadata_df(
            input_df, full_flat_config_dict, None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "input_sex": ["F", "Male"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            "sex": ["female", "male"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)

    def test__populate_metadata_df_with_post_transformer(self):
        """Test metadata population with post-transformer."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            METADATA_TRANSFORMERS_KEY: {
                POST_TRANSFORMERS_KEY: {
                    "copied_sample_type": {
                        SOURCES_KEY: [SAMPLE_TYPE_KEY],
                        FUNCTION_KEY: "pass_through"
                    }
                }
            },
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs_df = _populate_metadata_df(
            input_df, full_flat_config_dict, None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "copied_sample_type": ["stool", "stool"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)

    def test__populate_metadata_df_unknown_host_type(self):
        """Test that unknown host type adds QC note."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        full_flat_config_dict = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {}
                }
            }
        }

        result_df, validation_msgs_df = _populate_metadata_df(
            input_df, full_flat_config_dict, None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: ["invalid host_type"]
        })
        assert_frame_equal(expected_df, result_df)

    def test__populate_metadata_df_columns_reordered(self):
        """Test that columns are reordered correctly."""
        input_df = pandas.DataFrame({
            "zebra_field": ["z1", "z2"],
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "apple_field": ["a1", "a2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs_df = _populate_metadata_df(
            input_df, full_flat_config_dict, None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "apple_field": ["a1", "a2"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            "zebra_field": ["z1", "z2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)

    def test__populate_metadata_df_with_custom_transformer(self):
        """Test metadata population with custom transformer function."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            "source_field": ["hello", "world"]
        })
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "upper_field": {
                        SOURCES_KEY: ["source_field"],
                        FUNCTION_KEY: "custom_upper"
                    }
                }
            },
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        def custom_upper(row, source_fields):
            return row[source_fields[0]].upper()

        transformer_funcs_dict = {"custom_upper": custom_upper}

        result_df, validation_msgs_df = _populate_metadata_df(
            input_df, full_flat_config_dict, transformer_funcs_dict)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            "source_field": ["hello", "world"],
            "upper_field": ["HELLO", "WORLD"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)

    def test__populate_metadata_df_nan_sample_name_raises(self):
        """Test that NaN sample name raises ValueError."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", np.nan],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {}
        }

        with self.assertRaisesRegex(ValueError, "Metadata contains NaN sample names"):
            _populate_metadata_df(input_df, full_flat_config_dict, None)

    # Tests for extend_metadata_df

    TEST_DIR = path.dirname(__file__)
    TEST_STDS_FP = path.join(TEST_DIR, "data/test_standards.yml")

    def test_extend_metadata_df_basic(self):
        """Test basic metadata extension with study config."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {
                            "custom_field": {
                                DEFAULT_KEY: "custom_value",
                                TYPE_KEY: "string"
                            }
                        },
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs_df = extend_metadata_df(
            input_df, study_config, None, None, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            # body_product from human stool in test_standards.yml
            "body_product": ["UBERON:feces", "UBERON:feces"],
            # body_site inherited from host_associated stool
            "body_site": ["gut", "gut"],
            # custom_field from study_specific_metadata
            "custom_field": ["custom_value", "custom_value"],
            # description overridden at human level
            "description": ["human sample", "human sample"],
            # host_common_name from human level
            "host_common_name": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)

    def test_extend_metadata_df_with_pre_transformer(self):
        """Test metadata extension with pre-transformer."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            "input_sex": ["F", "Male"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "sex": {
                        SOURCES_KEY: ["input_sex"],
                        FUNCTION_KEY: "transform_input_sex_to_std_sex"
                    }
                }
            },
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {},
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs_df = extend_metadata_df(
            input_df, study_config, None, None, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            # body_product from human stool in test_standards.yml
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            # description overridden at human level
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
            "input_sex": ["F", "Male"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            "sex": ["female", "male"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)

    def test_extend_metadata_df_with_custom_transformer(self):
        """Test metadata extension with custom transformer function."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            "source_field": ["hello", "world"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "upper_field": {
                        SOURCES_KEY: ["source_field"],
                        FUNCTION_KEY: "custom_upper"
                    }
                }
            },
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {},
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        def custom_upper(row, source_fields):
            return row[source_fields[0]].upper()

        transformer_funcs_dict = {"custom_upper": custom_upper}

        result_df, validation_msgs_df = extend_metadata_df(
            input_df, study_config, transformer_funcs_dict, None, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            "source_field": ["hello", "world"],
            "upper_field": ["HELLO", "WORLD"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)

    def test_extend_metadata_df_missing_required_columns_raises(self):
        """Test that missing required columns raises ValueError."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"]
            # Missing HOSTTYPE_SHORTHAND_KEY and SAMPLETYPE_SHORTHAND_KEY
        })
        study_config = {}

        with self.assertRaisesRegex(ValueError, "metadata missing required columns"):
            extend_metadata_df(input_df, study_config, None, None, self.TEST_STDS_FP)

    def test_extend_metadata_df_none_study_config(self):
        """Test metadata extension with None study config uses standards only."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })

        result_df, validation_msgs_df = extend_metadata_df(
            input_df, None, None, None, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            "body_product": ["UBERON:feces"],
            "body_site": ["gut"],
            "description": ["human sample"],
            "host_common_name": ["human"],
            QIITA_SAMPLE_TYPE: ["stool"],
            SAMPLE_TYPE_KEY: ["stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""]
        })
        assert_frame_equal(expected_df, result_df)

    def test_extend_metadata_df_unknown_host_type(self):
        """Test that unknown host type adds QC note."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {},
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs_df = extend_metadata_df(
            input_df, study_config, None, None, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: ["invalid host_type"]
        })
        assert_frame_equal(expected_df, result_df)

    def test_extend_metadata_df_multiple_host_types(self):
        """Test metadata extension with multiple host types."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "mouse", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "blood"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {},
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            },
                            "blood": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    },
                    "mouse": {
                        METADATA_FIELDS_KEY: {},
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs_df = extend_metadata_df(
            input_df, study_config, None, None, self.TEST_STDS_FP)

        # After processing multiple host types, rows may be reordered
        # Human samples are processed together, then mouse samples
        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample3", "sample2"],
            # body_product: human stool/blood have it, mouse stool uses default
            "body_product": ["UBERON:feces", "UBERON:blood", "not provided"],
            "body_site": ["gut", "blood", "gut"],
            # description: human overrides to "human sample",
            # mouse inherits "host associated sample"
            "description": ["human sample", "human sample", "host associated sample"],
            "host_common_name": ["human", "human", "mouse"],
            QIITA_SAMPLE_TYPE: ["stool", "blood", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "blood", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "mouse"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "blood", "stool"],
            QC_NOTE_KEY: ["", "", ""]
        })
        assert_frame_equal(expected_df, result_df)

    def test_extend_metadata_df_with_software_config(self):
        """Test metadata extension with custom software config overrides defaults."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })
        # Software config with custom default value
        software_config = {
            DEFAULT_KEY: "custom_software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False
        }
        # Study config that doesn't override DEFAULT_KEY
        study_config = {
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {
                            "study_field": {
                                DEFAULT_KEY: "study_value",
                                TYPE_KEY: "string"
                            }
                        },
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        result_df, validation_msgs_df = extend_metadata_df(
            input_df, study_config, None, software_config, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            "study_field": ["study_value", "study_value"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)

    # Tests for _get_study_specific_config

    def test__get_study_specific_config_with_valid_file(self):
        """Test loading study-specific config from a valid YAML file."""
        config_fp = path.join(self.TEST_DIR, "data/test_config.yml")

        result = _get_study_specific_config(config_fp)

        expected = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    METADATA_FIELDS_KEY: {
                        "sample_name": {
                            TYPE_KEY: "string",
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
        self.assertDictEqual(expected, result)

    def test__get_study_specific_config_with_none(self):
        """Test that None file path returns None."""
        result = _get_study_specific_config(None)

        self.assertIsNone(result)

    def test__get_study_specific_config_with_empty_string(self):
        """Test that empty string file path returns None."""
        result = _get_study_specific_config("")

        self.assertIsNone(result)

    def test__get_study_specific_config_nonexistent_file_raises(self):
        """Test that nonexistent file raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            _get_study_specific_config("/nonexistent/path/config.yml")

    def test__get_study_specific_config_invalid_yaml_raises(self):
        """Test that invalid YAML file raises an error."""
        invalid_fp = path.join(self.TEST_DIR, "data/invalid.yml")

        with self.assertRaises(Exception):
            _get_study_specific_config(invalid_fp)

    # Tests for _output_metadata_df_to_files

    def test__output_metadata_df_to_files_basic(self):
        """Test basic output of metadata DataFrame to file."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            _output_metadata_df_to_files(
                input_df, tmpdir, "test_output", INTERNAL_COL_KEYS,
                sep="\t", remove_internals_and_fails=False)

            # Find the output file (has timestamp prefix)
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))

            # Read and verify contents (keep_default_na=False preserves empty strings)
            result_df = pandas.read_csv(output_files[0], sep="\t", keep_default_na=False)
            expected_df = input_df
            assert_frame_equal(expected_df, result_df)

    def test__output_metadata_df_to_files_remove_internals_and_fails(self):
        """Test output with internal columns and failures removed."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            "field_a": ["a1", "a2", "a3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "stool"],
            QC_NOTE_KEY: ["", "invalid host_type", ""]
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            _output_metadata_df_to_files(
                input_df, tmpdir, "test_output", INTERNAL_COL_KEYS,
                sep="\t", remove_internals_and_fails=True)

            # Find the main output file
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))

            # Verify main output has internal cols removed and no failures
            result_df = pandas.read_csv(output_files[0], sep="\t")
            expected_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample3"],
                "field_a": ["a1", "a3"]
            })
            assert_frame_equal(expected_df, result_df)

            # Find the fails file
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))

            # Verify fails file contains the failed row
            fails_df = pandas.read_csv(fails_files[0], sep=",")
            expected_fails_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample2"],
                "field_a": ["a2"],
                HOSTTYPE_SHORTHAND_KEY: ["human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool"],
                QC_NOTE_KEY: ["invalid host_type"]
            })
            assert_frame_equal(expected_fails_df, fails_df)

    def test__output_metadata_df_to_files_no_failures_creates_empty_file(self):
        """Test that empty fails file is created when there are no failures."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            _output_metadata_df_to_files(
                input_df, tmpdir, "test_output", INTERNAL_COL_KEYS,
                sep="\t", remove_internals_and_fails=True,
                suppress_empty_fails=False)

            # Find the fails file
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))

            # Verify fails file is empty (zero bytes)
            self.assertEqual(0, os.path.getsize(fails_files[0]))

    def test__output_metadata_df_to_files_suppress_empty_fails(self):
        """Test that empty fails file is not created when suppress_empty_fails=True."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            _output_metadata_df_to_files(
                input_df, tmpdir, "test_output", INTERNAL_COL_KEYS,
                sep="\t", remove_internals_and_fails=True,
                suppress_empty_fails=True)

            # Find the fails file - should not exist
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(0, len(fails_files))

            # Main output file should still exist
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))

    def test__output_metadata_df_to_files_csv_separator(self):
        """Test output with comma separator creates .csv file."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            _output_metadata_df_to_files(
                input_df, tmpdir, "test_output", INTERNAL_COL_KEYS,
                sep=",", remove_internals_and_fails=False)

            # Find the output file with .csv extension
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.csv"))
            self.assertEqual(1, len(output_files))

            # Read and verify contents (keep_default_na=False preserves empty strings)
            result_df = pandas.read_csv(output_files[0], sep=",", keep_default_na=False)
            expected_df = input_df
            assert_frame_equal(expected_df, result_df)

    def test__output_metadata_df_to_files_all_failures(self):
        """Test output when all rows are failures."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "field_a": ["a1", "a2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["invalid host_type", "invalid sample_type"]
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            _output_metadata_df_to_files(
                input_df, tmpdir, "test_output", INTERNAL_COL_KEYS,
                sep="\t", remove_internals_and_fails=True)

            # Main output file should have only headers (empty data)
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            result_df = pandas.read_csv(output_files[0], sep="\t")
            self.assertTrue(result_df.empty)
            self.assertEqual([SAMPLE_NAME_KEY, "field_a"], list(result_df.columns))

            # Fails file should have both rows
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))
            fails_df = pandas.read_csv(fails_files[0], sep=",")
            self.assertEqual(2, len(fails_df))

    # Tests for get_extended_metadata_from_df_and_yaml

    TEST_STUDY_CONFIG_FP = path.join(TEST_DIR, "data/test_study_config.yml")

    def test_get_extended_metadata_from_df_and_yaml_with_config(self):
        """Test extending metadata with a study-specific YAML config file."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })

        result_df, validation_msgs_df = get_extended_metadata_from_df_and_yaml(
            input_df, self.TEST_STUDY_CONFIG_FP, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            "study_custom_field": ["custom_value", "custom_value"],
            "study_stool_field": ["stool_custom", "stool_custom"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)

    def test_get_extended_metadata_from_df_and_yaml_none_config(self):
        """Test extending metadata with None for study_specific_config_fp."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })

        result_df, validation_msgs_df = get_extended_metadata_from_df_and_yaml(
            input_df, None, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)

    def test_get_extended_metadata_from_df_and_yaml_invalid_host_type(self):
        """Test that invalid host types are flagged with QC note."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })

        result_df, validation_msgs_df = get_extended_metadata_from_df_and_yaml(
            input_df, self.TEST_STUDY_CONFIG_FP, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["not provided", "UBERON:feces"],
            "body_site": ["not provided", "gut"],
            "description": ["not provided", "human sample"],
            "host_common_name": ["not provided", "human"],
            QIITA_SAMPLE_TYPE: ["not provided", "stool"],
            SAMPLE_TYPE_KEY: ["not provided", "stool"],
            "study_custom_field": ["not provided", "custom_value"],
            "study_stool_field": ["not provided", "stool_custom"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["invalid host_type", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)

    # Tests for write_extended_metadata_from_df

    def test_write_extended_metadata_from_df_basic(self):
        """Test basic writing of extended metadata to files."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {
                            "custom_field": {
                                DEFAULT_KEY: "custom_value",
                                TYPE_KEY: "string"
                            }
                        },
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata_from_df(
                input_df, study_config, tmpdir, "test_output",
                stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame
            expected_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "custom_field": ["custom_value", "custom_value"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
                QC_NOTE_KEY: ["", ""]
            })
            assert_frame_equal(expected_df, result_df)

            # Verify main output file was created (internal cols removed by default)
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            output_df = pandas.read_csv(output_files[0], sep="\t")
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "custom_field": ["custom_value", "custom_value"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"]
            })
            assert_frame_equal(expected_output_df, output_df)

            # Verify empty fails file was created
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))
            self.assertEqual(0, os.path.getsize(fails_files[0]))

            # Verify validation errors file was created (empty)
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self.assertEqual(0, os.path.getsize(validation_files[0]))

    def test_write_extended_metadata_from_df_with_qc_failures(self):
        """Test writing extended metadata when some rows have QC failures."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "unknown_host", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "stool"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {},
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata_from_df(
                input_df, study_config, tmpdir, "test_output",
                stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame includes all rows (including failures)
            # Note: rows are reordered by host type processing (valid hosts first)
            expected_result_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample3", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces", "not provided"],
                "body_site": ["gut", "gut", "not provided"],
                "description": ["human sample", "human sample", "not provided"],
                "host_common_name": ["human", "human", "not provided"],
                QIITA_SAMPLE_TYPE: ["stool", "stool", "not provided"],
                SAMPLE_TYPE_KEY: ["stool", "stool", "not provided"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human", "unknown_host"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "stool"],
                QC_NOTE_KEY: ["", "", "invalid host_type"]
            })
            assert_frame_equal(expected_result_df, result_df)

            # Verify main output file excludes failure rows
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            output_df = pandas.read_csv(output_files[0], sep="\t")
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample3"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"]
            })
            assert_frame_equal(expected_output_df, output_df)

            # Verify fails file contains the failed row
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))
            fails_df = pandas.read_csv(fails_files[0], sep=",")
            expected_fails_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample2"],
                "body_product": ["not provided"],
                "body_site": ["not provided"],
                "description": ["not provided"],
                "host_common_name": ["not provided"],
                QIITA_SAMPLE_TYPE: ["not provided"],
                SAMPLE_TYPE_KEY: ["not provided"],
                HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool"],
                QC_NOTE_KEY: ["invalid host_type"]
            })
            assert_frame_equal(expected_fails_df, fails_df)

    def test_write_extended_metadata_from_df_with_validation_errors(self):
        """Test writing extended metadata when validation errors occur."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            "restricted_field": ["invalid_value", "allowed_value"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {
                            "restricted_field": {
                                TYPE_KEY: "string",
                                ALLOWED_KEY: ["allowed_value"]
                            }
                        },
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata_from_df(
                input_df, study_config, tmpdir, "test_output",
                stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame
            expected_result_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                "restricted_field": ["invalid_value", "allowed_value"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
                QC_NOTE_KEY: ["", ""]
            })
            assert_frame_equal(expected_result_df, result_df)

            # Verify validation errors file contains the error
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            validation_df = pandas.read_csv(validation_files[0], sep=",")
            expected_validation_df = pandas.DataFrame({
                "sample_name": ["sample1"],
                "field_name": ["restricted_field"],
                "error_message": ["['unallowed value invalid_value']"]
            })
            assert_frame_equal(expected_validation_df, validation_df)

    def test_write_extended_metadata_from_df_remove_internals_false(self):
        """Test writing extended metadata with remove_internals=False."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        study_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "human": {
                        METADATA_FIELDS_KEY: {},
                        SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                            "stool": {
                                METADATA_FIELDS_KEY: {}
                            }
                        }
                    }
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            write_extended_metadata_from_df(
                input_df, study_config, tmpdir, "test_output",
                remove_internals=False, stds_fp=self.TEST_STDS_FP)

            # Verify main output file includes internal columns
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            output_df = pandas.read_csv(output_files[0], sep="\t", keep_default_na=False)
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1"],
                "body_product": ["UBERON:feces"],
                "body_site": ["gut"],
                "description": ["human sample"],
                "host_common_name": ["human"],
                QIITA_SAMPLE_TYPE: ["stool"],
                SAMPLE_TYPE_KEY: ["stool"],
                HOSTTYPE_SHORTHAND_KEY: ["human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool"],
                QC_NOTE_KEY: [""]
            })
            assert_frame_equal(expected_output_df, output_df)

            # Verify no fails file was created (since remove_internals=False)
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(0, len(fails_files))

    # Tests for write_extended_metadata

    TEST_METADATA_CSV_FP = path.join(TEST_DIR, "data/test_metadata.csv")
    TEST_METADATA_TXT_FP = path.join(TEST_DIR, "data/test_metadata.txt")
    TEST_METADATA_WITH_ERRORS_FP = path.join(
        TEST_DIR, "data/test_metadata_with_errors.csv")
    TEST_STUDY_CONFIG_WITH_VALIDATION_FP = path.join(
        TEST_DIR, "data/test_study_config_with_validation.yml")

    def test_write_extended_metadata_csv_input(self):
        """Test writing extended metadata from a CSV input file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata(
                self.TEST_METADATA_CSV_FP, self.TEST_STUDY_CONFIG_FP,
                tmpdir, "test_output", stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame
            expected_result_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
                QC_NOTE_KEY: ["", ""]
            })
            assert_frame_equal(expected_result_df, result_df)

            # Verify main output file was created (internal cols removed by default)
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            output_df = pandas.read_csv(output_files[0], sep="\t")
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"]
            })
            assert_frame_equal(expected_output_df, output_df)

            # Verify empty fails file was created
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))
            self.assertEqual(0, os.path.getsize(fails_files[0]))

            # Verify empty validation errors file was created
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self.assertEqual(0, os.path.getsize(validation_files[0]))

    def test_write_extended_metadata_txt_input(self):
        """Test writing extended metadata from a tab-delimited TXT input file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata(
                self.TEST_METADATA_TXT_FP, self.TEST_STUDY_CONFIG_FP,
                tmpdir, "test_output", stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame
            expected_result_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
                QC_NOTE_KEY: ["", ""]
            })
            assert_frame_equal(expected_result_df, result_df)

            # Verify main output file was created
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            output_df = pandas.read_csv(output_files[0], sep="\t")
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"]
            })
            assert_frame_equal(expected_output_df, output_df)

    def test_write_extended_metadata_with_validation_errors(self):
        """Test writing extended metadata when validation errors occur."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata(
                self.TEST_METADATA_WITH_ERRORS_FP,
                self.TEST_STUDY_CONFIG_WITH_VALIDATION_FP,
                tmpdir, "test_output", stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame
            expected_result_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                "restricted_field": ["invalid_value", "allowed_value"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
                QC_NOTE_KEY: ["", ""]
            })
            assert_frame_equal(expected_result_df, result_df)

            # Verify main output file was created
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            output_df = pandas.read_csv(output_files[0], sep="\t")
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                "restricted_field": ["invalid_value", "allowed_value"],
                SAMPLE_TYPE_KEY: ["stool", "stool"]
            })
            assert_frame_equal(expected_output_df, output_df)

            # Verify validation errors file contains the error
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            validation_df = pandas.read_csv(validation_files[0], sep=",")
            expected_validation_df = pandas.DataFrame({
                "sample_name": ["sample1"],
                "field_name": ["restricted_field"],
                "error_message": ["['unallowed value invalid_value']"]
            })
            assert_frame_equal(expected_validation_df, validation_df)

    def test_write_extended_metadata_unrecognized_extension_raises(self):
        """Test that unrecognized file extension raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_fp = path.join(tmpdir, "test.json")
            # Create a dummy file so the path exists
            with open(fake_fp, "w") as f:
                f.write("{}")

            with self.assertRaisesRegex(
                    ValueError, "Unrecognized input file extension"):
                write_extended_metadata(
                    fake_fp, self.TEST_STUDY_CONFIG_FP,
                    tmpdir, "test_output", stds_fp=self.TEST_STDS_FP)

    def test_write_extended_metadata_csv_separator_output(self):
        """Test writing extended metadata with CSV separator for output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata(
                self.TEST_METADATA_CSV_FP, self.TEST_STUDY_CONFIG_FP,
                tmpdir, "test_output", sep=",", stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame
            expected_result_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
                QC_NOTE_KEY: ["", ""]
            })
            assert_frame_equal(expected_result_df, result_df)

            # Verify output file has .csv extension
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.csv"))
            self.assertEqual(1, len(output_files))
            output_df = pandas.read_csv(output_files[0], sep=",")
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"]
            })
            assert_frame_equal(expected_output_df, output_df)

    def test_write_extended_metadata_remove_internals_false(self):
        """Test writing extended metadata with remove_internals=False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata(
                self.TEST_METADATA_CSV_FP, self.TEST_STUDY_CONFIG_FP,
                tmpdir, "test_output", remove_internals=False,
                stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame
            expected_result_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
                QC_NOTE_KEY: ["", ""]
            })
            assert_frame_equal(expected_result_df, result_df)

            # Verify main output file includes internal columns
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            output_df = pandas.read_csv(output_files[0], sep="\t", keep_default_na=False)
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
                QC_NOTE_KEY: ["", ""]
            })
            assert_frame_equal(expected_output_df, output_df)

            # Verify no fails file was created (since remove_internals=False)
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(0, len(fails_files))

    def test_write_extended_metadata_suppress_empty_fails(self):
        """Test writing extended metadata with suppress_empty_fails=True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata(
                self.TEST_METADATA_CSV_FP, self.TEST_STUDY_CONFIG_FP,
                tmpdir, "test_output", suppress_empty_fails=True,
                stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame
            expected_result_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"],
                HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
                SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
                QC_NOTE_KEY: ["", ""]
            })
            assert_frame_equal(expected_result_df, result_df)

            # Verify main output file was created
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            output_df = pandas.read_csv(output_files[0], sep="\t")
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"]
            })
            assert_frame_equal(expected_output_df, output_df)

            # Verify no empty fails file was created (since suppress_empty_fails=True)
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(0, len(fails_files))

            # Verify no empty validation errors file was created
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(0, len(validation_files))

    # Integration tests

    TEST_PROJECT1_METADATA_FP = path.join(TEST_DIR, "data/test_project1_input_metadata.csv")
    TEST_PROJECT1_CONFIG_FP = path.join(TEST_DIR, "data/test_project1_config.yml")
    TEST_PROJECT1_EXPECTED_OUTPUT_FP = path.join(
        TEST_DIR, "data/test_project1_output_metadata.txt")
    TEST_PROJECT1_EXPECTED_FAILS_FP = path.join(
        TEST_DIR, "data/test_project1_output_fails.csv")
    def test_write_extended_metadata_from_df_project1_integration(self):
        """Integration test using project1 test data files."""

        def write_mismatched_debug_files(expected_content, actual_content, file_name):
            """Write debug files to Desktop for unmatched content."""
            debug_dir = path.join(path.expanduser("~"), "Desktop")
            with open(path.join(debug_dir, f"UNMATCHED_1_{file_name}"), 'w') as debug_expected_file:
                debug_expected_file.write(expected_content)
            with open(path.join(debug_dir, f"UNMATCHED_2_{file_name}"), 'w') as debug_actual_file:
                debug_actual_file.write(actual_content)


        # Load input metadata CSV
        input_df = pandas.read_csv(self.TEST_PROJECT1_METADATA_FP, dtype=str)
        # for the columns "plating_notes" and "notes", fill NaN with empty string
        input_df["plating_notes"] = input_df["plating_notes"].fillna("")
        input_df["notes"] = input_df["notes"].fillna("")

        # Load study config
        study_config = _get_study_specific_config(self.TEST_PROJECT1_CONFIG_FP)

        with tempfile.TemporaryDirectory() as tmpdir:
            write_extended_metadata_from_df(
                input_df, study_config, tmpdir, "test_output",
                remove_internals=True)

            # Compare main output file directly to expected file
            output_files = glob.glob(os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            with open(output_files[0], 'r') as actual_file:
                actual_content = actual_file.read()
            with open(self.TEST_PROJECT1_EXPECTED_OUTPUT_FP, 'r') as expected_file:
                expected_content = expected_file.read()
            try:
                self.assertEqual(expected_content, actual_content)
            except AssertionError:
                write_mismatched_debug_files(
                    expected_content, actual_content,
                    "project1_output.txt")
                raise

            # Compare fails file directly to expected file
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))
            with open(fails_files[0], 'r') as actual_file:
                actual_fails_content = actual_file.read()
            with open(self.TEST_PROJECT1_EXPECTED_FAILS_FP, 'r') as expected_file:
                expected_fails_content = expected_file.read()
            try:
                self.assertEqual(expected_fails_content, actual_fails_content)
            except AssertionError:
                write_mismatched_debug_files(
                    expected_fails_content, actual_fails_content,
                    "project1_fails.csv")
                raise

            # Verify validation errors file is empty
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self.assertEqual(0, os.path.getsize(validation_files[0]))
