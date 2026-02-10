import glob
import os
import os.path as path
import pandas
import tempfile
from pandas.testing import assert_frame_equal
from metameq.src.util import \
    SAMPLE_NAME_KEY, \
    HOSTTYPE_SHORTHAND_KEY, \
    SAMPLETYPE_SHORTHAND_KEY, \
    QC_NOTE_KEY, \
    DEFAULT_KEY, \
    METADATA_FIELDS_KEY, \
    ALLOWED_KEY, \
    TYPE_KEY, \
    SAMPLE_TYPE_KEY, \
    QIITA_SAMPLE_TYPE, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, \
    OVERWRITE_NON_NANS_KEY, \
    LEAVE_REQUIREDS_BLANK_KEY, \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    STUDY_SPECIFIC_METADATA_KEY
from metameq.src.metadata_extender import \
    get_extended_metadata_from_df_and_yaml, \
    write_extended_metadata_from_df, \
    write_extended_metadata, \
    _get_study_specific_config
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestGetExtendedMetadataFromDfAndYaml(ExtenderTestBase):
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


class TestWriteExtendedMetadataFromDf(ExtenderTestBase):
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
            output_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
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
            output_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
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
            fails_df = pandas.read_csv(fails_files[0], sep=",", dtype=str, keep_default_na=False)
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
                                ALLOWED_KEY: ["allowed_value"],
                                "regex": "^allowed_.*$"
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

            # Verify validation errors file contains the errors
            # (two flattened rows for sample1's restricted_field)
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            validation_df = pandas.read_csv(validation_files[0], sep=",", dtype=str, keep_default_na=False)
            expected_validation_df = pandas.DataFrame({
                "sample_name": ["sample1", "sample1"],
                "field_name": ["restricted_field", "restricted_field"],
                "error_message": [
                    "unallowed value invalid_value",
                    "value does not match regex '^allowed_.*$'"
                ]
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
            output_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
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


class TestWriteExtendedMetadata(ExtenderTestBase):
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
                "dna_extracted": ["TRUE", "FALSE"],
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
            output_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "dna_extracted": ["TRUE", "FALSE"],
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
                "dna_extracted": ["TRUE", "FALSE"],
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
            output_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "dna_extracted": ["TRUE", "FALSE"],
                "host_common_name": ["human", "human"],
                QIITA_SAMPLE_TYPE: ["stool", "stool"],
                SAMPLE_TYPE_KEY: ["stool", "stool"],
                "study_custom_field": ["custom_value", "custom_value"],
                "study_stool_field": ["stool_custom", "stool_custom"]
            })
            assert_frame_equal(expected_output_df, output_df)

    def test_write_extended_metadata_xlsx_input(self):
        """Test writing extended metadata from an Excel XLSX input file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result_df = write_extended_metadata(
                self.TEST_METADATA_XLSX_FP, self.TEST_STUDY_CONFIG_FP,
                tmpdir, "test_output", stds_fp=self.TEST_STDS_FP)

            # Verify returned DataFrame
            expected_result_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "dna_extracted": ["TRUE", "FALSE"],
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
            output_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "dna_extracted": ["TRUE", "FALSE"],
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
                "dna_extracted": ["TRUE", "FALSE"],
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
            output_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "dna_extracted": ["TRUE", "FALSE"],
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
            validation_df = pandas.read_csv(validation_files[0], sep=",", dtype=str, keep_default_na=False)
            expected_validation_df = pandas.DataFrame({
                "sample_name": ["sample1"],
                "field_name": ["restricted_field"],
                "error_message": ["unallowed value invalid_value"]
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
                "dna_extracted": ["TRUE", "FALSE"],
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
            output_df = pandas.read_csv(output_files[0], sep=",", dtype=str, keep_default_na=False)
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "dna_extracted": ["TRUE", "FALSE"],
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
                "dna_extracted": ["TRUE", "FALSE"],
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
            output_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "dna_extracted": ["TRUE", "FALSE"],
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
                "dna_extracted": ["TRUE", "FALSE"],
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
            output_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
            expected_output_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "body_product": ["UBERON:feces", "UBERON:feces"],
                "body_site": ["gut", "gut"],
                "description": ["human sample", "human sample"],
                "dna_extracted": ["TRUE", "FALSE"],
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

    def test_write_extended_metadata_preserves_string_booleans(self):
        """Test that TRUE/FALSE string values are not converted to booleans.

        This tests for a bug where loading a CSV without dtype=str causes
        pandas to convert 'TRUE'/'FALSE' strings to boolean True/False,
        which then fail validation against allowed string values.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a CSV file with TRUE/FALSE string values
            csv_content = (
                "sample_name,hosttype_shorthand,sampletype_shorthand,dna_extracted\n"
                "sample1,human,stool,TRUE\n"
                "sample2,human,stool,FALSE\n"
            )
            csv_fp = path.join(tmpdir, "test_bool_strings.csv")
            with open(csv_fp, "w") as f:
                f.write(csv_content)

            # Create a config that defines TRUE/FALSE as allowed string values
            config_content = """
default: "not provided"
leave_requireds_blank: false
overwrite_non_nans: false
study_specific_metadata:
  host_type_specific_metadata:
    human:
      default: "not provided"
      leave_requireds_blank: false
      overwrite_non_nans: false
      sample_type_specific_metadata:
        stool:
          metadata_fields:
            dna_extracted:
              type: string
              allowed:
                - "TRUE"
                - "FALSE"
"""
            config_fp = path.join(tmpdir, "test_bool_config.yml")
            with open(config_fp, "w") as f:
                f.write(config_content)

            # Call write_extended_metadata
            result_df = write_extended_metadata(
                csv_fp, config_fp, tmpdir, "test_output",
                stds_fp=self.TEST_STDS_FP)

            # Verify the dna_extracted values are preserved as strings
            self.assertEqual("TRUE", result_df.loc[0, "dna_extracted"])
            self.assertEqual("FALSE", result_df.loc[1, "dna_extracted"])

            # Verify no validation errors occurred
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            # The validation errors file should be empty (0 bytes)
            self.assertEqual(0, os.path.getsize(validation_files[0]))

    def test_write_extended_metadata_project1_integration(self):
        """Integration test for write_extended_metadata using project1 test data files."""

        def write_mismatched_debug_files(expected_content, actual_content, file_name):
            """Write debug files to Desktop for unmatched content."""
            debug_dir = path.join(path.expanduser("~"), "Desktop")
            with open(path.join(debug_dir, f"UNMATCHED_1_{file_name}"), 'w') as debug_expected_file:
                debug_expected_file.write(expected_content)
            with open(path.join(debug_dir, f"UNMATCHED_2_{file_name}"), 'w') as debug_actual_file:
                debug_actual_file.write(actual_content)

        with tempfile.TemporaryDirectory() as tmpdir:
            write_extended_metadata(
                self.TEST_PROJECT1_METADATA_FP, self.TEST_PROJECT1_CONFIG_FP,
                tmpdir, "test_output", remove_internals=True)

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
