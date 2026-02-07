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
    METADATA_FIELDS_KEY, \
    TYPE_KEY, \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    METADATA_TRANSFORMERS_KEY, \
    PRE_TRANSFORMERS_KEY
from metameq.src.metadata_extender import \
    get_qc_failures, \
    write_metadata_results, \
    _get_study_specific_config, \
    _output_metadata_df_to_files, \
    INTERNAL_COL_KEYS
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestWriteMetadataResults(ExtenderTestBase):
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
                metadata_files[0], sep="\t", dtype=str, keep_default_na=False)
            assert_frame_equal(metadata_df, result_df)

            # Find the validation errors file (uses comma separator)
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))

            # Verify validation errors file contents
            result_validation_df = pandas.read_csv(validation_files[0], sep=",", dtype=str, keep_default_na=False)
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
            result_df = pandas.read_csv(metadata_files[0], sep="\t", dtype=str, keep_default_na=False)
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
            fails_df = pandas.read_csv(fails_files[0], sep=",", dtype=str, keep_default_na=False)
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
            result_df = pandas.read_csv(metadata_files[0], sep="\t", dtype=str, keep_default_na=False)
            expected_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample2"],
                "field_a": ["a1", "a2"]
            })
            assert_frame_equal(expected_df, result_df)


class TestGetQcFailures(ExtenderTestBase):
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


class TestGetStudySpecificConfig(ExtenderTestBase):
    def test__get_study_specific_config_with_valid_file(self):
        """Test loading study-specific config from a valid YAML file."""
        config_fp = path.join(self.TEST_DIR, "data/test_config.yml")

        result = _get_study_specific_config(config_fp)

        expected = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
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


class TestOutputMetadataDfToFiles(ExtenderTestBase):
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
            result_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
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
            result_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
            expected_df = pandas.DataFrame({
                SAMPLE_NAME_KEY: ["sample1", "sample3"],
                "field_a": ["a1", "a3"]
            })
            assert_frame_equal(expected_df, result_df)

            # Find the fails file
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))

            # Verify fails file contains the failed row
            fails_df = pandas.read_csv(fails_files[0], sep=",", dtype=str, keep_default_na=False)
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
            result_df = pandas.read_csv(output_files[0], sep=",", dtype=str, keep_default_na=False)
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
            result_df = pandas.read_csv(output_files[0], sep="\t", dtype=str, keep_default_na=False)
            self.assertTrue(result_df.empty)
            self.assertEqual([SAMPLE_NAME_KEY, "field_a"], list(result_df.columns))

            # Fails file should have both rows
            fails_files = glob.glob(os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))
            fails_df = pandas.read_csv(fails_files[0], sep=",", dtype=str, keep_default_na=False)
            self.assertEqual(2, len(fails_df))
