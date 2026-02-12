import glob
import os
import os.path as path
import tempfile
import yaml
from metameq.src.util import \
    SAMPLE_NAME_KEY, \
    HOSTTYPE_SHORTHAND_KEY, \
    SAMPLETYPE_SHORTHAND_KEY, \
    DEFAULT_KEY, \
    METADATA_FIELDS_KEY, \
    ALLOWED_KEY, \
    TYPE_KEY, \
    SAMPLE_TYPE_KEY, \
    QIITA_SAMPLE_TYPE, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, \
    OVERWRITE_NON_NANS_KEY, \
    LEAVE_REQUIREDS_BLANK_KEY, \
    HOST_TYPE_SPECIFIC_METADATA_KEY
from metameq.src.metadata_extender import write_validator_metadata
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestWriteValidatorMetadata(ExtenderTestBase):
    """Tests for write_validator_metadata."""

    BASIC_FLAT_CONFIG = {
        DEFAULT_KEY: "not provided",
        LEAVE_REQUIREDS_BLANK_KEY: False,
        OVERWRITE_NON_NANS_KEY: False,
        HOST_TYPE_SPECIFIC_METADATA_KEY: {
            "human": {
                DEFAULT_KEY: "not provided",
                LEAVE_REQUIREDS_BLANK_KEY: False,
                OVERWRITE_NON_NANS_KEY: False,
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
                DEFAULT_KEY: "not provided",
                LEAVE_REQUIREDS_BLANK_KEY: False,
                OVERWRITE_NON_NANS_KEY: False,
                METADATA_FIELDS_KEY: {
                    "host_field": {
                        DEFAULT_KEY: "mouse_host_value",
                        TYPE_KEY: "string"
                    }
                },
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "cecum": {
                        METADATA_FIELDS_KEY: {
                            "host_field": {
                                DEFAULT_KEY: "mouse_host_value",
                                TYPE_KEY: "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                ALLOWED_KEY: ["cecum"],
                                DEFAULT_KEY: "cecum",
                                TYPE_KEY: "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                ALLOWED_KEY: ["cecum"],
                                DEFAULT_KEY: "cecum",
                                TYPE_KEY: "string"
                            }
                        }
                    }
                }
            }
        }
    }

    def _write_config_and_metadata(self, tmpdir, config_dict,
                                   metadata_csv_content):
        """Write a YAML config file and CSV metadata file to tmpdir.

        Returns (config_fp, metadata_fp).
        """
        config_fp = path.join(tmpdir, "test_config.yml")
        with open(config_fp, "w") as f:
            yaml.dump(config_dict, f)

        metadata_fp = path.join(tmpdir, "test_metadata.csv")
        with open(metadata_fp, "w") as f:
            f.write(metadata_csv_content)

        return config_fp, metadata_fp

    def _assert_file_matches_expected(self, actual_fp, expected_filename):
        """Assert that actual file contents match expected data file."""
        expected_fp = path.join(self.TEST_DIR, "data", expected_filename)
        with open(actual_fp, "r") as actual_file:
            actual_content = actual_file.read()
        with open(expected_fp, "r") as expected_file:
            expected_content = expected_file.read()
        self.assertEqual(expected_content, actual_content)

    def test_write_validator_metadata_valid_suppress_empty(self):
        """Test valid input with suppress_empty_fails=True creates no validation file."""
        metadata_csv = (
            f"{SAMPLE_NAME_KEY},{HOSTTYPE_SHORTHAND_KEY},{SAMPLETYPE_SHORTHAND_KEY}\n"
            "sample1,human,stool\n"
            "sample2,human,stool\n"
            "sample3,mouse,cecum\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = self._write_config_and_metadata(
                tmpdir, self.BASIC_FLAT_CONFIG, metadata_csv)

            write_validator_metadata(
                metadata_fp, config_fp, tmpdir, "test_output",
                suppress_empty_fails=True)

            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            self._assert_file_matches_expected(
                output_files[0], "test_validator_valid_output.txt")

            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(0, len(validation_files))

    def test_write_validator_metadata_valid_creates_empty_errors(self):
        """Test valid input without suppress creates empty validation file."""
        metadata_csv = (
            f"{SAMPLE_NAME_KEY},{HOSTTYPE_SHORTHAND_KEY},{SAMPLETYPE_SHORTHAND_KEY}\n"
            "sample1,human,stool\n"
            "sample2,human,stool\n"
            "sample3,mouse,cecum\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = self._write_config_and_metadata(
                tmpdir, self.BASIC_FLAT_CONFIG, metadata_csv)

            write_validator_metadata(
                metadata_fp, config_fp, tmpdir, "test_output")

            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            self._assert_file_matches_expected(
                output_files[0], "test_validator_valid_output.txt")

            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self.assertEqual(0, os.path.getsize(validation_files[0]))

    def test_write_validator_metadata_qc_failure_internal_names(self):
        """Test QC failure uses internal column names in validation file."""
        metadata_csv = (
            f"{SAMPLE_NAME_KEY},{HOSTTYPE_SHORTHAND_KEY},{SAMPLETYPE_SHORTHAND_KEY}\n"
            "sample1,unknown_host,stool\n"
            "sample2,human,stool\n"
            "sample3,mouse,cecum\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = self._write_config_and_metadata(
                tmpdir, self.BASIC_FLAT_CONFIG, metadata_csv)

            write_validator_metadata(
                metadata_fp, config_fp, tmpdir, "test_output",
                suppress_empty_fails=True)

            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            self._assert_file_matches_expected(
                output_files[0],
                "test_validator_qc_failure_output.txt")

            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self._assert_file_matches_expected(
                validation_files[0],
                "test_validator_qc_failure_validation_errors.csv")

    def test_write_validator_metadata_qc_failure_alt_col_names(self):
        """Test QC failure uses user-facing column names in validation file."""
        metadata_csv = (
            f"{SAMPLE_NAME_KEY},host_type,{SAMPLETYPE_SHORTHAND_KEY}\n"
            "sample1,unknown_host,stool\n"
            "sample2,human,stool\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = self._write_config_and_metadata(
                tmpdir, self.BASIC_FLAT_CONFIG, metadata_csv)

            write_validator_metadata(
                metadata_fp, config_fp, tmpdir, "test_output",
                hosttype_col_name="host_type",
                suppress_empty_fails=True)

            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            self._assert_file_matches_expected(
                output_files[0],
                "test_validator_alt_col_output.txt")

            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self._assert_file_matches_expected(
                validation_files[0],
                "test_validator_alt_col_validation_errors.csv")

    def test_write_validator_metadata_all_fail_keeps_all_rows(self):
        """Test all QC failures still keeps all rows in output."""
        metadata_csv = (
            f"{SAMPLE_NAME_KEY},{HOSTTYPE_SHORTHAND_KEY},{SAMPLETYPE_SHORTHAND_KEY},dna_extracted\n"
            "sample1,bad1,stool,TRUE\n"
            "sample2,bad2,stool,FALSE\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = self._write_config_and_metadata(
                tmpdir, self.BASIC_FLAT_CONFIG, metadata_csv)

            write_validator_metadata(
                metadata_fp, config_fp, tmpdir, "test_output",
                suppress_empty_fails=True)

            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            self._assert_file_matches_expected(
                output_files[0],
                "test_validator_all_fail_output.txt")

            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self._assert_file_matches_expected(
                validation_files[0],
                "test_validator_all_fail_validation_errors.csv")

    def test_write_validator_metadata_keep_internals(self):
        """Test remove_internals=False keeps internal columns in output."""
        metadata_csv = (
            f"{SAMPLE_NAME_KEY},{HOSTTYPE_SHORTHAND_KEY},{SAMPLETYPE_SHORTHAND_KEY}\n"
            "sample1,unknown_host,stool\n"
            "sample2,human,stool\n"
            "sample3,mouse,cecum\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = self._write_config_and_metadata(
                tmpdir, self.BASIC_FLAT_CONFIG, metadata_csv)

            write_validator_metadata(
                metadata_fp, config_fp, tmpdir, "test_output",
                remove_internals=False,
                suppress_empty_fails=True)

            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            self._assert_file_matches_expected(
                output_files[0],
                "test_validator_keep_internals_output.txt")

            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self._assert_file_matches_expected(
                validation_files[0],
                "test_validator_qc_failure_validation_errors.csv")
