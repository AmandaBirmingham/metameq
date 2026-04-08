import glob
import os
import os.path as path
import tempfile
import yaml
from click.testing import CliRunner
from unittest import TestCase
from metameq.src.__main__ import root
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


TEST_DIR = path.dirname(__file__)
TEST_PROJECT1_METADATA_FP = path.join(
    TEST_DIR, "data/test_project1_input_metadata.csv")
TEST_PROJECT1_CONFIG_FP = path.join(
    TEST_DIR, "data/test_project1_config.yml")
TEST_PROJECT1_EXPECTED_OUTPUT_FP = path.join(
    TEST_DIR, "data/test_project1_output_metadata.txt")
TEST_PROJECT1_EXPECTED_FAILS_FP = path.join(
    TEST_DIR, "data/test_project1_output_fails.csv")

# Minimal flat config for write-validator-metadata tests
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
        }
    }
}

VALID_METADATA_CSV = (
    f"{SAMPLE_NAME_KEY},{HOSTTYPE_SHORTHAND_KEY},{SAMPLETYPE_SHORTHAND_KEY}\n"
    "sample1,human,stool\n"
    "sample2,human,stool\n"
)


def _write_validator_test_files(tmpdir, config_dict=None,
                                metadata_csv=None):
    """Write a YAML config and CSV metadata to tmpdir.

    Returns (config_fp, metadata_fp).
    """
    if config_dict is None:
        config_dict = BASIC_FLAT_CONFIG
    if metadata_csv is None:
        metadata_csv = VALID_METADATA_CSV

    config_fp = path.join(tmpdir, "test_config.yml")
    with open(config_fp, "w") as f:
        yaml.dump(config_dict, f)

    metadata_fp = path.join(tmpdir, "test_metadata.csv")
    with open(metadata_fp, "w") as f:
        f.write(metadata_csv)

    return config_fp, metadata_fp


class TestWriteExtendedMetadataCli(TestCase):
    """CLI tests for the write-extended-metadata command."""

    def test_write_extended_metadata_cli(self):
        """Test basic invocation with project1 data produces expected output."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(root, [
                "write-extended-metadata",
                TEST_PROJECT1_METADATA_FP,
                TEST_PROJECT1_CONFIG_FP,
                "test_output",
                "--out_dir", tmpdir])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify main output matches expected
            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            with open(output_files[0], 'r') as f:
                actual_content = f.read()
            with open(TEST_PROJECT1_EXPECTED_OUTPUT_FP, 'r') as f:
                expected_content = f.read()
            self.assertEqual(expected_content, actual_content)

            # Verify fails file matches expected
            fails_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))
            with open(fails_files[0], 'r') as f:
                actual_fails = f.read()
            with open(TEST_PROJECT1_EXPECTED_FAILS_FP, 'r') as f:
                expected_fails = f.read()
            self.assertEqual(expected_fails, actual_fails)

    def test_write_extended_metadata_cli_missing_metadata_file(self):
        """Test that a nonexistent metadata file produces a non-zero exit code."""
        runner = CliRunner()
        result = runner.invoke(root, [
            "write-extended-metadata",
            "/nonexistent/metadata.csv",
            TEST_PROJECT1_CONFIG_FP,
            "test_output"])

        self.assertNotEqual(0, result.exit_code)

    def test_write_extended_metadata_cli_missing_config_file(self):
        """Test that a nonexistent config file produces a non-zero exit code."""
        runner = CliRunner()
        result = runner.invoke(root, [
            "write-extended-metadata",
            TEST_PROJECT1_METADATA_FP,
            "/nonexistent/config.yml",
            "test_output"])

        self.assertNotEqual(0, result.exit_code)

    def test_write_extended_metadata_cli_out_dir(self):
        """Test that --out_dir controls where output files are written."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a subdirectory to use as --out_dir
            sub_dir = path.join(tmpdir, "subdir")
            os.makedirs(sub_dir)

            result = runner.invoke(root, [
                "write-extended-metadata",
                TEST_PROJECT1_METADATA_FP,
                TEST_PROJECT1_CONFIG_FP,
                "test_output",
                "--out_dir", sub_dir])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify output landed in the subdirectory
            output_files = glob.glob(
                os.path.join(sub_dir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))

            # Verify nothing was written to the parent directory
            parent_output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(0, len(parent_output_files))

    def test_write_extended_metadata_cli_csv_separator(self):
        """Test that --sep ',' produces a .csv output file."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(root, [
                "write-extended-metadata",
                TEST_PROJECT1_METADATA_FP,
                TEST_PROJECT1_CONFIG_FP,
                "test_output",
                "--out_dir", tmpdir,
                "--sep", ","])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify output has .csv extension
            csv_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.csv"))
            self.assertEqual(1, len(csv_files))

            # Verify no .txt output was created
            txt_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(0, len(txt_files))

    def test_write_extended_metadata_cli_suppress_fails(self):
        """Test that --suppress_fails_files suppresses empty error files.

        Project1 data has non-empty fails (QC failures from NaN host/sample
        types) but empty validation errors, so only the validation errors
        file should be suppressed.
        """
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(root, [
                "write-extended-metadata",
                TEST_PROJECT1_METADATA_FP,
                TEST_PROJECT1_CONFIG_FP,
                "test_output",
                "--out_dir", tmpdir,
                "--suppress_fails_files"])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify main output file was created
            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))

            # Verify non-empty fails file still exists
            fails_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_fails.csv"))
            self.assertEqual(1, len(fails_files))
            self.assertGreater(os.path.getsize(fails_files[0]), 0)

            # Verify empty validation errors file was suppressed
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(0, len(validation_files))


class TestWriteValidatorMetadataCli(TestCase):
    """CLI tests for the write-validator-metadata command."""

    def test_write_validator_metadata_cli(self):
        """Test basic invocation with valid input produces expected output."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = _write_validator_test_files(tmpdir)

            result = runner.invoke(root, [
                "write-validator-metadata",
                metadata_fp,
                config_fp,
                "test_output",
                "--out_dir", tmpdir])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify main output file was created
            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))

            # Verify empty validation errors file was created
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(1, len(validation_files))
            self.assertEqual(0, os.path.getsize(validation_files[0]))

    def test_write_validator_metadata_cli_missing_metadata_file(self):
        """Test that a nonexistent metadata file produces a non-zero exit code."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, _ = _write_validator_test_files(tmpdir)

            result = runner.invoke(root, [
                "write-validator-metadata",
                "/nonexistent/metadata.csv",
                config_fp,
                "test_output"])

            self.assertNotEqual(0, result.exit_code)

    def test_write_validator_metadata_cli_missing_flat_config_file(self):
        """Test that a nonexistent flat config file produces a non-zero exit code."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            _, metadata_fp = _write_validator_test_files(tmpdir)

            result = runner.invoke(root, [
                "write-validator-metadata",
                metadata_fp,
                "/nonexistent/flat_config.yml",
                "test_output"])

            self.assertNotEqual(0, result.exit_code)

    def test_write_validator_metadata_cli_out_dir(self):
        """Test that --out_dir controls where output files are written."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = _write_validator_test_files(tmpdir)

            # Create a subdirectory to use as --out_dir
            sub_dir = path.join(tmpdir, "subdir")
            os.makedirs(sub_dir)

            result = runner.invoke(root, [
                "write-validator-metadata",
                metadata_fp,
                config_fp,
                "test_output",
                "--out_dir", sub_dir])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify output landed in the subdirectory
            output_files = glob.glob(
                os.path.join(sub_dir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))

            # Verify nothing was written to the parent directory
            parent_output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(0, len(parent_output_files))

    def test_write_validator_metadata_cli_csv_separator(self):
        """Test that --sep ',' produces a .csv output file."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = _write_validator_test_files(tmpdir)

            result = runner.invoke(root, [
                "write-validator-metadata",
                metadata_fp,
                config_fp,
                "test_output",
                "--out_dir", tmpdir,
                "--sep", ","])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify output has .csv extension
            csv_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.csv"))
            self.assertEqual(1, len(csv_files))

            # Verify no .txt output was created
            txt_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(0, len(txt_files))

    def test_write_validator_metadata_cli_keep_internals(self):
        """Test that --keep_internals keeps internal columns in output."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = _write_validator_test_files(tmpdir)

            result = runner.invoke(root, [
                "write-validator-metadata",
                metadata_fp,
                config_fp,
                "test_output",
                "--out_dir", tmpdir,
                "--keep_internals"])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify output file contains internal columns
            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
            with open(output_files[0], 'r') as f:
                header = f.readline().strip()
            header_cols = header.split("\t")
            self.assertIn(HOSTTYPE_SHORTHAND_KEY, header_cols)
            self.assertIn(SAMPLETYPE_SHORTHAND_KEY, header_cols)

    def test_write_validator_metadata_cli_suppress_fails(self):
        """Test that --suppress_fails_files suppresses empty error files."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = _write_validator_test_files(tmpdir)

            result = runner.invoke(root, [
                "write-validator-metadata",
                metadata_fp,
                config_fp,
                "test_output",
                "--out_dir", tmpdir,
                "--suppress_fails_files"])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify main output file was created
            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))

            # Verify no empty validation errors file was created
            validation_files = glob.glob(
                os.path.join(tmpdir, "*_test_output_validation_errors.csv"))
            self.assertEqual(0, len(validation_files))

    def test_write_validator_metadata_cli_hosttype_col_name(self):
        """Test that --hosttype_col_name accepts an alternative column name."""
        # Create metadata with alternative host type column name
        alt_metadata_csv = (
            f"{SAMPLE_NAME_KEY},host_type,{SAMPLETYPE_SHORTHAND_KEY}\n"
            "sample1,human,stool\n"
        )

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = _write_validator_test_files(
                tmpdir, metadata_csv=alt_metadata_csv)

            result = runner.invoke(root, [
                "write-validator-metadata",
                metadata_fp,
                config_fp,
                "test_output",
                "--out_dir", tmpdir,
                "--hosttype_col_name", "host_type"])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify output file was created successfully
            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))

    def test_write_validator_metadata_cli_sampletype_col_name(self):
        """Test that --sampletype_col_name accepts an alternative column name."""
        # Create metadata with alternative sample type column name
        alt_metadata_csv = (
            f"{SAMPLE_NAME_KEY},{HOSTTYPE_SHORTHAND_KEY},sample_category\n"
            "sample1,human,stool\n"
        )

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_fp, metadata_fp = _write_validator_test_files(
                tmpdir, metadata_csv=alt_metadata_csv)

            result = runner.invoke(root, [
                "write-validator-metadata",
                metadata_fp,
                config_fp,
                "test_output",
                "--out_dir", tmpdir,
                "--sampletype_col_name", "sample_category"])

            self.assertEqual(0, result.exit_code, result.output)

            # Verify output file was created successfully
            output_files = glob.glob(
                os.path.join(tmpdir, "*_test_output.txt"))
            self.assertEqual(1, len(output_files))
