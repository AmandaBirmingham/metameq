import os.path as path
from unittest import TestCase


class ExtenderTestBase(TestCase):
    """Base class for metadata extender tests with shared path constants."""

    TEST_DIR = path.dirname(path.dirname(__file__))
    TEST_STDS_FP = path.join(TEST_DIR, "data/test_standards.yml")
    TEST_STUDY_CONFIG_FP = path.join(TEST_DIR, "data/test_study_config.yml")
    TEST_METADATA_CSV_FP = path.join(TEST_DIR, "data/test_metadata.csv")
    TEST_METADATA_TXT_FP = path.join(TEST_DIR, "data/test_metadata.txt")
    TEST_METADATA_XLSX_FP = path.join(TEST_DIR, "data/test_metadata.xlsx")
    TEST_METADATA_WITH_ERRORS_FP = path.join(
        TEST_DIR, "data/test_metadata_with_errors.csv")
    TEST_STUDY_CONFIG_WITH_VALIDATION_FP = path.join(
        TEST_DIR, "data/test_study_config_with_validation.yml")
    TEST_PROJECT1_METADATA_FP = path.join(
        TEST_DIR, "data/test_project1_input_metadata.csv")
    TEST_PROJECT1_CONFIG_FP = path.join(
        TEST_DIR, "data/test_project1_config.yml")
    TEST_PROJECT1_EXPECTED_OUTPUT_FP = path.join(
        TEST_DIR, "data/test_project1_output_metadata.txt")
    TEST_PROJECT1_EXPECTED_FAILS_FP = path.join(
        TEST_DIR, "data/test_project1_output_fails.csv")
