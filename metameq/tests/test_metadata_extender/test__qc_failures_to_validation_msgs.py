import pandas
from metameq.src.util import \
    SAMPLE_NAME_KEY, \
    HOSTTYPE_SHORTHAND_KEY, \
    SAMPLETYPE_SHORTHAND_KEY, \
    QC_NOTE_KEY
from metameq.src.metadata_extender import _qc_failures_to_validation_msgs
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestQcFailuresToValidationMsgs(ExtenderTestBase):
    """Tests for _qc_failures_to_validation_msgs."""

    INTERNAL_MAPPING = {
        HOSTTYPE_SHORTHAND_KEY: HOSTTYPE_SHORTHAND_KEY,
        SAMPLETYPE_SHORTHAND_KEY: SAMPLETYPE_SHORTHAND_KEY
    }

    def _make_metadata_df(self, sample_names, host_types,
                          sample_types, qc_notes):
        return pandas.DataFrame({
            SAMPLE_NAME_KEY: sample_names,
            HOSTTYPE_SHORTHAND_KEY: host_types,
            SAMPLETYPE_SHORTHAND_KEY: sample_types,
            QC_NOTE_KEY: qc_notes
        })

    def test_no_qc_failures(self):
        """Test that no QC failures produces an empty list."""
        df = self._make_metadata_df(
            ["s1"], ["human"], ["stool"], [""])
        result = _qc_failures_to_validation_msgs(df, self.INTERNAL_MAPPING)
        self.assertEqual([], result)

    def test_host_type_failure_internal_names(self):
        """Test host type failure with internal column names."""
        df = self._make_metadata_df(
            ["s1"], ["unknown_host"], ["stool"], ["invalid host_type"])
        result = _qc_failures_to_validation_msgs(df, self.INTERNAL_MAPPING)

        expected = [{
            SAMPLE_NAME_KEY: "s1",
            "field_name": HOSTTYPE_SHORTHAND_KEY,
            "field_value": "unknown_host",
            "error_message": ["invalid host_type"]
        }]
        self.assertEqual(expected, result)

    def test_host_type_failure_alternate_name(self):
        """Test host type failure uses user-facing column name."""
        df = self._make_metadata_df(
            ["s1"], ["bad_host"], ["stool"], ["invalid host_type"])
        mapping = {
            HOSTTYPE_SHORTHAND_KEY: "host_type",
            SAMPLETYPE_SHORTHAND_KEY: SAMPLETYPE_SHORTHAND_KEY
        }
        result = _qc_failures_to_validation_msgs(df, mapping)

        expected = [{
            SAMPLE_NAME_KEY: "s1",
            "field_name": "host_type",
            "field_value": "bad_host",
            "error_message": ["invalid host_type"]
        }]
        self.assertEqual(expected, result)

    def test_sample_type_failure_alternate_name(self):
        """Test sample type failure uses user-facing column name."""
        df = self._make_metadata_df(
            ["s1"], ["human"], ["bad_sample"], ["invalid sample_type"])
        mapping = {
            HOSTTYPE_SHORTHAND_KEY: HOSTTYPE_SHORTHAND_KEY,
            SAMPLETYPE_SHORTHAND_KEY: "sample"
        }
        result = _qc_failures_to_validation_msgs(df, mapping)

        expected = [{
            SAMPLE_NAME_KEY: "s1",
            "field_name": "sample",
            "field_value": "bad_sample",
            "error_message": ["invalid sample_type"]
        }]
        self.assertEqual(expected, result)

    def test_mixed_failures(self):
        """Test multiple QC failures with different types."""
        df = self._make_metadata_df(
            ["s1", "s2", "s3"],
            ["bad_host", "human", "human"],
            ["stool", "bad_sample", "stool"],
            ["invalid host_type", "invalid sample_type", ""])
        mapping = {
            HOSTTYPE_SHORTHAND_KEY: "host_type",
            SAMPLETYPE_SHORTHAND_KEY: "sample"
        }
        result = _qc_failures_to_validation_msgs(df, mapping)

        expected = [
            {
                SAMPLE_NAME_KEY: "s1",
                "field_name": "host_type",
                "field_value": "bad_host",
                "error_message": ["invalid host_type"]
            },
            {
                SAMPLE_NAME_KEY: "s2",
                "field_name": "sample",
                "field_value": "bad_sample",
                "error_message": ["invalid sample_type"]
            }
        ]
        self.assertEqual(expected, result)

    def test_all_rows_are_failures(self):
        """Test when every row has a QC failure."""
        df = self._make_metadata_df(
            ["s1", "s2"],
            ["bad1", "bad2"],
            ["stool", "stool"],
            ["invalid host_type", "invalid host_type"])
        result = _qc_failures_to_validation_msgs(df, self.INTERNAL_MAPPING)

        expected = [
            {
                SAMPLE_NAME_KEY: "s1",
                "field_name": HOSTTYPE_SHORTHAND_KEY,
                "field_value": "bad1",
                "error_message": ["invalid host_type"]
            },
            {
                SAMPLE_NAME_KEY: "s2",
                "field_name": HOSTTYPE_SHORTHAND_KEY,
                "field_value": "bad2",
                "error_message": ["invalid host_type"]
            }
        ]
        self.assertEqual(expected, result)
