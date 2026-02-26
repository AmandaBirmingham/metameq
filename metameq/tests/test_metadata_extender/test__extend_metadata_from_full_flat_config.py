import pandas
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
    HOSTTYPE_COL_OPTIONS_KEY
from metameq.src.metadata_extender import \
    _extend_metadata_from_full_flat_config
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestExtendMetadataFromFullFlatConfig(ExtenderTestBase):
    """Tests for _extend_metadata_from_full_flat_config."""

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

    def test_basic(self):
        """Test metadata extension with multiple host and sample types."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "mouse"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "cecum"]
        })

        result_df, validation_msgs_df, col_name_mapping = \
            _extend_metadata_from_full_flat_config(
                input_df, self.BASIC_FLAT_CONFIG, None, None, None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2", "sample3"],
            "host_field": ["host_value", "host_value", "mouse_host_value"],
            QIITA_SAMPLE_TYPE: ["stool", "stool", "cecum"],
            SAMPLE_TYPE_KEY: ["stool", "stool", "cecum"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human", "mouse"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool", "cecum"],
            QC_NOTE_KEY: ["", "", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)
        self.assertEqual({
            HOSTTYPE_SHORTHAND_KEY: HOSTTYPE_SHORTHAND_KEY,
            SAMPLETYPE_SHORTHAND_KEY: SAMPLETYPE_SHORTHAND_KEY
        }, col_name_mapping)

    def test_missing_required_columns_raises(self):
        """Test that missing required columns raises ValueError."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"]
        })

        with self.assertRaisesRegex(ValueError, "metadata missing required columns"):
            _extend_metadata_from_full_flat_config(
                input_df, self.BASIC_FLAT_CONFIG, None, None, None)

    def test_with_hosttype_col_name(self):
        """Test column resolution with hosttype_col_name parameter."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_type": ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
        })

        result_df, validation_msgs_df, col_name_mapping = \
            _extend_metadata_from_full_flat_config(
                input_df, self.BASIC_FLAT_CONFIG, None,
                "host_type", None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_field": ["host_value", "host_value"],
            "host_type": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)
        self.assertEqual({
            HOSTTYPE_SHORTHAND_KEY: "host_type",
            SAMPLETYPE_SHORTHAND_KEY: SAMPLETYPE_SHORTHAND_KEY
        }, col_name_mapping)

    def test_with_sampletype_col_name(self):
        """Test column resolution with sampletype_col_name parameter."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            "sample": ["stool", "stool"]
        })

        result_df, validation_msgs_df, col_name_mapping = \
            _extend_metadata_from_full_flat_config(
                input_df, self.BASIC_FLAT_CONFIG, None,
                None, "sample")

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_field": ["host_value", "host_value"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            "sample": ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)
        self.assertEqual({
            HOSTTYPE_SHORTHAND_KEY: HOSTTYPE_SHORTHAND_KEY,
            SAMPLETYPE_SHORTHAND_KEY: "sample"
        }, col_name_mapping)

    def test_with_both_col_name_params(self):
        """Test column resolution with both col name parameters."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_type": ["human", "human"],
            "sample": ["stool", "stool"]
        })

        result_df, validation_msgs_df, col_name_mapping = \
            _extend_metadata_from_full_flat_config(
                input_df, self.BASIC_FLAT_CONFIG, None,
                "host_type", "sample")

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_field": ["host_value", "host_value"],
            "host_type": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            "sample": ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)
        self.assertEqual({
            HOSTTYPE_SHORTHAND_KEY: "host_type",
            SAMPLETYPE_SHORTHAND_KEY: "sample"
        }, col_name_mapping)

    def test_col_name_from_config_options(self):
        """Test column resolution via config col_options when no param is given."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            "host_type": ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        config = dict(self.BASIC_FLAT_CONFIG)
        config[HOSTTYPE_COL_OPTIONS_KEY] = ["host_type"]

        result_df, validation_msgs_df, col_name_mapping = \
            _extend_metadata_from_full_flat_config(
                input_df, config, None, None, None)

        self.assertIn(HOSTTYPE_SHORTHAND_KEY, result_df.columns)
        self.assertEqual("human", result_df[HOSTTYPE_SHORTHAND_KEY].iloc[0])
        self.assertTrue(validation_msgs_df.empty)
        self.assertEqual({
            HOSTTYPE_SHORTHAND_KEY: "host_type",
            SAMPLETYPE_SHORTHAND_KEY: SAMPLETYPE_SHORTHAND_KEY
        }, col_name_mapping)

    def test_col_name_not_found_raises(self):
        """Test that a non-existent column name raises ValueError."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })

        with self.assertRaisesRegex(ValueError, "not found in metadata"):
            _extend_metadata_from_full_flat_config(
                input_df, self.BASIC_FLAT_CONFIG, None,
                "nonexistent_col", None)

    def test_col_name_conflict_warns_and_uses_internal_key(self):
        """Test that both internal and alternate columns warns and uses internal key."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            "host_type": ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })

        with self.assertLogs("metameq.src.metadata_extender", level="WARNING") as cm:
            result_df, validation_msgs_df, col_name_mapping = \
                _extend_metadata_from_full_flat_config(
                    input_df, self.BASIC_FLAT_CONFIG, None,
                    "host_type", None)

        self.assertTrue(any("contains both" in msg for msg in cm.output))
        self.assertEqual(HOSTTYPE_SHORTHAND_KEY,
                         col_name_mapping[HOSTTYPE_SHORTHAND_KEY])

    def test_unknown_host_type(self):
        """Test that unknown host type adds QC note."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })

        result_df, validation_msgs_df, col_name_mapping = \
            _extend_metadata_from_full_flat_config(
                input_df, self.BASIC_FLAT_CONFIG, None, None, None)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["unknown_host"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: ["invalid host_type"]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertEqual({
            HOSTTYPE_SHORTHAND_KEY: HOSTTYPE_SHORTHAND_KEY,
            SAMPLETYPE_SHORTHAND_KEY: SAMPLETYPE_SHORTHAND_KEY
        }, col_name_mapping)
