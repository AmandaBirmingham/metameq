import numpy as np
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
    METADATA_TRANSFORMERS_KEY, \
    SOURCES_KEY, \
    FUNCTION_KEY, \
    PRE_TRANSFORMERS_KEY, \
    POST_TRANSFORMERS_KEY, \
    STUDY_SPECIFIC_METADATA_KEY, \
    HOSTTYPE_COL_OPTIONS_KEY, \
    SAMPLETYPE_COL_OPTIONS_KEY
from metameq.src.metadata_extender import \
    _populate_metadata_df, \
    extend_metadata_df
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestPopulateMetadataDf(ExtenderTestBase):
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
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
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
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
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
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
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
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
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
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
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


class TestExtendMetadataDf(ExtenderTestBase):
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

    def test_extend_metadata_df_with_alternate_column_names(self):
        """Test metadata extension with alternate hosttype and sampletype column names."""
        # Use alternate column names instead of hosttype_shorthand and sampletype_shorthand
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_type": ["human", "human"],
            "sample": ["stool", "stool"]
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
        # Software config specifies alternate column names
        software_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            HOSTTYPE_COL_OPTIONS_KEY: ["host_type"],
            SAMPLETYPE_COL_OPTIONS_KEY: ["sample"]
        }

        result_df, validation_msgs_df = extend_metadata_df(
            input_df, study_config, None, software_config, self.TEST_STDS_FP)

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
            # Alternate column names from input are preserved
            "host_type": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            # Alternate column names from input are preserved
            "sample": ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            # Standard internal columns added at end (in order of INTERNAL_COL_KEYS)
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)

    def test_extend_metadata_df_with_hosttype_col_name(self):
        """Test metadata extension with hosttype_col_name parameter."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_type": ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
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
            input_df, study_config, None, None, self.TEST_STDS_FP,
            hosttype_col_name="host_type")

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
            "host_type": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)

    def test_extend_metadata_df_with_sampletype_col_name(self):
        """Test metadata extension with sampletype_col_name parameter."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            "sample": ["stool", "stool"]
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
            input_df, study_config, None, None, self.TEST_STDS_FP,
            sampletype_col_name="sample")

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            "sample": ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)

    def test_extend_metadata_df_with_both_col_name_params(self):
        """Test metadata extension with both col name parameters specified."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_type": ["human", "human"],
            "sample": ["stool", "stool"]
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
            input_df, study_config, None, None, self.TEST_STDS_FP,
            hosttype_col_name="host_type", sampletype_col_name="sample")

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
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

    def test_extend_metadata_df_col_name_param_overrides_config(self):
        """Test that col name parameter takes priority over config col options."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "host_type": ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"]
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
        # Config points to a column that doesn't exist in the df
        software_config = {
            DEFAULT_KEY: "not provided",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            HOSTTYPE_COL_OPTIONS_KEY: ["nonexistent_col"]
        }

        # Parameter should override config, so "host_type" is used
        result_df, validation_msgs_df = extend_metadata_df(
            input_df, study_config, None, software_config, self.TEST_STDS_FP,
            hosttype_col_name="host_type")

        expected_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            "body_product": ["UBERON:feces", "UBERON:feces"],
            "body_site": ["gut", "gut"],
            "description": ["human sample", "human sample"],
            "host_common_name": ["human", "human"],
            "host_type": ["human", "human"],
            QIITA_SAMPLE_TYPE: ["stool", "stool"],
            SAMPLE_TYPE_KEY: ["stool", "stool"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })
        assert_frame_equal(expected_df, result_df)
        self.assertTrue(validation_msgs_df.empty)

    def test_extend_metadata_df_col_name_not_found_raises(self):
        """Test that specifying a non-existent column name raises ValueError."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            "other_col": ["value"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        study_config = {}

        with self.assertRaisesRegex(ValueError, "not found in metadata"):
            extend_metadata_df(
                input_df, study_config, None, None, self.TEST_STDS_FP,
                hosttype_col_name="nonexistent_col")

    def test_extend_metadata_df_col_name_conflicts_raises(self):
        """Test that both internal and alternate columns existing raises ValueError."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            "host_type": ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"]
        })
        study_config = {}

        with self.assertRaisesRegex(ValueError, "contains both"):
            extend_metadata_df(
                input_df, study_config, None, None, self.TEST_STDS_FP,
                hosttype_col_name="host_type")
