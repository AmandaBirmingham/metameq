import pandas
from pandas.testing import assert_frame_equal
from metameq.src.util import \
    SAMPLE_NAME_KEY, \
    HOSTTYPE_SHORTHAND_KEY, \
    SAMPLETYPE_SHORTHAND_KEY, \
    QC_NOTE_KEY, \
    DEFAULT_KEY, \
    REQUIRED_KEY, \
    METADATA_FIELDS_KEY, \
    ALLOWED_KEY, \
    TYPE_KEY, \
    SAMPLE_TYPE_KEY, \
    QIITA_SAMPLE_TYPE, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, \
    OVERWRITE_NON_NANS_KEY, \
    LEAVE_REQUIREDS_BLANK_KEY, \
    LEAVE_BLANK_VAL, \
    HOST_TYPE_SPECIFIC_METADATA_KEY
from metameq.src.metadata_extender import \
    _generate_metadata_for_a_sample_type_in_a_host_type, \
    _generate_metadata_for_a_host_type, \
    _generate_metadata_for_host_types
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestGenerateMetadataForASampleTypeInAHostType(ExtenderTestBase):
    def test__generate_metadata_for_a_sample_type_in_a_host_type_basic(self):
        """Test basic metadata generation for a known sample type."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })

        # Config is pre-resolved: sample type's metadata_fields already includes
        # host fields merged in, plus sample_type and qiita_sample_type
        host_type_config_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided",
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
            input_df, "stool", host_type_config_dict)

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

        host_type_config_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {},
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {}
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_sample_type_in_a_host_type(
            input_df, "unknown_type", host_type_config_dict)

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

        host_type_config_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided",
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
            input_df, "stool", host_type_config_dict)

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

        host_type_config_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: True,
            DEFAULT_KEY: "not provided",
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
            input_df, "stool", host_type_config_dict)

        self.assertEqual(LEAVE_BLANK_VAL, result_df["required_field"].iloc[0])

    def test__generate_metadata_for_a_sample_type_in_a_host_type_leave_requireds_blank_false(self):
        """Test that required fields get default when leave_requireds_blank is False."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool"],
            QC_NOTE_KEY: [""]
        })

        host_type_config_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "global_default",
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
            input_df, "stool", host_type_config_dict)

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

        host_type_config_dict = {
            OVERWRITE_NON_NANS_KEY: True,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided",
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
            input_df, "stool", host_type_config_dict)

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

        host_type_config_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided",
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
            input_df, "stool", host_type_config_dict)

        self.assertEqual("original_value", result_df["existing_field"].iloc[0])

    def test__generate_metadata_for_a_sample_type_in_a_host_type_with_alias(self):
        """Test that sample type aliases are resolved correctly."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1"],
            HOSTTYPE_SHORTHAND_KEY: ["human"],
            SAMPLETYPE_SHORTHAND_KEY: ["feces"],
            QC_NOTE_KEY: [""]
        })

        # Config is pre-resolved: alias "feces" has its own metadata_fields
        # that is a copy of "stool"'s resolved fields with sample_type="stool"
        host_type_config_dict = {
            OVERWRITE_NON_NANS_KEY: False,
            LEAVE_REQUIREDS_BLANK_KEY: False,
            DEFAULT_KEY: "not provided",
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
            input_df, "feces", host_type_config_dict)

        self.assertEqual("stool_value", result_df["stool_field"].iloc[0])
        # sample_type should be set to the resolved type "stool"
        self.assertEqual("stool", result_df[SAMPLE_TYPE_KEY].iloc[0])


class TestGenerateMetadataForAHostType(ExtenderTestBase):
    def test__generate_metadata_for_a_host_type_basic(self):
        """Test basic metadata generation for a known host type."""
        input_df = pandas.DataFrame({
            SAMPLE_NAME_KEY: ["sample1", "sample2"],
            HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
            SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
            QC_NOTE_KEY: ["", ""]
        })

        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    DEFAULT_KEY: "human_default",
                    OVERWRITE_NON_NANS_KEY: False,
                    LEAVE_REQUIREDS_BLANK_KEY: False,
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
            input_df, "human", full_flat_config_dict)

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

        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    OVERWRITE_NON_NANS_KEY: False,
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    DEFAULT_KEY: "global_default",
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {}
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_host_type(
            input_df, "unknown_host", full_flat_config_dict)

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

        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    OVERWRITE_NON_NANS_KEY: False,
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    DEFAULT_KEY: "global_default",
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
            input_df, "human", full_flat_config_dict)

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

        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    OVERWRITE_NON_NANS_KEY: False,
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    DEFAULT_KEY: "global_default",
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
                    OVERWRITE_NON_NANS_KEY: False,
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    DEFAULT_KEY: "global_default",
                    METADATA_FIELDS_KEY: {},
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {}
                }
            }
        }

        result_df, validation_msgs = _generate_metadata_for_a_host_type(
            input_df, "human", full_flat_config_dict)

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

        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    DEFAULT_KEY: "human_specific_default",
                    OVERWRITE_NON_NANS_KEY: False,
                    LEAVE_REQUIREDS_BLANK_KEY: False,
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
            input_df, "human", full_flat_config_dict)

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
        # Config is pre-resolved: sample type's metadata_fields includes
        # host fields merged in, plus sample_type and qiita_sample_type
        full_flat_config_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "human": {
                    OVERWRITE_NON_NANS_KEY: False,
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    DEFAULT_KEY: "global_default",
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
            input_df, "human", full_flat_config_dict)

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


class TestGenerateMetadataForHostTypes(ExtenderTestBase):
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
                    DEFAULT_KEY: "global_default",
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
                    DEFAULT_KEY: "global_default",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
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
                    DEFAULT_KEY: "global_default",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
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
                    DEFAULT_KEY: "global_default",
                    LEAVE_REQUIREDS_BLANK_KEY: True,  # This causes required fields to get LEAVE_BLANK_VAL
                    OVERWRITE_NON_NANS_KEY: False,
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
