import pandas
from metameq.src.util import \
    SAMPLE_NAME_KEY, \
    HOSTTYPE_SHORTHAND_KEY, \
    SAMPLETYPE_SHORTHAND_KEY, \
    QC_NOTE_KEY, \
    DEFAULT_KEY, \
    REQUIRED_RAW_METADATA_FIELDS, \
    METADATA_FIELDS_KEY, \
    TYPE_KEY, \
    SAMPLE_TYPE_KEY, \
    QIITA_SAMPLE_TYPE, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, \
    OVERWRITE_NON_NANS_KEY, \
    LEAVE_REQUIREDS_BLANK_KEY, \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    STUDY_SPECIFIC_METADATA_KEY, \
    HOSTTYPE_COL_OPTIONS_KEY, \
    SAMPLETYPE_COL_OPTIONS_KEY
from metameq.src.metadata_extender import \
    id_missing_cols, \
    get_reserved_cols, \
    find_standard_cols, \
    find_nonstandard_cols, \
    _get_specified_column_name
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestIdMissingCols(ExtenderTestBase):
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


class TestGetReservedCols(ExtenderTestBase):
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


class TestFindStandardCols(ExtenderTestBase):
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


class TestFindNonstandardCols(ExtenderTestBase):
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


class TestGetSpecifiedColumnName(ExtenderTestBase):
    def test__get_specified_column_name_finds_column(self):
        """Test that _get_specified_column_name finds a column that exists."""
        input_df = pandas.DataFrame({
            "sample_name": ["s1"],
            "host_type": ["human"]
        })
        config_dict = {
            HOSTTYPE_COL_OPTIONS_KEY: ["host_type", "host_common_name"]
        }
        result = _get_specified_column_name(
            HOSTTYPE_COL_OPTIONS_KEY, input_df, config_dict)
        self.assertEqual("host_type", result)

    def test__get_specified_column_name_returns_first_match(self):
        """Test that _get_specified_column_name returns the first match when multiple options exist."""
        input_df = pandas.DataFrame({
            "sample_name": ["s1"],
            "host_type": ["human"],
            "host_common_name": ["human"]
        })
        config_dict = {
            HOSTTYPE_COL_OPTIONS_KEY: ["host_type", "host_common_name"]
        }
        result = _get_specified_column_name(
            HOSTTYPE_COL_OPTIONS_KEY, input_df, config_dict)
        self.assertEqual("host_type", result)

    def test__get_specified_column_name_returns_none_when_no_match(self):
        """Test that _get_specified_column_name returns None when no options match."""
        input_df = pandas.DataFrame({
            "sample_name": ["s1"],
            "other_column": ["value"]
        })
        config_dict = {
            HOSTTYPE_COL_OPTIONS_KEY: ["host_type", "host_common_name"]
        }
        result = _get_specified_column_name(
            HOSTTYPE_COL_OPTIONS_KEY, input_df, config_dict)
        self.assertIsNone(result)

    def test__get_specified_column_name_returns_none_when_key_missing(self):
        """Test that _get_specified_column_name returns None when col_options_key is not in config."""
        input_df = pandas.DataFrame({
            "sample_name": ["s1"],
            "host_type": ["human"]
        })
        config_dict = {}
        result = _get_specified_column_name(
            HOSTTYPE_COL_OPTIONS_KEY, input_df, config_dict)
        self.assertIsNone(result)

    def test__get_specified_column_name_returns_none_when_options_empty(self):
        """Test that _get_specified_column_name returns None when col_options is empty list."""
        input_df = pandas.DataFrame({
            "sample_name": ["s1"],
            "host_type": ["human"]
        })
        config_dict = {
            HOSTTYPE_COL_OPTIONS_KEY: []
        }
        result = _get_specified_column_name(
            HOSTTYPE_COL_OPTIONS_KEY, input_df, config_dict)
        self.assertIsNone(result)

    def test__get_specified_column_name_with_sampletype_key(self):
        """Test that _get_specified_column_name works with sampletype column options."""
        input_df = pandas.DataFrame({
            "sample_name": ["s1"],
            "sample_type": ["stool"]
        })
        config_dict = {
            SAMPLETYPE_COL_OPTIONS_KEY: ["sample_type", "sampletype"]
        }
        result = _get_specified_column_name(
            SAMPLETYPE_COL_OPTIONS_KEY, input_df, config_dict)
        self.assertEqual("sample_type", result)
    # endregion _get_specified_column_name tests
