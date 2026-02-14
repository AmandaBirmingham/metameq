from metameq.src.util import \
    METADATA_FIELDS_KEY, \
    DEFAULT_KEY, \
    ALIAS_KEY, \
    ALLOWED_KEY, \
    BASE_TYPE_KEY, \
    TYPE_KEY, \
    SAMPLE_TYPE_KEY, \
    QIITA_SAMPLE_TYPE
from metameq.src.metadata_configurator import \
    _construct_sample_type_metadata_fields_dict
from metameq.tests.test_metadata_configurator.conftest import \
    ConfiguratorTestBase


class TestConstructSampleTypeMetadataFieldsDict(ConfiguratorTestBase):
    def test__construct_sample_type_metadata_fields_dict_simple(self):
        """Test combining host and sample type fields for a simple sample type."""
        host_sample_types_config_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "sample_field": {
                        DEFAULT_KEY: "sample_default"
                    }
                }
            }
        }
        host_metadata_fields_dict = {
            "host_field": {
                DEFAULT_KEY: "host_default"
            }
        }

        result = _construct_sample_type_metadata_fields_dict(
            "stool", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
            "host_field": {
                DEFAULT_KEY: "host_default"
            },
            "sample_field": {
                DEFAULT_KEY: "sample_default"
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
        self.assertDictEqual(expected, result)

    def test__construct_sample_type_metadata_fields_dict_with_alias(self):
        """Test that alias resolves to target sample type."""
        host_sample_types_config_dict = {
            "feces": {
                ALIAS_KEY: "stool"
            },
            "stool": {
                METADATA_FIELDS_KEY: {
                    "stool_field": {
                        DEFAULT_KEY: "stool_value"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _construct_sample_type_metadata_fields_dict(
            "feces", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
            "stool_field": {
                DEFAULT_KEY: "stool_value"
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
        self.assertDictEqual(expected, result)

    def test__construct_sample_type_metadata_fields_dict_chained_alias_raises(self):
        """Test that chained aliases raise ValueError."""
        host_sample_types_config_dict = {
            "feces": {
                ALIAS_KEY: "stool"
            },
            "stool": {
                ALIAS_KEY: "poop"
            },
            "poop": {
                METADATA_FIELDS_KEY: {}
            }
        }
        host_metadata_fields_dict = {}

        with self.assertRaisesRegex(ValueError, "May not chain aliases"):
            _construct_sample_type_metadata_fields_dict(
                "feces", host_sample_types_config_dict, host_metadata_fields_dict)

    def test__construct_sample_type_metadata_fields_dict_with_base_type(self):
        """Test that base type fields are inherited and overlaid."""
        host_sample_types_config_dict = {
            "base_sample": {
                METADATA_FIELDS_KEY: {
                    "base_field": {
                        DEFAULT_KEY: "base_value"
                    }
                }
            },
            "derived_sample": {
                BASE_TYPE_KEY: "base_sample",
                METADATA_FIELDS_KEY: {
                    "derived_field": {
                        DEFAULT_KEY: "derived_value"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _construct_sample_type_metadata_fields_dict(
            "derived_sample", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
            "base_field": {
                DEFAULT_KEY: "base_value"
            },
            "derived_field": {
                DEFAULT_KEY: "derived_value"
            },
            SAMPLE_TYPE_KEY: {
                ALLOWED_KEY: ["derived_sample"],
                DEFAULT_KEY: "derived_sample",
                TYPE_KEY: "string"
            },
            QIITA_SAMPLE_TYPE: {
                ALLOWED_KEY: ["derived_sample"],
                DEFAULT_KEY: "derived_sample",
                TYPE_KEY: "string"
            }
        }
        self.assertDictEqual(expected, result)

    def test__construct_sample_type_metadata_fields_dict_sets_sample_type(self):
        """Test that sample_type field is set with correct allowed/default values."""
        host_sample_types_config_dict = {
            "blood": {
                METADATA_FIELDS_KEY: {}
            }
        }
        host_metadata_fields_dict = {}

        result = _construct_sample_type_metadata_fields_dict(
            "blood", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
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
        self.assertDictEqual(expected, result)

    def test__construct_sample_type_metadata_fields_dict_preserves_existing_qiita_sample_type(self):
        """Test that existing qiita_sample_type is not overwritten."""
        host_sample_types_config_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    QIITA_SAMPLE_TYPE: {
                        ALLOWED_KEY: ["custom_type"],
                        DEFAULT_KEY: "custom_type",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _construct_sample_type_metadata_fields_dict(
            "stool", host_sample_types_config_dict, host_metadata_fields_dict)

        expected = {
            SAMPLE_TYPE_KEY: {
                ALLOWED_KEY: ["stool"],
                DEFAULT_KEY: "stool",
                TYPE_KEY: "string"
            },
            QIITA_SAMPLE_TYPE: {
                ALLOWED_KEY: ["custom_type"],
                DEFAULT_KEY: "custom_type",
                TYPE_KEY: "string"
            }
        }
        self.assertDictEqual(expected, result)
