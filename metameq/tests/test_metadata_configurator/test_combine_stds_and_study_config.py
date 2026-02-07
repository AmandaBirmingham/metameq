import os.path as path
from metameq.src.util import \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    METADATA_TRANSFORMERS_KEY, \
    PRE_TRANSFORMERS_KEY, \
    TYPE_KEY, \
    DEFAULT_KEY, \
    STUDY_SPECIFIC_METADATA_KEY, \
    METADATA_FIELDS_KEY
from metameq.src.metadata_configurator import \
    combine_stds_and_study_config
from metameq.tests.test_metadata_configurator.conftest import \
    ConfiguratorTestBase


class TestCombineStdsAndStudyConfig(ConfiguratorTestBase):
    def test_combine_stds_and_study_config_empty_study(self):
        """Test combining with an empty study config dict uses only standards."""
        study_config = {}

        result = combine_stds_and_study_config(
            study_config,
            path.join(self.TEST_DIR, "data/test_config.yml"))

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

    def test_combine_stds_and_study_config_with_study_specific_metadata(self):
        """Test combining when study config has STUDY_SPECIFIC_METADATA_KEY section."""
        study_config = {
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "base": {
                        METADATA_FIELDS_KEY: {
                            "new_field": {
                                TYPE_KEY: "string",
                                DEFAULT_KEY: "study_value"
                            }
                        }
                    }
                }
            },
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "new_field_2": {
                        "sources": ["new_field"],
                        "function": "pass_through",
                    },
                    "days_since_first_day": {
                        "sources": ["days_since_first_day"],
                        "function": "pass_through",
                        "overwrite_non_nans": True
                    }
                }
            }
        }

        result = combine_stds_and_study_config(
            study_config,
            path.join(self.TEST_DIR, "data/test_config.yml"))

        expected = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "collection_date": {
                        "sources": ["collection_timestamp"],
                        "function": "pass_through",
                    },
                    # overwritten by study config
                    "days_since_first_day": {
                        "sources": ["days_since_first_day"],
                        "function": "pass_through",
                        "overwrite_non_nans": True
                    },
                    # from study config
                    "new_field_2": {
                        "sources": ["new_field"],
                        "function": "pass_through",
                    },
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
                        },
                        "new_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "study_value"
                        }
                    }
                }
            }
        }

        self.assertDictEqual(expected, result)

    def test_combine_stds_and_study_config_study_overrides_standards(self):
        """Test that study config values override standards values."""
        study_config = {
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "base": {
                        METADATA_FIELDS_KEY: {
                            "sample_type": {
                                "empty": True
                            }
                        }
                    }
                }
            }
        }

        result = combine_stds_and_study_config(
            study_config,
            path.join(self.TEST_DIR, "data/test_config.yml"))

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
                            "empty": True,
                            "is_phi": False
                        }
                    }
                }
            }
        }

        self.assertDictEqual(expected, result)

    def test_combine_stds_and_study_config_does_not_mutate_input(self):
        """Verify study_config_dict is not modified by the function."""
        study_config = {
            STUDY_SPECIFIC_METADATA_KEY: {
                HOST_TYPE_SPECIFIC_METADATA_KEY: {
                    "base": {
                        METADATA_FIELDS_KEY: {
                            "new_field": {
                                TYPE_KEY: "string",
                                DEFAULT_KEY: "study_value"
                            }
                        }
                    }
                }
            },
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "custom_transformer": {
                        "sources": ["some_field"],
                        "function": "pass_through",
                    }
                }
            }
        }

        # Make a deep copy to compare after the function call
        import copy
        study_config_original = copy.deepcopy(study_config)

        # Call the function
        combine_stds_and_study_config(
            study_config,
            path.join(self.TEST_DIR, "data/test_config.yml"))

        # Verify the input dict was not mutated
        self.assertDictEqual(
            study_config_original, study_config,
            "combine_stds_and_study_config should not mutate the input study_config_dict")
