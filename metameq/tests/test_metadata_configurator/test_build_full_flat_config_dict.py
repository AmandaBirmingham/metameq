from metameq.src.util import \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    METADATA_FIELDS_KEY, \
    METADATA_TRANSFORMERS_KEY, \
    PRE_TRANSFORMERS_KEY, \
    POST_TRANSFORMERS_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, \
    DEFAULT_KEY, \
    TYPE_KEY, \
    ALLOWED_KEY, \
    LEAVE_REQUIREDS_BLANK_KEY, \
    OVERWRITE_NON_NANS_KEY, \
    REQUIRED_KEY, \
    SAMPLE_TYPE_KEY, \
    QIITA_SAMPLE_TYPE, \
    HOSTTYPE_COL_OPTIONS_KEY, \
    SAMPLETYPE_COL_OPTIONS_KEY, \
    STUDY_SPECIFIC_METADATA_KEY
from metameq.src.metadata_configurator import \
    build_full_flat_config_dict
from metameq.tests.test_metadata_configurator.conftest import \
    ConfiguratorTestBase


class TestBuildFullFlatConfigDict(ConfiguratorTestBase):
    def test_build_full_flat_config_dict_no_inputs(self):
        """Test build_full_flat_config_dict with no arguments uses all defaults."""
        result = build_full_flat_config_dict()

        # These tests are less specific because they depend on the actual contents
        # of the default standards file, which may change over time, so
        # we just verify the presence of key structures rather than exact contents.

        # Should have HOST_TYPE_SPECIFIC_METADATA_KEY
        self.assertIn(HOST_TYPE_SPECIFIC_METADATA_KEY, result)
        hosts_dict = result[HOST_TYPE_SPECIFIC_METADATA_KEY]
        self.assertIsInstance(hosts_dict, dict)

        # Should have "base" host type with sample_name metadata field
        self.assertIn("base", hosts_dict)
        base_host = hosts_dict["base"]
        self.assertIn(METADATA_FIELDS_KEY, base_host)
        self.assertIn("sample_name", base_host[METADATA_FIELDS_KEY])

        # Should have "human" host type with host_common_name defaulting to "human"
        self.assertIn("human", hosts_dict)
        human_host = hosts_dict["human"]
        self.assertIn(METADATA_FIELDS_KEY, human_host)
        self.assertIn("host_common_name", human_host[METADATA_FIELDS_KEY])
        self.assertEqual(
            "human",
            human_host[METADATA_FIELDS_KEY]["host_common_name"][DEFAULT_KEY])

        # Should have default software config keys with expected default value
        self.assertIn(DEFAULT_KEY, result)
        self.assertEqual("not applicable", result[DEFAULT_KEY])

    def test_build_full_flat_config_dict_with_study_config(self):
        """Test build_full_flat_config_dict with study config merges correctly.

        test_standards.yml structure: base -> host_associated -> human/mouse
        This tests that:
        1. Fields are inherited through the nesting hierarchy
        2. Study-specific fields are merged into the flattened output
        """
        software_config = {
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False
        }
        study_config = {
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

        result = build_full_flat_config_dict(
            study_config, software_config, self.TEST_STDS_FP)

        expected = {
            # Top-level keys from software_config
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            # Transformers from test_standards.yml
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "collection_date": {
                        "sources": ["collection_timestamp"],
                        "function": "transform_date_to_formatted_date"
                    }
                }
            },
            # Flattened host types from standards + study
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                # base: top level in test_standards.yml, no default
                "base": {
                    DEFAULT_KEY: "software_default",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        # sample_name defined at base level
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        # sample_type defined at base level
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    }
                },
                # host_associated: nested under base, inherits sample_name/sample_type
                "host_associated": {
                    # default defined at host_associated level
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        # description defined at host_associated level
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        # sample_name inherited from base
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        # sample_type inherited from base
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        # stool defined at host_associated level
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "body_site": {
                                    DEFAULT_KEY: "gut",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                # human: nested under host_associated
                "human": {
                    # default inherited from host_associated
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        # custom_field added from study_specific_metadata
                        "custom_field": {
                            DEFAULT_KEY: "custom_value",
                            TYPE_KEY: "string"
                        },
                        # description overrides host_associated value at human level
                        "description": {
                            DEFAULT_KEY: "human sample",
                            TYPE_KEY: "string"
                        },
                        # host_common_name defined at human level
                        "host_common_name": {
                            DEFAULT_KEY: "human",
                            TYPE_KEY: "string"
                        },
                        # sample_name inherited from base -> host_associated -> human
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        # sample_type inherited from base -> host_associated -> human
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        # blood defined only at human level
                        "blood": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:blood",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "custom_field": {
                                    DEFAULT_KEY: "custom_value",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        },
                        # stool: body_site inherited from host_associated,
                        # body_product added at human level
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:feces",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "gut",
                                    TYPE_KEY: "string"
                                },
                                "custom_field": {
                                    DEFAULT_KEY: "custom_value",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                # mouse: nested under host_associated (not in study config)
                "mouse": {
                    # default inherited from host_associated
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        # description inherited from host_associated (not overridden)
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        # host_common_name defined at mouse level
                        "host_common_name": {
                            DEFAULT_KEY: "mouse",
                            TYPE_KEY: "string"
                        },
                        # sample_name inherited from base -> host_associated -> mouse
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        # sample_type inherited from base -> host_associated -> mouse
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        # stool: body_site inherited from host_associated,
                        # cage_id added at mouse level
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "body_site": {
                                    DEFAULT_KEY: "gut",
                                    TYPE_KEY: "string"
                                },
                                "cage_id": {
                                    REQUIRED_KEY: False,
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "mouse",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        self.assertEqual(expected, result)

    def test_build_full_flat_config_dict_without_study_config(self):
        """Test build_full_flat_config_dict with no study config uses standards only.

        test_standards.yml structure: base -> host_associated -> human/mouse
        With no study config, output is pure flattened standards.
        """
        software_config = {
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False
        }

        result = build_full_flat_config_dict(
            None, software_config, self.TEST_STDS_FP)

        expected = {
            # Top-level keys from software_config
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            # Transformers from test_standards.yml
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "collection_date": {
                        "sources": ["collection_timestamp"],
                        "function": "transform_date_to_formatted_date"
                    }
                }
            },
            # No STUDY_SPECIFIC_METADATA_KEY since no study config provided
            # Flattened host types from standards only
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                # base: top level, no default, just sample_name/sample_type
                "base": {
                    DEFAULT_KEY: "software_default",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    }
                },
                # host_associated: inherits from base, adds default and description
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                # human: inherits from host_associated, overrides description
                "human": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "human sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "human",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "blood": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:blood",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        },
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:feces",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "gut",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                # mouse: inherits from host_associated, keeps parent description
                "mouse": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "mouse",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "cage_id": {
                                    REQUIRED_KEY: False,
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "mouse",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(expected, result)

    def test_build_full_flat_config_dict_study_overrides_stds_transformer(self):
        """Test that study transformer overrides standards transformer for same field."""
        software_config = {
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False
        }
        study_config = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    # Override the collection_date transformer from test_standards.yml
                    "collection_date": {
                        "sources": ["different_source"],
                        "function": "different_function",
                        "overwrite_non_nans": True
                    }
                }
            }
        }

        result = build_full_flat_config_dict(
            study_config, software_config, self.TEST_STDS_FP)

        expected = {
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            # collection_date should have study's definition, not standards'
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "collection_date": {
                        "sources": ["different_source"],
                        "function": "different_function",
                        "overwrite_non_nans": True
                    }
                }
            },
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    DEFAULT_KEY: "software_default",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    }
                },
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "human": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "human sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "human",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "blood": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:blood",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        },
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:feces",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "gut",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "mouse": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "mouse",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "cage_id": {
                                    REQUIRED_KEY: False,
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "mouse",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(expected, result)

    def test_build_full_flat_config_dict_study_adds_new_transformer(self):
        """Test that study config can add a new transformer merged with standards."""
        software_config = {
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False
        }
        study_config = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    # New transformer not in test_standards.yml
                    "study_specific_field": {
                        "sources": ["raw_field"],
                        "function": "pass_through"
                    }
                }
            }
        }

        result = build_full_flat_config_dict(
            study_config, software_config, self.TEST_STDS_FP)

        expected = {
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            # Should have both standards and study transformers merged
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    # From standards
                    "collection_date": {
                        "sources": ["collection_timestamp"],
                        "function": "transform_date_to_formatted_date"
                    },
                    # From study
                    "study_specific_field": {
                        "sources": ["raw_field"],
                        "function": "pass_through"
                    }
                }
            },
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    DEFAULT_KEY: "software_default",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    }
                },
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "human": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "human sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "human",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "blood": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:blood",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        },
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:feces",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "gut",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "mouse": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "mouse",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "cage_id": {
                                    REQUIRED_KEY: False,
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "mouse",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(expected, result)

    def test_build_full_flat_config_dict_pre_and_post_transformers(self):
        """Test combining pre_transformers from standards with post_transformers from study."""
        software_config = {
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False
        }
        study_config = {
            METADATA_TRANSFORMERS_KEY: {
                # Add post_transformers (not in test_standards.yml)
                POST_TRANSFORMERS_KEY: {
                    "final_field": {
                        "sources": ["intermediate_field"],
                        "function": "finalize_value"
                    }
                }
            }
        }

        result = build_full_flat_config_dict(
            study_config, software_config, self.TEST_STDS_FP)

        expected = {
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: False,
            METADATA_TRANSFORMERS_KEY: {
                # pre_transformers from standards
                PRE_TRANSFORMERS_KEY: {
                    "collection_date": {
                        "sources": ["collection_timestamp"],
                        "function": "transform_date_to_formatted_date"
                    }
                },
                # post_transformers from study
                POST_TRANSFORMERS_KEY: {
                    "final_field": {
                        "sources": ["intermediate_field"],
                        "function": "finalize_value"
                    }
                }
            },
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    DEFAULT_KEY: "software_default",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    }
                },
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "human": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "human sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "human",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "blood": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:blood",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        },
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:feces",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "gut",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "mouse": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "mouse",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "cage_id": {
                                    REQUIRED_KEY: False,
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "mouse",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(expected, result)

    def test_build_full_flat_config_dict_merges_software_and_study(self):
        """Test that study config values override software config values.

        Tests that top-level config keys (default, leave_requireds_blank, etc.)
        from study_config override matching keys from software_config.
        """
        software_config = {
            DEFAULT_KEY: "software_default",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: True
        }
        study_config = {
            # These override software_config values
            DEFAULT_KEY: "study_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
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

        result = build_full_flat_config_dict(
            study_config, software_config, self.TEST_STDS_FP)

        expected = {
            # default from study_config overrides software_config
            DEFAULT_KEY: "study_default",
            # leave_requireds_blank from study_config overrides software_config
            LEAVE_REQUIREDS_BLANK_KEY: True,
            # overwrite_non_nans from software_config (not overridden by study)
            OVERWRITE_NON_NANS_KEY: True,
            # Transformers from test_standards.yml
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "collection_date": {
                        "sources": ["collection_timestamp"],
                        "function": "transform_date_to_formatted_date"
                    }
                }
            },
            # Flattened host types
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    # default from study_config overrides software_config
                    DEFAULT_KEY: "study_default",
                    # leave_requireds_blank from study_config overrides software_config
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    # overwrite_non_nans from software_config (not overridden by study)
                    OVERWRITE_NON_NANS_KEY: True,
                    METADATA_FIELDS_KEY: {
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    }
                },
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    # leave_requireds_blank from study_config overrides software_config
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    # overwrite_non_nans from software_config (not overridden by study)
                    OVERWRITE_NON_NANS_KEY: True,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "human": {
                    DEFAULT_KEY: "not provided",
                    # leave_requireds_blank from study_config overrides software_config
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    # overwrite_non_nans from software_config (not overridden by study)
                    OVERWRITE_NON_NANS_KEY: True,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "human sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "human",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "blood": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:blood",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        },
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:feces",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "gut",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "mouse": {
                    DEFAULT_KEY: "not provided",
                    # leave_requireds_blank from study_config overrides software_config
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    # overwrite_non_nans from software_config (not overridden by study)
                    OVERWRITE_NON_NANS_KEY: True,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "mouse",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "cage_id": {
                                    REQUIRED_KEY: False,
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "mouse",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(expected, result)

    def test_build_full_flat_config_dict_none_software_config(self):
        """Test that None software_config loads defaults from config.yml.

        When software_config is None, the function loads defaults from the
        software's config.yml file (default="not applicable", etc.).
        """
        study_config = {
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

        result = build_full_flat_config_dict(
            study_config, None, self.TEST_STDS_FP)

        expected = {
            # Top-level keys loaded from software's config.yml defaults
            DEFAULT_KEY: "not applicable",
            LEAVE_REQUIREDS_BLANK_KEY: False,
            OVERWRITE_NON_NANS_KEY: False,
            HOSTTYPE_COL_OPTIONS_KEY: ["host_common_name"],
            SAMPLETYPE_COL_OPTIONS_KEY: ["sample_type"],
            # Transformers from test_standards.yml
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "collection_date": {
                        "sources": ["collection_timestamp"],
                        "function": "transform_date_to_formatted_date"
                    }
                }
            },
            # Flattened host types
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    DEFAULT_KEY: "not applicable",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    }
                },
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                },
                "human": {
                    DEFAULT_KEY: "not provided",
                    LEAVE_REQUIREDS_BLANK_KEY: False,
                    OVERWRITE_NON_NANS_KEY: False,
                    METADATA_FIELDS_KEY: {
                        "description": {
                            DEFAULT_KEY: "human sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "human",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "blood": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:blood",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["blood"],
                                    DEFAULT_KEY: "blood",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        },
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "body_product": {
                                    DEFAULT_KEY: "UBERON:feces",
                                    TYPE_KEY: "string"
                                },
                                "body_site": {
                                    DEFAULT_KEY: "gut",
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "human sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "human",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
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
                        "description": {
                            DEFAULT_KEY: "host associated sample",
                            TYPE_KEY: "string"
                        },
                        "host_common_name": {
                            DEFAULT_KEY: "mouse",
                            TYPE_KEY: "string"
                        },
                        "sample_name": {
                            REQUIRED_KEY: True,
                            TYPE_KEY: "string",
                            "unique": True
                        },
                        "sample_type": {
                            REQUIRED_KEY: True,
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
                                "cage_id": {
                                    REQUIRED_KEY: False,
                                    TYPE_KEY: "string"
                                },
                                "description": {
                                    DEFAULT_KEY: "host associated sample",
                                    TYPE_KEY: "string"
                                },
                                "host_common_name": {
                                    DEFAULT_KEY: "mouse",
                                    TYPE_KEY: "string"
                                },
                                QIITA_SAMPLE_TYPE: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    TYPE_KEY: "string"
                                },
                                "sample_name": {
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string",
                                    "unique": True
                                },
                                SAMPLE_TYPE_KEY: {
                                    ALLOWED_KEY: ["stool"],
                                    DEFAULT_KEY: "stool",
                                    REQUIRED_KEY: True,
                                    TYPE_KEY: "string"
                                }
                            }
                        }
                    }
                }
            }
        }

        self.assertEqual(expected, result)
