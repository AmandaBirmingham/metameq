from metameq.src.util import \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    METADATA_FIELDS_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, \
    DEFAULT_KEY, \
    TYPE_KEY, \
    ALIAS_KEY, \
    ALLOWED_KEY, \
    SAMPLE_TYPE_KEY, \
    QIITA_SAMPLE_TYPE
from metameq.src.metadata_configurator import \
    _make_combined_stds_and_study_host_type_dicts, \
    flatten_nested_stds_dict
from metameq.tests.test_metadata_configurator.conftest import \
    ConfiguratorTestBase


class TestMakeCombinedStdsAndStudyHostTypeDicts(ConfiguratorTestBase):
    def test__make_combined_stds_and_study_host_type_dicts(self):
        """Test making a combined standards and study host type dictionary."""
        out_nested_dict = _make_combined_stds_and_study_host_type_dicts(
            self.FLAT_STUDY_DICT, self.NESTED_STDS_DICT, )

        self.maxDiff = None
        self.assertDictEqual(
            self.NESTED_STDS_W_STUDY_DICT[HOST_TYPE_SPECIFIC_METADATA_KEY],
            out_nested_dict)


class TestFlattenNestedStdsDict(ConfiguratorTestBase):
    def test_flatten_nested_stds_dict(self):
        """Test flattening a nested standards dictionary."""
        out_flattened_dict = flatten_nested_stds_dict(
            self.NESTED_STDS_W_STUDY_DICT,
            None)  # , None)

        self.maxDiff = None
        self.assertDictEqual(
            self.FLATTENED_STDS_W_STUDY_DICT[HOST_TYPE_SPECIFIC_METADATA_KEY],
            out_flattened_dict)

    def test_flatten_nested_stds_dict_empty_input(self):
        """Test flattening an empty dictionary returns empty dict."""
        input_dict = {}

        result = flatten_nested_stds_dict(input_dict, None)

        self.assertDictEqual({}, result)

    def test_flatten_nested_stds_dict_empty_host_types(self):
        """Test flattening when HOST_TYPE_SPECIFIC_METADATA_KEY exists but is empty."""
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {}
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.assertDictEqual({}, result)

    def test_flatten_nested_stds_dict_single_level(self):
        """Test flattening a dictionary with only one host type level (no nesting)."""
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "host_a": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        "field1": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "value1"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "sample1": {
                            METADATA_FIELDS_KEY: {
                                "sample_field": {TYPE_KEY: "string"}
                            }
                        }
                    }
                    # No HOST_TYPE_SPECIFIC_METADATA_KEY here (no nesting)
                },
                "host_b": {
                    DEFAULT_KEY: "not collected",
                    METADATA_FIELDS_KEY: {
                        "field2": {
                            TYPE_KEY: "integer"
                        }
                    }
                }
            }
        }

        # After resolution, sample types have host metadata merged in
        # plus sample_type and qiita_sample_type fields
        expected = {
            "host_a": {
                DEFAULT_KEY: "not provided",
                METADATA_FIELDS_KEY: {
                    "field1": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "value1"
                    }
                },
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "sample1": {
                        METADATA_FIELDS_KEY: {
                            "field1": {
                                TYPE_KEY: "string",
                                DEFAULT_KEY: "value1"
                            },
                            "sample_field": {TYPE_KEY: "string"},
                            SAMPLE_TYPE_KEY: {
                                ALLOWED_KEY: ["sample1"],
                                DEFAULT_KEY: "sample1",
                                TYPE_KEY: "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                ALLOWED_KEY: ["sample1"],
                                DEFAULT_KEY: "sample1",
                                TYPE_KEY: "string"
                            }
                        }
                    }
                }
            },
            "host_b": {
                DEFAULT_KEY: "not collected",
                METADATA_FIELDS_KEY: {
                    "field2": {
                        TYPE_KEY: "integer"
                    }
                }
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.assertDictEqual(expected, result)

    def test_flatten_nested_stds_dict_deeply_nested(self):
        """Test flattening with 4 levels of host type nesting.

        Tests that metadata inheritance works correctly through multiple
        levels of nesting: level1 -> level2 -> level3 -> level4.
        """
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "host_level1": {
                    DEFAULT_KEY: "level1_default",
                    METADATA_FIELDS_KEY: {
                        "field_a": {TYPE_KEY: "string", DEFAULT_KEY: "a1"}
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "host_level2": {
                            METADATA_FIELDS_KEY: {
                                "field_b": {TYPE_KEY: "string", DEFAULT_KEY: "b2"}
                            },
                            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                                "host_level3": {
                                    DEFAULT_KEY: "level3_default",
                                    METADATA_FIELDS_KEY: {
                                        "field_c": {TYPE_KEY: "string", DEFAULT_KEY: "c3"}
                                    },
                                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                                        "host_level4": {
                                            METADATA_FIELDS_KEY: {
                                                "field_d": {TYPE_KEY: "string", DEFAULT_KEY: "d4"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        expected = {
            "host_level1": {
                DEFAULT_KEY: "level1_default",
                METADATA_FIELDS_KEY: {
                    "field_a": {TYPE_KEY: "string", DEFAULT_KEY: "a1"}
                }
            },
            "host_level2": {
                DEFAULT_KEY: "level1_default",
                METADATA_FIELDS_KEY: {
                    "field_a": {TYPE_KEY: "string", DEFAULT_KEY: "a1"},
                    "field_b": {TYPE_KEY: "string", DEFAULT_KEY: "b2"}
                }
            },
            "host_level3": {
                DEFAULT_KEY: "level3_default",
                METADATA_FIELDS_KEY: {
                    "field_a": {TYPE_KEY: "string", DEFAULT_KEY: "a1"},
                    "field_b": {TYPE_KEY: "string", DEFAULT_KEY: "b2"},
                    "field_c": {TYPE_KEY: "string", DEFAULT_KEY: "c3"}
                }
            },
            "host_level4": {
                DEFAULT_KEY: "level3_default",
                METADATA_FIELDS_KEY: {
                    "field_a": {TYPE_KEY: "string", DEFAULT_KEY: "a1"},
                    "field_b": {TYPE_KEY: "string", DEFAULT_KEY: "b2"},
                    "field_c": {TYPE_KEY: "string", DEFAULT_KEY: "c3"},
                    "field_d": {TYPE_KEY: "string", DEFAULT_KEY: "d4"}
                }
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.assertDictEqual(expected, result)

    def test_flatten_nested_stds_dict_preserves_sample_types(self):
        """Test that sample_type_specific_metadata is correctly inherited through nesting."""
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "parent_host": {
                    DEFAULT_KEY: "not provided",
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "parent_field": {TYPE_KEY: "string", DEFAULT_KEY: "parent"}
                            }
                        },
                        "fe": {
                            ALIAS_KEY: "stool"
                        }
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "child_host": {
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "stool": {
                                    METADATA_FIELDS_KEY: {
                                        "child_field": {TYPE_KEY: "string", DEFAULT_KEY: "child"}
                                    }
                                },
                                "blood": {
                                    METADATA_FIELDS_KEY: {
                                        "blood_field": {TYPE_KEY: "string"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        # After resolution, each sample type has resolved metadata_fields
        # with host metadata merged in plus sample_type and qiita_sample_type
        expected = {
            "parent_host": {
                DEFAULT_KEY: "not provided",
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            "parent_field": {TYPE_KEY: "string", DEFAULT_KEY: "parent"},
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
                    "fe": {
                        METADATA_FIELDS_KEY: {
                            "parent_field": {TYPE_KEY: "string", DEFAULT_KEY: "parent"},
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
            "child_host": {
                DEFAULT_KEY: "not provided",
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            "parent_field": {TYPE_KEY: "string", DEFAULT_KEY: "parent"},
                            "child_field": {TYPE_KEY: "string", DEFAULT_KEY: "child"},
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
                    "fe": {
                        METADATA_FIELDS_KEY: {
                            "parent_field": {TYPE_KEY: "string", DEFAULT_KEY: "parent"},
                            "child_field": {TYPE_KEY: "string", DEFAULT_KEY: "child"},
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
                            "blood_field": {TYPE_KEY: "string"},
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
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.assertDictEqual(expected, result)
