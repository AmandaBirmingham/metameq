from metameq.src.util import \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    METADATA_FIELDS_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, \
    DEFAULT_KEY, \
    TYPE_KEY, \
    ALIAS_KEY, \
    BASE_TYPE_KEY, \
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

    def test_flatten_nested_stds_dict_base_type_at_child_level(self):
        """Test when base_type is defined at the same host level as the
        child stool that references it.

        host-associated defines stool with env_biome="ancestor_biome".
        Human (child) redefines stool with base_type="generic_stool"
        plus its own stool_only_field. generic_stool is also defined at
        the human level with env_biome="base_type_biome".

        Finding: the ANCESTOR-inherited value wins. This happens because
        _combine_base_and_added_sample_type_specific_metadata merges the
        ancestor's stool fields into the child's stool entry (since both
        have metadata_fields). The ancestor's env_biome becomes part of
        stool's accumulated metadata_fields. Then when
        _construct_sample_type_metadata_fields_dict resolves the base_type,
        it starts with generic_stool's fields and overlays stool's
        accumulated fields on top--so the ancestor's env_biome overwrites
        the base_type's env_biome.
        """
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "env_biome": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "ancestor_biome",
                                    ALLOWED_KEY: ["ancestor_biome"]
                                }
                            }
                        }
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "human": {
                            METADATA_FIELDS_KEY: {
                                "human_field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "human_val"
                                }
                            },
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "generic_stool": {
                                    METADATA_FIELDS_KEY: {
                                        "env_biome": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "base_type_biome",
                                            ALLOWED_KEY: [
                                                "base_type_biome"]
                                        }
                                    }
                                },
                                "stool": {
                                    BASE_TYPE_KEY: "generic_stool",
                                    METADATA_FIELDS_KEY: {
                                        "stool_only_field": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "stool_only_val"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        expected_human = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "host_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "host_val"},
                "human_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "human_val"}
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        # KEY FIELD: env_biome comes from ANCESTOR
                        # (host-associated stool), NOT from base_type
                        # (generic_stool at human level)
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "ancestor_biome",
                            ALLOWED_KEY: ["ancestor_biome"]},
                        "stool_only_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_only_val"},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"}
                    }
                },
                "generic_stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "base_type_biome",
                            ALLOWED_KEY: ["base_type_biome"]},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.maxDiff = None
        self.assertDictEqual(expected_human, result["human"])

    def test_flatten_nested_stds_dict_base_type_at_same_level_as_ancestor_stool(self):
        """Test when base_type is defined at the same host level as the
        ancestor stool.

        Both stool and generic_stool are defined at host-associated.
        Human (child) redefines stool with base_type="generic_stool"
        plus its own stool_only_field.

        Finding: ancestor wins. The level where generic_stool is defined
        does not matter--the ancestor's stool fields still bleed through
        during the sample-type merge and overwrite the base_type's values
        during resolution.
        """
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "env_biome": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "ancestor_biome",
                                    ALLOWED_KEY: ["ancestor_biome"]
                                }
                            }
                        },
                        "generic_stool": {
                            METADATA_FIELDS_KEY: {
                                "env_biome": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "base_type_biome",
                                    ALLOWED_KEY: ["base_type_biome"]
                                }
                            }
                        }
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "human": {
                            METADATA_FIELDS_KEY: {
                                "human_field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "human_val"
                                }
                            },
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "stool": {
                                    BASE_TYPE_KEY: "generic_stool",
                                    METADATA_FIELDS_KEY: {
                                        "stool_only_field": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "stool_only_val"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        expected_human = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "host_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "host_val"},
                "human_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "human_val"}
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        # KEY FIELD: env_biome comes from ANCESTOR
                        # (host-associated stool), NOT from base_type
                        # (generic_stool at host-associated)
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "ancestor_biome",
                            ALLOWED_KEY: ["ancestor_biome"]},
                        "stool_only_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_only_val"},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"}
                    }
                },
                "generic_stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "base_type_biome",
                            ALLOWED_KEY: ["base_type_biome"]},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.maxDiff = None
        self.assertDictEqual(expected_human, result["human"])

    def test_flatten_nested_stds_dict_base_type_above_ancestor_stool(self):
        """Test when base_type is defined at a higher host level than the
        ancestor stool.

        generic_stool is at top_level, stool is at host_associated (child
        of top_level), and human (child of host_associated) redefines
        stool with base_type="generic_stool".

        Finding: ancestor wins. Even though generic_stool is defined
        higher in the hierarchy than the ancestor stool, the ancestor's
        stool fields still bleed through and overwrite the base_type's
        values.
        """
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "top_level": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        "top_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "top_val"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "generic_stool": {
                            METADATA_FIELDS_KEY: {
                                "env_biome": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "base_type_biome",
                                    ALLOWED_KEY: ["base_type_biome"]
                                }
                            }
                        }
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "host_associated": {
                            METADATA_FIELDS_KEY: {
                                "host_field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "host_val"
                                }
                            },
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "stool": {
                                    METADATA_FIELDS_KEY: {
                                        "env_biome": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY:
                                                "ancestor_biome",
                                            ALLOWED_KEY: [
                                                "ancestor_biome"]
                                        }
                                    }
                                }
                            },
                            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                                "human": {
                                    METADATA_FIELDS_KEY: {
                                        "human_field": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "human_val"
                                        }
                                    },
                                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                        "stool": {
                                            BASE_TYPE_KEY:
                                                "generic_stool",
                                            METADATA_FIELDS_KEY: {
                                                "stool_only_field": {
                                                    TYPE_KEY: "string",
                                                    DEFAULT_KEY:
                                                        "stool_only_val"
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
        }

        expected_human = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "top_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "top_val"},
                "host_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "host_val"},
                "human_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "human_val"}
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "top_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "top_val"},
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        # KEY FIELD: env_biome comes from ANCESTOR
                        # (host-associated stool), NOT from base_type
                        # (generic_stool at top_level, which is ABOVE
                        # the ancestor)
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "ancestor_biome",
                            ALLOWED_KEY: ["ancestor_biome"]},
                        "stool_only_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_only_val"},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"}
                    }
                },
                "generic_stool": {
                    METADATA_FIELDS_KEY: {
                        "top_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "top_val"},
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "base_type_biome",
                            ALLOWED_KEY: ["base_type_biome"]},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.maxDiff = None
        self.assertDictEqual(expected_human, result["human"])

    def test_flatten_nested_stds_dict_base_type_between_ancestor_and_child_stool(self):
        """Test when base_type is defined at an intermediate host level
        between the ancestor stool and the child that uses base_type.

        stool is at host_associated, generic_stool is at mammal (child
        of host_associated), and human (child of mammal) redefines stool
        with base_type="generic_stool".

        Finding: ancestor wins. The intermediate placement of
        generic_stool does not change the outcome--the ancestor's stool
        fields still bleed through and overwrite the base_type's values.
        """
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "env_biome": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "ancestor_biome",
                                    ALLOWED_KEY: ["ancestor_biome"]
                                }
                            }
                        }
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "mammal": {
                            METADATA_FIELDS_KEY: {
                                "mammal_field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "mammal_val"
                                }
                            },
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "generic_stool": {
                                    METADATA_FIELDS_KEY: {
                                        "env_biome": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY:
                                                "base_type_biome",
                                            ALLOWED_KEY: [
                                                "base_type_biome"]
                                        }
                                    }
                                }
                            },
                            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                                "human": {
                                    METADATA_FIELDS_KEY: {
                                        "human_field": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "human_val"
                                        }
                                    },
                                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                        "stool": {
                                            BASE_TYPE_KEY:
                                                "generic_stool",
                                            METADATA_FIELDS_KEY: {
                                                "stool_only_field": {
                                                    TYPE_KEY: "string",
                                                    DEFAULT_KEY:
                                                        "stool_only_val"
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
        }

        expected_human = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "host_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "host_val"},
                "mammal_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "mammal_val"},
                "human_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "human_val"}
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "mammal_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "mammal_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        # KEY FIELD: env_biome comes from ANCESTOR
                        # (host-associated stool), NOT from base_type
                        # (generic_stool at mammal, which is BETWEEN
                        # the ancestor and human)
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "ancestor_biome",
                            ALLOWED_KEY: ["ancestor_biome"]},
                        "stool_only_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_only_val"},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"}
                    }
                },
                "generic_stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "mammal_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "mammal_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "base_type_biome",
                            ALLOWED_KEY: ["base_type_biome"]},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.maxDiff = None
        self.assertDictEqual(expected_human, result["human"])

    def test_flatten_nested_stds_dict_child_stool_explicitly_sets_contested_field(self):
        """Test when the child's stool override explicitly sets the same
        field that the ancestor and base_type disagree on.

        host-associated defines stool with env_biome="ancestor_biome".
        Human defines generic_stool with env_biome="base_type_biome",
        and human's stool override has base_type="generic_stool" PLUS
        explicitly sets env_biome="human_biome".

        Finding: the human-level explicit value wins over both the
        ancestor and the base_type. During the sample-type merge,
        human's env_biome overwrites the ancestor's. Then during
        base_type resolution, human's env_biome overwrites the
        base_type's.
        """
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "env_biome": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "ancestor_biome",
                                    ALLOWED_KEY: ["ancestor_biome"]
                                }
                            }
                        }
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "human": {
                            METADATA_FIELDS_KEY: {
                                "human_field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "human_val"
                                }
                            },
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "generic_stool": {
                                    METADATA_FIELDS_KEY: {
                                        "env_biome": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY:
                                                "base_type_biome",
                                            ALLOWED_KEY: [
                                                "base_type_biome"]
                                        }
                                    }
                                },
                                "stool": {
                                    BASE_TYPE_KEY: "generic_stool",
                                    METADATA_FIELDS_KEY: {
                                        "env_biome": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY:
                                                "human_biome",
                                            ALLOWED_KEY: [
                                                "human_biome"]
                                        },
                                        "stool_only_field": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY:
                                                "stool_only_val"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        expected_human = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "host_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "host_val"},
                "human_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "human_val"}
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        # KEY FIELD: env_biome comes from HUMAN's
                        # explicit override, beating both ancestor
                        # ("ancestor_biome") and base_type
                        # ("base_type_biome")
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_biome",
                            ALLOWED_KEY: ["human_biome"]},
                        "stool_only_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_only_val"},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"}
                    }
                },
                "generic_stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "base_type_biome",
                            ALLOWED_KEY: ["base_type_biome"]},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["generic_stool"],
                            DEFAULT_KEY: "generic_stool",
                            TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.maxDiff = None
        self.assertDictEqual(expected_human, result["human"])

    def test_flatten_nested_stds_dict_child_base_ancestor_layering(self):
        """Test combination and layering of child, base, and ancestor sample types.

        This test shows that:
        (a) The priority order for a field defined in multiple places is:
                child > name ancestor > base type.
            So, for example, a field defined on human stool and host-associated
            stool and also on the human stool base type intestinal content
            would have the human stool value, because the child level takes
            precedence over both the name ancestor and the base type. But if
            the field were defined on host-associated stool and the
            human stool base type but NOT on human stool itself,
            then the host-associated stool value would win over the base type value
            even if the base type was a sibling of human stool whereas the
            name ancestor was farther up the tree.
        (b) The base type field put into a child are fully resolved with the
            base type's own ancestor fields before being merged with the child
            and the child's name ancestor fields.  So when you use a base type,
            you really are getting the whole package of its metadata fields from
            all levels of its own hierarchy, not just the fields defined directly on
            the closest definition of the base type itself.
        (c) Fields defined anywhere in the hierarchy of the name ancestors or the
            base type ancestors  that are not re-defined anywhere else
            are carried through to the child.

        """
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"
                        }
                    },
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_biome",
                        ALLOWED_KEY: ["host_biome"]
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                "env_biome": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "host_stool_biome",
                                    ALLOWED_KEY: ["host_stool_biome"]
                                },
                                "env_material": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "host_stool_material",
                                    ALLOWED_KEY: ["host_stool_material"]
                                },
                                "host-stool-only field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "host_stool_only_val"
                                }
                            }
                        },
                        "intestinal content": {
                            METADATA_FIELDS_KEY: {
                                "env_biome": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "host_intestinal_content_biome",
                                    ALLOWED_KEY: [
                                        "host_intestinal_content_biome"]
                                },
                                "env_material": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "host_intestinal_content_material",
                                    ALLOWED_KEY: ["host_intestinal_content_material"]
                                },
                                "host-intestinal-content-only field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "host_ic_only_val"
                                }
                            }
                        },
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "human": {
                            METADATA_FIELDS_KEY: {
                                "human_field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "human_val"
                                }
                            },
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "intestinal content": {
                                    METADATA_FIELDS_KEY: {
                                        "env_biome": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "human_intestinal_content_biome",
                                            ALLOWED_KEY: [
                                                "human_intestinal_content_biome"]
                                        },
                                        "env_material": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "human_intestinal_content_material",
                                            ALLOWED_KEY: ["human_intestinal_content_material"]
                                        },
                                        "human-intestinal-content-only field": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "hic_only_val"
                                        }
                                    }
                                },
                                "stool": {
                                    BASE_TYPE_KEY: "intestinal content",
                                    METADATA_FIELDS_KEY: {
                                        "env_material": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "human_stool_material",
                                            ALLOWED_KEY: ["human_stool_material"]
                                        },
                                        "human stool_only_field": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "human_stool_only_val"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        expected_human = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "host_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "host_val"},
                "human_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "human_val"}
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        # env_biome is not defined in human stool
                        # but is defined in the name ancestor
                        # (host-associated stool), the base_type
                        # (intestinial content stool at human level),
                        # and the host type (host);
                        # the name ancestor's value wins.
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_stool_biome",
                            ALLOWED_KEY: ["host_stool_biome"]},
                        # env_material is defined in human stool, and also in the
                        # ancestor and base_type, but the human stool value wins
                        # because it's an explicit override at the child level.
                        "env_material": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_stool_material",
                            ALLOWED_KEY: ["human_stool_material"]
                        },
                        # fields from the name ancestor (host-associated stool)
                        # that are not re-defined anywhere else are carried
                        # through to the name child (human stool).
                        "host-stool-only field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_stool_only_val"
                        },
                        # fields from the base type's ancestor
                        # (host-associated intestinal content)
                        # that are not re-defined anywhere else are carried
                        # through to the child with that base type (human stool).
                        "host-intestinal-content-only field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_ic_only_val"
                        },
                        # fields from the local base type (intestinal content at human),
                        # sibling of human stool,
                        # that are not re-defined anywhere else are carried
                        # through to the child with that base type (human stool).
                        "human-intestinal-content-only field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "hic_only_val"
                        },
                        # fields defined only on the child type (human stool)
                        # are of course included as well.
                        "human stool_only_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_stool_only_val"},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"}
                    }
                },
                "intestinal content": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_intestinal_content_biome",
                            ALLOWED_KEY: [
                                "human_intestinal_content_biome"]
                        },
                        "env_material": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_intestinal_content_material",
                            ALLOWED_KEY: ["human_intestinal_content_material"]
                        },
                        "host-intestinal-content-only field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_ic_only_val"
                        },
                        "human-intestinal-content-only field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "hic_only_val"
                        },
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["intestinal content"],
                            DEFAULT_KEY: "intestinal content",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["intestinal content"],
                            DEFAULT_KEY: "intestinal content",
                            TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.assertDictEqual(expected_human, result["human"])

    def test_flatten_nested_stds_dict_different_base_type_at_lower_level(self):
        """Test when a sample type has base_type at an internal level and is
        redefined with a DIFFERENT base_type at a lower level.

        host_associated defines stool with base_type="generic_stool_a".
        Human (child) redefines stool with base_type="generic_stool_b".

        Finding: the child's base_type replaces the ancestor's base_type.
        During the combine phase, both ancestor and child stool have
        metadata_fields, so they merge; the child's base_type overwrites
        the ancestor's (line 444-445 of metadata_configurator). During
        resolution, only generic_stool_b's fields are used as the base
        for human stool--generic_stool_a's fields do not contribute.
        However, the ancestor stool's directly-defined metadata_fields
        (not from its base_type, which hasn't been resolved yet at
        combine time) ARE carried through to the child.
        """
        input_dict = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "generic_stool_a": {
                            METADATA_FIELDS_KEY: {
                                "env_biome": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "biome_from_a",
                                    ALLOWED_KEY: ["biome_from_a"]
                                },
                                "field_unique_to_a": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "a_only_val"
                                }
                            }
                        },
                        "stool": {
                            BASE_TYPE_KEY: "generic_stool_a",
                            METADATA_FIELDS_KEY: {
                                "stool_specific_field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "ancestor_stool_val"
                                }
                            }
                        }
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "human": {
                            METADATA_FIELDS_KEY: {
                                "human_field": {
                                    TYPE_KEY: "string",
                                    DEFAULT_KEY: "human_val"
                                }
                            },
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "generic_stool_b": {
                                    METADATA_FIELDS_KEY: {
                                        "env_biome": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "biome_from_b",
                                            ALLOWED_KEY: ["biome_from_b"]
                                        },
                                        "field_unique_to_b": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "b_only_val"
                                        }
                                    }
                                },
                                "stool": {
                                    BASE_TYPE_KEY: "generic_stool_b",
                                    METADATA_FIELDS_KEY: {
                                        "human_stool_field": {
                                            TYPE_KEY: "string",
                                            DEFAULT_KEY: "human_stool_val"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        expected_human = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "host_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "host_val"},
                "human_field": {
                    TYPE_KEY: "string", DEFAULT_KEY: "human_val"}
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        # env_biome comes from generic_stool_b (the
                        # child's base_type), NOT generic_stool_a (the
                        # ancestor's base_type), because the child's
                        # base_type replaced the ancestor's.
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "biome_from_b",
                            ALLOWED_KEY: ["biome_from_b"]},
                        # field_unique_to_b comes from generic_stool_b
                        "field_unique_to_b": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "b_only_val"},
                        # field_unique_to_a does NOT appear because
                        # generic_stool_a is no longer the base_type
                        # for human stool.

                        # stool_specific_field from ancestor stool's
                        # directly-defined metadata_fields IS carried
                        # through (it was merged during combine phase
                        # before base_type resolution).
                        "stool_specific_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "ancestor_stool_val"},
                        "human_stool_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_stool_val"},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["stool"],
                            DEFAULT_KEY: "stool",
                            TYPE_KEY: "string"}
                    }
                },
                # generic_stool_a is inherited from host_associated
                "generic_stool_a": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "biome_from_a",
                            ALLOWED_KEY: ["biome_from_a"]},
                        "field_unique_to_a": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "a_only_val"},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["generic_stool_a"],
                            DEFAULT_KEY: "generic_stool_a",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["generic_stool_a"],
                            DEFAULT_KEY: "generic_stool_a",
                            TYPE_KEY: "string"}
                    }
                },
                # generic_stool_b is defined at the human level
                "generic_stool_b": {
                    METADATA_FIELDS_KEY: {
                        "host_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "host_val"},
                        "human_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "human_val"},
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "biome_from_b",
                            ALLOWED_KEY: ["biome_from_b"]},
                        "field_unique_to_b": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "b_only_val"},
                        SAMPLE_TYPE_KEY: {
                            ALLOWED_KEY: ["generic_stool_b"],
                            DEFAULT_KEY: "generic_stool_b",
                            TYPE_KEY: "string"},
                        QIITA_SAMPLE_TYPE: {
                            ALLOWED_KEY: ["generic_stool_b"],
                            DEFAULT_KEY: "generic_stool_b",
                            TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = flatten_nested_stds_dict(input_dict, None)

        self.maxDiff = None
        self.assertDictEqual(expected_human, result["human"])

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
