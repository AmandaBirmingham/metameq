from metameq.src.util import \
    METADATA_FIELDS_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, \
    DEFAULT_KEY, \
    TYPE_KEY, \
    ALIAS_KEY, \
    ALLOWED_KEY, \
    BASE_TYPE_KEY
from metameq.src.metadata_configurator import \
    _apply_host_overrides_to_inherited_sample_types, \
    _combine_base_and_added_metadata_fields, \
    _combine_base_and_added_host_type, \
    _combine_base_and_added_sample_type_specific_metadata
from metameq.tests.test_metadata_configurator.conftest import \
    ConfiguratorTestBase


class TestCombineBaseAndAddedMetadataFields(ConfiguratorTestBase):
    def test__combine_base_and_added_metadata_fields(self):
        """Test combining base and additional metadata fields."""
        base_dict = {
            METADATA_FIELDS_KEY: {
                # in both, add wins
                "field1": {
                    "allowed": ["value1"],
                    "type": "string"
                },
                # in base only
                "fieldX": {
                    "type": "string",
                    "allowed": ["valueX"]
                }
            }
        }

        add_dict = {
            # in both, add wins
            METADATA_FIELDS_KEY: {
                "field1": {
                    "allowed": ["value2"],
                    "type": "string"
                },
                # in add only
                "field2": {
                    "type": "string"
                }
            }
        }

        expected = {
            "field1": {
                "allowed": ["value2"],
                "type": "string"
            },
            "field2": {
                "type": "string"
            },
            "fieldX": {
                "type": "string",
                "allowed": ["valueX"]
            }
        }

        result = _combine_base_and_added_metadata_fields(base_dict, add_dict)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_metadata_fields_empty_base(self):
        """Test combining when base_dict has no metadata_fields key."""
        base_dict = {}

        add_dict = {
            METADATA_FIELDS_KEY: {
                "field1": {TYPE_KEY: "string", DEFAULT_KEY: "value1"}
            }
        }

        expected = add_dict[METADATA_FIELDS_KEY]

        result = _combine_base_and_added_metadata_fields(base_dict, add_dict)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_metadata_fields_empty_add(self):
        """Test combining when add_dict has no metadata_fields key."""
        base_dict = {
            METADATA_FIELDS_KEY: {
                "field1": {TYPE_KEY: "string", DEFAULT_KEY: "value1"}
            }
        }

        add_dict = {}

        expected = base_dict[METADATA_FIELDS_KEY]

        result = _combine_base_and_added_metadata_fields(base_dict, add_dict)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_metadata_fields_both_empty(self):
        """Test combining when both dicts have no metadata_fields key."""
        base_dict = {}
        add_dict = {}

        expected = {}

        result = _combine_base_and_added_metadata_fields(base_dict, add_dict)
        self.assertDictEqual(expected, result)


class TestCombineBaseAndAddedHostType(ConfiguratorTestBase):
    def test__combine_base_and_added_host_type_default_key_override(self):
        """Test that DEFAULT_KEY from add_dict overwrites DEFAULT_KEY from base_dict."""
        base_dict = {
            DEFAULT_KEY: "not provided"
        }
        add_dict = {
            DEFAULT_KEY: "not collected"
        }

        result = _combine_base_and_added_host_type(base_dict, add_dict)

        self.assertEqual("not collected", result[DEFAULT_KEY])

    def test__combine_base_and_added_host_type_default_key_preserved(self):
        """Test that DEFAULT_KEY from base_dict is preserved when add_dict has none."""
        base_dict = {
            DEFAULT_KEY: "not provided"
        }
        add_dict = {}

        result = _combine_base_and_added_host_type(base_dict, add_dict)

        self.assertEqual("not provided", result[DEFAULT_KEY])

    def test__combine_base_and_added_host_type_default_key_added(self):
        """Test that DEFAULT_KEY from add_dict is added when base_dict has none."""
        base_dict = {}
        add_dict = {
            DEFAULT_KEY: "not collected"
        }

        result = _combine_base_and_added_host_type(base_dict, add_dict)

        self.assertEqual("not collected", result[DEFAULT_KEY])

    def test__combine_base_and_added_host_type_empty_base(self):
        """Test combining when base_dict is empty."""
        base_dict = {}
        add_dict = {
            DEFAULT_KEY: "not collected",
            METADATA_FIELDS_KEY: {
                "field1": {TYPE_KEY: "string"}
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "description": {TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = _combine_base_and_added_host_type(base_dict, add_dict)

        self.assertDictEqual(add_dict, result)

    def test__combine_base_and_added_host_type_empty_add(self):
        """Test combining when add_dict is empty (result should match base)."""
        base_dict = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "field1": {TYPE_KEY: "string", DEFAULT_KEY: "value1"}
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "description": {TYPE_KEY: "string"}
                    }
                }
            }
        }
        add_dict = {}

        result = _combine_base_and_added_host_type(base_dict, add_dict)

        self.assertDictEqual(base_dict, result)

    def test__combine_base_and_added_host_type_both_empty(self):
        """Test combining when both base_dict and add_dict are empty."""
        base_dict = {}
        add_dict = {}

        result = _combine_base_and_added_host_type(base_dict, add_dict)

        self.assertDictEqual({}, result)

    def test__combine_base_and_added_host_type_full_combination(self):
        """Test full combination with all components: DEFAULT_KEY, metadata_fields, and sample_types."""
        base_dict = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "country": {
                    TYPE_KEY: "string",
                    ALLOWED_KEY: ["USA"],
                    DEFAULT_KEY: "USA"
                },
                "description": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "base description"
                }
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "location": {TYPE_KEY: "string", DEFAULT_KEY: "UCSD"}
                    }
                },
                "fe": {
                    ALIAS_KEY: "stool"
                }
            }
        }
        add_dict = {
            DEFAULT_KEY: "not collected",
            METADATA_FIELDS_KEY: {
                # Override existing field
                "description": {
                    DEFAULT_KEY: "add description"
                },
                # Add new field
                "new_field": {
                    TYPE_KEY: "integer"
                }
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                # Override existing sample type
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "location": {DEFAULT_KEY: "UCLA"}
                    }
                },
                # Add new sample type
                "blood": {
                    METADATA_FIELDS_KEY: {
                        "volume": {TYPE_KEY: "number"}
                    }
                }
            }
        }

        expected = {
            # DEFAULT_KEY overwritten by add
            DEFAULT_KEY: "not collected",
            METADATA_FIELDS_KEY: {
                # Preserved from base
                "country": {
                    TYPE_KEY: "string",
                    ALLOWED_KEY: ["USA"],
                    DEFAULT_KEY: "USA"
                },
                # Combined: base type preserved, add default overwrites
                "description": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "add description"
                },
                # New from add
                "new_field": {
                    TYPE_KEY: "integer"
                }
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                # Combined: base type preserved, add default overwrites
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "location": {TYPE_KEY: "string", DEFAULT_KEY: "UCLA"}
                    }
                },
                # Preserved from base
                "fe": {
                    ALIAS_KEY: "stool"
                },
                # New from add
                "blood": {
                    METADATA_FIELDS_KEY: {
                        "volume": {TYPE_KEY: "number"}
                    }
                }
            }
        }

        result = _combine_base_and_added_host_type(base_dict, add_dict)

        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_host_type_empty_metadata_fields_result(self):
        """Test that METADATA_FIELDS_KEY is not included when result would be empty."""
        base_dict = {
            DEFAULT_KEY: "not provided"
            # No METADATA_FIELDS_KEY
        }
        add_dict = {
            # No METADATA_FIELDS_KEY
        }

        result = _combine_base_and_added_host_type(base_dict, add_dict)

        self.assertEqual("not provided", result[DEFAULT_KEY])
        self.assertNotIn(METADATA_FIELDS_KEY, result)

    def test__combine_base_and_added_host_type_empty_sample_types_result(self):
        """Test that SAMPLE_TYPE_SPECIFIC_METADATA_KEY is not included when result would be empty."""
        base_dict = {
            DEFAULT_KEY: "not provided",
            METADATA_FIELDS_KEY: {
                "field1": {TYPE_KEY: "string"}
            }
            # No SAMPLE_TYPE_SPECIFIC_METADATA_KEY
        }
        add_dict = {
            # No SAMPLE_TYPE_SPECIFIC_METADATA_KEY
        }

        result = _combine_base_and_added_host_type(base_dict, add_dict)

        self.assertEqual("not provided", result[DEFAULT_KEY])
        self.assertIn(METADATA_FIELDS_KEY, result)
        self.assertNotIn(SAMPLE_TYPE_SPECIFIC_METADATA_KEY, result)


class TestCombineBaseAndAddedSampleTypeSpecificMetadata(ConfiguratorTestBase):
    def test__combine_base_and_added_sample_type_specific_metadata(self):
        """Test combining base and additional sample type specific metadata."""
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                # defined in stds w metadata fields but in add as an alias
                "sample_type1": {
                    METADATA_FIELDS_KEY: {
                        "confuse": {
                            "allowed": ["value1"],
                            "type": "string"
                        },
                    }
                },
                # defined in both w metadata fields, must combine, add wins
                "sample_type2": {
                    METADATA_FIELDS_KEY: {
                        "field1": {
                            "type": "string"
                        },
                        "fieldX": {
                            "type": "string",
                            "allowed": ["valueX"]
                        }
                    }
                },
                # defined only in base
                "sample_type4": {
                    METADATA_FIELDS_KEY: {
                        "field1": {
                            "type": "string"
                        }
                    }
                }
            }
        }

        add_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                # defined here as an alias, defined in stds w metadata fields
                "sample_type1": {
                    "alias": "sample_type2"
                },
                # defined in both w metadata fields, must combine, add wins
                "sample_type2": {
                    METADATA_FIELDS_KEY: {
                        "field1": {
                            "allowed": ["value1"],
                            "type": "string"
                        },
                        "field2": {
                            "type": "string"
                        }
                    }
                },
                # defined only in add
                "sample_type3": {
                    "base_type": "sample_type2"
                }
            }
        }

        expected = {
            "sample_type1": {
                "alias": "sample_type2"
            },
            "sample_type2": {
                METADATA_FIELDS_KEY: {
                    "field1": {
                        "allowed": ["value1"],
                        "type": "string"
                    },
                    "field2": {
                        "type": "string"
                    },
                    "fieldX": {
                        "type": "string",
                        "allowed": ["valueX"]
                    }
                }
            },
            "sample_type3": {
                "base_type": "sample_type2"
            },
            "sample_type4": {
                METADATA_FIELDS_KEY: {
                    "field1": {
                        "type": "string"
                    }
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(base_dict, add_dict)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_empty_base(self):
        """Test combining when base has no sample_type_specific_metadata."""
        base_dict = {}

        add_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "field1": {"type": "string"}
                    }
                }
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "field1": {"type": "string"}
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(base_dict, add_dict)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_empty_add(self):
        """Test combining when add has no sample_type_specific_metadata."""
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "field1": {"type": "string"}
                    }
                }
            }
        }

        add_dict = {}

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "field1": {"type": "string"}
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(base_dict, add_dict)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_base_type_with_metadata(self):
        """Test sample type with both base_type AND metadata_fields.

        This is a valid configuration where base_type indicates inheritance and
        metadata_fields contains overrides. If both base_dict and add_dict have
        base_type for the same sample type, add_dict's base_type overwrites base_dict's.
        The metadata_fields are combined as usual (add wins for overlapping fields).
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    BASE_TYPE_KEY: "original_base",
                    METADATA_FIELDS_KEY: {
                        "description": {
                            "allowed": ["stool sample"],
                            "type": "string"
                        },
                        "location": {
                            "allowed": ["UCSD"],
                            "type": "string"
                        }
                    }
                }
            }
        }

        add_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    BASE_TYPE_KEY: "new_base",
                    METADATA_FIELDS_KEY: {
                        "description": {
                            "allowed": ["human stool"],
                            "type": "string"
                        }
                    }
                }
            }
        }

        expected = {
            "stool": {
                # base_type from add_dict overwrites base_type from base_dict
                BASE_TYPE_KEY: "new_base",
                METADATA_FIELDS_KEY: {
                    # description from add_dict overwrites base_dict
                    "description": {
                        "allowed": ["human stool"],
                        "type": "string"
                    },
                    # location preserved from base_dict (not in add_dict)
                    "location": {
                        "allowed": ["UCSD"],
                        "type": "string"
                    }
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(base_dict, add_dict)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_mismatched_types_add_wins(self):
        """Test that when definition types differ between base and add, add always wins.

        When the sample type definition type (alias, base_type, or metadata_fields)
        differs between base_dict and add_dict, the add_dict entry completely
        replaces the base_dict entry rather than attempting to combine them.

        This test covers all possible type mismatch scenarios:
        - base has alias, add has metadata_fields
        - base has alias, add has base_type
        - base has metadata_fields, add has alias
        - base has metadata_fields, add has base_type
        - base has base_type, add has alias
        - base has base_type, add has metadata_fields
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                # alias -> metadata_fields
                "sample_alias_to_metadata": {
                    ALIAS_KEY: "stool"
                },
                # alias -> base_type
                "sample_alias_to_base": {
                    ALIAS_KEY: "stool"
                },
                # metadata_fields -> alias
                "sample_metadata_to_alias": {
                    METADATA_FIELDS_KEY: {
                        "field1": {"type": "string"}
                    }
                },
                # metadata_fields -> base_type
                "sample_metadata_to_base": {
                    METADATA_FIELDS_KEY: {
                        "field1": {"type": "string"}
                    }
                },
                # base_type -> alias
                "sample_base_to_alias": {
                    BASE_TYPE_KEY: "stool"
                },
                # base_type -> metadata_fields
                "sample_base_to_metadata": {
                    BASE_TYPE_KEY: "stool"
                }
            }
        }

        add_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "sample_alias_to_metadata": {
                    METADATA_FIELDS_KEY: {
                        "new_field": {"type": "integer"}
                    }
                },
                "sample_alias_to_base": {
                    BASE_TYPE_KEY: "saliva"
                },
                "sample_metadata_to_alias": {
                    ALIAS_KEY: "saliva"
                },
                "sample_metadata_to_base": {
                    BASE_TYPE_KEY: "saliva"
                },
                "sample_base_to_alias": {
                    ALIAS_KEY: "saliva"
                },
                "sample_base_to_metadata": {
                    METADATA_FIELDS_KEY: {
                        "new_field": {"type": "integer"}
                    }
                }
            }
        }

        # All entries should match add_dict exactly; base_dict is replaced
        expected = add_dict[SAMPLE_TYPE_SPECIFIC_METADATA_KEY]

        result = _combine_base_and_added_sample_type_specific_metadata(base_dict, add_dict)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_overrides_alias_entry_preserved(self):
        """Test that when current_host_overrides_ancestor_sample_type=True,
        alias entries in the base are left unchanged while sample types
        with metadata_fields get host-level fields layered on top
        (overriding on overlap).

        The alias will inherit the host fields later when it is resolved
        to its target (which already has the host fields applied).
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "ancestor_biome",
                            ALLOWED_KEY: ["ancestor_biome"]
                        },
                        "stool_only_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_only_val"
                        }
                    }
                },
                "fe": {
                    ALIAS_KEY: "stool"
                }
            }
        }

        add_dict = {
            METADATA_FIELDS_KEY: {
                # overlaps with stool's env_biome; host should win
                "env_biome": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "host_biome",
                    ALLOWED_KEY: ["host_biome"]
                }
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    # host value overrides ancestor sample type value
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_biome",
                        ALLOWED_KEY: ["host_biome"]
                    },
                    # non-overlapping field preserved from base
                    "stool_only_field": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "stool_only_val"
                    }
                }
            },
            # alias entry is unchanged -- no METADATA_FIELDS_KEY added
            "fe": {
                ALIAS_KEY: "stool"
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(
            base_dict, add_dict,
            current_host_overrides_ancestor_sample_type=True)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_overrides_base_type_only_entry_preserved(self):
        """Test that when current_host_overrides_ancestor_sample_type=True,
        base_type-only entries (no metadata_fields) in the base are left
        unchanged while sample types with metadata_fields get host-level
        fields layered on top (overriding on overlap).

        The base_type entry will inherit the host fields later when it is
        resolved to its target (which already has the host fields applied).
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "generic_stool": {
                    METADATA_FIELDS_KEY: {
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "base_biome",
                            ALLOWED_KEY: ["base_biome"]
                        },
                        "generic_only_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "generic_only_val"
                        }
                    }
                },
                "special_stool": {
                    BASE_TYPE_KEY: "generic_stool"
                }
            }
        }

        add_dict = {
            METADATA_FIELDS_KEY: {
                # overlaps with generic_stool's env_biome; host should win
                "env_biome": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "host_biome",
                    ALLOWED_KEY: ["host_biome"]
                }
            }
        }

        expected = {
            "generic_stool": {
                METADATA_FIELDS_KEY: {
                    # host value overrides base sample type value
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_biome",
                        ALLOWED_KEY: ["host_biome"]
                    },
                    # non-overlapping field preserved from base
                    "generic_only_field": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "generic_only_val"
                    }
                }
            },
            # base_type-only entry is unchanged -- no METADATA_FIELDS_KEY added
            "special_stool": {
                BASE_TYPE_KEY: "generic_stool"
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(
            base_dict, add_dict,
            current_host_overrides_ancestor_sample_type=True)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_overrides_host_fields_override(self):
        """Test core overriding behavior: when
        current_host_overrides_ancestor_sample_type=True, the add_dict's
        host-level metadata_fields override the base_dict's
        sample-type-level metadata_fields where they overlap.
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "description": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "ancestor_stool_desc",
                            ALLOWED_KEY: ["ancestor_stool_desc"]
                        }
                    }
                }
            }
        }

        add_dict = {
            METADATA_FIELDS_KEY: {
                "description": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "host_desc",
                    ALLOWED_KEY: ["host_desc"]
                }
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    # host value wins over ancestor sample type value
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_desc",
                        ALLOWED_KEY: ["host_desc"]
                    }
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(
            base_dict, add_dict,
            current_host_overrides_ancestor_sample_type=True)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_overrides_no_overlap_merges_both(self):
        """Test that when there is no field overlap between the host
        metadata and the ancestor sample type metadata, both are present
        in the result.
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_biome"
                        }
                    }
                }
            }
        }

        add_dict = {
            METADATA_FIELDS_KEY: {
                "host_only_field": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "host_val"
                }
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "stool_biome"
                    },
                    "host_only_field": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_val"
                    }
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(
            base_dict, add_dict,
            current_host_overrides_ancestor_sample_type=True)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_overrides_applies_to_all_base_sample_types(self):
        """Test that the host-level metadata fields are layered into
        every base sample type that has metadata_fields, not just one.
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "description": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_desc"
                        }
                    }
                },
                "urine": {
                    METADATA_FIELDS_KEY: {
                        "description": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "urine_desc"
                        }
                    }
                }
            }
        }

        add_dict = {
            METADATA_FIELDS_KEY: {
                "description": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "host_desc"
                }
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_desc"
                    }
                }
            },
            "urine": {
                METADATA_FIELDS_KEY: {
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_desc"
                    }
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(
            base_dict, add_dict,
            current_host_overrides_ancestor_sample_type=True)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_overrides_empty_base_sample_types(self):
        """Test that when the base has no sample_type_specific_metadata,
        the overriding loop has nothing to iterate and the result is empty.
        """
        base_dict = {}

        add_dict = {
            METADATA_FIELDS_KEY: {
                "host_field": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "host_val"
                }
            }
        }

        expected = {}

        result = _combine_base_and_added_sample_type_specific_metadata(
            base_dict, add_dict,
            current_host_overrides_ancestor_sample_type=True)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_overrides_empty_add_metadata_fields(self):
        """Test that when add_dict has no host-level metadata_fields,
        the base sample types' metadata remain unchanged.
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "env_biome": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_biome"
                        }
                    }
                }
            }
        }

        add_dict = {}

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "stool_biome"
                    }
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(
            base_dict, add_dict,
            current_host_overrides_ancestor_sample_type=True)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_overrides_combined_with_add_sample_types(self):
        """Test that both the overriding behavior (for inherited base sample
        types) and the existing combining logic (for sample types in the
        add dict) work correctly together.

        Additionally, when a field appears in both the host-level metadata
        and in an add-dict sample type's metadata, the sample-type-specific
        value wins. The overriding step layers host fields onto inherited
        sample types first, then the existing combine logic layers the add
        dict's sample-type fields on top of that.
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "description": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "ancestor_val"
                        },
                        "base_only_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "base_only_val"
                        }
                    }
                }
            }
        }

        add_dict = {
            METADATA_FIELDS_KEY: {
                # overlaps with ancestor stool's description
                "description": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "host_val"
                }
            },
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        # overlaps with host-level description;
                        # add sample type should win over host
                        "description": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "add_sample_type_val"
                        },
                        "stool_field": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "stool_add_val"
                        }
                    }
                },
                # new sample type only in add
                "blood": {
                    METADATA_FIELDS_KEY: {
                        "volume": {TYPE_KEY: "number"}
                    }
                }
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    # add sample type value beats host value:
                    # overriding put "host_val" on inherited stool, then
                    # existing combine layered "add_sample_type_val" on top
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "add_sample_type_val"
                    },
                    # preserved from base (no overlap)
                    "base_only_field": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "base_only_val"
                    },
                    # new from add sample type
                    "stool_field": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "stool_add_val"
                    }
                }
            },
            # new sample type from add, as-is
            "blood": {
                METADATA_FIELDS_KEY: {
                    "volume": {TYPE_KEY: "number"}
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(
            base_dict, add_dict,
            current_host_overrides_ancestor_sample_type=True)
        self.assertDictEqual(expected, result)

    def test__combine_base_and_added_sample_type_specific_metadata_overrides_false_preserves_existing_behavior(self):
        """Test that when current_host_overrides_ancestor_sample_type=False
        (the default), the add_dict's host-level metadata does NOT
        override inherited sample type metadata. This is a regression
        guard: the same scenario as the override test, but with False,
        confirms the opposite outcome.
        """
        base_dict = {
            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                "stool": {
                    METADATA_FIELDS_KEY: {
                        "description": {
                            TYPE_KEY: "string",
                            DEFAULT_KEY: "ancestor_stool_desc",
                            ALLOWED_KEY: ["ancestor_stool_desc"]
                        }
                    }
                }
            }
        }

        add_dict = {
            METADATA_FIELDS_KEY: {
                "description": {
                    TYPE_KEY: "string",
                    DEFAULT_KEY: "host_desc",
                    ALLOWED_KEY: ["host_desc"]
                }
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    # ancestor sample type value preserved; host does NOT override
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "ancestor_stool_desc",
                        ALLOWED_KEY: ["ancestor_stool_desc"]
                    }
                }
            }
        }

        result = _combine_base_and_added_sample_type_specific_metadata(
            base_dict, add_dict,
            current_host_overrides_ancestor_sample_type=False)
        self.assertDictEqual(expected, result)


class TestApplyHostOverridesToInheritedSampleTypes(ConfiguratorTestBase):
    def test__apply_host_overrides_to_inherited_sample_types_host_fields_override(self):
        """Test that host metadata fields override sample type metadata
        fields where they overlap.
        """
        wip_sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "ancestor_stool_desc",
                        ALLOWED_KEY: ["ancestor_stool_desc"]
                    }
                }
            }
        }

        host_metadata_fields_dict = {
            "description": {
                TYPE_KEY: "string",
                DEFAULT_KEY: "host_desc",
                ALLOWED_KEY: ["host_desc"]
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_desc",
                        ALLOWED_KEY: ["host_desc"]
                    }
                }
            }
        }

        result = _apply_host_overrides_to_inherited_sample_types(
            wip_sample_types_dict, host_metadata_fields_dict)
        self.assertDictEqual(expected, result)

    def test__apply_host_overrides_to_inherited_sample_types_no_overlap_merges_both(self):
        """Test that when there is no field overlap, both host and sample
        type fields are present in the result.
        """
        wip_sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "stool_biome"
                    }
                }
            }
        }

        host_metadata_fields_dict = {
            "host_only_field": {
                TYPE_KEY: "string",
                DEFAULT_KEY: "host_val"
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "stool_biome"
                    },
                    "host_only_field": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_val"
                    }
                }
            }
        }

        result = _apply_host_overrides_to_inherited_sample_types(
            wip_sample_types_dict, host_metadata_fields_dict)
        self.assertDictEqual(expected, result)

    def test__apply_host_overrides_to_inherited_sample_types_applies_to_all(self):
        """Test that host metadata fields are layered into every sample
        type that has metadata_fields.
        """
        wip_sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "stool_desc"
                    }
                }
            },
            "urine": {
                METADATA_FIELDS_KEY: {
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "urine_desc"
                    }
                }
            }
        }

        host_metadata_fields_dict = {
            "description": {
                TYPE_KEY: "string",
                DEFAULT_KEY: "host_desc"
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_desc"
                    }
                }
            },
            "urine": {
                METADATA_FIELDS_KEY: {
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_desc"
                    }
                }
            }
        }

        result = _apply_host_overrides_to_inherited_sample_types(
            wip_sample_types_dict, host_metadata_fields_dict)
        self.assertDictEqual(expected, result)

    def test__apply_host_overrides_to_inherited_sample_types_empty_wip(self):
        """Test that when the wip dictionary is empty, the result is empty."""
        wip_sample_types_dict = {}

        host_metadata_fields_dict = {
            "host_field": {
                TYPE_KEY: "string",
                DEFAULT_KEY: "host_val"
            }
        }

        result = _apply_host_overrides_to_inherited_sample_types(
            wip_sample_types_dict, host_metadata_fields_dict)
        self.assertDictEqual({}, result)

    def test__apply_host_overrides_to_inherited_sample_types_empty_host_fields(self):
        """Test that when host metadata fields dict is empty, sample types
        remain unchanged.
        """
        wip_sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "stool_biome"
                    }
                }
            }
        }

        host_metadata_fields_dict = {}

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "stool_biome"
                    }
                }
            }
        }

        result = _apply_host_overrides_to_inherited_sample_types(
            wip_sample_types_dict, host_metadata_fields_dict)
        self.assertDictEqual(expected, result)

    def test__apply_host_overrides_to_inherited_sample_types_alias_skipped(self):
        """Test that alias entries are left unchanged; only sample types
        with metadata_fields get host fields layered on.
        """
        wip_sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "ancestor_biome",
                        ALLOWED_KEY: ["ancestor_biome"]
                    }
                }
            },
            "fe": {
                ALIAS_KEY: "stool"
            }
        }

        host_metadata_fields_dict = {
            "env_biome": {
                TYPE_KEY: "string",
                DEFAULT_KEY: "host_biome",
                ALLOWED_KEY: ["host_biome"]
            }
        }

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_biome",
                        ALLOWED_KEY: ["host_biome"]
                    }
                }
            },
            "fe": {
                ALIAS_KEY: "stool"
            }
        }

        result = _apply_host_overrides_to_inherited_sample_types(
            wip_sample_types_dict, host_metadata_fields_dict)
        self.assertDictEqual(expected, result)

    def test__apply_host_overrides_to_inherited_sample_types_base_type_only_skipped(self):
        """Test that base_type-only entries (no metadata_fields) are left
        unchanged; only sample types with metadata_fields get host fields
        layered on.
        """
        wip_sample_types_dict = {
            "generic_stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "base_biome",
                        ALLOWED_KEY: ["base_biome"]
                    }
                }
            },
            "special_stool": {
                BASE_TYPE_KEY: "generic_stool"
            }
        }

        host_metadata_fields_dict = {
            "env_biome": {
                TYPE_KEY: "string",
                DEFAULT_KEY: "host_biome",
                ALLOWED_KEY: ["host_biome"]
            }
        }

        expected = {
            "generic_stool": {
                METADATA_FIELDS_KEY: {
                    "env_biome": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "host_biome",
                        ALLOWED_KEY: ["host_biome"]
                    }
                }
            },
            "special_stool": {
                BASE_TYPE_KEY: "generic_stool"
            }
        }

        result = _apply_host_overrides_to_inherited_sample_types(
            wip_sample_types_dict, host_metadata_fields_dict)
        self.assertDictEqual(expected, result)

    def test__apply_host_overrides_to_inherited_sample_types_does_not_modify_input(self):
        """Test that the input wip_sample_types_dict is not modified."""
        wip_sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "description": {
                        TYPE_KEY: "string",
                        DEFAULT_KEY: "original_val"
                    }
                }
            }
        }

        host_metadata_fields_dict = {
            "description": {
                TYPE_KEY: "string",
                DEFAULT_KEY: "host_val"
            }
        }

        _apply_host_overrides_to_inherited_sample_types(
            wip_sample_types_dict, host_metadata_fields_dict)

        # input should be unchanged
        self.assertEqual(
            "original_val",
            wip_sample_types_dict["stool"][METADATA_FIELDS_KEY]
            ["description"][DEFAULT_KEY])
