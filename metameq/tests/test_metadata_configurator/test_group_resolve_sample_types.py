from metameq.src.util import \
    METADATA_FIELDS_KEY, \
    DEFAULT_KEY, \
    TYPE_KEY, \
    ALIAS_KEY, \
    ALLOWED_KEY, \
    BASE_TYPE_KEY, \
    SAMPLE_TYPE_KEY, \
    QIITA_SAMPLE_TYPE
from metameq.src.metadata_configurator import \
    _id_sample_type_definition, \
    _resolve_sample_type_aliases_and_bases
from metameq.tests.test_metadata_configurator.conftest import \
    ConfiguratorTestBase


class TestIdSampleTypeDefinition(ConfiguratorTestBase):
    def test__id_sample_type_definition_alias(self):
        """Test identifying sample type definition as alias type."""
        sample_dict = {
            ALIAS_KEY: "other_sample"
        }
        result = _id_sample_type_definition("test_sample", sample_dict)
        self.assertEqual(ALIAS_KEY, result)

    def test__id_sample_type_definition_metadata(self):
        """Test identifying sample type definition as metadata type."""
        sample_dict = {
            METADATA_FIELDS_KEY: {
                "field1": {
                    "type": "string"
                }
            }
        }
        result = _id_sample_type_definition("test_sample", sample_dict)
        self.assertEqual(METADATA_FIELDS_KEY, result)

    def test__id_sample_type_definition_base_with_metadata(self):
        """Test sample type with both base_type AND metadata_fields returns metadata_fields.

        This is a valid configuration: base_type indicates inheritance from another
        sample type, while metadata_fields contains overrides specific to this sample type.
        The function should return METADATA_FIELDS_KEY since metadata takes precedence.
        """
        sample_dict = {
            BASE_TYPE_KEY: "stool",
            METADATA_FIELDS_KEY: {
                "description": {
                    "allowed": ["human dung"],
                    "type": "string"
                }
            }
        }
        result = _id_sample_type_definition("dung", sample_dict)
        self.assertEqual(METADATA_FIELDS_KEY, result)

    def test__id_sample_type_definition_base(self):
        """Test identifying sample type definition as base type."""
        sample_dict = {
            BASE_TYPE_KEY: "other_sample"
        }
        result = _id_sample_type_definition("test_sample", sample_dict)
        self.assertEqual(BASE_TYPE_KEY, result)

    def test__id_sample_type_definition_err_alias_metadata(self):
        """Test that sample type with both alias and metadata fields raises ValueError."""
        sample_dict = {
            ALIAS_KEY: "other_sample",
            METADATA_FIELDS_KEY: {
                "field1": {
                    "type": "string"
                }
            }
        }
        with self.assertRaisesRegex(ValueError, "Sample type 'test_sample' has both 'alias' and 'metadata_fields' keys"):
            _id_sample_type_definition("test_sample", sample_dict)

    def test__id_sample_type_definition_err_alias_base(self):
        """Test that sample type with both alias and base type raises ValueError."""
        sample_dict = {
            ALIAS_KEY: "other_sample",
            BASE_TYPE_KEY: "other_sample"
        }
        with self.assertRaisesRegex(ValueError, "Sample type 'test_sample' has both 'alias' and 'base_type' keys"):
            _id_sample_type_definition("test_sample", sample_dict)

    def test__id_sample_type_definition_err_no_keys(self):
        """Test that sample type with neither alias nor metadata fields raises ValueError."""
        sample_dict = {}
        with self.assertRaisesRegex(ValueError, "Sample type 'test_sample' has neither 'alias' nor 'metadata_fields' keys"):
            _id_sample_type_definition("test_sample", sample_dict)


class TestResolveSampleTypeAliasesAndBases(ConfiguratorTestBase):
    def test__resolve_sample_type_aliases_and_bases_simple(self):
        """Test basic resolution with no aliases or bases.

        Input: Single sample type with metadata fields, empty host metadata.
        Expected: Sample type has its metadata fields plus sample_type and qiita_sample_type added.
        """
        sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "body_site": {
                        DEFAULT_KEY: "gut",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _resolve_sample_type_aliases_and_bases(
            sample_types_dict, host_metadata_fields_dict)

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "body_site": {
                        DEFAULT_KEY: "gut",
                        TYPE_KEY: "string"
                    },
                    # sample_type field added by resolution
                    SAMPLE_TYPE_KEY: {
                        ALLOWED_KEY: ["stool"],
                        DEFAULT_KEY: "stool",
                        TYPE_KEY: "string"
                    },
                    # qiita_sample_type field added by resolution (same as sample_type)
                    QIITA_SAMPLE_TYPE: {
                        ALLOWED_KEY: ["stool"],
                        DEFAULT_KEY: "stool",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        self.assertDictEqual(expected, result)

    def test__resolve_sample_type_aliases_and_bases_with_alias(self):
        """Test that alias is resolved to target sample type's metadata.

        Input: 'feces' is alias to 'stool', 'stool' has metadata.
        Expected: Both 'feces' and 'stool' are resolved with same metadata,
                  but sample_type field uses the alias target name ('stool').
        """
        sample_types_dict = {
            "feces": {
                ALIAS_KEY: "stool"
            },
            "stool": {
                METADATA_FIELDS_KEY: {
                    "stool_field": {
                        DEFAULT_KEY: "stool_value",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _resolve_sample_type_aliases_and_bases(
            sample_types_dict, host_metadata_fields_dict)

        # Both entries resolve to same metadata, sample_type uses alias target name
        stool_resolved_metadata = {
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
        expected = {
            # Alias entry resolves to same metadata as target (sample_type="stool")
            "feces": {
                METADATA_FIELDS_KEY: stool_resolved_metadata
            },
            # Target sample type is fully resolved
            "stool": {
                METADATA_FIELDS_KEY: stool_resolved_metadata
            }
        }
        self.assertDictEqual(expected, result)

    def test__resolve_sample_type_aliases_and_bases_chained_alias_raises(self):
        """Test that chained aliases raise ValueError.

        Input: 'feces' aliases to 'stool', 'stool' aliases to 'poop'.
        Expected: ValueError because chained aliases are not allowed.
        """
        sample_types_dict = {
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
            _resolve_sample_type_aliases_and_bases(
                sample_types_dict, host_metadata_fields_dict)

    def test__resolve_sample_type_aliases_and_bases_with_base_type(self):
        """Test that base type fields are inherited and overlaid.

        Input: 'derived_sample' has base_type 'base_sample'.
        Expected: 'derived_sample' inherits base fields, adds own, base_type key removed.
        """
        sample_types_dict = {
            "base_sample": {
                METADATA_FIELDS_KEY: {
                    "base_field": {
                        DEFAULT_KEY: "base_value",
                        TYPE_KEY: "string"
                    }
                }
            },
            "derived_sample": {
                BASE_TYPE_KEY: "base_sample",
                METADATA_FIELDS_KEY: {
                    "derived_field": {
                        DEFAULT_KEY: "derived_value",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _resolve_sample_type_aliases_and_bases(
            sample_types_dict, host_metadata_fields_dict)

        expected = {
            # Base sample type is fully resolved
            "base_sample": {
                METADATA_FIELDS_KEY: {
                    "base_field": {
                        DEFAULT_KEY: "base_value",
                        TYPE_KEY: "string"
                    },
                    SAMPLE_TYPE_KEY: {
                        ALLOWED_KEY: ["base_sample"],
                        DEFAULT_KEY: "base_sample",
                        TYPE_KEY: "string"
                    },
                    QIITA_SAMPLE_TYPE: {
                        ALLOWED_KEY: ["base_sample"],
                        DEFAULT_KEY: "base_sample",
                        TYPE_KEY: "string"
                    }
                }
            },
            # Derived sample type inherits base fields, base_type key removed
            "derived_sample": {
                METADATA_FIELDS_KEY: {
                    # Inherited from base
                    "base_field": {
                        DEFAULT_KEY: "base_value",
                        TYPE_KEY: "string"
                    },
                    # Own field
                    "derived_field": {
                        DEFAULT_KEY: "derived_value",
                        TYPE_KEY: "string"
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
            }
        }
        self.assertDictEqual(expected, result)

    def test__resolve_sample_type_aliases_and_bases_sets_sample_type(self):
        """Test that sample_type field is added with correct allowed/default.

        Input: Sample type without sample_type field.
        Expected: sample_type field added with allowed=[sample_type_name], default=sample_type_name.
        """
        sample_types_dict = {
            "blood": {
                METADATA_FIELDS_KEY: {
                    "body_site": {
                        DEFAULT_KEY: "blood",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _resolve_sample_type_aliases_and_bases(
            sample_types_dict, host_metadata_fields_dict)

        expected = {
            "blood": {
                METADATA_FIELDS_KEY: {
                    "body_site": {
                        DEFAULT_KEY: "blood",
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
        self.assertDictEqual(expected, result)

    def test__resolve_sample_type_aliases_and_bases_preserves_existing_qiita_sample_type(self):
        """Test that existing qiita_sample_type is not overwritten.

        Input: Sample type already has qiita_sample_type defined with very different value.
        Expected: Existing qiita_sample_type preserved exactly, sample_type still added.
        """
        sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "body_site": {
                        DEFAULT_KEY: "gut",
                        TYPE_KEY: "string"
                    },
                    # Pre-existing qiita_sample_type with VERY different value
                    # to make it clear it's preserved, not overwritten
                    QIITA_SAMPLE_TYPE: {
                        ALLOWED_KEY: ["CUSTOM_QIITA_VALUE_12345"],
                        DEFAULT_KEY: "CUSTOM_QIITA_VALUE_12345",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        host_metadata_fields_dict = {}

        result = _resolve_sample_type_aliases_and_bases(
            sample_types_dict, host_metadata_fields_dict)

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "body_site": {
                        DEFAULT_KEY: "gut",
                        TYPE_KEY: "string"
                    },
                    # sample_type added (would be "stool")
                    SAMPLE_TYPE_KEY: {
                        ALLOWED_KEY: ["stool"],
                        DEFAULT_KEY: "stool",
                        TYPE_KEY: "string"
                    },
                    # Pre-existing qiita_sample_type preserved exactly (NOT "stool")
                    QIITA_SAMPLE_TYPE: {
                        ALLOWED_KEY: ["CUSTOM_QIITA_VALUE_12345"],
                        DEFAULT_KEY: "CUSTOM_QIITA_VALUE_12345",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        self.assertDictEqual(expected, result)

    def test__resolve_sample_type_aliases_and_bases_merges_with_host_metadata(self):
        """Test that host-level metadata fields are merged with sample-type fields.

        Input: Host has host_common_name field, sample type has body_site field.
        Expected: Resolved sample type has both fields merged.
        """
        sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    "body_site": {
                        DEFAULT_KEY: "gut",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        host_metadata_fields_dict = {
            "host_common_name": {
                DEFAULT_KEY: "human",
                TYPE_KEY: "string"
            }
        }

        result = _resolve_sample_type_aliases_and_bases(
            sample_types_dict, host_metadata_fields_dict)

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    # Host-level field merged in
                    "host_common_name": {
                        DEFAULT_KEY: "human",
                        TYPE_KEY: "string"
                    },
                    # Sample-type field
                    "body_site": {
                        DEFAULT_KEY: "gut",
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
        self.assertDictEqual(expected, result)

    def test__resolve_sample_type_aliases_and_bases_sample_overrides_host(self):
        """Test that sample-level field overrides host-level field with same name.

        Input: Host has description="host description", sample type also has description="sample description".
        Expected: Sample-level description value wins.
        """
        sample_types_dict = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    # Sample-level description should override host-level
                    "description": {
                        DEFAULT_KEY: "sample-level description value",
                        TYPE_KEY: "string"
                    }
                }
            }
        }
        host_metadata_fields_dict = {
            # Host-level description should be overridden
            "description": {
                DEFAULT_KEY: "host-level description value",
                TYPE_KEY: "string"
            },
            "host_common_name": {
                DEFAULT_KEY: "human",
                TYPE_KEY: "string"
            }
        }

        result = _resolve_sample_type_aliases_and_bases(
            sample_types_dict, host_metadata_fields_dict)

        expected = {
            "stool": {
                METADATA_FIELDS_KEY: {
                    # Host-level field that wasn't overridden
                    "host_common_name": {
                        DEFAULT_KEY: "human",
                        TYPE_KEY: "string"
                    },
                    # Description: sample-level value wins over host-level
                    "description": {
                        DEFAULT_KEY: "sample-level description value",
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
        self.assertDictEqual(expected, result)
