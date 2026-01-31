import os.path as path
from unittest import TestCase
from metameq.src.util import \
    HOST_TYPE_SPECIFIC_METADATA_KEY, METADATA_FIELDS_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, DEFAULT_KEY, \
    ALIAS_KEY, BASE_TYPE_KEY, ALLOWED_KEY, ANYOF_KEY, TYPE_KEY, \
    STUDY_SPECIFIC_METADATA_KEY, LEAVE_REQUIREDS_BLANK_KEY, \
    OVERWRITE_NON_NANS_KEY, REQUIRED_KEY, SAMPLE_TYPE_KEY, QIITA_SAMPLE_TYPE
from metameq.src.metadata_configurator import \
    combine_stds_and_study_config, \
    _make_combined_stds_and_study_host_type_dicts, \
    flatten_nested_stds_dict,  \
    _combine_base_and_added_metadata_fields, \
    _combine_base_and_added_sample_type_specific_metadata, \
    _combine_base_and_added_host_type, \
    _id_sample_type_definition, \
    update_wip_metadata_dict, \
    build_full_flat_config_dict, \
    _resolve_sample_type_aliases_and_bases


class TestMetadataConfigurator(TestCase):
    TEST_DIR = path.dirname(__file__)
    TEST_STDS_FP = path.join(TEST_DIR, "data/test_standards.yml")

    NESTED_STDS_DICT = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                # Top host level (host_associated in this example) has
                # *complete* definitions for all metadata fields it includes.
                # Lower levels include only the elements of the definition that
                # are different from the parent level (but if a field is NEW at
                # a lower level, the lower level must include the complete
                # definition for that field).
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        # not overridden
                        "country": {
                            "allowed": ["USA"],
                            DEFAULT_KEY: "USA",
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in stds same level host + sample type,
                        # again in stds lower host, and *again* in
                        # stds lower host + sample type
                        "description": {
                            "allowed": ["host associated"],
                            DEFAULT_KEY: "host associated",
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in stds lower host
                        "dna_extracted": {
                            "allowed": ["true", "false"],
                            DEFAULT_KEY: "true",
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in stds lower host + sample type
                        "elevation": {
                            "anyof": [
                                {
                                    "allowed": [
                                        "not collected",
                                        "not provided",
                                        "restricted access"],
                                    "type": "string"
                                },
                                {
                                    "min": -413.0,
                                    "type": "number"
                                }],
                            "empty": False,
                            "is_phi": False,
                            "required": True
                        },
                        # overridden in STUDY for this host
                        "geo_loc_name": {
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in STUDY for this host
                        "host_type": {
                            "allowed": ["human", "animal", "plant"],
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "fe": {
                            "alias": "stool",
                        },
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                # overrides stds host,
                                # overridden in stds lower host, and
                                # in stds lower host + sample type
                                "description": {
                                    "allowed": ["host associated stool"],
                                    DEFAULT_KEY: "host associated stool",
                                    "type": "string"
                                },
                                # overridden in STUDY for this host + sample type
                                "physical_specimen_location": {
                                    "allowed": ["UCSD"],
                                    DEFAULT_KEY: "UCSD",
                                    "empty": False,
                                    "is_phi": False,
                                    "required": True,
                                    "type": "string"
                                },
                                # overridden in stds lower host + sample type
                                "physical_specimen_remaining": {
                                    "allowed": ["true", "false"],
                                    DEFAULT_KEY: "true",
                                    "empty": False,
                                    "is_phi": False,
                                    "required": True,
                                    "type": "string"
                                }
                            }
                        }
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "human": {
                            METADATA_FIELDS_KEY: {
                                # overrides stds parent host
                                "description": {
                                    "allowed": ["human"],
                                    DEFAULT_KEY: "human",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                # BUT overridden in turn in STUDY for this host
                                "dna_extracted": {
                                    "allowed": ["false"],
                                    DEFAULT_KEY: "false",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                "host_type": {
                                    "allowed": ["human"],
                                    DEFAULT_KEY: "human",
                                    "type": "string"
                                }
                            },
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "stool": {
                                    METADATA_FIELDS_KEY: {
                                        # overrides stds parent host + sample type
                                        "description": {
                                            "allowed": ["human stool"],
                                            DEFAULT_KEY: "human stool",
                                            "type": "string"
                                        },
                                        # overrides stds parent host
                                        "elevation": {
                                            DEFAULT_KEY: 14,
                                            "type": "number"
                                        }
                                    }
                                },
                                "dung": {
                                    METADATA_FIELDS_KEY: {
                                        # overrides stds parent host + sample type
                                        "description": {
                                            "allowed": ["human dung"],
                                            DEFAULT_KEY: "human dung",
                                            "type": "string"
                                        }
                                    }
                                }
                            },
                            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                                "dude": {
                                    METADATA_FIELDS_KEY: {
                                        # overrides stds parent host
                                        "host_type": {
                                            "allowed": ["dude"],
                                            DEFAULT_KEY: "dude",
                                            "type": "string"
                                        }
                                    }
                                }
                            }
                        },
                        "control": {
                            METADATA_FIELDS_KEY: {
                                # overrides stds parent host
                                "description": {
                                    "allowed": ["control"],
                                    DEFAULT_KEY: "control",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                "host_type": {
                                    "allowed": ["control"],
                                    DEFAULT_KEY: "control",
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            }
        }

    FLAT_STUDY_DICT = {
        HOST_TYPE_SPECIFIC_METADATA_KEY: {
            # FLAT list of host types
            "host_associated": {
                METADATA_FIELDS_KEY: {
                    # override of standard for this host type
                    "geo_loc_name": {
                        "allowed": ["USA:CA:San Diego"],
                        DEFAULT_KEY: "USA:CA:San Diego",
                        "type": "string"
                    },
                    # note: this overrides the standard for this host type
                    # BUT the std lower host type overrides this,
                    # and the lowest (most specific) directive wins,
                    # so this will NOT be included in output
                    "host_type": {
                        "allowed": ["human", "non-human"],
                        "type": "string"
                    },
                },
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # override of standard for this
                            # host + sample type
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "type": "string"
                            }
                        }
                    }
                }
            },
            "human": {
                DEFAULT_KEY: "not collected",
                METADATA_FIELDS_KEY: {
                    # overrides std parent host type
                    "dna_extracted": {
                        "allowed": ["true"],
                        DEFAULT_KEY: "true",
                        "type": "string"
                    },
                },
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "feces": {
                        "alias": "stool"
                    },
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # override of std parent
                            # host + sample type
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "type": "string"
                            }
                        }
                    },
                    "dung": {
                        "base_type": "stool",
                        METADATA_FIELDS_KEY: {
                            # overrides stds parent host + sample type
                            "physical_specimen_location": {
                                "allowed": ["FIELD"],
                                DEFAULT_KEY: "FIELD",
                                "type": "string"
                            }
                        }
                    },
                    "f": {
                        "base_type": "stool"
                    }
                }
            }
        }
    }

    NESTED_STDS_W_STUDY_DICT = {
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                # Top host level (host_associated in this example) has
                # *complete* definitions for all metadata fields it includes.
                # Lower levels include only the elements of the definition that
                # are different from the parent level (but if a field is NEW at
                # a lower level, the lower level must include the complete
                # definition for that field).
                "host_associated": {
                    DEFAULT_KEY: "not provided",
                    METADATA_FIELDS_KEY: {
                        # not overridden
                        "country": {
                            "allowed": ["USA"],
                            DEFAULT_KEY: "USA",
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in stds same level host + sample type,
                        # again in stds lower host, and *again* in
                        # stds lower host + sample type
                        "description": {
                            "allowed": ["host associated"],
                            DEFAULT_KEY: "host associated",
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in stds lower host
                        "dna_extracted": {
                            "allowed": ["true", "false"],
                            DEFAULT_KEY: "true",
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in stds lower host + sample type
                        "elevation": {
                            "anyof": [
                                {
                                    "allowed": [
                                        "not collected",
                                        "not provided",
                                        "restricted access"],
                                    "type": "string"
                                },
                                {
                                    "min": -413.0,
                                    "type": "number"
                                }],
                            "empty": False,
                            "is_phi": False,
                            "required": True
                        },
                        # not overridden (NB: comes from study)
                        "geo_loc_name": {
                            "allowed": ["USA:CA:San Diego"],
                            DEFAULT_KEY: "USA:CA:San Diego",
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in stds lower host
                        # (NB: comes from study)
                        "host_type": {
                            "allowed": ["human", "non-human"],
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        }
                    },
                    SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                        "fe": {
                            "alias": "stool",
                        },
                        "stool": {
                            METADATA_FIELDS_KEY: {
                                # overrides stds host,
                                # overridden in stds lower host, and
                                # in stds lower host + sample type
                                "description": {
                                    "allowed": ["host associated stool"],
                                    DEFAULT_KEY: "host associated stool",
                                    "type": "string"
                                },
                                # not overridden
                                # (NB: comes from study)
                                "physical_specimen_location": {
                                    "allowed": ["UCSDST"],
                                    DEFAULT_KEY: "UCSDST",
                                    "empty": False,
                                    "is_phi": False,
                                    "required": True,
                                    "type": "string"
                                },
                                # overridden in stds lower host + sample type
                                "physical_specimen_remaining": {
                                    "allowed": ["true", "false"],
                                    DEFAULT_KEY: "true",
                                    "empty": False,
                                    "is_phi": False,
                                    "required": True,
                                    "type": "string"
                                }
                            }
                        }
                    },
                    HOST_TYPE_SPECIFIC_METADATA_KEY: {
                        "human": {
                            DEFAULT_KEY: "not collected",
                            METADATA_FIELDS_KEY: {
                                # overrides stds parent host
                                "description": {
                                    "allowed": ["human"],
                                    DEFAULT_KEY: "human",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                # (NB: comes from study)
                                "dna_extracted": {
                                    "allowed": ["true"],
                                    DEFAULT_KEY: "true",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                "host_type": {
                                    "allowed": ["human"],
                                    DEFAULT_KEY: "human",
                                    "type": "string"
                                }
                            },
                            SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                                "feces": {
                                    "alias": "stool",
                                },
                                "stool": {
                                    METADATA_FIELDS_KEY: {
                                        # overrides stds parent host + sample type
                                        "description": {
                                            "allowed": ["human stool"],
                                            DEFAULT_KEY: "human stool",
                                            "type": "string"
                                        },
                                        # overrides stds parent host
                                        "elevation": {
                                            DEFAULT_KEY: 14,
                                            "type": "number"
                                        },
                                        # overrides stds parent host + sample type
                                        # (NB: comes from study)
                                        "physical_specimen_remaining": {
                                            "allowed": ["false"],
                                            DEFAULT_KEY: "false",
                                            "type": "string"
                                        }
                                    }
                                },
                                "dung": {
                                    "base_type": "stool",
                                    METADATA_FIELDS_KEY: {
                                        # overrides stds parent host + sample type
                                        "description": {
                                            "allowed": ["human dung"],
                                            DEFAULT_KEY: "human dung",
                                            "type": "string"
                                        },
                                        # overrides stds parent host + sample type
                                        "physical_specimen_location": {
                                            "allowed": ["FIELD"],
                                            DEFAULT_KEY: "FIELD",
                                            "type": "string"
                                        }
                                    }
                                },
                                "f": {
                                    "base_type": "stool"
                                }
                            },
                            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                                "dude": {
                                    METADATA_FIELDS_KEY: {
                                        # overrides stds parent host
                                        "host_type": {
                                            "allowed": ["dude"],
                                            DEFAULT_KEY: "dude",
                                            "type": "string"
                                        }
                                    }
                                }
                            }
                        },
                        "control": {
                            METADATA_FIELDS_KEY: {
                                # overrides stds parent host
                                "description": {
                                    "allowed": ["control"],
                                    DEFAULT_KEY: "control",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                "host_type": {
                                    "allowed": ["control"],
                                    DEFAULT_KEY: "control",
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            }
        }

    FLATTENED_STDS_W_STUDY_DICT = {
        HOST_TYPE_SPECIFIC_METADATA_KEY: {
            "host_associated": {
                DEFAULT_KEY: "not provided",
                METADATA_FIELDS_KEY: {
                    # from stds same level host
                    "country": {
                        "allowed": ["USA"],
                        DEFAULT_KEY: "USA",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "description": {
                        "allowed": ["host associated"],
                        DEFAULT_KEY: "host associated",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "dna_extracted": {
                        "allowed": ["true", "false"],
                        DEFAULT_KEY: "true",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "elevation": {
                        "anyof": [
                            {
                                "allowed": [
                                    "not collected",
                                    "not provided",
                                    "restricted access"],
                                "type": "string"
                            },
                            {
                                "min": -413.0,
                                "type": "number"
                            }],
                        "empty": False,
                        "is_phi": False,
                        "required": True
                    },
                    # from stds same level host
                    "geo_loc_name": {
                        "allowed": ["USA:CA:San Diego"],
                        DEFAULT_KEY: "USA:CA:San Diego",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # overridden in stds lower host
                    "host_type": {
                        "allowed": ["human", "non-human"],
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    }
                },
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "fe": {
                        # Resolved alias to stool - gets stool's resolved fields
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "description": {
                                "allowed": ["host associated stool"],
                                DEFAULT_KEY: "host associated stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true", "false"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                "empty": False,
                                "is_phi": False,
                                "required": True
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["human", "non-human"],
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["true", "false"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    },
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # Host fields merged in
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["host associated stool"],
                                DEFAULT_KEY: "host associated stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true", "false"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                "empty": False,
                                "is_phi": False,
                                "required": True
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["human", "non-human"],
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type (NB: comes from study)
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type (NB: comes from study)
                            "physical_specimen_remaining": {
                                "allowed": ["true", "false"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    }
                }
            },
            "control": {
                DEFAULT_KEY: "not provided",
                METADATA_FIELDS_KEY: {
                    # from stds same level host
                    "country": {
                        "allowed": ["USA"],
                        DEFAULT_KEY: "USA",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "description": {
                        "allowed": ["control"],
                        DEFAULT_KEY: "control",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "dna_extracted": {
                        "allowed": ["true", "false"],
                        DEFAULT_KEY: "true",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "elevation": {
                        "anyof": [
                            {
                                "allowed": [
                                    "not collected",
                                    "not provided",
                                    "restricted access"],
                                "type": "string"
                            },
                            {
                                "min": -413.0,
                                "type": "number"
                            }],
                        "empty": False,
                        "is_phi": False,
                        "required": True
                    },
                    # from stds same level host
                    "geo_loc_name": {
                        "allowed": ["USA:CA:San Diego"],
                        DEFAULT_KEY: "USA:CA:San Diego",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # overridden in stds lower host
                    "host_type": {
                        "allowed": ["control"],
                        DEFAULT_KEY: "control",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    }
                },
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "fe": {
                        # Resolved alias to stool - gets stool's resolved fields
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "description": {
                                "allowed": ["host associated stool"],
                                DEFAULT_KEY: "host associated stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true", "false"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                "empty": False,
                                "is_phi": False,
                                "required": True
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["control"],
                                DEFAULT_KEY: "control",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["true", "false"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    },
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # Host fields merged in
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["host associated stool"],
                                DEFAULT_KEY: "host associated stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true", "false"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                "empty": False,
                                "is_phi": False,
                                "required": True
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["control"],
                                DEFAULT_KEY: "control",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type (NB: comes from study)
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type (NB: comes from study)
                            "physical_specimen_remaining": {
                                "allowed": ["true", "false"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    }
                }
            },
            "human": {
                DEFAULT_KEY: "not collected",
                METADATA_FIELDS_KEY: {
                    # from stds parent host
                    "country": {
                        "allowed": ["USA"],
                        DEFAULT_KEY: "USA",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "description": {
                        "allowed": ["human"],
                        DEFAULT_KEY: "human",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    # (NB: comes from study)
                    "dna_extracted": {
                        "allowed": ["true"],
                        DEFAULT_KEY: "true",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds parent host
                    "elevation": {
                        "anyof": [
                            {
                                "allowed": [
                                    "not collected",
                                    "not provided",
                                    "restricted access"],
                                "type": "string"
                            },
                            {
                                "min": -413.0,
                                "type": "number"
                            }],
                        "empty": False,
                        "is_phi": False,
                        "required": True
                    },
                    # from stds parent host
                    "geo_loc_name": {
                        "allowed": ["USA:CA:San Diego"],
                        DEFAULT_KEY: "USA:CA:San Diego",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "host_type": {
                        "allowed": ["human"],
                        DEFAULT_KEY: "human",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    }
                },
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "dung": {
                        # Resolved base_type=stool - has stool's fields + dung's overrides
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # overrides stds parent host + sample type
                            "description": {
                                "allowed": ["human dung"],
                                DEFAULT_KEY: "human dung",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["human"],
                                DEFAULT_KEY: "human",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # overrides stds parent host + sample type
                            "physical_specimen_location": {
                                "allowed": ["FIELD"],
                                DEFAULT_KEY: "FIELD",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["dung"],
                                DEFAULT_KEY: "dung",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["dung"],
                                DEFAULT_KEY: "dung",
                                "type": "string"
                            }
                        }
                    },
                    "f": {
                        # Resolved base_type=stool - has stool's fields
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["human"],
                                DEFAULT_KEY: "human",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["f"],
                                DEFAULT_KEY: "f",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["f"],
                                DEFAULT_KEY: "f",
                                "type": "string"
                            }
                        }
                    },
                    "fe": {
                        # Resolved alias to stool - gets stool's resolved fields
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["human"],
                                DEFAULT_KEY: "human",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    },
                    "feces": {
                        # Resolved alias to stool - gets stool's resolved fields
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["human"],
                                DEFAULT_KEY: "human",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    },
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # Host fields merged in
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["human"],
                                DEFAULT_KEY: "human",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds parent level host + sample type
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    }
                }
            },
            "dude": {
                DEFAULT_KEY: "not collected",
                METADATA_FIELDS_KEY: {
                    # from stds parent host
                    "country": {
                        "allowed": ["USA"],
                        DEFAULT_KEY: "USA",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "description": {
                        "allowed": ["human"],
                        DEFAULT_KEY: "human",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    # (NB: comes from study)
                    "dna_extracted": {
                        "allowed": ["true"],
                        DEFAULT_KEY: "true",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds parent host
                    "elevation": {
                        "anyof": [
                            {
                                "allowed": [
                                    "not collected",
                                    "not provided",
                                    "restricted access"],
                                "type": "string"
                            },
                            {
                                "min": -413.0,
                                "type": "number"
                            }],
                        "empty": False,
                        "is_phi": False,
                        "required": True
                    },
                    # from stds parent host
                    "geo_loc_name": {
                        "allowed": ["USA:CA:San Diego"],
                        DEFAULT_KEY: "USA:CA:San Diego",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "host_type": {
                        "allowed": ["dude"],
                        DEFAULT_KEY: "dude",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    }
                },
                SAMPLE_TYPE_SPECIFIC_METADATA_KEY: {
                    "dung": {
                        # Resolved base_type=stool - has stool's fields + dung's overrides
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # overrides stds parent host + sample type
                            "description": {
                                "allowed": ["human dung"],
                                DEFAULT_KEY: "human dung",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["dude"],
                                DEFAULT_KEY: "dude",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # overrides stds parent host + sample type
                            "physical_specimen_location": {
                                "allowed": ["FIELD"],
                                DEFAULT_KEY: "FIELD",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["dung"],
                                DEFAULT_KEY: "dung",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["dung"],
                                DEFAULT_KEY: "dung",
                                "type": "string"
                            }
                        }
                    },
                    "f": {
                        # Resolved base_type=stool - has stool's fields
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["dude"],
                                DEFAULT_KEY: "dude",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["f"],
                                DEFAULT_KEY: "f",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["f"],
                                DEFAULT_KEY: "f",
                                "type": "string"
                            }
                        }
                    },
                    "fe": {
                        # Resolved alias to stool - gets stool's resolved fields
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["dude"],
                                DEFAULT_KEY: "dude",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    },
                    "feces": {
                        # Resolved alias to stool - gets stool's resolved fields
                        METADATA_FIELDS_KEY: {
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["dude"],
                                DEFAULT_KEY: "dude",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    },
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # Host fields merged in
                            "country": {
                                "allowed": ["USA"],
                                DEFAULT_KEY: "USA",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "dna_extracted": {
                                "allowed": ["true"],
                                DEFAULT_KEY: "true",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "elevation": {
                                "anyof": [
                                    {
                                        "allowed": [
                                            "not collected",
                                            "not provided",
                                            "restricted access"],
                                        "type": "string"
                                    },
                                    {
                                        "min": -413.0,
                                        "type": "number"
                                    }],
                                DEFAULT_KEY: 14,
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "number"
                            },
                            "geo_loc_name": {
                                "allowed": ["USA:CA:San Diego"],
                                DEFAULT_KEY: "USA:CA:San Diego",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            "host_type": {
                                "allowed": ["dude"],
                                DEFAULT_KEY: "dude",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds parent level host + sample type
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                DEFAULT_KEY: "false",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            QIITA_SAMPLE_TYPE: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            },
                            SAMPLE_TYPE_KEY: {
                                "allowed": ["stool"],
                                DEFAULT_KEY: "stool",
                                "type": "string"
                            }
                        }
                    }
                }
            }
        }
    }

    # Tests for combine_stds_and_study_config

    TEST_DIR = path.dirname(__file__)

    def test_combine_stds_and_study_config_empty_study(self):
        """Test combining with an empty study config dict uses only standards."""
        study_config = {}

        result = combine_stds_and_study_config(
            study_config,
            path.join(self.TEST_DIR, "data/test_config.yml"))

        expected = {
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
            }
        }

        result = combine_stds_and_study_config(
            study_config,
            path.join(self.TEST_DIR, "data/test_config.yml"))

        expected = {
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

    def test__make_combined_stds_and_study_host_type_dicts(self):
        """Test making a combined standards and study host type dictionary."""
        out_nested_dict = _make_combined_stds_and_study_host_type_dicts(
            self.FLAT_STUDY_DICT, self.NESTED_STDS_DICT, )

        self.maxDiff = None
        self.assertDictEqual(
            self.NESTED_STDS_W_STUDY_DICT[HOST_TYPE_SPECIFIC_METADATA_KEY],
            out_nested_dict)

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

    # Tests for update_wip_metadata_dict

    def test_update_wip_metadata_dict_new_field(self):
        """Test adding a completely new metadata field to wip dict."""
        wip = {}
        stds = {
            "field1": {
                TYPE_KEY: "string",
                ALLOWED_KEY: ["value1", "value2"]
            }
        }

        result = update_wip_metadata_dict(wip, stds)

        expected = stds
        self.assertDictEqual(expected, result)

    def test_update_wip_metadata_dict_update_existing_field(self):
        """Test updating an existing field with additional properties."""
        wip = {
            "field1": {
                TYPE_KEY: "string"
            }
        }
        stds = {
            "field1": {
                DEFAULT_KEY: "default_value"
            }
        }

        result = update_wip_metadata_dict(wip, stds)

        expected = {
            "field1": {
                TYPE_KEY: "string",
                DEFAULT_KEY: "default_value"
            }
        }
        self.assertDictEqual(expected, result)

    def test_update_wip_metadata_dict_allowed_replaces_anyof(self):
        """Test that adding 'allowed' key removes existing 'anyof' key."""
        wip = {
            "field1": {
                ANYOF_KEY: [
                    {TYPE_KEY: "string"},
                    {TYPE_KEY: "number"}
                ],
                "required": True
            }
        }
        stds = {
            "field1": {
                ALLOWED_KEY: ["value1", "value2"]
            }
        }

        result = update_wip_metadata_dict(wip, stds)

        # anyof should be removed, allowed should be added, required preserved
        self.assertNotIn(ANYOF_KEY, result["field1"])
        self.assertIn(ALLOWED_KEY, result["field1"])
        self.assertEqual(["value1", "value2"], result["field1"][ALLOWED_KEY])
        self.assertTrue(result["field1"]["required"])

    def test_update_wip_metadata_dict_anyof_replaces_allowed_and_type(self):
        """Test that adding 'anyof' key removes existing 'allowed' and 'type' keys."""
        wip = {
            "field1": {
                ALLOWED_KEY: ["old_value"],
                TYPE_KEY: "string",
                "required": True
            }
        }
        stds = {
            "field1": {
                ANYOF_KEY: [
                    {TYPE_KEY: "string", ALLOWED_KEY: ["a", "b"]},
                    {TYPE_KEY: "number", "min": 0}
                ]
            }
        }

        result = update_wip_metadata_dict(wip, stds)

        # allowed and type should be removed, anyof should be added, required preserved
        self.assertNotIn(ALLOWED_KEY, result["field1"])
        self.assertNotIn(TYPE_KEY, result["field1"])
        self.assertIn(ANYOF_KEY, result["field1"])
        self.assertTrue(result["field1"]["required"])

    def test_update_wip_metadata_dict_preserves_unrelated_keys(self):
        """Test that keys not in stds dict are preserved in wip dict."""
        wip = {
            "field1": {
                "required": True,
                "is_phi": False,
                "empty": False
            }
        }
        stds = {
            "field1": {
                DEFAULT_KEY: "new_default"
            }
        }

        result = update_wip_metadata_dict(wip, stds)

        expected = {
            "field1": {
                "required": True,
                "is_phi": False,
                "empty": False,
                DEFAULT_KEY: "new_default"
            }
        }
        self.assertDictEqual(expected, result)

    def test_update_wip_metadata_dict_multiple_fields(self):
        """Test updating multiple fields at once."""
        wip = {
            "field1": {TYPE_KEY: "string"},
            "field2": {TYPE_KEY: "integer"}
        }
        stds = {
            "field1": {DEFAULT_KEY: "default1"},
            "field2": {DEFAULT_KEY: 42},
            "field3": {TYPE_KEY: "boolean", DEFAULT_KEY: True}
        }

        result = update_wip_metadata_dict(wip, stds)

        expected = {
            "field1": {TYPE_KEY: "string", DEFAULT_KEY: "default1"},
            "field2": {TYPE_KEY: "integer", DEFAULT_KEY: 42},
            "field3": {TYPE_KEY: "boolean", DEFAULT_KEY: True}
        }
        self.assertDictEqual(expected, result)

    def test_update_wip_metadata_dict_returns_same_object(self):
        """Test that the function returns the same dict object it modifies (not a copy).

        This verifies the documented in-place modification behavior, which is
        relied upon by other parts of the codebase.
        """
        wip = {"field1": {TYPE_KEY: "string"}}
        stds = {"field1": {DEFAULT_KEY: "x"}}

        result = update_wip_metadata_dict(wip, stds)

        # result should be the exact same object as wip, not a copy
        self.assertIs(result, wip)
        # and wip should have been modified in place
        self.assertIn(DEFAULT_KEY, wip["field1"])

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

    # Tests for _combine_base_and_added_host_type

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

    # Tests for _resolve_sample_type_aliases_and_bases

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

    def test__resolve_sample_type_aliases_and_bases_base_type_invalid_raises(self):
        """Test that base type with non-metadata-fields keys raises ValueError.

        Input: Base sample type has extra keys beyond metadata_fields.
        Expected: ValueError because base must only have metadata_fields.
        """
        sample_types_dict = {
            "base_sample": {
                METADATA_FIELDS_KEY: {
                    "base_field": {DEFAULT_KEY: "value", TYPE_KEY: "string"}
                },
                "extra_key": "not_allowed"
            },
            "derived_sample": {
                BASE_TYPE_KEY: "base_sample",
                METADATA_FIELDS_KEY: {}
            }
        }
        host_metadata_fields_dict = {}

        with self.assertRaisesRegex(ValueError, "must only have metadata fields"):
            _resolve_sample_type_aliases_and_bases(
                sample_types_dict, host_metadata_fields_dict)

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

    # Tests for build_full_flat_config_dict

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
            # Flattened host types from standards + study
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                # base: top level in test_standards.yml, no default
                "base": {
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
            # No STUDY_SPECIFIC_METADATA_KEY since no study config provided
            # Flattened host types from standards only
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                # base: top level, no default, just sample_name/sample_type
                "base": {
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
            # Flattened host types
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
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
            # Flattened host types
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
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
