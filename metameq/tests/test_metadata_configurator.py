from unittest import TestCase
from qiimp.src.util import \
    HOST_TYPE_SPECIFIC_METADATA_KEY, METADATA_FIELDS_KEY, \
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY, DEFAULT_KEY, \
    ALIAS_KEY, BASE_TYPE_KEY
from qiimp.src.metadata_configurator import \
    _make_combined_stds_and_study_host_type_dicts, \
    flatten_nested_stds_dict,  \
    _combine_base_and_added_metadata_fields, \
    _combine_base_and_added_sample_type_specific_metadata, \
    _id_sample_type_definition


class TestMetadataConfigurator(TestCase):
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
                        "alias": "stool"
                    },
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["host associated stool"],
                                DEFAULT_KEY: "host associated stool",
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            # (NB: comes from study)
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            # (NB: comes from study)
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
                        "alias": "stool"
                    },
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["host associated stool"],
                                DEFAULT_KEY: "host associated stool",
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            # (NB: comes from study)
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                DEFAULT_KEY: "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            # (NB: comes from study)
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
                    },
                    "fe": {
                        "alias": "stool"
                    },
                    "feces": {
                        "alias": "stool"
                    },
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "elevation": {
                                DEFAULT_KEY: 14,
                                "type": "number"
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
                    },
                    "fe": {
                        "alias": "stool"
                    },
                    "feces": {
                        "alias": "stool"
                    },
                    "stool": {
                        METADATA_FIELDS_KEY: {
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["human stool"],
                                DEFAULT_KEY: "human stool",
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "elevation": {
                                DEFAULT_KEY: 14,
                                "type": "number"
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
                            }
                        }
                    }
                }
            }
        }
    }

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
