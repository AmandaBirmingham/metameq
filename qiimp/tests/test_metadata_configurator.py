from unittest import TestCase
from qiimp.src.metadata_configurator import \
    _make_combined_stds_and_study_host_type_dicts, \
    flatten_nested_stds_dict


class TestMetadataExtender(TestCase):
    NESTED_STDS_DICT = {
            "host_type_specific_metadata": {
                # Top host level (host_associated in this example) has
                # *complete* definitions for all metadata fields it includes.
                # Lower levels include only the elements of the definition that
                # are different from the parent level (but if a field is NEW at
                # a lower level, the lower level must include the complete
                # definition for that field).
                "host_associated": {
                    "default": "not provided",
                    "metadata_fields": {
                        # not overridden
                        "country": {
                            "allowed": ["USA"],
                            "default": "USA",
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
                            "default": "host associated",
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in stds lower host
                        "dna_extracted": {
                            "allowed": ["true", "false"],
                            "default": "true",
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
                    "sample_type_specific_metadata": {
                        "fe": {
                            "alias": "stool",
                        },
                        "stool": {
                            "metadata_fields": {
                                # overrides stds host,
                                # overridden in stds lower host, and
                                # in stds lower host + sample type
                                "description": {
                                    "allowed": ["host associated stool"],
                                    "default": "host associated stool",
                                    "type": "string"
                                },
                                # overridden in STUDY for this host + sample type
                                "physical_specimen_location": {
                                    "allowed": ["UCSD"],
                                    "default": "UCSD",
                                    "empty": False,
                                    "is_phi": False,
                                    "required": True,
                                    "type": "string"
                                },
                                # overridden in stds lower host + sample type
                                "physical_specimen_remaining": {
                                    "allowed": ["true", "false"],
                                    "default": "true",
                                    "empty": False,
                                    "is_phi": False,
                                    "required": True,
                                    "type": "string"
                                }
                            }
                        }
                    },
                    "host_type_specific_metadata": {
                        "human": {
                            "metadata_fields": {
                                # overrides stds parent host
                                "description": {
                                    "allowed": ["human"],
                                    "default": "human",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                # BUT overridden in turn in STUDY for this host
                                "dna_extracted": {
                                    "allowed": ["false"],
                                    "default": "false",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                "host_type": {
                                    "allowed": ["human"],
                                    "default": "human",
                                    "type": "string"
                                }
                            },
                            "sample_type_specific_metadata": {
                                "stool": {
                                    "metadata_fields": {
                                        # overrides stds parent host + sample type
                                        "description": {
                                            "allowed": ["human stool"],
                                            "default": "human stool",
                                            "type": "string"
                                        },
                                        # overrides stds parent host
                                        "elevation": {
                                            "default": 14,
                                            "type": "number"
                                        }
                                    }
                                }
                            },
                            "host_type_specific_metadata": {
                                "dude": {
                                    "metadata_fields": {
                                        # overrides stds parent host
                                        "host_type": {
                                            "allowed": ["dude"],
                                            "default": "dude",
                                            "type": "string"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    FLAT_STUDY_DICT = {
        "host_type_specific_metadata": {
            # FLAT list of host types
            "host_associated": {
                "metadata_fields": {
                    # override of standard for this host type
                    "geo_loc_name": {
                        "allowed": ["USA:CA:San Diego"],
                        "default": "USA:CA:San Diego",
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
                "sample_type_specific_metadata": {
                    "stool": {
                        "metadata_fields": {
                            # override of standard for this
                            # host + sample type
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                "default": "UCSDST",
                                "type": "string"
                            }
                        }
                    }
                }
            },
            "human": {
                "default": "not collected",
                "metadata_fields": {
                    # overrides std parent host type
                    "dna_extracted": {
                        "allowed": ["true"],
                        "default": "true",
                        "type": "string"
                    },
                },
                "sample_type_specific_metadata": {
                    "feces": {
                        "alias": "stool"
                    },
                    "stool": {
                        "metadata_fields": {
                            # override of std parent
                            # host + sample type
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                "default": "false",
                                "type": "string"
                            }
                        }
                    }
                }
            }
        }
    }

    NESTED_STDS_W_STUDY_DICT = {
            "host_type_specific_metadata": {
                # Top host level (host_associated in this example) has
                # *complete* definitions for all metadata fields it includes.
                # Lower levels include only the elements of the definition that
                # are different from the parent level (but if a field is NEW at
                # a lower level, the lower level must include the complete
                # definition for that field).
                "host_associated": {
                    "default": "not provided",
                    "metadata_fields": {
                        # not overridden
                        "country": {
                            "allowed": ["USA"],
                            "default": "USA",
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
                            "default": "host associated",
                            "empty": False,
                            "is_phi": False,
                            "required": True,
                            "type": "string"
                        },
                        # overridden in stds lower host
                        "dna_extracted": {
                            "allowed": ["true", "false"],
                            "default": "true",
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
                            "default": "USA:CA:San Diego",
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
                    "sample_type_specific_metadata": {
                        "fe": {
                            "alias": "stool",
                        },
                        "stool": {
                            "metadata_fields": {
                                # overrides stds host,
                                # overridden in stds lower host, and
                                # in stds lower host + sample type
                                "description": {
                                    "allowed": ["host associated stool"],
                                    "default": "host associated stool",
                                    "type": "string"
                                },
                                # not overridden
                                # (NB: comes from study)
                                "physical_specimen_location": {
                                    "allowed": ["UCSDST"],
                                    "default": "UCSDST",
                                    "empty": False,
                                    "is_phi": False,
                                    "required": True,
                                    "type": "string"
                                },
                                # overridden in stds lower host + sample type
                                "physical_specimen_remaining": {
                                    "allowed": ["true", "false"],
                                    "default": "true",
                                    "empty": False,
                                    "is_phi": False,
                                    "required": True,
                                    "type": "string"
                                }
                            }
                        }
                    },
                    "host_type_specific_metadata": {
                        "human": {
                            "default": "not collected",
                            "metadata_fields": {
                                # overrides stds parent host
                                "description": {
                                    "allowed": ["human"],
                                    "default": "human",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                # (NB: comes from study)
                                "dna_extracted": {
                                    "allowed": ["true"],
                                    "default": "true",
                                    "type": "string"
                                },
                                # overrides stds parent host
                                "host_type": {
                                    "allowed": ["human"],
                                    "default": "human",
                                    "type": "string"
                                }
                            },
                            "sample_type_specific_metadata": {
                                "feces": {
                                    "alias": "stool",
                                },
                                "stool": {
                                    "metadata_fields": {
                                        # overrides stds parent host + sample type
                                        "description": {
                                            "allowed": ["human stool"],
                                            "default": "human stool",
                                            "type": "string"
                                        },
                                        # overrides stds parent host
                                        "elevation": {
                                            "default": 14,
                                            "type": "number"
                                        },
                                        # overrides stds parent host + sample type
                                        # (NB: comes from study)
                                        "physical_specimen_remaining": {
                                            "allowed": ["false"],
                                            "default": "false",
                                            "type": "string"
                                        }
                                    }
                                }
                            },
                            "host_type_specific_metadata": {
                                "dude": {
                                    "metadata_fields": {
                                        # overrides stds parent host
                                        "host_type": {
                                            "allowed": ["dude"],
                                            "default": "dude",
                                            "type": "string"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    FLATTENED_STDS_W_STUDY_DICT = {
        "host_type_specific_metadata": {
            "host_associated": {
                "default": "not provided",
                "metadata_fields": {
                    # from stds same level host
                    "country": {
                        "allowed": ["USA"],
                        "default": "USA",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "description": {
                        "allowed": ["host associated"],
                        "default": "host associated",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "dna_extracted": {
                        "allowed": ["true", "false"],
                        "default": "true",
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
                        "default": "USA:CA:San Diego",
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
                "sample_type_specific_metadata": {
                    "fe": {
                        "alias": "stool"
                    },
                    "stool": {
                        "metadata_fields": {
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["host associated stool"],
                                "default": "host associated stool",
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            # (NB: comes from study)
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                "default": "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            # (NB: comes from study)
                            "physical_specimen_remaining": {
                                "allowed": ["true", "false"],
                                "default": "true",
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
                "default": "not collected",
                "metadata_fields": {
                    # from stds parent host
                    "country": {
                        "allowed": ["USA"],
                        "default": "USA",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "description": {
                        "allowed": ["human"],
                        "default": "human",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    # (NB: comes from study)
                    "dna_extracted": {
                        "allowed": ["true"],
                        "default": "true",
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
                        "default": "USA:CA:San Diego",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "host_type": {
                        "allowed": ["human"],
                        "default": "human",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    }
                },
                "sample_type_specific_metadata": {
                    "fe": {
                        "alias": "stool"
                    },
                    "feces": {
                        "alias": "stool"
                    },
                    "stool": {
                        "metadata_fields": {
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["human stool"],
                                "default": "human stool",
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "elevation": {
                                "default": 14,
                                "type": "number"
                            },
                            # from stds parent level host + sample type
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                "default": "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                "default": "false",
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
                "default": "not collected",
                "metadata_fields": {
                    # from stds parent host
                    "country": {
                        "allowed": ["USA"],
                        "default": "USA",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "description": {
                        "allowed": ["human"],
                        "default": "human",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    # (NB: comes from study)
                    "dna_extracted": {
                        "allowed": ["true"],
                        "default": "true",
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
                        "default": "USA:CA:San Diego",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    },
                    # from stds same level host
                    "host_type": {
                        "allowed": ["dude"],
                        "default": "dude",
                        "empty": False,
                        "is_phi": False,
                        "required": True,
                        "type": "string"
                    }
                },
                "sample_type_specific_metadata": {
                    "fe": {
                        "alias": "stool"
                    },
                    "feces": {
                        "alias": "stool"
                    },
                    "stool": {
                        "metadata_fields": {
                            # from stds same level host + sample type
                            "description": {
                                "allowed": ["human stool"],
                                "default": "human stool",
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "elevation": {
                                "default": 14,
                                "type": "number"
                            },
                            # from stds parent level host + sample type
                            "physical_specimen_location": {
                                "allowed": ["UCSDST"],
                                "default": "UCSDST",
                                "empty": False,
                                "is_phi": False,
                                "required": True,
                                "type": "string"
                            },
                            # from stds same level host + sample type
                            "physical_specimen_remaining": {
                                "allowed": ["false"],
                                "default": "false",
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
        out_nested_dict = _make_combined_stds_and_study_host_type_dicts(
            self.FLAT_STUDY_DICT, self.NESTED_STDS_DICT, )

        self.maxDiff = None
        self.assertDictEqual(
            self.NESTED_STDS_W_STUDY_DICT["host_type_specific_metadata"],
            out_nested_dict)

    def test_flatten_nested_stds_dict(self):
        out_flattened_dict = flatten_nested_stds_dict(
            self.NESTED_STDS_W_STUDY_DICT,
            None)  # , None)

        self.maxDiff = None
        self.assertDictEqual(
            self.FLATTENED_STDS_W_STUDY_DICT["host_type_specific_metadata"],
            out_flattened_dict)
