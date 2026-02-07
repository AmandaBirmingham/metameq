from metameq.src.util import \
    METADATA_TRANSFORMERS_KEY, \
    PRE_TRANSFORMERS_KEY, \
    POST_TRANSFORMERS_KEY
from metameq.src.metadata_configurator import \
    _combine_metadata_transformers_dicts
from metameq.tests.test_metadata_configurator.conftest import \
    ConfiguratorTestBase


class TestCombineMetadataTransformers(ConfiguratorTestBase):
    def test__combine_metadata_transformers_dicts_both_empty(self):
        """Test combining when both dicts have no transformers."""
        stds_dict = {}
        study_dict = {}

        result = _combine_metadata_transformers_dicts(stds_dict, study_dict)

        expected = {}
        self.assertDictEqual(expected, result)

    def test__combine_metadata_transformers_dicts_stds_only(self):
        """Test combining when only standards has transformers."""
        stds_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "field_a": {
                        "sources": ["source_a"],
                        "function": "func_a"
                    }
                }
            }
        }
        study_dict = {}

        result = _combine_metadata_transformers_dicts(stds_dict, study_dict)

        expected = {
            PRE_TRANSFORMERS_KEY: {
                "field_a": {
                    "sources": ["source_a"],
                    "function": "func_a"
                }
            }
        }
        self.assertDictEqual(expected, result)

    def test__combine_metadata_transformers_dicts_study_only(self):
        """Test combining when only study has transformers."""
        stds_dict = {}
        study_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "field_b": {
                        "sources": ["source_b"],
                        "function": "func_b"
                    }
                }
            }
        }

        result = _combine_metadata_transformers_dicts(stds_dict, study_dict)

        expected = {
            PRE_TRANSFORMERS_KEY: {
                "field_b": {
                    "sources": ["source_b"],
                    "function": "func_b"
                }
            }
        }
        self.assertDictEqual(expected, result)

    def test__combine_metadata_transformers_dicts_study_overrides_stds(self):
        """Test that study transformer overrides standards for same field."""
        stds_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "shared_field": {
                        "sources": ["stds_source"],
                        "function": "stds_func"
                    }
                }
            }
        }
        study_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "shared_field": {
                        "sources": ["study_source"],
                        "function": "study_func"
                    }
                }
            }
        }

        result = _combine_metadata_transformers_dicts(stds_dict, study_dict)

        expected = {
            PRE_TRANSFORMERS_KEY: {
                "shared_field": {
                    "sources": ["study_source"],
                    "function": "study_func"
                }
            }
        }
        self.assertDictEqual(expected, result)

    def test__combine_metadata_transformers_dicts_merges_same_type(self):
        """Test that transformers of same type are merged, with study adding new ones."""
        stds_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "stds_field": {
                        "sources": ["stds_source"],
                        "function": "stds_func"
                    }
                }
            }
        }
        study_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "study_field": {
                        "sources": ["study_source"],
                        "function": "study_func"
                    }
                }
            }
        }

        result = _combine_metadata_transformers_dicts(stds_dict, study_dict)

        expected = {
            PRE_TRANSFORMERS_KEY: {
                "stds_field": {
                    "sources": ["stds_source"],
                    "function": "stds_func"
                },
                "study_field": {
                    "sources": ["study_source"],
                    "function": "study_func"
                }
            }
        }
        self.assertDictEqual(expected, result)

    def test__combine_metadata_transformers_dicts_different_types(self):
        """Test combining pre_transformers from stds with post_transformers from study."""
        stds_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "pre_field": {
                        "sources": ["pre_source"],
                        "function": "pre_func"
                    }
                }
            }
        }
        study_dict = {
            METADATA_TRANSFORMERS_KEY: {
                POST_TRANSFORMERS_KEY: {
                    "post_field": {
                        "sources": ["post_source"],
                        "function": "post_func"
                    }
                }
            }
        }

        result = _combine_metadata_transformers_dicts(stds_dict, study_dict)

        expected = {
            PRE_TRANSFORMERS_KEY: {
                "pre_field": {
                    "sources": ["pre_source"],
                    "function": "pre_func"
                }
            },
            POST_TRANSFORMERS_KEY: {
                "post_field": {
                    "sources": ["post_source"],
                    "function": "post_func"
                }
            }
        }
        self.assertDictEqual(expected, result)

    def test__combine_metadata_transformers_dicts_does_not_mutate_inputs(self):
        """Test that input dictionaries are not mutated."""
        import copy
        stds_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "stds_field": {
                        "sources": ["stds_source"],
                        "function": "stds_func"
                    }
                }
            }
        }
        study_dict = {
            METADATA_TRANSFORMERS_KEY: {
                PRE_TRANSFORMERS_KEY: {
                    "study_field": {
                        "sources": ["study_source"],
                        "function": "study_func"
                    }
                }
            }
        }
        stds_original = copy.deepcopy(stds_dict)
        study_original = copy.deepcopy(study_dict)

        _combine_metadata_transformers_dicts(stds_dict, study_dict)

        self.assertDictEqual(
            stds_original, stds_dict,
            "_combine_metadata_transformers_dicts should not mutate stds input")
        self.assertDictEqual(
            study_original, study_dict,
            "_combine_metadata_transformers_dicts should not mutate study input")
