from metameq.src.util import \
    HOST_TYPE_SPECIFIC_METADATA_KEY, \
    METADATA_FIELDS_KEY, \
    DEFAULT_KEY, \
    TYPE_KEY, \
    LEAVE_REQUIREDS_BLANK_KEY, \
    OVERWRITE_NON_NANS_KEY
from metameq.src.metadata_configurator import \
    _push_global_settings_into_top_host
from metameq.tests.test_metadata_configurator.conftest import \
    ConfiguratorTestBase


class TestPushGlobalSettings(ConfiguratorTestBase):
    def test__push_global_settings_into_top_host_single_setting(self):
        """Test pushing a single global setting into the top-level host."""
        config_dict = {
            DEFAULT_KEY: "custom_default",
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    METADATA_FIELDS_KEY: {
                        "field1": {TYPE_KEY: "string"}
                    }
                }
            }
        }

        expected = {
            DEFAULT_KEY: "custom_default",
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    DEFAULT_KEY: "custom_default",
                    METADATA_FIELDS_KEY: {
                        "field1": {TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = _push_global_settings_into_top_host(config_dict)

        self.assertEqual(expected, result)
        # Original should be unchanged
        self.assertNotIn(
            DEFAULT_KEY,
            config_dict[HOST_TYPE_SPECIFIC_METADATA_KEY]["base"])

    def test__push_global_settings_into_top_host_multiple_settings(self):
        """Test pushing multiple global settings into the top-level host."""
        config_dict = {
            DEFAULT_KEY: "custom_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: True,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    METADATA_FIELDS_KEY: {
                        "field1": {TYPE_KEY: "string"}
                    }
                }
            }
        }

        expected = {
            DEFAULT_KEY: "custom_default",
            LEAVE_REQUIREDS_BLANK_KEY: True,
            OVERWRITE_NON_NANS_KEY: True,
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    DEFAULT_KEY: "custom_default",
                    LEAVE_REQUIREDS_BLANK_KEY: True,
                    OVERWRITE_NON_NANS_KEY: True,
                    METADATA_FIELDS_KEY: {
                        "field1": {TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = _push_global_settings_into_top_host(config_dict)

        self.assertEqual(expected, result)

    def test__push_global_settings_into_top_host_no_settings(self):
        """Test that function returns copy when no global settings present."""
        config_dict = {
            "some_other_key": "value",
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    METADATA_FIELDS_KEY: {
                        "field1": {TYPE_KEY: "string"}
                    }
                }
            }
        }

        expected = {
            "some_other_key": "value",
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "base": {
                    METADATA_FIELDS_KEY: {
                        "field1": {TYPE_KEY: "string"}
                    }
                }
            }
        }

        result = _push_global_settings_into_top_host(config_dict)

        self.assertEqual(expected, result)

    def test__push_global_settings_into_top_host_raises_on_zero_hosts(self):
        """Test that ValueError is raised when no top-level hosts exist."""
        config_dict = {
            DEFAULT_KEY: "custom_default",
            HOST_TYPE_SPECIFIC_METADATA_KEY: {}
        }

        with self.assertRaisesRegex(
                ValueError,
                r"Expected exactly one top-level key.*found: \[\]"):
            _push_global_settings_into_top_host(config_dict)

    def test__push_global_settings_into_top_host_raises_on_multiple_hosts(self):
        """Test that ValueError is raised when multiple top-level hosts exist."""
        config_dict = {
            DEFAULT_KEY: "custom_default",
            HOST_TYPE_SPECIFIC_METADATA_KEY: {
                "host1": {METADATA_FIELDS_KEY: {}},
                "host2": {METADATA_FIELDS_KEY: {}}
            }
        }

        with self.assertRaisesRegex(
                ValueError,
                r"Expected exactly one top-level key"):
            _push_global_settings_into_top_host(config_dict)
