from metameq.src.util import \
    TYPE_KEY, \
    DEFAULT_KEY, \
    ALLOWED_KEY, \
    ANYOF_KEY
from metameq.src.metadata_configurator import \
    update_wip_metadata_dict
from metameq.tests.test_metadata_configurator.conftest import \
    ConfiguratorTestBase


class TestUpdateWipMetadataDict(ConfiguratorTestBase):
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
