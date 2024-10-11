import numpy as np
import pandas
from pandas.testing import assert_frame_equal
from os.path import dirname, join
from unittest import TestCase
from qiimp.src.util import _get_grandparent_dir, extract_config_dict, \
    extract_yaml_dict, extract_stds_config, deepcopy_dict, \
    validate_required_columns_exist, update_metadata_df_field


class TestUtil(TestCase):
    # get the parent directory of the current file
    TEST_DIR = dirname(__file__)

    TEST_CONFIG_DICT = {
            "host_type_specific_metadata": {
                "base": {
                    "metadata_fields": {
                        "sample_name": {
                            "type": "string",
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

    def test__get_grandparent_dir_no_fp(self):
        obs = _get_grandparent_dir()
        self.assertTrue(obs.endswith("qiimp/src/../.."))

    def test__get_grandparent_dir_with_fp(self):
        obs = _get_grandparent_dir("/Users/username/Desktop/hello/world.py")
        self.assertTrue(obs.endswith("Desktop/hello/../.."))

    def test_extract_config_dict_w_config_fp(self):
        config_fp = join(self.TEST_DIR, "data/test_config.yml")
        obs = extract_config_dict(config_fp)
        self.assertDictEqual(self.TEST_CONFIG_DICT, obs)

    def test_extract_config_dict_starting_fp(self):
        # NB: this test is looking at the *real* config, which may change, so
        # just checking that a couple of the expected keys (which are not in
        # the test config) are present.
        starting_fp = join(self.TEST_DIR, "data")
        obs = extract_config_dict(None, starting_fp)
        self.assertIn("default", obs)
        self.assertIn("leave_requireds_blank", obs)

    def test_extract_yaml_dict(self):
        config_fp = join(self.TEST_DIR, "data/test_config.yml")
        obs = extract_yaml_dict(config_fp)
        self.assertDictEqual(self.TEST_CONFIG_DICT, obs)

    def test_extract_stds_config(self):
        # NB: this is looking at the real standards file, which may change, so
        # just checking that one of the expected keys (which is not in
        # the test config) is present.
        obs = extract_stds_config(None)
        self.assertIn("ebi_null_vals_all", obs)

    def test_deepcopy_dict(self):
        obs = deepcopy_dict(self.TEST_CONFIG_DICT)
        self.assertDictEqual(self.TEST_CONFIG_DICT, obs)
        self.assertIsNot(self.TEST_CONFIG_DICT, obs)
        obs["host_type_specific_metadata"]["base"]["metadata_fields"].pop(
            "sample_name")
        self.assertFalse(self.TEST_CONFIG_DICT == obs)

    def test_validate_required_columns_exist_no_err(self):
        test_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        validate_required_columns_exist(
            test_df, ["sample_name", "sample_type"], "test_df missing")
        # if no error at step above, this test passed
        self.assertTrue(True)

    def test_validate_required_columns_exist_err(self):
        test_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_tye": ["st1", "st2"]
        })

        err_msg = r"test_df missing column: \['sample_type'\]"
        with self.assertRaisesRegex(ValueError, err_msg):
            validate_required_columns_exist(
                test_df, ["sample_name", "sample_type"],
                "test_df missing column")

    def test_update_metadata_df_field_constant_overwrite(self):
        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["bacon", "bacon"]
        })

        update_metadata_df_field(
            working_df, "sample_type", "bacon",
            overwrite_non_nans=True)
        # with overwrite set to True, the column in question should have
        # every entry set to the input constant value
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_constant_no_overwrite_no_nan(self):
        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", "bacon",
            overwrite_non_nans=False)
        # with overwrite set to False, no change should be made because there
        # are no NaN values in the column in question
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_constant_no_overwrite_w_nan(self):
        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": [np.NaN, "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["bacon", "st2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", "bacon",
            overwrite_non_nans=False)
        # with overwrite set to False, only one change should be made because
        # there is only one NaN value in the column in question
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_function_overwrite(self):
        def test_func(row, source_fields):
            source_field = source_fields[0]
            last_char = row[source_field][-1]
            return f"bacon{last_char}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["bacon1", "bacon2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", test_func,
            ["sample_name"], overwrite_non_nans=True)
        # with overwrite set to True, the column in question should have
        # every entry set to result of running the input function on the input
        # source fields in the same row
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_function_no_overwrite_no_nan(self):
        def test_func(row, source_fields):
            source_field = source_fields[0]
            last_char = row[source_field][-1]
            return f"bacon{last_char}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", test_func,
            ["sample_name"], overwrite_non_nans=False)
        # with overwrite set to False, no change should be made because there
        # are no NaN values in the column in question
        assert_frame_equal(exp_df, working_df)

    def test_update_metadata_df_field_function_no_overwrite_w_nan(self):
        def test_func(row, source_fields):
            source_field = source_fields[0]
            last_char = row[source_field][-1]
            return f"bacon{last_char}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": [np.NaN, "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["bacon1", "st2"]
        })

        update_metadata_df_field(
            working_df, "sample_type", test_func,
            ["sample_name"], overwrite_non_nans=False)
        # with overwrite set to False, only one change should be made because
        # there is only one NaN value in the column in question
        assert_frame_equal(exp_df, working_df)
