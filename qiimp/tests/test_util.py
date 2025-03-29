import numpy as np
import pandas
from pandas.testing import assert_frame_equal
import os
import os.path as path
from unittest import TestCase
from qiimp.src.util import _get_grandparent_dir, extract_config_dict, \
    extract_yaml_dict, extract_stds_config, deepcopy_dict, \
    validate_required_columns_exist, update_metadata_df_field, get_extension, \
    load_df_with_best_fit_encoding


class TestUtil(TestCase):
    # get the parent directory of the current file
    TEST_DIR = path.dirname(__file__)

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

    # Tests for extract_config_dict
    def test_extract_config_dict_w_config_fp(self):
        config_fp = path.join(self.TEST_DIR, "data/test_config.yml")
        obs = extract_config_dict(config_fp)
        self.assertDictEqual(self.TEST_CONFIG_DICT, obs)

    def test_extract_config_dict_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            extract_config_dict("nonexistent.yml")

    def test_extract_config_dict_starting_fp(self):
        # NB: this test is looking at the *real* config, which may change, so
        # just checking that a couple of the expected keys (which are not in
        # the test config) are present.
        starting_fp = path.join(self.TEST_DIR, "data")
        obs = extract_config_dict(None, starting_fp)
        self.assertIn("default", obs)
        self.assertIn("leave_requireds_blank", obs)

    def test_extract_config_dict_invalid_yaml(self):
        # Create a temporary invalid YAML file
        invalid_yaml_path = path.join(self.TEST_DIR, "data/invalid.yml")
        with open(invalid_yaml_path, "w") as f:
            f.write("invalid: yaml: content: - [")
        
        with self.assertRaises(Exception):
            extract_config_dict(invalid_yaml_path)

    # Tests for extract_yaml_dict
    def test_extract_yaml_dict(self):
        config_fp = path.join(self.TEST_DIR, "data/test_config.yml")
        obs = extract_yaml_dict(config_fp)
        self.assertDictEqual(self.TEST_CONFIG_DICT, obs)

    # Tests for extract_stds_config
    def test_extract_stds_config(self):
        # NB: this is looking at the real standards file, which may change, so
        # just checking that one of the expected keys (which is not in
        # the test config) is present.
        obs = extract_stds_config(None)
        self.assertIn("ebi_null_vals_all", obs)

    def test_extract_stds_config_default_path(self):
        # This test assumes the default standards.yml exists
        config = extract_stds_config(None)
        self.assertIsInstance(config, dict)
        self.assertIn("host_type_specific_metadata", config)

    def test_extract_stds_config_custom_path(self):
        config = extract_stds_config(path.join(self.TEST_DIR, "data/test_config.yml"))
        self.assertDictEqual(config, self.TEST_CONFIG_DICT)

    # Tests for deepcopy_dict
    def test_deepcopy_dict(self):
        # copy a dict with a nested structure and ensure that the original
        # is not modified when the copy is modified
        obs = deepcopy_dict(self.TEST_CONFIG_DICT)
        self.assertDictEqual(self.TEST_CONFIG_DICT, obs)
        self.assertIsNot(self.TEST_CONFIG_DICT, obs)
        obs["host_type_specific_metadata"]["base"]["metadata_fields"].pop(
            "sample_name")
        self.assertFalse(self.TEST_CONFIG_DICT == obs)

    # Tests for load_df_with_best_fit_encoding
    def test_load_df_with_best_fit_encoding_utf8(self):
        """Test loading DataFrame with UTF-8 encoding"""
        test_data = "col1,col2\nval1,val2"
        test_file = path.join(self.TEST_DIR, "data/test_utf8.csv")
        with open(test_file, "w", encoding="utf-8") as f:
            f.write(test_data)
        
        try:
            df = load_df_with_best_fit_encoding(test_file, ",")
            self.assertEqual(len(df), 1)
            self.assertEqual(df.columns.tolist(), ["col1", "col2"])
            self.assertEqual(df.iloc[0]["col1"], "val1")
            self.assertEqual(df.iloc[0]["col2"], "val2")
        finally:
            if path.exists(test_file):
                os.remove(test_file)

    def test_load_df_with_best_fit_encoding_utf8_sig(self):
        """Test loading DataFrame with UTF-8 with BOM signature encoding"""
        test_data = "col1,col2\nval1,val2"
        test_file = path.join(self.TEST_DIR, "data/test_utf8_sig.csv")
        with open(test_file, "w", encoding="utf-8-sig") as f:
            f.write(test_data)
        
        try:
            df = load_df_with_best_fit_encoding(test_file, ",")
            self.assertEqual(len(df), 1)
            self.assertEqual(df.columns.tolist(), ["col1", "col2"])
            self.assertEqual(df.iloc[0]["col1"], "val1")
            self.assertEqual(df.iloc[0]["col2"], "val2")
        finally:
            if path.exists(test_file):
                os.remove(test_file)

    def test_load_df_with_best_fit_encoding_invalid_file(self):
        """Test loading DataFrame with invalid file"""
        with self.assertRaises(ValueError):
            load_df_with_best_fit_encoding("nonexistent.csv", ",")

    def test_load_df_with_best_fit_encoding_unsupported_encoding(self):
        """Test loading DataFrame with unsupported encoding"""
        test_file = os.path.join(self.TEST_DIR, "data/test.biom")
        
        try:
            with self.assertRaisesRegex(ValueError, "Unable to decode .* with any available encoder"):
                load_df_with_best_fit_encoding(test_file, ",")
        finally:
            if path.exists(test_file):
                os.remove(test_file)

    # Tests for validate_required_columns_exist
    def test_validate_required_columns_exist_empty_df(self):
        empty_df = pandas.DataFrame()
        with self.assertRaisesRegex(ValueError, "test_df missing columns: \\['sample_name', 'sample_type'\\]"):
            validate_required_columns_exist(
                empty_df, ["sample_name", "sample_type"],
                "test_df missing columns")

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

    # Tests for get_extension
    def test_get_extension(self):
        # Test comma separator
        self.assertEqual(get_extension(","), "csv")
        
        # Test tab separator
        self.assertEqual(get_extension("\t"), "txt")
        
        # Test other separators
        self.assertEqual(get_extension(";"), "txt")
        self.assertEqual(get_extension("|"), "txt")

    # Tests for update_metadata_df_field
    def test_update_metadata_df_field_constant_new_field(self):
        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"],
            "new_field": ["bacon", "bacon"]
        })

        update_metadata_df_field(
            working_df, "new_field", "bacon",
            overwrite_non_nans=True)
        assert_frame_equal(exp_df, working_df)

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

    def test_update_metadata_df_field_function_new_field(self):
        def test_func(row, source_fields):
            return f"processed_{row[source_fields[0]]}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", np.nan],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", np.nan],
            "sample_type": ["st1", "st2"],
            "processed": ["processed_s1", "processed_nan"]
        })

        update_metadata_df_field(
            working_df, "processed", test_func,
            ["sample_name"], overwrite_non_nans=True)
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


    def test_update_metadata_df_field_function_multiple_sources(self):
        def test_func(row, source_fields):
            return f"{row[source_fields[0]]}_{row[source_fields[1]]}"

        working_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"]
        })

        exp_df = pandas.DataFrame({
            "sample_name": ["s1", "s2"],
            "sample_type": ["st1", "st2"],
            "combined": ["s1_st1", "s2_st2"]
        })

        update_metadata_df_field(
            working_df, "combined", test_func,
            ["sample_name", "sample_type"], overwrite_non_nans=True)
        assert_frame_equal(exp_df, working_df)



    # Tests for _get_grandparent_dir
    def test__get_grandparent_dir_no_fp(self):
        obs = _get_grandparent_dir()
        self.assertTrue(obs.endswith("qiimp/src/../.."))

    def test__get_grandparent_dir_with_fp(self):
        obs = _get_grandparent_dir("/Users/username/Desktop/hello/world.py")
        self.assertTrue(obs.endswith("Desktop/hello/../.."))
