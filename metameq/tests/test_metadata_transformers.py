from datetime import datetime
import pandas
import numpy as np
from unittest import TestCase
from metameq.src.metadata_transformers import (
    pass_through,
    transform_input_sex_to_std_sex,
    transform_age_to_life_stage,
    transform_date_to_formatted_date,
    help_transform_mapping,
    standardize_input_sex,
    set_life_stage_from_age_yrs,
    format_a_datetime,
    _get_one_source_field,
    _help_transform_mapping
)


class TestMetadataTransformers(TestCase):
    def setUp(self):
        self.test_row = pandas.Series({
            'sample_name': 'test_sample',
            'patient_sex': 'M',
            'patient_age': 25,
            'start_date': '2023-01-01'
        })

    # Tests for pass_through
    def test_pass_through(self):
        """Test pass_through"""
        result = pass_through(self.test_row, ['patient_sex'])
        self.assertEqual(result, 'M')

    def test_pass_through_err_multiple_source_fields(self):
        """Test pass_through errors with multiple source fields"""
        with self.assertRaisesRegex(ValueError, "pass_through requires exactly one source field"):
            pass_through(self.test_row, ['patient_sex', 'patient_age'])

    def test_pass_through_nan(self):
        """Test pass_through with NaN value"""
        test_row = self.test_row.copy()
        test_row['patient_sex'] = np.nan
        result = pass_through(test_row, ['patient_sex'])
        self.assertTrue(pandas.isna(result))

    # Tests for transform_input_sex_to_std_sex
    def test_transform_input_sex_to_std_sex_male(self):
        """Test transform_input_sex_to_std_sex with male input"""
        result = transform_input_sex_to_std_sex(self.test_row, ['patient_sex'])
        self.assertEqual(result, 'male')

    def test_transform_input_sex_to_std_sex_female(self):
        """Test transform_input_sex_to_std_sex with female input"""
        test_row = self.test_row.copy()
        test_row['patient_sex'] = 'F'
        result = transform_input_sex_to_std_sex(test_row, ['patient_sex'])
        self.assertEqual(result, 'female')

    def test_transform_input_sex_to_std_sex_invalid(self):
        """Test transform_input_sex_to_std_sex with invalid input"""
        test_row = self.test_row.copy()
        test_row['patient_sex'] = 'invalid'
        with self.assertRaisesRegex(ValueError, "Unrecognized sex: invalid"):
            transform_input_sex_to_std_sex(test_row, ['patient_sex'])

    # Tests for transform_age_to_life_stage
    def test_transform_age_to_life_stage_child(self):
        """Test transform_age_to_life_stage with child age"""
        test_row = self.test_row.copy()
        test_row['patient_age'] = 16
        result = transform_age_to_life_stage(test_row, ['patient_age'])
        self.assertEqual(result, 'child')

    def test_transform_age_to_life_stage_adult(self):
        """Test transform_age_to_life_stage with adult age"""
        result = transform_age_to_life_stage(self.test_row, ['patient_age'])
        self.assertEqual(result, 'adult')

    def test_transform_age_to_life_stage_invalid(self):
        """Test transform_age_to_life_stage with invalid age"""
        test_row = self.test_row.copy()
        test_row['patient_age'] = 'invalid'
        with self.assertRaisesRegex(ValueError, "patient_age must be an integer"):
            transform_age_to_life_stage(test_row, ['patient_age'])

    # Tests for transform_date_to_formatted_date
    def test_transform_date_to_formatted_date_valid(self):
        """Test transform_date_to_formatted_date with valid date"""
        result = transform_date_to_formatted_date(self.test_row, ['start_date'])
        self.assertEqual(result, '2023-01-01 00:00')

    def test_transform_date_to_formatted_date_invalid(self):
        """Test transform_date_to_formatted_date with invalid date"""
        test_row = self.test_row.copy()
        test_row['start_date'] = 'invalid'
        with self.assertRaisesRegex(ValueError, "start_date cannot be parsed to a date"):
            transform_date_to_formatted_date(test_row, ['start_date'])

    # Tests for help_transform_mapping
    def test_help_transform_mapping_valid(self):
        """Test help_transform_mapping with valid input"""
        mapping = {'M': '2', 'F': '1'}
        result = help_transform_mapping(self.test_row, ['patient_sex'], mapping)
        self.assertEqual(result, '2')  # 'M' maps to '2' in this test mapping

    def test_help_transform_mapping_invalid(self):
        """Test help_transform_mapping with invalid input"""
        mapping = {'A': '1', 'B': '2'}
        test_row = self.test_row.copy()
        test_row['patient_sex'] = 'C'
        with self.assertRaisesRegex(ValueError, "Unrecognized help_transform_mapping: C"):
            help_transform_mapping(test_row, ['patient_sex'], mapping)

    # Tests for standardize_input_sex
    def test_standardize_input_sex_M(self):
        """Test standardize_input_sex with 'M' input"""
        result = standardize_input_sex('M')
        self.assertEqual(result, 'male')

    def test_standardize_input_sex_m(self):
        """Test standardize_input_sex with 'm' input"""
        result = standardize_input_sex('m')
        self.assertEqual(result, 'male')

    def test_standardize_input_sex_Male(self):
        """Test standardize_input_sex with 'Male' input"""
        result = standardize_input_sex('Male')
        self.assertEqual(result, 'male')

    def test_standardize_input_sex_male(self):
        """Test standardize_input_sex with 'male' input"""
        result = standardize_input_sex('male')
        self.assertEqual(result, 'male')

    def test_standardize_input_sex_MALE(self):
        """Test standardize_input_sex with 'MALE' input"""
        result = standardize_input_sex('MALE')
        self.assertEqual(result, 'male')

    def test_standardize_input_sex_F(self):
        """Test standardize_input_sex with 'F' input"""
        result = standardize_input_sex('F')
        self.assertEqual(result, 'female')

    def test_standardize_input_sex_f(self):
        """Test standardize_input_sex with 'f' input"""
        result = standardize_input_sex('f')
        self.assertEqual(result, 'female')

    def test_standardize_input_sex_Female(self):
        """Test standardize_input_sex with 'Female' input"""
        result = standardize_input_sex('Female')
        self.assertEqual(result, 'female')

    def test_standardize_input_sex_female(self):
        """Test standardize_input_sex with 'female' input"""
        result = standardize_input_sex('female')
        self.assertEqual(result, 'female')

    def test_standardize_input_sex_FEMALE(self):
        """Test standardize_input_sex with 'FEMALE' input"""
        result = standardize_input_sex('FEMALE')
        self.assertEqual(result, 'female')

    def test_standardize_input_sex_intersex(self):
        """Test standardize_input_sex with 'intersex' input"""
        result = standardize_input_sex('intersex')
        self.assertEqual(result, 'intersex')

    def test_standardize_input_sex_INTERSEX(self):
        """Test standardize_input_sex with 'INTERSEX' input"""
        result = standardize_input_sex('INTERSEX')
        self.assertEqual(result, 'intersex')

    def test_standardize_input_sex_prefernottoanswer(self):
        """Test standardize_input_sex with 'prefernottoanswer' input"""
        result = standardize_input_sex('prefernottoanswer')
        self.assertEqual(result, 'not provided')

    def test_standardize_input_sex_PREFERNOTTOANSWER(self):
        """Test standardize_input_sex with 'PREFERNOTTOANSWER' input"""
        result = standardize_input_sex('PREFERNOTTOANSWER')
        self.assertEqual(result, 'not provided')

    def test_standardize_input_sex_invalid(self):
        """Test standardize_input_sex with invalid input"""
        with self.assertRaisesRegex(ValueError, "Unrecognized sex: invalid"):
            standardize_input_sex('invalid')

    def test_standardize_input_sex_nan(self):
        """Test standardize_input_sex with NaN input"""
        result = standardize_input_sex(np.nan)
        self.assertTrue(pandas.isna(result))

    # Tests for set_life_stage_from_age_yrs
    def test_set_life_stage_from_age_yrs_child(self):
        """Test set_life_stage_from_age_yrs with child age"""
        result = set_life_stage_from_age_yrs(16)
        self.assertEqual(result, 'child')

    def test_set_life_stage_from_age_yrs_adult(self):
        """Test set_life_stage_from_age_yrs with adult age"""
        result = set_life_stage_from_age_yrs(17)
        self.assertEqual(result, 'adult')

    def test_set_life_stage_from_age_yrs_nan(self):
        """Test set_life_stage_from_age_yrs with NaN input"""
        result = set_life_stage_from_age_yrs(np.nan)
        self.assertTrue(pandas.isna(result))

    def test_set_life_stage_from_age_yrs_invalid(self):
        """Test set_life_stage_from_age_yrs with invalid age"""
        with self.assertRaisesRegex(ValueError, "input must be an integer"):
            set_life_stage_from_age_yrs('twelve')

    # Tests for format_a_datetime
    def test_format_a_datetime_valid(self):
        """Test format_a_datetime with valid date"""
        result = format_a_datetime('2023-01-01')
        self.assertEqual(result, '2023-01-01 00:00')

    def test_format_a_datetime_invalid(self):
        """Test format_a_datetime with invalid date"""
        with self.assertRaisesRegex(ValueError, "input cannot be parsed to a date"):
            format_a_datetime('invalid')

    def test_format_a_datetime_invalid_w_custom_source_name(self):
        """Test format_a_datetime with invalid date"""
        with self.assertRaisesRegex(ValueError, "my_date cannot be parsed to a date"):
            format_a_datetime('invalid', source_name='my_date')

    def test_format_a_datetime_nan(self):
        """Test format_a_datetime with NaN value"""
        result = format_a_datetime(np.nan)
        self.assertTrue(pandas.isna(result))

    def test_format_a_datetime_datetime_obj(self):
        """Test format_a_datetime with datetime object input"""
        dt = datetime(2023, 1, 1, 12, 30, 45)
        result = format_a_datetime(dt)
        self.assertEqual(result, '2023-01-01 12:30')

    # Tests for _get_one_source_field
    def test__get_one_source_field_valid(self):
        """Test _get_one_source_field with valid input"""
        result = _get_one_source_field(self.test_row, ['patient_sex'], 'test')
        self.assertEqual(result, 'M')

    def test__get_one_source_field_multiple_fields(self):
        """Test _get_one_source_field with multiple source fields"""
        with self.assertRaisesRegex(ValueError, "test requires exactly one source field"):
            _get_one_source_field(self.test_row, ['patient_sex', 'patient_age'], 'test')

    # Tests for _help_transform_mapping
    def test__help_transform_mapping_valid(self):
        """Test _help_transform_mapping with valid input"""
        mapping = {'A': '1', 'B': '2'}
        result = _help_transform_mapping('A', mapping)
        self.assertEqual(result, '1')

    def test__help_transform_mapping_invalid(self):
        """Test _help_transform_mapping with invalid input"""
        mapping = {'A': '1', 'B': '2'}
        with self.assertRaisesRegex(ValueError, "Unrecognized value: C"):
            _help_transform_mapping('C', mapping)

    def test__help_transform_mapping_nan(self):
        """Test _help_transform_mapping with NaN value"""
        mapping = {'A': '1', 'B': '2'}
        result = _help_transform_mapping(np.nan, mapping)
        self.assertTrue(pandas.isna(result))

    def test__help_transform_mapping_make_lower(self):
        """Test _help_transform_mapping with make_lower=True"""
        mapping = {'a': '1', 'b': '2'}
        result = _help_transform_mapping('A', mapping, make_lower=True)
        self.assertEqual(result, '1')
