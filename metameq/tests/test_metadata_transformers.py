from datetime import datetime
import pandas
import numpy as np
from unittest import TestCase
from metameq.src.metadata_transformers import (
    pass_through,
    transform_input_sex_to_std_sex,
    transform_age_to_life_stage,
    transform_date_to_formatted_date,
    transform_format_field_as_int,
    transform_format_field_as_location,
    help_transform_mapping,
    standardize_input_sex,
    set_life_stage_from_age_yrs,
    format_a_datetime,
    _get_one_source_field,
    _help_transform_mapping,
    _format_field_val
)


class TransformerTestBase(TestCase):
    def setUp(self):
        self.test_row = pandas.Series({
            'sample_name': 'test_sample',
            'patient_sex': 'M',
            'patient_age': 25,
            'start_date': '2023-01-01'
        })


class TestPassThrough(TransformerTestBase):
    def test_pass_through(self):
        """Test pass_through"""
        result = pass_through(self.test_row, ['patient_sex'])
        self.assertEqual(result, 'M')

    def test_pass_through_err_multiple_source_fields(self):
        """Test pass_through errors with multiple source fields"""
        with self.assertRaisesRegex(
                ValueError,
                "pass_through requires exactly one source field"):
            pass_through(self.test_row, ['patient_sex', 'patient_age'])

    def test_pass_through_nan(self):
        """Test pass_through with NaN value"""
        test_row = self.test_row.copy()
        test_row['patient_sex'] = np.nan
        result = pass_through(test_row, ['patient_sex'])
        self.assertTrue(pandas.isna(result))


class TestTransformInputSexToStdSex(TransformerTestBase):
    def test_transform_input_sex_to_std_sex_male(self):
        """Test transform_input_sex_to_std_sex with male input"""
        result = transform_input_sex_to_std_sex(
            self.test_row, ['patient_sex'])
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


class TestTransformAgeToLifeStage(TransformerTestBase):
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
        with self.assertRaisesRegex(
                ValueError, "patient_age must be an integer"):
            transform_age_to_life_stage(test_row, ['patient_age'])


class TestTransformDateToFormattedDate(TransformerTestBase):
    def test_transform_date_to_formatted_date_valid(self):
        """Test transform_date_to_formatted_date with valid date"""
        result = transform_date_to_formatted_date(
            self.test_row, ['start_date'])
        self.assertEqual(result, '2023-01-01 00:00')

    def test_transform_date_to_formatted_date_invalid(self):
        """Test transform_date_to_formatted_date with invalid date"""
        test_row = self.test_row.copy()
        test_row['start_date'] = 'invalid'
        with self.assertRaisesRegex(
                ValueError, "start_date cannot be parsed to a date"):
            transform_date_to_formatted_date(test_row, ['start_date'])


class TestHelpTransformMapping(TransformerTestBase):
    def test_help_transform_mapping_valid(self):
        """Test help_transform_mapping with valid input"""
        mapping = {'M': '2', 'F': '1'}
        result = help_transform_mapping(
            self.test_row, ['patient_sex'], mapping)
        self.assertEqual(result, '2')

    def test_help_transform_mapping_invalid(self):
        """Test help_transform_mapping with invalid input"""
        mapping = {'A': '1', 'B': '2'}
        test_row = self.test_row.copy()
        test_row['patient_sex'] = 'C'
        with self.assertRaisesRegex(
                ValueError,
                "Unrecognized help_transform_mapping: C"):
            help_transform_mapping(test_row, ['patient_sex'], mapping)

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


class TestStandardizeInputSex(TestCase):
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


class TestSetLifeStageFromAgeYrs(TestCase):
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


class TestFormatADatetime(TestCase):
    def test_format_a_datetime_valid(self):
        """Test format_a_datetime with valid date"""
        result = format_a_datetime('2023-01-01')
        self.assertEqual(result, '2023-01-01 00:00')

    def test_format_a_datetime_invalid(self):
        """Test format_a_datetime with invalid date"""
        with self.assertRaisesRegex(
                ValueError, "input cannot be parsed to a date"):
            format_a_datetime('invalid')

    def test_format_a_datetime_invalid_w_custom_source_name(self):
        """Test format_a_datetime with invalid date"""
        with self.assertRaisesRegex(
                ValueError, "my_date cannot be parsed to a date"):
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


class TestGetOneSourceField(TransformerTestBase):
    def test__get_one_source_field_valid(self):
        """Test _get_one_source_field with valid input"""
        result = _get_one_source_field(
            self.test_row, ['patient_sex'], 'test')
        self.assertEqual(result, 'M')

    def test__get_one_source_field_multiple_fields(self):
        """Test _get_one_source_field with multiple source fields"""
        with self.assertRaisesRegex(
                ValueError, "test requires exactly one source field"):
            _get_one_source_field(
                self.test_row, ['patient_sex', 'patient_age'], 'test')


class TestFormatFieldVal(TestCase):
    def test__format_field_val_float_to_two_decimals(self):
        """Test formatting a float string to two decimal places."""
        row = pandas.Series({'latitude': '32.8812345678'})
        result = _format_field_val(row, ['latitude'], float, '{0:.2f}')
        self.assertEqual(result, '32.88')

    def test__format_field_val_negative_float(self):
        """Test formatting a negative float string."""
        row = pandas.Series({'longitude': '-117.2345678901'})
        result = _format_field_val(row, ['longitude'], float, '{0:.2f}')
        self.assertEqual(result, '-117.23')

    def test__format_field_val_zero(self):
        """Test formatting zero."""
        row = pandas.Series({'value': '0.0'})
        result = _format_field_val(row, ['value'], float, '{0:.2f}')
        self.assertEqual(result, '0.00')

    def test__format_field_val_g_format_removes_trailing_zeros(self):
        """Test formatting with 'g' format removes trailing zeros."""
        row = pandas.Series({'value': '100.00'})
        result = _format_field_val(row, ['value'], float, '{0:g}')
        self.assertEqual(result, '100')

    def test__format_field_val_nan_returns_nan(self):
        """Test that NaN value is returned as-is."""
        row = pandas.Series({'latitude': np.nan})
        result = _format_field_val(row, ['latitude'], float, '{0:.2f}')
        self.assertTrue(pandas.isna(result))

    def test__format_field_val_cast_failure_returns_original(self):
        """Test that cast failure returns the original value."""
        row = pandas.Series({'value': 'hello'})
        result = _format_field_val(row, ['value'], float, '{0:.2f}')
        self.assertEqual(result, 'hello')

    def test__format_field_val_integer_string(self):
        """Test formatting an integer string."""
        row = pandas.Series({'count': '42'})
        result = _format_field_val(row, ['count'], int, '{0:d}')
        self.assertEqual(result, '42')

    def test__format_field_val_integer_string_no_format(self):
        """Test formatting an integer string."""
        row = pandas.Series({'count': '42'})
        result = _format_field_val(row, ['count'], int, None)
        self.assertEqual(result, '42')

    def test__format_field_val_float_with_int_format_returns_original(self):
        """Test that a float cast with int format returns a string (format fails)."""
        row = pandas.Series({'value': '42.7'})
        result = _format_field_val(row, ['value'], float, '{0:d}')
        # Format fails because {0:d} requires int, so returns string of cast value
        self.assertEqual(result, '42.7')
        self.assertIsInstance(result, str)

    def test__format_field_val_float_to_int_returns_original(self):
        """Test trying to cast a float with nonzero decimal places to an int returns the original string."""
        row = pandas.Series({'value': '42.7'})
        result = _format_field_val(row, ['value'], int, None)
        self.assertEqual(result, '42.7')
        self.assertIsInstance(result, str)

    def test__format_field_val_multiple_source_fields_raises(self):
        """Test that multiple source fields raises ValueError."""
        row = pandas.Series({'a': '1', 'b': '2'})
        with self.assertRaisesRegex(
                ValueError,
                "format_field_val requires exactly one source field"):
            _format_field_val(row, ['a', 'b'], float, '{0:.2f}')

    def test__format_field_val_empty_source_fields_raises(self):
        """Test that empty source fields raises ValueError."""
        row = pandas.Series({'a': '1'})
        with self.assertRaisesRegex(
                ValueError,
                "format_field_val requires exactly one source field"):
            _format_field_val(row, [], float, '{0:.2f}')

    def test__format_field_val_string_true_to_bool_no_format(self):
        """Test casting 'true' string to bool with no format string."""
        row = pandas.Series({'flag': 'true'})
        result = _format_field_val(row, ['flag'], bool, None)
        self.assertEqual(result, 'True')
        self.assertIsInstance(result, str)

    def test__format_field_val_string_false_to_bool_no_format(self):
        """Test casting 'false' string to bool with no format string."""
        row = pandas.Series({'flag': 'false'})
        result = _format_field_val(row, ['flag'], bool, None)
        self.assertEqual(result, 'False')
        self.assertIsInstance(result, str)

    def test__format_field_val_string_yes_to_bool_decimal_format(self):
        """Test casting 'yes' string to bool with decimal format."""
        row = pandas.Series({'flag': 'yes'})
        result = _format_field_val(row, ['flag'], bool, '{0:d}')
        self.assertEqual(result, '1')
        self.assertIsInstance(result, str)

    def test__format_field_val_string_no_to_bool_decimal_format(self):
        """Test casting 'no' string to bool with decimal format."""
        row = pandas.Series({'flag': 'no'})
        result = _format_field_val(row, ['flag'], bool, '{0:d}')
        self.assertEqual(result, '0')
        self.assertIsInstance(result, str)

    def test__format_field_val_string_1_to_bool_no_format(self):
        """Test casting '1' string to bool with no format string."""
        row = pandas.Series({'flag': '1'})
        result = _format_field_val(row, ['flag'], bool, None)
        self.assertEqual(result, 'True')
        self.assertIsInstance(result, str)

    def test__format_field_val_string_0_to_bool_no_format(self):
        """Test casting '0' string to bool with no format string."""
        row = pandas.Series({'flag': '0'})
        result = _format_field_val(row, ['flag'], bool, None)
        self.assertEqual(result, 'False')
        self.assertIsInstance(result, str)

    def test__format_field_val_string_maybe_to_bool_fails(self):
        """Test that non-boolean string returns original when cast to bool fails."""
        row = pandas.Series({'flag': 'maybe'})
        result = _format_field_val(row, ['flag'], bool, None)
        self.assertEqual(result, 'maybe')
        self.assertIsInstance(result, str)

    def test__format_field_val_float_input_to_float_with_format(self):
        """Test formatting a float input to two decimal places."""
        row = pandas.Series({'latitude': 32.8812345678})
        result = _format_field_val(row, ['latitude'], float, '{0:.2f}')
        self.assertEqual(result, '32.88')
        self.assertIsInstance(result, str)

    def test__format_field_val_int_input_to_int_with_format(self):
        """Test formatting an int input with decimal format."""
        row = pandas.Series({'count': 42})
        result = _format_field_val(row, ['count'], int, '{0:d}')
        self.assertEqual(result, '42')
        self.assertIsInstance(result, str)

    def test__format_field_val_int_input_to_int_no_format(self):
        """Test casting an int input to int with no format string."""
        row = pandas.Series({'count': 42})
        result = _format_field_val(row, ['count'], int, None)
        self.assertEqual(result, '42')
        self.assertIsInstance(result, str)

    def test__format_field_val_int_input_to_float_with_format(self):
        """Test formatting an int input as float to two decimal places."""
        row = pandas.Series({'value': 42})
        result = _format_field_val(row, ['value'], float, '{0:.2f}')
        self.assertEqual(result, '42.00')
        self.assertIsInstance(result, str)

    def test__format_field_val_float_input_to_int_success(self):
        """Test casting a float with zero decimal to int."""
        row = pandas.Series({'value': 42.0})
        result = _format_field_val(row, ['value'], int, '{0:d}')
        self.assertEqual(result, '42')
        self.assertIsInstance(result, str)

    def test__format_field_val_float_input_to_int_fail(self):
        """Test that a float with nonzero decimal returns original as str when cast to int fails."""
        row = pandas.Series({'value': 42.7})
        result = _format_field_val(row, ['value'], int, '{0:d}')
        self.assertEqual(result, '42.7')
        self.assertIsInstance(result, str)

    def test__format_field_val_tiny_float_to_decimal_format(self):
        """Test that a tiny numeric float is formatted to decimal, not scientific notation."""
        # 0.00001 would be '1e-05' with str(), but fixed-point format gives decimal
        row = pandas.Series({'value': 0.00001})
        result = _format_field_val(row, ['value'], float, '{0:.10f}')
        self.assertEqual(result, '0.0000100000')
        self.assertIsInstance(result, str)

    def test__format_field_val_scientific_notation_string_to_decimal_format(self):
        """Test that a scientific notation string is formatted to decimal."""
        row = pandas.Series({'value': '1e-05'})
        result = _format_field_val(row, ['value'], float, '{0:.10f}')
        self.assertEqual(result, '0.0000100000')
        self.assertIsInstance(result, str)


class TestTransformFormatFieldAsInt(TestCase):
    def test_transform_format_field_as_int_valid_int_string(self):
        """Test formatting an integer string."""
        row = pandas.Series({'days': '42'})
        result = transform_format_field_as_int(row, ['days'])
        self.assertEqual(result, '42')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_int_float_with_zero_decimal(self):
        """Test formatting a float string with zero decimal part."""
        row = pandas.Series({'days': '42.0'})
        result = transform_format_field_as_int(row, ['days'])
        self.assertEqual(result, '42')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_int_float_with_nonzero_decimal(self):
        """Test that a float string with nonzero decimal returns original."""
        row = pandas.Series({'days': '42.7'})
        result = transform_format_field_as_int(row, ['days'])
        self.assertEqual(result, '42.7')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_int_nan(self):
        """Test that NaN value is returned as-is."""
        row = pandas.Series({'days': np.nan})
        result = transform_format_field_as_int(row, ['days'])
        self.assertTrue(pandas.isna(result))

    def test_transform_format_field_as_int_invalid_string(self):
        """Test that invalid string returns original."""
        row = pandas.Series({'days': 'hello'})
        result = transform_format_field_as_int(row, ['days'])
        self.assertEqual(result, 'hello')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_int_multiple_source_fields_raises(self):
        """Test that multiple source fields raises ValueError."""
        row = pandas.Series({'a': '1', 'b': '2'})
        with self.assertRaisesRegex(
                ValueError,
                "format_field_val requires exactly one source field"):
            transform_format_field_as_int(row, ['a', 'b'])

    def test_transform_format_field_as_int_numeric_input(self):
        """Test formatting a numeric (non-string) integer input."""
        row = pandas.Series({'days': 42})
        result = transform_format_field_as_int(row, ['days'])
        self.assertEqual(result, '42')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_int_negative_int(self):
        """Test formatting a negative integer string."""
        row = pandas.Series({'days': '-5'})
        result = transform_format_field_as_int(row, ['days'])
        self.assertEqual(result, '-5')
        self.assertIsInstance(result, str)


class TestTransformFormatFieldAsLocation(TestCase):
    def test_transform_format_field_as_location_valid_float_string(self):
        """Test formatting a float string as location."""
        row = pandas.Series({'latitude': '32.8812345678'})
        result = transform_format_field_as_location(row, ['latitude'])
        self.assertEqual(result, '32.8812345678')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_location_negative_float(self):
        """Test formatting a negative float string as location."""
        row = pandas.Series({'longitude': '-117.234567890'})
        result = transform_format_field_as_location(row, ['longitude'])
        self.assertEqual(result, '-117.23456789')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_location_integer_string(self):
        """Test formatting an integer string as location."""
        row = pandas.Series({'elevation': '100'})
        result = transform_format_field_as_location(row, ['elevation'])
        self.assertEqual(result, '100')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_location_small_decimal(self):
        """Test formatting a small decimal value as location."""
        row = pandas.Series({'value': '0.00012345678'})
        result = transform_format_field_as_location(row, ['value'])
        self.assertEqual(result, '0.00012345678')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_location_nan(self):
        """Test that NaN value is returned as-is."""
        row = pandas.Series({'latitude': np.nan})
        result = transform_format_field_as_location(row, ['latitude'])
        self.assertTrue(pandas.isna(result))

    def test_transform_format_field_as_location_invalid_string(self):
        """Test that invalid string returns original."""
        row = pandas.Series({'latitude': 'unknown'})
        result = transform_format_field_as_location(row, ['latitude'])
        self.assertEqual(result, 'unknown')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_location_multiple_source_fields_raises(self):
        """Test that multiple source fields raises ValueError."""
        row = pandas.Series({'lat': '32.88', 'lon': '-117.23'})
        with self.assertRaisesRegex(
                ValueError,
                "format_field_val requires exactly one source field"):
            transform_format_field_as_location(row, ['lat', 'lon'])

    def test_transform_format_field_as_location_numeric_input(self):
        """Test formatting a numeric (non-string) float input."""
        row = pandas.Series({'latitude': 32.8812345678})
        result = transform_format_field_as_location(row, ['latitude'])
        self.assertEqual(result, '32.8812345678')
        self.assertIsInstance(result, str)

    def test_transform_format_field_as_location_large_value(self):
        """Test formatting a large value as location."""
        row = pandas.Series({'elevation': '12345.6789'})
        result = transform_format_field_as_location(row, ['elevation'])
        self.assertEqual(result, '12345.6789')
        self.assertIsInstance(result, str)
