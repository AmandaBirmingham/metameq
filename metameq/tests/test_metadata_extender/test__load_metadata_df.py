import os.path as path
import tempfile
import pandas
from pandas.testing import assert_frame_equal
from metameq.src.util import \
    SAMPLE_NAME_KEY, \
    HOSTTYPE_SHORTHAND_KEY, \
    SAMPLETYPE_SHORTHAND_KEY
from metameq.src.metadata_extender import _load_metadata_df
from metameq.tests.test_metadata_extender.conftest import \
    ExtenderTestBase


class TestLoadMetadataDf(ExtenderTestBase):
    TEST_METADATA_TSV_FP = path.join(
        ExtenderTestBase.TEST_DIR, "data/test_metadata.tsv")

    EXPECTED_DF = pandas.DataFrame({
        SAMPLE_NAME_KEY: ["sample1", "sample2"],
        HOSTTYPE_SHORTHAND_KEY: ["human", "human"],
        SAMPLETYPE_SHORTHAND_KEY: ["stool", "stool"],
        "dna_extracted": ["TRUE", "FALSE"]
    })

    def test__load_metadata_df_csv(self):
        """Test loading a CSV file."""
        result = _load_metadata_df(self.TEST_METADATA_CSV_FP)
        assert_frame_equal(self.EXPECTED_DF, result)

    def test__load_metadata_df_txt(self):
        """Test loading a tab-delimited TXT file."""
        result = _load_metadata_df(self.TEST_METADATA_TXT_FP)
        assert_frame_equal(self.EXPECTED_DF, result)

    def test__load_metadata_df_tsv(self):
        """Test loading a tab-delimited TSV file."""
        result = _load_metadata_df(self.TEST_METADATA_TSV_FP)
        assert_frame_equal(self.EXPECTED_DF, result)

    def test__load_metadata_df_xlsx(self):
        """Test loading an Excel XLSX file."""
        result = _load_metadata_df(self.TEST_METADATA_XLSX_FP)
        assert_frame_equal(self.EXPECTED_DF, result)

    def test__load_metadata_df_unrecognized_extension_raises(self):
        """Test that an unrecognized file extension raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_fp = path.join(tmpdir, "test.json")
            with open(fake_fp, "w") as f:
                f.write("{}")

            with self.assertRaisesRegex(
                    ValueError, "Unrecognized input file extension"):
                _load_metadata_df(fake_fp)

    def test__load_metadata_df_all_columns_are_strings(self):
        """Test that all columns are loaded as string dtype."""
        result = _load_metadata_df(self.TEST_METADATA_CSV_FP)
        for col in result.columns:
            self.assertEqual("object", str(result[col].dtype))

    def test__load_metadata_df_tsv_matches_txt(self):
        """Test that TSV and TXT produce identical DataFrames."""
        tsv_result = _load_metadata_df(self.TEST_METADATA_TSV_FP)
        txt_result = _load_metadata_df(self.TEST_METADATA_TXT_FP)
        assert_frame_equal(txt_result, tsv_result)
