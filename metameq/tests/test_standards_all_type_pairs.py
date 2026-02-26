"""Snapshot tests for extend_metadata_df across all standard host/sample type pairs.

For every non-internal, non-alias (host_type, sample_type) pair defined in the
production standards.yml, this module creates a minimal dummy metadata DataFrame
and runs it through extend_metadata_df with default settings.  The output is
compared against a golden CSV file stored in data/expected_type_pair_outputs/.

To regenerate the golden files after an intentional standards change:
    pytest metameq/tests/test_standards_all_type_pairs.py --update-golden
"""

import glob
import os
import pandas
import pytest
from pandas.testing import assert_frame_equal

from metameq.src.metadata_configurator import build_full_flat_config_dict
from metameq.src.metadata_extender import extend_metadata_df, INTERNAL_COL_KEYS
from metameq.src.util import (
    SAMPLE_NAME_KEY,
    HOSTTYPE_SHORTHAND_KEY,
    SAMPLETYPE_SHORTHAND_KEY,
    HOST_TYPE_SPECIFIC_METADATA_KEY,
    SAMPLE_TYPE_SPECIFIC_METADATA_KEY,
    METADATA_FIELDS_KEY,
    SAMPLE_TYPE_KEY,
    DEFAULT_KEY,
)

GOLDEN_DIR = os.path.join(
    os.path.dirname(__file__), "data", "expected_type_pair_outputs")


def _get_all_type_pairs():
    """Return sorted list of (host_type, sample_type) tuples for all
    non-internal, non-alias pairs in the production standards."""
    config = build_full_flat_config_dict(
        None, None, None, exclude_internals=True)
    hosts = config[HOST_TYPE_SPECIFIC_METADATA_KEY]

    pairs = []
    for host_type, host_dict in hosts.items():
        sample_types = host_dict.get(SAMPLE_TYPE_SPECIFIC_METADATA_KEY, {})
        for sample_type, sample_dict in sample_types.items():
            # Detect aliases: after resolution, an alias's sample_type
            # metadata field has a default value different from its key name.
            metadata_fields = sample_dict.get(METADATA_FIELDS_KEY, {})
            st_field = metadata_fields.get(SAMPLE_TYPE_KEY, {})
            st_default = st_field.get(DEFAULT_KEY)
            if st_default != sample_type:
                continue  # skip alias
            pairs.append((host_type, sample_type))

    return sorted(pairs)


def _make_pair_id(host_type, sample_type):
    """Build a filesystem-safe identifier for a (host, sample) pair."""
    return f"{host_type}__{sample_type}"


def _build_dummy_df(host_type, sample_type):
    """Create a minimal 1-row metadata DataFrame for the given pair."""
    return pandas.DataFrame({
        SAMPLE_NAME_KEY: [_make_pair_id(host_type, sample_type)],
        HOSTTYPE_SHORTHAND_KEY: [host_type],
        SAMPLETYPE_SHORTHAND_KEY: [sample_type],
    })


def _golden_path(host_type, sample_type):
    """Return the path to the golden CSV for a (host, sample) pair."""
    return os.path.join(
        GOLDEN_DIR, f"{_make_pair_id(host_type, sample_type)}.csv")


_ALL_PAIRS = _get_all_type_pairs()


@pytest.mark.parametrize(
    "host_type,sample_type",
    _ALL_PAIRS,
    ids=[f"{h}-{s}" for h, s in _ALL_PAIRS],
)
def test_extend_type_pair(host_type, sample_type, update_golden):
    raw_df = _build_dummy_df(host_type, sample_type)

    # Note that we are not even looking at the validation
    # messages--there will almost certainly be some for fields
    # that require user input, which we aren't trying to mock.
    result_df, _ = extend_metadata_df(raw_df, None)

    # Drop internal columns (hosttype_shorthand, sampletype_shorthand,
    # qc_note) — these are metameq internals, not part of the output.
    result_df = result_df.drop(columns=INTERNAL_COL_KEYS)

    golden_fp = _golden_path(host_type, sample_type)

    if update_golden:
        os.makedirs(GOLDEN_DIR, exist_ok=True)
        result_df.to_csv(golden_fp, index=False)
        pytest.skip("golden file updated")

    assert os.path.exists(golden_fp), (
        f"Golden file not found: {golden_fp}\n"
        f"Run with --update-golden to generate it.")

    expected_df = pandas.read_csv(golden_fp, dtype=str, keep_default_na=False)

    assert_frame_equal(result_df, expected_df)


def test_no_stale_golden_files():
    """Fail if the golden directory contains CSVs that don't correspond to any
    current (host_type, sample_type) pair.  This catches leftovers from
    removed type pairs without requiring --update-golden."""
    expected_filenames = {
        f"{_make_pair_id(h, s)}.csv" for h, s in _ALL_PAIRS
    }
    actual_filenames = {
        os.path.basename(fp)
        for fp in glob.glob(os.path.join(GOLDEN_DIR, "*.csv"))
    }
    stale = sorted(actual_filenames - expected_filenames)
    assert not stale, (
        "Stale golden files found (no matching type pair in standards):\n"
        + "\n".join(f"  {f}" for f in stale)
        + "\nRun with --update-golden to regenerate."
    )
