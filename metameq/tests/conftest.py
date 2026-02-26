import glob
import os

import pytest

GOLDEN_DIR = os.path.join(
    os.path.dirname(__file__), "data", "expected_type_pair_outputs")


def pytest_addoption(parser):
    parser.addoption("--update-golden", action="store_true", default=False,
                     help="Regenerate golden output files for type pair tests")


@pytest.fixture
def update_golden(request):
    return request.config.getoption("--update-golden")


@pytest.fixture(scope="session", autouse=True)
def clean_golden_dir_before_update(request):
    """Remove all existing golden CSVs before regeneration so that stale
    files from removed type pairs do not survive."""
    if not request.config.getoption("--update-golden"):
        return
    for fp in glob.glob(os.path.join(GOLDEN_DIR, "*.csv")):
        os.remove(fp)
