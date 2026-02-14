import pytest


def pytest_addoption(parser):
    parser.addoption("--update-golden", action="store_true", default=False,
                     help="Regenerate golden output files for type pair tests")


@pytest.fixture
def update_golden(request):
    return request.config.getoption("--update-golden")
