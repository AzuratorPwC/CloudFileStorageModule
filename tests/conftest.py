
import pytest
def pytest_addoption(parser):
    parser.addoption(
        '--storageaccount-name', action='store', default='https://cloudstorageev.blob.core.windows.net/', help='Base URL for the API tests'
    )
    parser.addoption(
        '--account-key', action='store', default=None, help='Base URL for the API tests'
    )
    
@pytest.fixture
def storageaccount_name(request):
    return request.config.getoption('--storageaccount-name')

@pytest.fixture
def account_key(request):
    return request.config.getoption('--account-key')