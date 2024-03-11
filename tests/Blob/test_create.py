import pytest

from cloud_storage_pwc.AzureStorage.Exceptions import StorageAccountNotFound,StorageAccountAuthenticationError
from cloud_storage_pwc.AzureStorage import Blob
from cloud_storage_pwc import azure_storage

#@pytest.mark.order(2)
#@pytest.mark.dependency()

class TestBlobClass:
    def test_create_ok(self):
        """Sample test function"""
        account = azure_storage(pytest.blob_account,pytest.blob_account_key)
        assert isinstance(account , Blob)

    def create_notexistsblob(self):
        """Sample test function"""
        account = azure_storage("accountinvalid","invalidpass")

    #def test_notexistsblob(self):
    #    """Sample test function"""
    #    with pytest.raises(StorageAccountNotFound):
    #        self.create_notexistsblob()
    
    def create_invalidpass(self):
        """Sample test function"""
        account = azure_storage(pytest.blob_account,"invalidpass")

    #def test_invalidpass(self):
    #    """Sample test function"""
    #    with pytest.raises(StorageAccountAuthenticationError):
    #        self.create_invalidpass()
