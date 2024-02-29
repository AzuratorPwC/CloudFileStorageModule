import pytest
#from src.cloud_storage_pwc.AzureStorage.Exceptions import StorageAccountNotFound,StorageAccountAuthenticationError
#from src.cloud_storage_pwc.AzureStorage import Blob
from cloud_storage_pwc import azure_storage
from azure.storage.blob import BlobServiceClient


class TestContainerClass:
    """Sample test function"""
    def test_create_simple_container(self):
        """Sample test function"""
        container_name = "create1check"
        account = azure_storage(pytest.blob_account,pytest.blob_account_key)
    
        account.create_container(container_name)
        account.create_container(container_name)
        account2=BlobServiceClient(account_url=f"https://{pytest.blob_account}.blob.core.windows.net/",credential=pytest.blob_account_key) 
        check1 = account2.get_container_client(container_name).exists()
        account2.delete_container(container_name)
        assert check1

    
    def test_create_access_container(self):
        """Sample test function"""
        container_name = "create2check"
        account = azure_storage(pytest.blob_account,pytest.blob_account_key)
        account2=BlobServiceClient(account_url=f"https://{pytest.blob_account}.blob.core.windows.net/",credential=pytest.blob_account_key) 

        account.create_container(container_name,public_access="Private")
        account.create_container(container_name,public_access="Private")
        public_access = account2.get_container_client(container_name).get_container_properties().public_access
        account2.delete_container(container_name)
        assert public_access is None
    
    #container_name = "create3check"
    #account.create_container(container_name,public_access="Blob")
    #account.create_container(container_name,public_access="Blob")
    #public_access = account2.get_container_client(container_name).get_container_properties().public_access
    #account2.delete_container(container_name)
    #assert public_access is 'Blob'
    
    #container_name = "create4check"
    #account.create_container(container_name,public_access="Container")
    #account.create_container(container_name,public_access="Container")
    #public_access = account2.get_container_client(container_name).get_container_properties().public_access
    #account2.delete_container(container_name)
    #assert public_access is "Container"
    

    def test_delete_container(self):
        """Sample test function"""
        container_name = "delete1check"
        account = azure_storage(pytest.blob_account,pytest.blob_account_key)
        account2=BlobServiceClient(account_url=f"https://{pytest.blob_account}.blob.core.windows.net/",credential=pytest.blob_account_key) 
        account2.create_container(container_name)
        account.delete_container(container_name)
        check1 = account2.get_container_client(container_name).exists()
        assert not check1