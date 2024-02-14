from tabnanny import check
from src.cloud_storage_pwc import azure_storage
from azure.storage.blob import BlobServiceClient
import pytest

def test_create_empty_file():
    """Sample test function"""
    container_name = "foldercheck1"
    account = azure_storage(pytest.blob_account,pytest.blob_account_key)
    account2=BlobServiceClient(account_url=f"https://{pytest.blob_account}.blob.core.windows.net/",credential=pytest.blob_account_key)
    account.create_container(container_name)
    account.create_empty_file(container_name,"","file1.txt")
    account.create_empty_file(container_name,"folder1","file1.txt")
    file1=account2.get_blob_client(container_name,"file1.txt")
    check1 = file1.exists()
    file2=account2.get_blob_client(container_name,"folder1/file1.txt")
    check2 = file2.exists()
    account.delete_container(container_name)
    assert check1 and check2
    