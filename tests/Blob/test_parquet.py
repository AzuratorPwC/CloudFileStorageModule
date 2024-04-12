from io import BytesIO
from azure.storage.blob import BlobServiceClient
import pytest
import pandas as pd
from cloud_storage_pwc import azure_storage

class TestReadParquetClass:

    def test_save_dataframe_polars_as_parquet(self):
        
        container_name = "parquet1check"

        aa = azure_storage(pytest.blob_account,pytest.blob_account_key)

        #create dataframe
        data = [['tom', 10], ['nick', 15], ['juli', 14]]
        df_uploaded = pd.DataFrame(data, columns=['Name', 'Age'])

        #upload dataframe using aa
        aa.create_container(container_name)
        aa.save_dataframe_as_parquet(df_uploaded,container_name,'','test_parquet.parquet',engine='polars')

        #download parquet using pandas
        account2=BlobServiceClient(account_url=f"https://{pytest.blob_account}.blob.core.windows.net/",credential=pytest.blob_account_key)
        container= account2.get_container_client(container_name)
        blob = container.get_blob_client('test_parquet.parquet')
        download = blob.download_blob()
        download_bytes = download.readall()
        df_downloaded = pd.read_parquet(BytesIO(download_bytes))

        #remove files
        aa.delete_file(container_name,'','test_Parquet.Parquet')
        aa.delete_container(container_name)

        #check differences
        assert df_uploaded.equals(df_downloaded)

    
    def test_save_dataframe_pandas_as_parquet(self):
        
        container_name = "parquet2check"

        aa = azure_storage(pytest.blob_account,pytest.blob_account_key)

        #create dataframe
        data = [['tom', 10], ['nick', 15], ['juli', 14]]
        df_uploaded = pd.DataFrame(data, columns=['Name', 'Age'])

        #upload dataframe using aa
        aa.create_container(container_name)
        aa.save_dataframe_as_parquet(df_uploaded,container_name,'','test_parquet.parquet',engine='pandas')

        #download parquet using pandas
        account2=BlobServiceClient(account_url=f"https://{pytest.blob_account}.blob.core.windows.net/",credential=pytest.blob_account_key)
        container= account2.get_container_client(container_name)
        blob = container.get_blob_client('test_parquet.parquet')
        download = blob.download_blob()
        download_bytes = download.readall()
        df_downloaded = pd.read_parquet(BytesIO(download_bytes))

        #remove files
        aa.delete_file(container_name,'','test_Parquet.Parquet')
        aa.delete_container(container_name)

        #check differences
        assert df_uploaded.equals(df_downloaded)

