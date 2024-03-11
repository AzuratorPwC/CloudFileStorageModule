from io import StringIO
from azure.storage.blob import BlobServiceClient
import pytest
import pandas as pd
from cloud_storage_pwc import azure_storage

class TestReadCsvClass:

    def test_save_dataframe_polars_as_csv(self):
        
        container_name = "csv1check"
        
        aa = azure_storage(pytest.blob_account,pytest.blob_account_key)

        #create dataframe
        data = {'col 1': [1, 2, 3],
                    'col 2': ['A', 'B', 'C'],
                    'col 3': ['#', '$', '*']}
        df_uploaded = pd.DataFrame(data)

        #upload dataframe using aa
        aa.create_container(container_name)
        aa.save_dataframe_as_csv(df_uploaded,container_name,'','test_csv.csv',engine='polars',delimiter=';',is_first_row_as_header=True)

        #download csv using pandas
        account2=BlobServiceClient(account_url=f"https://{pytest.blob_account}.blob.core.windows.net/",credential=pytest.blob_account_key)
        container= account2.get_container_client(container_name)
        blob = container.get_blob_client('test_csv.csv')
        download = blob.download_blob()
        download_bytes = download.readall()
        decoded = download_bytes.decode('utf-8')
        df_downloaded = pd.read_csv(StringIO(decoded), sep=';')

        #remove files
        aa.delete_file(container_name,'','test_csv.csv')
        aa.delete_container(container_name)

        #check differences
        assert df_uploaded.equals(df_downloaded)

    
    def test_save_dataframe_pandas_as_csv(self):
        
        container_name = "csv2check"
        
        aa = azure_storage(pytest.blob_account,pytest.blob_account_key)

        #create dataframe
        data = {'1': ['col 1', 'A','#'],
                    '2': ['col 2', 'B', 'C'],
                    '3': ['col 3', '$', '*']}
        df_uploaded = pd.DataFrame(data)

        #upload dataframe using aa
        aa.create_container(container_name)
        aa.save_dataframe_as_csv(df_uploaded,container_name,'','test_csv.csv',engine='pandas',delimiter=';',is_first_row_as_header=False)

        # apply first row as a header in df_uploaded
        df_uploaded.columns = df_uploaded.iloc[0].tolist()
        df_uploaded = df_uploaded[1:].reset_index(drop=True)

        #download csv using pandas
        account2=BlobServiceClient(account_url=f"https://{pytest.blob_account}.blob.core.windows.net/",credential=pytest.blob_account_key)
        container= account2.get_container_client(container_name)
        blob = container.get_blob_client('test_csv.csv')
        download = blob.download_blob()
        download_bytes = download.readall()
        decoded = download_bytes.decode('utf-8')
        df_downloaded = pd.read_csv(StringIO(decoded), sep=';')

        #remove files
        aa.delete_file(container_name,'','test_csv.csv')
        aa.delete_container(container_name)

        #check differences
        assert df_uploaded.equals(df_downloaded)
            
        

