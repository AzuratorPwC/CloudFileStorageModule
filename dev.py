from io import StringIO
from azure.storage.blob import BlobServiceClient
import pytest
import pandas as pd
from cloud_storage_pwc import azure_storage
pytest.blob_account = "devstrpwcpack"
pytest.blob_account_key = "PsNSGXN0zIO8xTOHJe1d0ELmP08f0ktKzHgLxSHmQEPKjmwOzSPT7WdXmudbOyWeEuJBFfmeGopQ+AStfbaGYg=="

        
container_name = "csv1check"
        
aa = azure_storage(pytest.blob_account,pytest.blob_account_key)
#aa.create_container('wolny','Public')
#print(aa.file_exists("bryndza001",'','plikcsv.txt'))
a = aa.read_excel_file('wolny','','Book1.xlsx')
print(a['Sheet1'])
