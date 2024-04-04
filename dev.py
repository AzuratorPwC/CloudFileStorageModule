from io import StringIO
from azure.storage.blob import BlobServiceClient
import pytest
import pandas as pd
from cloud_storage_pwc import azure_storage
pytest.blob_account = "strfunpbilandingdev"
pytest.blob_account_key = "YoCndTiLJAGGmH/KsgwDTsm/Ly4ERapgCp6Q5Ftc4wQqpu8r92GaFutVMoHUuF36QSt2oapnPkeA+ASt8ZGe6A=="

        
container_name = "csv1check"
        
aa = azure_storage(pytest.blob_account,access_key=pytest.blob_account_key)

data=[{"col1":"art"},{"col1":"bart"},{"col1":"cart"},{"col1":"dart"},{"col1":"eart"}]

aa.save_dataframe_as_xlsx(data,"devv22",'aa',file_name="mojplik.xlsx",sheet_name="Sjeeet1",header=True, engine="pandas")

#aa.create_container("devv22")
#aa.create_container('wolny','Public')
#print(aa.file_exists("bryndza001",'','plikcsv.txt'))
#a = aa.read_excel_file('wolny','','Book1.xlsx')
#print(a['Sheet1'])
