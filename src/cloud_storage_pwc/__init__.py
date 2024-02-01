
'''usefull '''

from .AzureStorage.Blob import Blob
from .AzureStorage.DataLake import DataLake
import logging




def AzureStorage(accountname_or_url:str,accessKey:str=None,tenantId:str=None,applicationId:str=None,applicationSecret:str=None):
    create_object =None
    if "dfs.core.windows.net" in accountname_or_url and accountname_or_url.startswith("https://"):
        create_object = DataLake(accountname_or_url,accessKey,tenantId,applicationId,applicationSecret)
    elif ("blob.core.windows.net" in accountname_or_url and accountname_or_url.startswith("https://") ) or ("127.0.0.1:10000" in accountname_or_url and accountname_or_url.startswith("http://")):
        create_object = Blob(accountname_or_url,accessKey,tenantId,applicationId,applicationSecret)
    else:
        try:
            create_object = Blob(f"https://{accountname_or_url}.blob.core.windows.net/",accessKey,tenantId,applicationId,applicationSecret)
            if create_object._check_is_blob():
                raise Exception("DataLake not Blob")   
        except Exception as e :
            create_object_message =str(e)
            try:
                create_object = DataLake(f"https://{accountname_or_url}.dfs.core.windows.net/",accessKey,tenantId,applicationId,applicationSecret)
            except Exception as e:
                create_object_message = str(e)          
                raise Exception(create_object_message)
    return create_object
    
if __name__ == '__main__':
    pass