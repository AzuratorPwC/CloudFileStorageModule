from .AzureStorage import Blob
from .AzureStorage import DataLake
import logging

class AzureStorageConnectionError(Exception,):
    """Exception raised for errors in the input salary.

    Attributes:
        salary -- input salary which caused the error
        message -- explanation of the error
    """

    def __init__(self, name):
        self.message = "Blob/DataLake"
        super().__init__(self.message)


class AzureStorageBlobConnectionError(Exception):
    "Raised when the input value is less than 18"


def azure_storage(accountname_or_url:str,access_key:str=None,tenant_id:str=None,application_id:str=None,application_secret:str=None):
    """
    Creates and returns an object for interacting with Azure Blob Storage or Azure Data Lake Storage based on the provided account name or URL.

    Args:
        accountname_or_url (str): The account name or URL of the Azure storage account.
        access_key (str, optional): The access key for the storage account. Defaults to None.
        tenant_id (str, optional): The tenant ID for authentication. Defaults to None.
        application_id (str, optional): The application ID for authentication. Defaults to None.
        application_secret (str, optional): The application secret for authentication. Defaults to None.

    Returns:
        object: An object for interacting with Azure Blob Storage or Azure Data Lake Storage.

    """
    create_object = None
    
    if "dfs.core.windows.net" in accountname_or_url and accountname_or_url.startswith("https://"):
        logging.info("azure_storage-create Datalake %s",accountname_or_url)
        create_object = DataLake(accountname_or_url,access_key,tenant_id,application_id,application_secret)
    elif ("blob.core.windows.net" in accountname_or_url and accountname_or_url.startswith("https://") ) or ("127.0.0.1:10000" in accountname_or_url and accountname_or_url.startswith("http://")):
        logging.info("azure_storage-create Blob %s",accountname_or_url)
        create_object = Blob(accountname_or_url,access_key,tenant_id,application_id,application_secret)
    else:
        logging.info("azure_storage-create Blob %s",accountname_or_url)
        create_object = Blob(f"https://{accountname_or_url}.blob.core.windows.net/",access_key,tenant_id,application_id,application_secret)
        if create_object.check_is_dfs():
            logging.info("azure_storage-create Datalake %s",accountname_or_url)
            create_object = DataLake(f"https://{accountname_or_url}.dfs.core.windows.net/",access_key,tenant_id,application_id,application_secret)       
    return create_object
    
if __name__ == '__main__':
    pass