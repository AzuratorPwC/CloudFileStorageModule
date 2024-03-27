from .AzureStorage import Blob
from .AzureStorage import DataLake
from .AzureStorage.Exceptions import DataLakeCreateError
import logging
from .AzureStorage.Utils import AZURE_CREDENTIAL_TYPES
from azure.identity import DefaultAzureCredential,InteractiveBrowserCredential,DeviceCodeCredential
from azure.identity import ClientSecretCredential

def azure_storage(accountname_or_url:str,access_key:str=None,tenant_id:str=None,application_id:str=None,application_secret:str=None,azure_credential:AZURE_CREDENTIAL_TYPES="DefaultAzureCredential"):
    """
    Creates and returns an object for interacting with Azure Blob Storage or Azure Data Lake Storage based on the provided account name or URL.

    Args:
       | accountname_or_url (str): The account name or URL of the Azure storage account.
       | access_key (str, optional): The access key for the storage account. Defaults to None.
       | tenant_id (str, optional): The tenant ID for authentication. Defaults to None.
       | application_id (str, optional): The application ID for authentication. Defaults to None.
       | application_secret (str, optional): The application secret for authentication. Defaults to None.

    Returns:
       | object: An object for interacting with Azure Blob Storage or Azure Data Lake Storage.
    
    Raises:
       | None 

    """
    create_object = None
    
    
    if ("dfs.core.windows.net" in accountname_or_url and accountname_or_url.startswith("https://")):
        if access_key is not None and tenant_id is None and application_id is None and application_secret is None:
            create_object = DataLake(accountname_or_url,access_key=access_key)
        elif access_key is  None and tenant_id is not None and application_id is not None and application_secret is not None:    
            token_credential = ClientSecretCredential(
                            tenant_id, application_id,application_secret)
            create_object = DataLake(accountname_or_url,credential=token_credential)
        else:
            if azure_credential == "DefaultAzureCredential":
                credential = DefaultAzureCredential()
                create_object = DataLake(accountname_or_url,credential=credential)
            elif azure_credential == "InteractiveBrowserCredential":
                credential = InteractiveBrowserCredential()
            elif azure_credential == 'DeviceCodeCredential':
                credential = DeviceCodeCredential()
            create_object = DataLake(accountname_or_url,credential=credential)
        
    elif ("blob.core.windows.net" in accountname_or_url and accountname_or_url.startswith("https://") ) or ("127.0.0.1:10000" in accountname_or_url and accountname_or_url.startswith("http://")):        
        if access_key is not None and tenant_id is None and application_id is None and application_secret is None:
            create_object = Blob(accountname_or_url,access_key=access_key)
        elif access_key is  None and tenant_id is not None and application_id is not None and application_secret is not None:    
            token_credential = ClientSecretCredential(
                            tenant_id, application_id,application_secret)
            create_object = Blob(accountname_or_url,credential=token_credential)
        else:
            if azure_credential == "DefaultAzureCredential":
                credential = DefaultAzureCredential()
            elif azure_credential == "InteractiveBrowserCredential":
                credential = InteractiveBrowserCredential()
            elif azure_credential == 'DeviceCodeCredential':
                credential = DeviceCodeCredential()
            create_object = Blob(accountname_or_url,credential=credential)
    

    else:
        if access_key is not None and tenant_id is None and application_id is None and application_secret is None:
        
            #create_object = Blob(accountname_or_url,access_key=access_key)
            try:
                create_object = DataLake(f"https://{accountname_or_url}.dfs.core.windows.net/",access_key=access_key)
            except DataLakeCreateError:
                create_object = Blob(f"https://{accountname_or_url}.blob.core.windows.net/",access_key=access_key)
        
        
        elif access_key is  None and tenant_id is not None and application_id is not None and application_secret is not None:   
            token_credential = ClientSecretCredential(
                            tenant_id, application_id,application_secret)
            try:
                create_object = DataLake(f"https://{accountname_or_url}.dfs.core.windows.net/",credential=token_credential)
            except DataLakeCreateError:
                create_object = Blob(f"https://{accountname_or_url}.blob.core.windows.net/",credential=token_credential)
        else:
            if azure_credential == "DefaultAzureCredential":
                credential = DefaultAzureCredential()
            elif azure_credential == "InteractiveBrowserCredential":
                credential = DeviceCodeCredential()
            elif azure_credential == 'DeviceCodeCredential':
                credential = DeviceCodeCredential()
            try:
                create_object = DataLake(f"https://{accountname_or_url}.dfs.core.windows.net/",credential=credential)
            except DataLakeCreateError:
                create_object = Blob(f"https://{accountname_or_url}.blob.core.windows.net/",credential=credential)
            
    logging.info(f"Created {create_object.__class__.__name__} object for interacting with Azure Storage.")
    return create_object
    
if __name__ == '__main__':
    pass