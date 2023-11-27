from src.cloud_storage_pwc.Blob import Blob
from src.cloud_storage_pwc.DataLake import DataLake



 
def create_cloudstorage_reference(url:str,accessKey:str=None,tenantId:str=None,applicationId:str=None,applicationSecret:str=None):
    """
    Return a list of random ingredients as strings.

    :param kind: Optional "kind" of ingredients.
    :type kind: list[str] or None
    :raise lumache.InvalidKindError: If the kind is invalid.
    :return: The ingredients list.
    :rtype: list[str]
    """
    create_object =None
    if "dfs.core.windows.net" in url and url.startswith("https://"):
        create_object = DataLake(url,accessKey,tenantId,applicationId,applicationSecret)
    elif ("blob.core.windows.net" in url and url.startswith("https://") ) or ("127.0.0.1:10000" in url and url.startswith("http://")):
        create_object = Blob(url,accessKey,tenantId,applicationId,applicationSecret)
    else:
        raise Exception("Bledne sciezka do StorageAccount")
    
    
    if create_object is None:
        raise Exception("Obiekt nie istnieje")
    
    return create_object
        
    
