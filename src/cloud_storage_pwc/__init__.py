from src.cloud_storage_pwc.Blob import Blob
from src.cloud_storage_pwc.DataLake import DataLake



 
def create_cloudstorage_reference(url:str,accessKey:str=None,tenantId:str=None,applicationId:str=None,applicationSecret:str=None):
    """
    Create a cloud storage reference object based on the provided URL and credentials.

    :param url: The URL of the cloud storage.
    :type url: str
    :param accessKey: The access key for the cloud storage (optional).
    :type accessKey: str or None
    :param tenantId: The tenant ID for authentication (optional).
    :type tenantId: str or None
    :param applicationId: The application ID for authentication (optional).
    :type applicationId: str or None
    :param applicationSecret: The application secret for authentication (optional).
    :type applicationSecret: str or None
    :return: The cloud storage reference object.
    :rtype: DataLake or Blob
    :raise Exception: If the URL is invalid or the cloud storage object cannot be created.
    """
    create_object = None
    if "dfs.core.windows.net" in url and url.startswith("https://"):
        create_object = DataLake(url, accessKey, tenantId, applicationId, applicationSecret)
    elif ("blob.core.windows.net" in url and url.startswith("https://")) or ("127.0.0.1:10000" in url and url.startswith("http://")):
        create_object = Blob(url, accessKey, tenantId, applicationId, applicationSecret)
    else:
        raise Exception("Invalid StorageAccount URL")
    
    if create_object is None:
        raise Exception("Object does not exist")
    
    return create_object
    
