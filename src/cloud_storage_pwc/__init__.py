from .Blob import Blob
from .DataLake import DataLake


if __name__ == '__main__':
    pass




 
def create_cloudstorage_reference(accountname_or_url:str,accessKey:str=None,tenantId:str=None,applicationId:str=None,applicationSecret:str=None):
    """
    Return a list of random ingredients as strings.

    :param kind: Optional "kind" of ingredients.
    :type kind: list[str] or None
    :raise lumache.InvalidKindError: If the kind is invalid.
    :return: The ingredients list.
    :rtype: list[str]
    """
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
                
        #if create_object is None:
        #    try:
        #        create_object = Blob(f"http://127.0.0.1:10000/{accountname_or_url}",accessKey,tenantId,applicationId,applicationSecret)
        #    except:
        #        create_object =None
            
    
    return create_object

