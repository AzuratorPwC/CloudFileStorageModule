import os
import time
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import HttpResponseError,ResourceNotFoundError,ResourceExistsError
from ..Exceptions import *
from ..StorageUtillity import StorageUtillity
from ..Utils import *

class Blob(StorageUtillity):
    """Class representing Azure Blob Storage"""
    def __init__(self, url:str, access_key:str=None,credential=None):
        super().__init__()
        try:
            if access_key is not None and credential is None:
                self.__service_client = BlobServiceClient(account_url=url, credential=access_key)
            else:
                self.__service_client = BlobServiceClient(account_url=url, credential=credential)
                
            containers = self.__service_client.list_containers()     
            con_num=len(list(containers))
            blob = False
            if con_num > 0:  
                for page in containers.by_page():
                    for con in page:
                        try:                     
                            blobs=self.__service_client.get_container_client(con.name).list_blob_names()
                            b=blobs.by_page().__next__()
                            blob=True
                            break
                        except HttpResponseError:
                            blob = False
                    if blob:
                        break
            if blob is False:
                raise DataLakeCreateError()
        except ResourceNotFoundError as e:
            raise StorageAccountNotFound(f"Storage account {url} not found") from e
        except HttpResponseError as e:
            raise StorageAccountAuthenticationError(f"Storage account {url} authorization error") from e

    def ls_files(self, container_name:str, directory_path:str, recursive:bool=False) -> list:
        """
        List files under a specified path within the Azure Blob Storage container.

        Args:
           | **container_name** (str): The name of the Azure Blob Storage container.
           | **directory_path** (str): The path within the container to list files from.
           | **recursive** (bool, optional): Flag indicating whether to list files recursively. Defaults to False.

        Returns:
            list: A list of file paths relative to the specified directory.

        Raises:
           | ContainerNotFound: If container do not exists.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """

        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
            if not directory_path == '' and not directory_path.endswith('/'):
                directory_path += '/'

            blob_iter = container_client.list_blobs(name_starts_with=directory_path)
            files = []
            for blob in blob_iter:
                relative_path = os.path.relpath(blob.name, directory_path)
                if recursive or not '/' in relative_path:
                    files.append(relative_path)
            return files
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e

    def file_exists(self, container_name : str,directory_path : str,file_name:str)->bool:
        """
        Checks whether the file exists in the Azure Blob Storage.

        Args:
           | **container_name** (str):  The name of the container.
           | **directory_path** (str): The path of the directory where the file is located.
           | **file_name** (str): The name of the file to bo located.

        Returns:
            bool: The information about whether the file exists (True/False). 

        Raises:
           | ContainerNotFound: If container do not exists.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
            if not directory_path == '' and not directory_path.endswith('/'):
                directory_path += '/'
            
            path = directory_path + file_name
            blob_client = container_client.get_blob_client(path)
        
            return blob_client.exists()
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
          
    def folder_exists(self, container_name : str, directory_path : str)->bool:
        """
        Checks whether the folder exists in the Azure Blob Storage.

        Args:
           | **container_name** (str):  The name of the container.
           | **directory_path** (str): The path of the directory where the folder is located.

        Returns:
            bool: The information about whether the folder exists (True/False). 

        Raises:
            None
        """
        files = self.ls_files(container_name,directory_path,False)
        if files:
            return True
        else:
            return False


    def read_binary_file(self, container_name: str, directory_path: str, file_name: str) -> bytes:
        """
        Reads a binary file from the specified container and directory.

        Args:
           | **container_name** (str): The name of the container.
           | **directory_path** (str): The path of the directory where the file is located.
           | **file_name** (str): The name of the file to be read.

        Returns:
            bytes: The content of the binary file.

        Raises:
           | ContainerNotFound: If the specified container does not exist.
           | BlobNotFound: If the specified file does not exist in the container.
           | Exception: If there is an error reading the file.
        """
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
            if not directory_path == '' and not directory_path.endswith('/'):
                directory_path += '/'

            path = directory_path + file_name
            blob_client = container_client.get_blob_client(path)
            if blob_client.exists():
                download = blob_client.download_blob()
                download_bytes = download.readall()
                return download_bytes
            else:
                raise BlobNotFound(f"File {file_name} not found in container {container_name}")
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e


    def save_binary_file(self, input_bytes:bytes, container_name:str, directory_path:str,file_name:str,is_overwrite:bool=True,tries:int=3,bytes_length:int=None):
        """
        Save a binary file to the specified container in the Azure Blob Storage.

        Args:
           | **input_bytes** (bytes): Bytes to be saved.
           | **container_name** (str): The name of the container.
           | **directory_path** (str): The path of the directory where the file is located.
           | **file_name** (str): The name of the file to be saved.
           | **is_overwrite** (bool, optional): Should the file be overwritten? Defaults to True.
        
        Returns:
            None

        Raises:
           | ContainerNotFound: If the specified container does not exist.
           | BlobAlreadyExists: File already exists.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        temp_is_overwrite = True
        for i in range(tries):
            try:
                try:
                    container_client = self.__service_client.get_container_client(container=container_name)
                    if container_client.exists() is False:
                        raise ContainerNotFound(f"Container {container_name} not found")
                    if not directory_path == '' and not directory_path.endswith('/'):
                        directory_path += '/'
                    new_blob_client = container_client.get_blob_client(directory_path + file_name)
                    res=new_blob_client.upload_blob(bytes(input_bytes), overwrite=temp_is_overwrite)
                    
                    
                    check = self.file_exists(container_name,directory_path,file_name)
                    if check is False:
                        temp_is_overwrite = True
                        time.sleep(3)
                        for i in range(tries):
                            check = self.file_exists(container_name,directory_path,file_name)
                            if check:
                                break
                            time.sleep(3)
                            res=new_blob_client.upload_blob(bytes(input_bytes), overwrite=temp_is_overwrite)
                        if check is False:
                            raise FileNotFoundError(f"{container_name}/{directory_path}/{file_name} not found")
                  
                    break
                except ResourceExistsError as e:
                    raise BlobAlreadyExists(f"File {file_name} already exists") from e
                except HttpResponseError as e:
                    raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
            except:
                if i==tries-1:
                    raise
                time.sleep(3)
                continue


    def delete_file(self,container_name : str,directory_path : str,file_name:str,wait:bool=True):
        """
        Deletes a file from the Azure Blob Storage.

        Args:
           | **container_name** (str): The name of the container where the file to be deleted is located.
           | **directory_path** (str): The directory path within the container. Use an empty string for the root directory.
           | **file_name** (str): The name of the file to be deleted.
           | **wait** (bool, optional): The variable controls whether the code should pause briefly before checking the existence of a file in the container. Defaults to True.

        Returns:
            None

        Raises:
           | ContainerNotFound: If container do not exists.
           | BlobNotFound: If file cannot be found in the container.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
            if directory_path =="" or directory_path is None:
                path=file_name
            else:
                path = "/".join( (directory_path,file_name))
            blob_client = container_client.get_blob_client(path)
            if blob_client.exists():
                blob_client.delete_blob(delete_snapshots="include")
        
                if wait:
                    time.sleep(1)
                    blob_client = container_client.get_blob_client(path)
                    check_if_exist = blob_client.exists()
                    while check_if_exist:
                        time.sleep(1)
                        blob_client = container_client.get_blob_client(path)
                        check_if_exist = blob_client.exists()
        except ResourceNotFoundError as e:
            raise BlobNotFound(f"File {file_name} not found in container {container_name}") from e
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
                    
    def delete_files_by_prefix(self,container_name : str,directory_path : str,file_prefix:str, recursive:bool=False,wait:bool=True):
        """
        Deletes files from the Azure Blob Storage based on a prefix.

        Args:
           | **container_name** (str): The name of the container where the files to be deleted are located.
           | **directory_path** (str): The directory path within the container. Use an empty string for the root directory.
           | **file_prefix** (str): The prefix of files to be deleted.
           | **recursive** (bool, optional): Flag indicating whether to list files recursively. Defaults to False.
           | **wait** (bool, optional): The variable controls whether the code should pause briefly before checking the existence of a file in the container. Defaults to True.

        Returns:
            None

        Raises:
            ContainerNotFound: If container do not exists.
        """
        
        container_client = self.__service_client.get_container_client(container=container_name)
        if container_client.exists() is False:
            raise ContainerNotFound(f"Container {container_name} not found")

        files_exists = self.ls_files(container_name,directory_path,recursive)
        files = [f for f in files_exists if f.split("/")[-1].startswith(file_prefix)]
        for f in files:
            blob_client = container_client.get_blob_client(directory_path+"/"+f)
            blob_client.delete_blob(delete_snapshots="include")
        if wait and files:
            files_exists = self.ls_files(container_name,directory_path,recursive)
            files = [f for f in files_exists if f.split("/")[-1].startswith(file_prefix)]
            time.sleep(1)
            while files:
                time.sleep(1)
                files_exists = self.ls_files(container_name,directory_path,recursive)
                files = [f for f in files_exists if f.split("/")[-1].startswith(file_prefix)]
        
    def delete_folder(self,container_name : str,directory_path : str,wait:bool=True):
        """
        Deletes a folder and its contents from the Azure Blob Storage.

        Args:
           | **container_name** (str): The name of the container where the folder to be deleted is located.
           | **directory_path** (str): The directory path to the folder within the container.
           | **wait** (bool, optional): The variable controls whether the code should pause briefly before checking the existence of a file in the container. Defaults to True.

        Returns:
            None

        Raises:
            None

        """
        
        list_files = self.ls_files(container_name,directory_path,True)
        for f in list_files:
            path = directory_path + "/" + f.replace("\\","/")
            self.delete_file(container_name,path[0:path.rfind("/")],path[path.rfind("/")+1:len(path)])
        
        if wait and len(list_files)>0:
            time.sleep(2)
            list_files = self.ls_files(container_name,directory_path,True)
            while len(list_files)>0:
                time.sleep(2)
                list_files = self.ls_files(container_name,directory_path,True)
                        
    def move_file(self,container_name : str,directory_path : str,file_name:str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_file:bool=False):
        """
        Moves a file from one location to another within the Azure Blob Storage.

        Args:
           | **container_name** (str): The name of the container where the file to be moved is located.
           | **directory_path** (str): The directory path within the container. Use an empty string for the root directory.
           | **file_name** (str): The name of the file to be moved.
           | **new_container_name** (str): The name of the container where the file to be moved should be located.
           | **new_directory_path** (str):  The directory path within the container where the file to be moved should be located.
           | **is_overwrite** (bool, optional): Should the file be overwritten? Defaults to True.
           | **is_delete_source_file** (bool, optional): Should the source file be deleted?. Defaults to False.
        
        Returns:
            None

        Raises:
           None
        """

        self.save_binary_file(self.read_binary_file(container_name,directory_path,file_name),new_container_name,new_directory_path,file_name,is_overwrite)
        if is_delete_source_file:
            self.delete_file(container_name ,directory_path ,file_name)

            
    def move_folder(self,container_name : str,directory_path : str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_folder:bool=False):
        """
        Moves all files from one folder to another within the Azure Blob Storage.

        Args:
           | **container_name** (str): The name of the container where the folder to be moved is located.
           | **directory_path** (str): The directory path within the container. Use an empty string for the root directory.
           | **new_container_name** (str): The name of the container where the folder to be moved should be located.
           | **new_directory_path** (str):  The directory path within the container where the folder to be moved should be located.
           | **is_overwrite** (bool, optional): Should the folder be overwritten? Defaults to True.
           | **is_delete_source_folder** (bool, optional): Should the source folder be deleted? Defaults to False.

        Returns:
            None

        Raises:
           None
        """

        list_files = self.ls_files(container_name,directory_path,True)

        for i in list_files:
            self.move_file(container_name,directory_path,i,new_container_name,new_directory_path,is_overwrite,is_delete_source_folder)

        
    def rename_file(self,container_name : str,directory_path : str,file_name:str,newfile_name:str):
        """
        Renames a file within a specified container in the Azure Blob Storage.

        Args:
           | **container_name** (str): The name of the container where the file to be renamed is located.
           | **directory_path** (str): The directory path within the container.
           | **file_name** (str): The name of the file to be changed.
           | **newfile_name** (str): New file name.

        Returns:
            None

        Raises:
            None
        """
        self.save_binary_file(self.read_binary_file(container_name,directory_path,file_name),container_name,directory_path,newfile_name,False)
        self.delete_file(container_name ,directory_path ,file_name)
  
    def rename_folder(self,container_name : str,directory_path : str,new_directory_path:str):
        """
        Renames a folder within a specified container in the Azure Blob Storage.

        Args:
           | **container_name** (str): The name of the container where the folder to be renamed is located.
           | **directory_path** (str): The directory path within the container.
           | **new_directory_path** (str): New directory path within the container.

        Returns:
            None

        Raises:
            None
        """
        list_files = self.ls_files(container_name,directory_path,True)
        for i in list_files:
            self.move_file(container_name,directory_path,i,container_name,new_directory_path,False,True)
        
    def create_empty_file(self,container_name : str,directory_path:str,file_name:str):
        """
        Creates an empty file within a specified container in the Azure Blob Storage.

        Args:
           | **container_name** (str): The name of the container where the file will be created. 
           | **directory_path** (str): The directory path within the container. Use an empty string for the root directory. 
           | **file_name** (str): The name of the file to be created.

        Returns:
            None

        Raises:
            None
        """
        self.save_binary_file(b'',container_name,directory_path,file_name,False)

    def create_container(self,container_name : str,public_access:CONTAINER_ACCESS_TYPES='Private'):
        """
        Creates a container in the Azure Blob Storage.

        Args:
           | **container_name** (str): The name of the container to create. 
           | **public_access** (CONTAINER_ACCESS_TYPES, optional): The level of public access for the container. Defaults to Private.

        Returns:
            None

        Raises:
           | ContainerAccessTypes: Invalid container access type.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if not container_client.exists():
                if public_access == "Private":
                    public_access_ = None
                else:
                    public_access_ = None
                self.__service_client.create_container(name=container_name,public_access=public_access_ )
        except ResourceExistsError as e:
            raise ContainerAccessTypes(f"Container access {public_access} is not allowed in {container_name}") from e
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
        
    def delete_container(self,container_name : str):
        """
        Deletes a container from the Azure Blob Storage, if it exists.

        Args:
           | **container_name** (str): The name of the container to delete. 
        
        Returns:
           | None

        Raises:
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists():
                container_client.delete_container()
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
        
       