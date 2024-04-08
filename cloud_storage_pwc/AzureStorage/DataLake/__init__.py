

import sys
from ..StorageUtillity import *
from azure.storage.filedatalake import DataLakeServiceClient
import time
from ..Utils import *
from ..Exceptions import *
from azure.core.exceptions import HttpResponseError,ResourceNotFoundError,ResourceExistsError

class DataLake(StorageUtillity):
    """Class representing Data Lake"""
 
    def __init__(self, url:str, access_key:str=None, credential=None):
        super().__init__()
        tries=3
        for i in range(tries):
            try:
                try:
                    if access_key is not None and credential is None:
                        self.__service_client = DataLakeServiceClient(account_url=url, credential=access_key)
                    else:
                        self.__service_client = DataLakeServiceClient(account_url=url, credential=credential)
                    
                    containers = self.__service_client.list_file_systems()
                    con_num=len(list(containers))
                    dfs = False
                    if con_num > 0:     
                        for page in containers.by_page():
                            for con in page:
                                try:
                                    
                                    if self.__service_client.get_file_system_client(con.name).get_directory_client("/").exists():
                                        dfs = True
                                        break
                                except HttpResponseError:
                                    dfs = False
                    
                    if dfs is False:
                        raise DataLakeCreateError()
                    break
                except ResourceNotFoundError as e:
                    raise StorageAccountNotFound(f"Storage account {url} not found") from e
                except HttpResponseError as e:
                    raise StorageAccountAuthenticationError(f"Storage account {url} authorization error") from e
            except:
                if i==tries-1:
                    raise
                time.sleep(1)
                continue

    def save_binary_file(self,input_bytes:bytes, container_name:str, directory_path:str,file_name:str,is_overwrite:bool=True,tries:int=3,bytes_length:int=None):
        """
        Save a binary file to the specified container in the Data Lake.

        Args:
           | **input_bytes** (bytes): Bytes to be saved.
           | **container_name** (str): The name of the container.
           | **directory_path** (str): The path of the directory where the file is located.
           | **file_name** (str): The name of the file to be saved.
           | **is_overwrite** (bool, optional): Should the file be overwritten? Defaults to True.

        Returns:
           | None 

        Raises:
           | ContainerNotFound: If the specified container does not exist.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        
        for i in range(tries):
            temp_is_overwrite = True
            try:
                try:
                    file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
                    if file_system_client.exists() is False:
                        raise ContainerNotFound(f"Container {container_name} not found")
                    main_directory_client = file_system_client.get_directory_client("/")
                    if directory_path =="":
                        file_client = main_directory_client.get_file_client(file_name)
                        res=file_client.upload_data(bytes(input_bytes),length=bytes_length,  overwrite=temp_is_overwrite,validate_content=True)

                    else:
                        subdir_client = main_directory_client.get_sub_directory_client(directory_path)
                        if subdir_client.exists() is False:
                            subdir_client.create_directory()
                        file_client = subdir_client.get_file_client(file_name)
                #content_settings = ContentSettings(content_encoding=encoding,content_type = "text/csv")
                        res=file_client.upload_data(bytes(input_bytes),length=bytes_length, overwrite=temp_is_overwrite,validate_content=True)
                         
                    if self.file_exists(container_name,directory_path,file_name) is False:
                        raise FileNotFoundError(f"{container_name}/{directory_path}/{file_name} not found")
                    break
                
                except HttpResponseError as e:
                    raise NotAuthorizedToPerformThisOperation("User is not authorized to perform this operation") from e
            except:
                if i==tries-1:
                    raise
                time.sleep(3)
                continue
    
    def read_binary_file(self, container_name:str, directory_path:str, file_name:str,tries:int=3)->bytes:
        """
        Reads a binary file from the specified container and directory.

        Args:
           | **container_name** (str): The name of the container.
           | **directory_path** (str): The path to the directory where the file is located.
           | **file_name** (str): The name of the file.

        Returns:
            bytes: The content of the binary file.

        Raises:
           | ContainerNotFound: If the specified container does not exist.
           | BlobNotFound: If the specified file does not exist in the container.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        
        for i in range(tries):
            try:
                try:
                    file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
                    if file_system_client.exists() is False:
                        raise ContainerNotFound(f"Container {container_name} not found")
                    valid_file=False
                    main_directory_client = file_system_client.get_directory_client("/")
                    if directory_path =="":
                        file_client = main_directory_client.get_file_client(file_name)
                        if  file_client.exists():
                            valid_file=True
                    else:
                        subdir_client = main_directory_client.get_sub_directory_client(directory_path)
                        if subdir_client.exists() is True:
                            file_client = subdir_client.get_file_client(file_name)
                            if file_client.exists():
                                valid_file=True
                    
                    if valid_file:
                        download = file_client.download_file()
                        download_bytes = download.readall()
                    else:
                        raise BlobNotFound(f"{container_name}/{directory_path}/{file_name} not found")
                    return download_bytes
                except HttpResponseError as e:
                    raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
            except:
                if i==tries-1:
                    raise
                time.sleep(1)
                continue
    
    
    def delete_file(self,container_name : str,directory_path : str,file_name:str,wait:bool=True,tries:int=3):
        """
        Deletes a file from the Data Lake.

        Args:
           | **container_name** (str): The name of the container where the file to be deleted is located.
           | **directory_path** (str): The directory path within the container. Use an empty string for the root directory.
           | **file_name** (str): The name of the file to be deleted.
           | **wait** (bool, optional): The variable controls whether the code should pause briefly before checking the existence of a file in the container. Defaults to True.

        Returns:
            None

        Raises:
           | ContainerNotFound: If container do not exists.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        for i in range(tries):
            try:
                try:
                    file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
                    if file_system_client.exists() is False:
                        raise ContainerNotFound(f"Container {container_name} not found")
                    
                    main_directory = file_system_client.get_directory_client("/")
                    if directory_path != "":
                        main_directory = main_directory.get_sub_directory_client(directory_path)
                    
                    file_client= main_directory.get_file_client(file_name)
                    if file_client.exists():
                        file_client.delete_file()
                        
                    if wait:
                        check_if_exist = file_client.exists()
                        while check_if_exist:
                            time.sleep(1)
                            check_if_exist = file_client.exists()
                            
                    break
                except HttpResponseError as e:
                    raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
            except:
                if i==tries-1:
                    raise
                time.sleep(1)
                continue
                            
                            
                            
    def delete_files_by_prefix(self,container_name : str,directory_path : str,file_prefix:str, recursive:bool=False,wait:bool=True):
        """
        Deletes files from the Data Lake based on a prefix.

        Args:
           | **container_name** (str): The name of the container where files to be deleted are located.
           | **directory_path** (str): The directory path within the container.
           | **file_prefix** (str): The prefix of files to be deleted.
           | **recursive** (bool, optional): Flag indicating whether to list files recursively. Defaults to False.
           | **wait** (bool, optional): The variable controls whether the code should pause briefly before checking the existence of a file in the container. Defaults to True.

        Returns:
            None

        Raises:
            None
        """
            
        
        files_exists = self.ls_files(container_name,directory_path,recursive)
        files = [f for f in files_exists if f.split("/")[-1].startswith(file_prefix)]
        for f in files:
            self.delete_file(container_name,f.removesuffix(f"/{f.split('/')[-1]}"),f.split("/")[-1])

        if wait and files:
            time.sleep(1)
            files_exists = self.ls_files(container_name,directory_path,recursive)
            files = [f for f in files_exists if f.split("/")[-1].startswith(file_prefix)]
            while files:
                time.sleep(1)
                files_exists = self.ls_files(container_name,directory_path,recursive)
                files = [f for f in files_exists if f.split("/")[-1].startswith(file_prefix)]
        
    def delete_folder(self,container_name : str,directory_path : str,wait:bool=True):
        """
        Deletes a folder and its contents from the Data Lake.

        Args:
           | **container_name** (str): The name of the container containing the folder.
           | **directory_path** (str): The path of the folder to be deleted.
           | **wait** (bool, optional): If True, waits for the deletion to complete before returning. Defaults to True.

        Returns:
           | None
           
        Raises:
           | ContainerNotFound: If container do not exists.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
            if file_system_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
            main_directory = file_system_client.get_directory_client("/")
            
            if directory_path != "":
                main_directory = main_directory.get_sub_directory_client(directory_path)
            
              
         
            if main_directory.exists():
                main_directory.delete_directory()
                #main_directory = main_directory.get_sub_directory_client(directory_path)
                if wait:
                    time.sleep(1)
                    dict_exists = main_directory.exists()
                    while dict_exists:
                        time.sleep(1)
                        #main_directory = main_directory.get_sub_directory_client(directory_path)
                        dict_exists = main_directory.exists()
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
    
    def file_exists(self, container_name : str,directory_path : str,file_name:str)->bool:
        """
        Checks whether the file exists within the Data Lake.

        Args:
           | **container_name** (str):  The name of the container.
           | **directory_path** (str): The path of the directory where the file is located.
           | **file_name** (str): The name of the file to be located.

        Returns:
            bool: The information about whether the file exists (True/False). 

        Raises:
           | ContainerNotFound: If container do not exists.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
            if file_system_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
                
            main_directory = file_system_client.get_directory_client("/")
            if directory_path != "":
                main_directory = main_directory.get_sub_directory_client(directory_path)

            return main_directory.get_file_client(file_name).exists()
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
            
    def folder_exists(self, container_name : str, directory_path : str)->bool:
        """
        Checks whether the folder exists within the Data Lake.

        Args:
           | **container_name** (str):  The name of the container.
           | **directory_path** (str): The path of the directory where the folder is located.

        Returns:
            bool: The information about whether the folder exists (True/False). 

        Raises:
           | ContainerNotFound: If the specified container does not exist.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
            if file_system_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
        
            main_directory = file_system_client.get_directory_client("/")
            if directory_path != "":
                main_directory = main_directory.get_sub_directory_client(directory_path)

            return main_directory.exists()
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
                    
    
    def move_file(self,container_name : str,directory_path : str,file_name:str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_file:bool=False):
        """
        Moves a file from one location to another within the Data Lake.

        Args:
           | **container_name** (str): The name of the container containing the source file.
           | **directory_path** (str): The path of the directory containing the source file.
           | **file_name** (str): The name of the source file to be moved.
           | **new_container_name** (str): The name of the container where the file will be moved.
           | **new_directory_path** (str): The path of the directory where the file will be moved.
           | **is_overwrite** (bool, optional): If True, overwrites the destination file if it already exists. Defaults to True.
           | **is_delete_source_file** (bool, optional): If True, deletes the source file after moving. Defaults to False.

        Returns:
           | None

        Raises:
           | None

        """
  
        self.save_binary_file(self.read_binary_file(container_name,directory_path,file_name),new_container_name,new_directory_path,file_name,is_overwrite)
        if is_delete_source_file:
            self.delete_file(container_name ,directory_path ,file_name)

    def ls_files(self, container_name:str, directory_path:str, recursive:bool=False,tries:int=3)->list:
        """
        List files under a specified path within the Data Lake container.

        Args:
           | **container_name** (str): The name of the container.
           | **directory_path** (str): The path within the container to list files from.
           | **recursive** (bool, optional): Flag indicating whether to list files recursively. Defaults to False.

        Returns:
           | list: A list of file paths relative to the specified directory.
        
        Raises:
           | ContainerNotFound: If the specified container does not exist.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        for i in range(tries):
            try:
                try:
                    file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
                    if file_system_client.exists() is False:
                        raise ContainerNotFound(f"Container {container_name} not found")

                    files = []
                    if directory_path=="":
                        directory_path="/"
                    else:
                        main_directory = file_system_client.get_directory_client("/")
                        main_directory = main_directory.get_sub_directory_client(directory_path)
                        if main_directory.exists():
                            generator = file_system_client.get_paths(path=directory_path, recursive=recursive)
                            for file in generator:
                                if file.is_directory is False:
                                    files.append(file.name)
                    return files
                except HttpResponseError as e:
                    raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
            except:
                if i==tries-1:
                    raise
                time.sleep(1)
                continue
        
    def move_folder(self,container_name : str,directory_path : str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_folder:bool=False)->bool:  
        """
        Moves all files from one folder to another within the Data Lake.

        Args:
           | **container_name** (str): The name of the container containing the source folder.
           | **directory_path** (str): The path of the source folder.
           | **new_container_name** (str): The name of the container where the folder will be moved.
           | **new_directory_path** (str): The path of the target directory where the folder will be moved.
           | **is_overwrite** (bool, optional): If True, overwrites destination files if they already exist. Defaults to True.
           | **is_delete_source_folder** (bool, optional): If True, deletes the source folder after moving. Defaults to False.

        Returns:
           | bool: True if the folder move operation is successful.

        Raises:
           | None 
        """
        
        files_exists = self.ls_files(container_name,directory_path,True)
        for f in files_exists:
            self.move_file(container_name,directory_path,f"{f.removeprefix(directory_path)}/",new_container_name,new_directory_path,is_overwrite,is_delete_source_folder)
        
    def rename_file(self,container_name : str,directory_path : str,file_name:str,new_file_name:str):
        """
        Renames a file within a specified container in the Data Lake

        Args:
           | **container_name** (str): The name of the container containing the file.
           | **directory_path** (str): The path of the directory containing the file.
           | **file_name** (str): The name of the file to be renamed.
           | **new_file_name** (str): The new name for the renamed file.

        Returns:
           | None

        Raises:
           | ContainerNotFound: If the specified container does not exist.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
            if file_system_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
            main_directory = file_system_client.get_directory_client("/")
            if directory_path != "":
                main_directory = main_directory.get_sub_directory_client(directory_path)
                
            main_directory.get_file_client(file_name).rename_file(f"{container_name}/{directory_path}/{new_file_name}") 

        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
    
    def rename_folder(self,container_name : str,directory_path : str,new_directory_path:str):
        """
        Renames a folder within a specified container in the Data Lake.

        Args:
           | **container_name** (str): The name of the container containing the folder.
           | **directory_path** (str): The path of the folder to be renamed.
           | **new_directory_path** (str): The new path for the renamed folder.

        Raises:
           | ContainerNotFound: If the specified container does not exist.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
            if file_system_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
            
            main_directory = file_system_client.get_directory_client("/")
            if directory_path != "":
                main_directory = main_directory.get_sub_directory_client(directory_path)
                
            main_directory.rename_directory(f"{container_name}/{new_directory_path}")

        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e
        
    def create_empty_file(self,container_name : str,directory_path : str,file_name:str):
        """
        Creates an empty file within a specified container in teh Data Lake.

        Args:
           | **container_name** (str): The name of the container where the file will be created.
           | **directory_path** (str): The directory path within the container. Use an empty string for the root directory.
           | **file_name** (str): The name of the file to be created.

        Returns:
           | None

        Raises:
           | None
        """
        self.save_binary_file(b' ',container_name,directory_path,file_name)
        
    def create_container(self,container_name : str,public_access:CONTAINER_ACCESS_TYPES ='Private'):
        """
        Creates a container in the Data Lake.

        Args:
           | **container_name** (str): The name of the container to create. 
           | **public_access** (CONTAINER_ACCESS_TYPES, optional): The level of public access for the container. Defaults to 'Private'.

        Returns:
           | None 
        
        Raises:
           | ContainerNotFound: If the specified container does not exist.
           | NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            container_client = self.__service_client.get_file_system_client(file_system=container_name)
            if not container_client.exists():
                if public_access == "Private":
                    public_access_ = None
                else:
                    public_access_ = None
                self.__service_client.create_file_system(file_system=container_name,public_access=public_access_)
        except ResourceExistsError as e:
            raise ContainerAccessTypes(f"Container access {public_access} is not allowe in {container_name}") from e
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e

        
    def delete_container(self,container_name : str):
        """
        Deletes a container from the Data Lake.

        Args:
            **container_name** (str): The name of the container to delete. 

        Raises:
            NotAuthorizedToPerformThisOperation: If user is not authorized to perform this operation.
        """
        try:
            container_client = self.__service_client.get_file_system_client(file_system=container_name)
            if container_client.exists():
                container_client.delete_file_system()
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e

