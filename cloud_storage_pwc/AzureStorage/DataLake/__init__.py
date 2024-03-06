

import sys
from ..StorageAccountVirtualClass import *

from azure.storage.filedatalake import DataLakeServiceClient

from azure.identity import DefaultAzureCredential
from azure.identity import ClientSecretCredential
import time
from ..Utils import *
from ..Exceptions import *
import logging
from azure.core.exceptions import HttpResponseError,ResourceNotFoundError,ResourceExistsError

class DataLake(StorageAccountVirtualClass):
 
    def __init__(self, url:str, access_key:str=None, tenant_id:str=None, application_id:str=None,application_secret:str=None):
        super().__init__()
        tries=3
        for i in range(tries):
            try:
                try:
                    if access_key is not None and tenant_id is None and application_id is None and application_secret is None:
                        self.__service_client = DataLakeServiceClient(account_url=url, credential=access_key)
                    elif access_key is  None and tenant_id is None and application_id is None and application_secret is None:
                        credential = DefaultAzureCredential()
                        self.__service_client = DataLakeServiceClient(account_url=url, credential=credential)
                    elif access_key is  None and tenant_id is not None and application_id is not None and application_secret is not None:
                        token_credential = ClientSecretCredential(
                            tenant_id, application_id,application_secret)
                        self.__service_client = DataLakeServiceClient(account_url=url, credential=token_credential)
                    
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

    def save_binary_file(self,input_bytes:bytes, container_name:str, directory_path:str,file_name:str,is_overwrite:bool=True,tries:int=3):
        
        
        temp_is_overwrite = is_overwrite
        for i in range(tries):
            try:
                try:
                    file_system_client = self.__service_client.get_file_system_client(file_system=container_name)
                    if file_system_client.exists() is False:
                        raise ContainerNotFound(f"Container {container_name} not found")
                    main_directory_client = file_system_client.get_directory_client("/")
                    if directory_path =="":
                        file_client = main_directory_client.get_file_client(file_name)
                    else:
                        subdir_client = main_directory_client.get_sub_directory_client(directory_path)
                        if subdir_client.exists() is False:
                            subdir_client.create_directory()
                        file_client = subdir_client.get_file_client(file_name)
                #content_settings = ContentSettings(content_encoding=encoding,content_type = "text/csv")
                        file_client.upload_data(bytes(input_bytes), overwrite=temp_is_overwrite)
                    
                    check = self.file_exists(container_name,directory_path,file_name)
                    
                    if check is False:
                        temp_is_overwrite = True
                        time.sleep(3)
                        for i in range(tries):
                            check = self.file_exists(container_name,directory_path,file_name)
                            if check:
                                break
                            time.sleep(3)
                            res=file_client.upload_data(bytes(input_bytes), overwrite=temp_is_overwrite)
                        if check is False:
                            raise FileNotFoundError(f"{container_name}/{directory_path}/{file_name} not found")
                    break
                except HttpResponseError as e:
                    raise NotAuthorizedToPerformThisOperation("User is not authorized to perform this operation") from e
            except NotAuthorizedToPerformThisOperation:
                if i==tries-1:
                    raise
                time.sleep(1)
                continue
    
    def read_binary_file(self, container_name:str, directory_path:str, file_name:str,tries:int=3)->bytes:
        
        
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
  
        self.save_binary_file(self.read_binary_file(container_name,directory_path,file_name),new_container_name,new_directory_path,file_name,is_overwrite)
        if is_delete_source_file:
            self.delete_file(container_name ,directory_path ,file_name)

    def ls_files(self, container_name:str, directory_path:str, recursive:bool=False,tries:int=3)->list:
        
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
        
        files_exists = self.ls_files(container_name,directory_path,True)
        for f in files_exists:
            self.move_file(container_name,directory_path,f"{f.removeprefix(directory_path)}/",new_container_name,new_directory_path,is_overwrite,is_delete_source_folder)
        
    def rename_file(self,container_name : str,directory_path : str,file_name:str,new_file_name:str):
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
        self.save_binary_file(b' ',container_name,directory_path,file_name)
        
    def create_container(self,container_name : str,public_access:CONTAINER_ACCESS_TYPES ='Private'):
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
        try:
            container_client = self.__service_client.get_file_system_client(file_system=container_name)
            if container_client.exists():
                container_client.delete_file_system()
        except HttpResponseError as e:
            raise NotAuthorizedToPerformThisOperation(f"User is not authorized to perform this operation") from e