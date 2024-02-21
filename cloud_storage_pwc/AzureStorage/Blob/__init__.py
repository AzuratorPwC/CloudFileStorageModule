#from typing import override
#from multiprocessing import AuthenticationError
#import csv
#from shutil import ExecError
#from tkinter import E
#import uuid
import os
#from io import BytesIO
#from itertools import product
import time
import logging
#import numpy as np
#import pandas as pd
#import polars as pl
#from  openpyxl import load_workbook
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import HttpResponseError,ResourceNotFoundError,ResourceExistsError
from ..Exceptions import *
#import pyarrow as pa
#import pyarrow.parquet as pq
from ..StorageAccountVirtualClass import StorageAccountVirtualClass
from ..Utils import *
from azure.storage.blob._shared.authentication import SharedKeyCredentialPolicy

class Blob(StorageAccountVirtualClass):
    """
    Classs
    """
    def __init__(self, url:str, access_key:str=None, tenant_id:str=None, application_id:str=None,
                 application_secret:str=None):
        super().__init__()
        try:
            if access_key is not None and tenant_id is None and application_id is None \
                and application_secret is None:
                logging.info("Blob-create by accesskey %s",url)
                self.__service_client = BlobServiceClient(account_url=url, credential=access_key,logging_enable=False)
            elif access_key is None and tenant_id is None and application_id is None \
                and application_secret is None:
                logging.info("Blob-create by defaultazurecredential %s", url)
                credential = DefaultAzureCredential()
                self.__service_client = BlobServiceClient(account_url=url, credential=credential,logging_enable=False)
            elif access_key is None and tenant_id is not None and application_id is not None \
                and application_secret is not None:
                logging.info("Blob-create by clientsecretcredential %s", url)
                token_credential = ClientSecretCredential(tenant_id, application_id,
                                                          application_secret)
                self.__service_client = BlobServiceClient(account_url=url,
                                                          credential=token_credential,logging_enable=False)
            self.__service_client.get_service_properties()
        except ResourceNotFoundError as e:
            logging.error(f"Storage account {url} not found")
            raise StorageAccountNotFound(f"Storage account {url} not found") from e
        except HttpResponseError as e:
            logging.error(f"Storage account {url} authorization error")
            raise StorageAccountAuthenticationError(f"Storage account {url} authorization error") from e
        #except 
        #except Exception as e:
        #    logging.critical(str(e))


    def check_is_dfs(self) -> bool:
        account_info = self.__service_client.get_account_information()
        return account_info['account_kind'] == 'StorageV2' and account_info['is_hns_enabled']


    def ls_files(self, container_name:str, directory_path:str, recursive:bool=False) -> list:
        """
        List files under a specified path within an Azure Blob Storage container.

        Args:
            container_name (str): The name of the Azure Blob Storage container.
            directory_path (str): The path within the container to list files from.
            recursive (bool, optional): Flag indicating whether to list files recursively. Defaults 
                to False.

        Returns:
            list: A list of file paths relative to the specified directory.
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
        except ContainerNotFound as e:
            raise e

    def file_exists(self, container_name : str,directory_path : str,file_name:str)->bool:
        container_client = self.__service_client.get_container_client(container=container_name)
        if container_client.exists() is False:
            raise ContainerNotFound(f"Container {container_name} not found")
        if not directory_path == '' and not directory_path.endswith('/'):
            directory_path += '/'
            
        path = directory_path + file_name
        blob_client = container_client.get_blob_client(path)
        
        return blob_client.exists()
          
    def folder_exists(self, container_name : str, directory_path : str)->bool:
        
        files = self.ls_files(container_name,directory_path,False)
        if files:
            return True
        else:
            return False


    def read_binary_file(self, container_name: str, directory_path: str, file_name: str) -> bytes:
        """
        Reads a binary file from the specified container, directory, and file name.

        Args:
            container_name (str): The name of the container.
            directory_path (str): The path of the directory where the file is located.
            file_name (str): The name of the file to read.

        Returns:
            bytes: The content of the binary file.

        Raises:
            ContainerNotFound: If the specified container does not exist.
            BlobNotFound: If the specified file does not exist in the container.
            Exception: If there is an error reading the file.
        """
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


    def save_binary_file(self, input_bytes:bytes, container_name:str, directory_path:str,file_name:str,is_overwrite:bool=True):
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists() is False:
                raise ContainerNotFound(f"Container {container_name} not found")
            if not directory_path == '' and not directory_path.endswith('/'):
                directory_path += '/'
            new_blob_client = container_client.get_blob_client(directory_path + file_name)
            new_blob_client.upload_blob(bytes(input_bytes), overwrite=is_overwrite)
        except ResourceExistsError as e:
            raise BlobAlreadyExists(f"File {file_name} already exists") from e


    def delete_file(self,container_name : str,directory_path : str,file_name:str,wait:bool=True):
        logging.info(f"delete_file {container_name}/{directory_path}/{file_name}")

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
                    
    def delete_files_by_prefix(self,container_name : str,directory_path : str,file_prefix:str, recursive:bool=False,wait:bool=True):
        container_client = self.__service_client.get_container_client(container=container_name)
        if container_client.exists() is False:
            raise ContainerNotFound(f"Container {container_name} not found")

        files_exists = self.ls_files(container_name,directory_path,recursive)
        logging.info(f"delete_files_by_prefix {container_name}/{directory_path}/{file_prefix} {recursive}")
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
        logging.info(f"delete_files_by_prefix {container_name}/{directory_path}")
        container_client = self.__service_client.get_container_client(container=container_name)
        if container_client.exists() is False:
            raise ContainerNotFound(f"Container {container_name} not found")
        
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
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists() is False:
                raise ContainerNotFound(f"Container source {container_name} not found")
            container_client = self.__service_client.get_container_client(container=new_container_name)
            if container_client.exists() is False:
                raise ContainerNotFound(f"Container target {new_container_name} not found")

            self.save_binary_file(self.read_binary_file(container_name,directory_path,file_name),new_container_name,new_directory_path,file_name,is_overwrite)
            if is_delete_source_file:
                self.delete_file(container_name ,directory_path ,file_name)
        except ContainerNotFound as e:
            raise e
        except ResourceNotFoundError as e:
            raise BlobNotFound(f"File {file_name} not found in container {container_name}") from e
        except Exception as e:
            raise Exception(f"Error moving file {file_name} in container {container_name} to container {new_container_name}") from e
            
    def move_folder(self,container_name : str,directory_path : str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_folder:bool=False):
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists() == False:
                raise ContainerNotFound(f"Container source {container_name} not found")
            container_client = self.__service_client.get_container_client(container=new_container_name)
            if container_client.exists() == False:
                raise ContainerNotFound(f"Container target {container_name} not found")
        
            list_files = self.ls_files(container_name,directory_path,True)

            for i in list_files:
                self.move_file(container_name,directory_path,i,new_container_name,new_directory_path,is_overwrite,is_delete_source_folder)
        except ContainerNotFound as e:
            raise e
        except Exception as e:
            raise Exception(f"Error moving file {file_name} in container {container_name} to container {new_container_name}") from e

        
    def rename_file(self,container_name : str,directory_path : str,file_name:str,newfile_name:str):
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists() is False:
                raise ContainerNotFound(f"Container source {container_name} not found")

            self.save_binary_file(self.read_binary_file(container_name,directory_path,file_name),container_name,directory_path,newfile_name,False)
            self.delete_file(container_name ,directory_path ,file_name)
        except ContainerNotFound as e:
            raise e
        except ResourceNotFoundError as e:
            raise BlobNotFound(f"File {file_name} not found in container {container_name}") from e
        except Exception as e:
            raise Exception(f"Error moving folder {directory_path} in container {container_name}") from e
  
    def rename_folder(self,container_name : str,directory_path : str,new_directory_path:str):
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists() is False:
                raise ContainerNotFound(f"Container source {container_name} not found")
        
            list_files = self.ls_files(container_name,directory_path,True)

            for i in list_files:
                self.move_file(container_name,directory_path,i,container_name,new_directory_path,False,True)
        except ContainerNotFound as e:
            raise e
        except Exception as e:
            raise Exception(f"Error moving folder {directory_path} in container {container_name}") from e
        
    def create_empty_file(self,container_name : str,directory_path:str,file_name:str):
        try:
            container_client = self.__service_client.get_container_client(container=container_name)
            if directory_path!='':
                directory_path=directory_path+'/'
            path = directory_path +file_name
            blob_client = container_client.get_blob_client(path)
            blob_client.upload_blob('')
        except ResourceNotFoundError as e:
            raise ContainerNotFound(f"Container {container_name} not found") from e
        except ResourceExistsError as e:
            raise BlobAlreadyExists(f"File {file_name} already exists") from e
        except Exception as e:
            raise Exception(f"Error creating file {file_name} in container {container_name}") from e

    def create_container(self,container_name : str,public_access:CONTAINER_ACCESS_TYPES='Private'):
        try:
            logging.info(f"create_container {container_name} {public_access}")
            container_client = self.__service_client.get_container_client(container=container_name)
            if not container_client.exists():
                if public_access == "Private":
                    public_access_ = None
                else:
                    public_access_ = None
                self.__service_client.create_container(name=container_name,public_access=public_access_ )
        except ResourceExistsError as e:
            raise ContainerAccessTypes(f"Container access {public_access} is not allowe in {container_name}") from e
        except Exception as e:
            raise Exception(f"Error creating container {container_name}") from e
    def delete_container(self,container_name : str):
        try:
            logging.info(f"delete_container {container_name}")
            container_client = self.__service_client.get_container_client(container=container_name)
            if container_client.exists():
                container_client.delete_container()
        except Exception as e:
            raise Exception(f"Error deleting container {container_name}") from e