
from src.cloud_storage_pwc.StorageAccountVirtualClass import *
from azure.storage.filedatalake import DataLakeServiceClient

from azure.identity import DefaultAzureCredential
from azure.identity import ClientSecretCredential
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import os
import numpy as np 
from itertools import product


class DataLake(StorageAccountVirtualClass):
 
    def __init__(self, url:str,accessKey:str=None,tenantId:str=None,applicationId:str=None,applicationSecret:str=None):
        if accessKey is not None and tenantId is None and applicationId is None and applicationSecret is None: 
            self.__service_client = DataLakeServiceClient(account_url="{}".format(url), credential=accessKey)
        elif accessKey is  None and tenantId is None and applicationId is None and applicationSecret is None: 
            credential = DefaultAzureCredential()
            self.__service_client = DataLakeServiceClient(account_url="{}".format(url), credential=credential)
        elif accessKey is  None and tenantId is not None and applicationId is not None and applicationSecret is not None:
            token_credential = ClientSecretCredential(
            tenantId, #self.active_directory_tenant_id,
            applicationId, #self.active_directory_application_id,
            applicationSecret #self.active_directory_application_secret
            )
            self.__service_client = DataLakeServiceClient(account_url="{}".format(url), credential=token_credential)
        else:
            raise Exception("Blad logowania")
    

    def save_binary_file(self, bytes:bytes,containerName : str,directoryPath : str,fileName:str,isOverWrite :bool=True):
        file_system_client = self.__service_client.get_file_system_client(file_system=containerName)        
        directory_client = file_system_client.get_directory_client(directoryPath)
        new_file_client = directory_client.create_file(fileName)
        new_file_client.upload_data(bytes,overwrite=isOverWrite)
        
    def read_csv_file(self,containerName:str,directoryPath:str,sourceFileName:str,sourceEncoding:str = "UTF-8", columnDelimiter:str = ";",isFirstRowAsHeader:bool = False,skipRows:int=0):
        file_system_client = self.__service_client.get_file_system_client(file_system=containerName) 
        directory_client = file_system_client.get_directory_client(directoryPath)
        file_client = directory_client.get_file_client(sourceFileName)
        download = file_client.download_file()
        download_bytes = download.readall()
        df = self.__read_csv_bytes(download_bytes,sourceEncoding,columnDelimiter,isFirstRowAsHeader,skipRows)
        
        df['techContainer'] = containerName
        df['techFolderPath'] = directoryPath.replace("\\","/")
        df['techSourceFile'] = sourceFileName
        
        return df
    
    def read_csv_folder(self,containerName:str,directoryPath:str,sourceEncoding :str= "UTF-8", columnDelimiter:str = ";",isFirstRowAsHeader:bool = False,skipRows:int=0,recursive:bool=False) ->pd.DataFrame:
        file_system_client = self.__service_client.get_file_system_client(file_system=containerName) 
        listFiles = file_system_client.get_paths(directoryPath,recursive)
        df = pd.DataFrame()
        if listFiles:
            for i,r in enumerate(listFiles):
                file = r.name.replace(directoryPath + "/","")
                dfNew = self.read_csv_file(containerName,directoryPath,file,sourceEncoding,columnDelimiter,isFirstRowAsHeader,skipRows)
                df = pd.concat([df, dfNew], axis=0, join="outer", ignore_index=True)
        return df
    
    
    def save_dataframe_as_parquet_pandas(self,df:pd.DataFrame,containerName : str,directoryPath:str,partitionCols:list=None,compression:str=None):
        VALID_compression = {'snappy', 'gzip', 'brotli', None}
        if compression is not None:
            compression=compression.lower()
        if compression not in VALID_compression:
            raise ValueError("results: status must be one of %r." % VALID_compression)
        
        file_system_client = self.__service_client.get_file_system_client(file_system=containerName) 
        buf = BytesIO()
        df.to_parquet(buf,allow_truncated_timestamps=True, use_deprecated_int96_timestamps=True)
        buf.seek(0)
        directory_client = file_system_client.get_directory_client(directoryPath)
        new_client_parq = directory_client.create_file(f"{uuid.uuid4().hex}.parquet")
        new_client_parq.upload_data(buf.getvalue(),overwrite=True)
        
        
    def save_dataframe_as_parquet_arrow(self,df:pd.DataFrame,containerName : str,directoryPath:str,partitionCols:list=None,compression:str=None):
        VALID_compression = {'NONE','SNAPPY', 'GZIP', 'BROTLI', 'LZ4', 'ZSTD'}
        if compression is not None:
            compression=compression.upper()
        elif compression is None:
            compression = 'NONE'
            
        if compression not in VALID_compression:
            raise ValueError("results: status must be one of %r." % VALID_compression)
        
        file_system_client = self.__service_client.get_file_system_client(file_system=containerName) 
        
        table = pa.Table.from_pandas(df)
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf,use_deprecated_int96_timestamps=True, allow_truncated_timestamps=True,compression=compression)
        directory_client = file_system_client.get_directory_client(directoryPath)
        new_client_parq = directory_client.create_file(f"{uuid.uuid4().hex}.parquet")
        new_client_parq.upload_data(buf.getvalue().to_pybytes(),overwrite=True)
    
    def read_parquet_file(self, containerName: str, directoryPath: str,sourceFileName:str, columns: list = None):
        file_system_client = self.__service_client.get_file_system_client(file_system=containerName)
        directory_client = file_system_client.get_directory_client(directoryPath)
        file_client= directory_client.get_file_client(sourceFileName)
        download = file_client.download_file()
        download_bytes = download.readall()
        df = self.__read_parquet_bytes(download_bytes,columns)
        
        df['techContainer'] = containerName
        df['techFolderPath'] = directoryPath.replace("\\","/")
        df['techSourceFile'] = sourceFileName
        
        return df
        
        
    
    def read_parquet_folder(self,containerName:str,directoryPath:str,columns:list = None,recursive:bool=False) ->pd.DataFrame:
        file_system_client = self.__service_client.get_file_system_client(file_system=containerName)
        #directory_client = file_system_client.get_directory_client(directoryPath)
        listFiles = file_system_client.get_paths(directoryPath,recursive)
        df = pd.DataFrame()
        if listFiles:
            for i,r in enumerate(listFiles):
                file = str(r.name).replace(directoryPath + "/","")
                dfNew = self.read_parquet_file(containerName,directoryPath,file,columns)
                df = pd.concat([df, dfNew], axis=0, join="outer", ignore_index=True)
        return df