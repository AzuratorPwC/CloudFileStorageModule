


from src.cloud_storage_pwc.StorageAccountVirtualClass import *
from azure.storage.blob import BlobServiceClient  #, BlobClient, ContainerClient

from azure.identity import DefaultAzureCredential
from azure.identity import ClientSecretCredential
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import os
import numpy as np 
from itertools import product
class Blob(StorageAccountVirtualClass):
 
    def __init__(self, url:str,accessKey:str=None,tenantId:str=None,applicationId:str=None,applicationSecret:str=None):
        if accessKey is not None and tenantId is None and applicationId is None and applicationSecret is None: 
            self.__service_client = BlobServiceClient(account_url="{}".format(url), credential=accessKey)
        elif accessKey is  None and tenantId is None and applicationId is None and applicationSecret is None: 
            credential = DefaultAzureCredential()
            self.__service_client = BlobServiceClient(account_url="{}".format(url), credential=credential)
        elif accessKey is  None and tenantId is not None and applicationId is not None and applicationSecret is not None:
            token_credential = ClientSecretCredential(
            tenantId, #self.active_directory_tenant_id,
            applicationId, #self.active_directory_application_id,
            applicationSecret #self.active_directory_application_secret
            )
            self.__service_client = BlobServiceClient(account_url="{}".format(url), credential=token_credential)
        else:
            raise Exception("Blad logowania")
            

    def ls_files(self,containerName : str, directoryPath : str, recursive:bool=False)->list:
        """
        List files under a path, optionally recursively
        """
        if not directoryPath == '' and not directoryPath.endswith('/'):
            directoryPath += '/'

        container_client = self.__service_client.get_container_client(container=containerName) 
        blob_iter = container_client.list_blobs(name_starts_with=directoryPath)
        files = []
        for blob in blob_iter:
            relative_path = os.path.relpath(blob.name, directoryPath)
            if recursive or not '\\' in relative_path:
                files.append(relative_path)
        return files
    
    def read_binary_file(self,containerName : str,directoryPath : str,fileName:str)->bytes:
        """
        dev1
        """
        container_client = self.__service_client.get_container_client(container=containerName)
        path = directoryPath +"/"+fileName
        blob_client = container_client.get_blob_client(path)
        download = blob_client.download_blob()
        download_bytes = download.readall()
        return download_bytes

    def save_binary_file(self, bytes:bytes,containerName : str,directoryPath : str,fileName:str,isOverWrite :bool=True):
        container_client = self.__service_client.get_container_client(container=containerName)        
        new_blob_client = container_client.get_blob_client(directoryPath +"/"+fileName)
        new_blob_client.upload_blob(bytes,overwrite=isOverWrite)
        
    def read_csv_file(self,containerName:str,directoryPath:str,sourceFileName:str,sourceEncoding:str = "UTF-8", columnDelimiter:str = ";",isFirstRowAsHeader:bool = False,skipRows:int=0)->pd.DataFrame:
        path = directoryPath +"/"+sourceFileName
        download_bytes = self.read_binary_file(containerName,directoryPath,sourceFileName)
        df = self.__read_csv_bytes(download_bytes,sourceEncoding,columnDelimiter,isFirstRowAsHeader,skipRows)
        df['techContainer'] = containerName
        path = path.replace("\\","/")
        df['techFolderPath'] = path[0:path.rfind("/")]
        df['techSourceFile'] = path[path.rfind("/")+1:len(path)]
        return df
    
    def read_csv_folder(self,containerName:str,mainDirectoryPath:str,includeSubfolders:list=None,sourceEncoding :str= "UTF-8", columnDelimiter:str = ";",isFirstRowAsHeader:bool = False,skipRows:int=0,recursive:bool=False) ->pd.DataFrame:
        listFiles = self.ls_files(containerName,mainDirectoryPath, recursive=recursive)
        
        if  includeSubfolders:
            newList=[]
            for i in includeSubfolders:
                for j in listFiles:
                    if  j.startswith(i.replace("/","\\")):
                        newList.append(j)
                listFiles = list(set(listFiles) - set(newList))
            listFiles = newList
        
        df = pd.DataFrame()
        if listFiles:
            for f in listFiles:
                dfNew = self.read_csv_file(containerName,mainDirectoryPath,f,sourceEncoding,columnDelimiter,isFirstRowAsHeader,skipRows)
                df = pd.concat([df, dfNew], axis=0, join="outer", ignore_index=True)
        return df
    
    
    def save_dataframe_as_parquet_pandas(self,df:pd.DataFrame,containerName : str,directoryPath:str,partitionCols:list=None,compression:str=None):
        VALID_compression = {'snappy', 'gzip', 'brotli', None}
        if compression is not None:
            compression=compression.lower()
        if compression not in VALID_compression:
            raise ValueError("results: status must be one of %r." % VALID_compression)
               
        if partitionCols:
            partitionDict = {}
            for x in partitionCols:
                partitionDict[x] = df[x].unique()
    
            partitionGroups = [dict(zip(partitionDict.keys(),items)) for items in product(*partitionDict.values())]
            partitionGroups = [d for d in partitionGroups if np.nan not in d.values()   ]
            
            for d in partitionGroups:
                df_part = df
                partitionPath=[]
                
                for d1 in d:
                    df_part = df_part[df_part[d1] == d[d1]]
                    partitionPath.append(f"{d1}={str(d[d1])}")
        
                if not(df_part.empty):
                    buf = BytesIO()  
                    df_part.to_parquet(buf,allow_truncated_timestamps=True, use_deprecated_int96_timestamps=True,compression=compression)
                    buf.seek(0)
                    self.save_binary_file(buf.getvalue(),containerName ,directoryPath+"/" +"/".join(partitionPath),f"{uuid.uuid4().hex}.parquet",True)
                    
                    #new_client_parq = container_client.get_blob_client(directoryPath+"/" +"/".join(partitionPath)+"/" +f"{uuid.uuid4().hex}.parquet")
                    #new_client_parq.upload_blob(buf.getvalue(),overwrite=True)
        else:
            if not(df.empty):
                buf = BytesIO()
                df.to_parquet(buf,allow_truncated_timestamps=True, use_deprecated_int96_timestamps=True,compression=compression)
                buf.seek(0)
                self.save_binary_file(buf.getvalue(),containerName ,directoryPath,f"{uuid.uuid4().hex}.parquet",True)

                #new_client_parq = container_client.get_blob_client(directoryPath +"/"+f"{uuid.uuid4().hex}.parquet")
                #new_client_parq.upload_blob(buf.getvalue(),overwrite=True)

        
    def save_dataframe_as_parquet_arrow(self,df:pd.DataFrame,containerName : str,directoryPath:str,partitionCols:list=None,compression:str=None):
        VALID_compression = {'NONE','SNAPPY', 'GZIP', 'BROTLI', 'LZ4', 'ZSTD'}
        if compression is not None:
            compression=compression.upper()
        elif compression is None:
            compression = 'NONE'
            
        if compression not in VALID_compression:
            raise ValueError("results: status must be one of %r." % VALID_compression)
        
        if partitionCols:
            partitionDict = {}
            for x in partitionCols:
                partitionDict[x] = df[x].unique()
    
            partitionGroups = [dict(zip(partitionDict.keys(),items)) for items in product(*partitionDict.values())]
            partitionGroups = [d for d in partitionGroups if np.nan not in d.values()   ]
            
            for d in partitionGroups:
                df_part = df
                partitionPath=[]
                
                for d1 in d:
                    df_part = df_part[df_part[d1] == d[d1]]
                    partitionPath.append(f"{d1}={str(d[d1])}")
        
                if not(df_part.empty):
                    table = pa.Table.from_pandas(df_part)
                    buf = pa.BufferOutputStream()
                    pq.write_table(table, buf,use_deprecated_int96_timestamps=True, allow_truncated_timestamps=True,compression=compression)
                    self.save_binary_file(buf.getvalue().to_pybytes(),containerName ,directoryPath+"/" +"/".join(partitionPath),f"{uuid.uuid4().hex}.parquet",True)

                    #new_client_parq = container_client.get_blob_client(directoryPath +"/" +"/".join(partitionPath)+"/"+f"{uuid.uuid4().hex}.parquet")
                    #new_client_parq.upload_blob(buf.getvalue().to_pybytes(),overwrite=True)
        else:
            if not(df.empty):
                table = pa.Table.from_pandas(df)
                buf = pa.BufferOutputStream()
                pq.write_table(table, buf,use_deprecated_int96_timestamps=True, allow_truncated_timestamps=True,compression=compression)
                self.save_binary_file(buf.getvalue().to_pybytes(),containerName ,directoryPath,f"{uuid.uuid4().hex}.parquet",True)

                #new_client_parq = container_client.get_blob_client(directoryPath +"/"+f"{uuid.uuid4().hex}.parquet")
                #new_client_parq.upload_blob(buf.getvalue().to_pybytes(),overwrite=True)
    
    def read_parquet_file(self, containerName: str, directoryPath: str,sourceFileName:str, columns: list = None)->pd.DataFrame:
        
        path = directoryPath +"/"+sourceFileName
        download_bytes = self.read_binary_file(containerName,directoryPath,sourceFileName)
        df = self.__read_parquet_bytes(download_bytes,columns)
        df['techContainer'] = containerName
        path = path.replace("\\","/")
        df['techFolderPath'] = path[0:path.rfind("/")]
        df['techSourceFile'] = path[path.rfind("/")+1:len(path)]
        return df
    
    def read_parquet_folder(self,containerName:str,mainDirectoryPath:str,includeSubfolders:list=None,columns:list = None,recursive:bool=False) ->pd.DataFrame:
        
        listFiles = self.ls_files(containerName,mainDirectoryPath, recursive=recursive)
        
        if  includeSubfolders:
            newList=[]
            for i in includeSubfolders:
                for j in listFiles:
                    if  j.startswith(i.replace("/","\\")):
                        newList.append(j)
                listFiles = list(set(listFiles) - set(newList))
            listFiles = newList
            
        df = pd.DataFrame()
        if listFiles:
            for f in listFiles:
                dfNew = self.read_parquet_file(containerName,mainDirectoryPath,f,columns)
                df = pd.concat([df, dfNew], axis=0, join="outer", ignore_index=True)
        return df
    
    def delete_file(self,containerName : str,directoryPath : str,fileName:str):
        container_client = self.__service_client.get_container_client(container=containerName)
        if directoryPath =="" or directoryPath is None:
            path=fileName
        else:
            path = "/".join( (directoryPath,fileName)) 
        blob_client = container_client.get_blob_client(path)
        blob_client.delete_blob(delete_snapshots="include")
        
    def delete_folder(self,containerName : str,directoryPath : str):
        listFiles = self.ls_files(containerName,directoryPath,True)
        for f in listFiles:
            path = directoryPath + "/" + f.replace("\\","/")
            self.delete_file(containerName,path[0:path.rfind("/")],path[path.rfind("/")+1:len(path)])
        
    def move_file(self,containerName : str,directoryPath : str,fileName:str,newContainerName : str,newDirectoryPath : str,newFileName:str,isOverWrite :bool=True,isDeleteSourceFile:bool=False):
        self.save_binary_file(self.read_binary_file(containerName,directoryPath,fileName),newContainerName,newDirectoryPath,newFileName,isOverWrite)
        if isDeleteSourceFile:
            self.delete_file(containerName ,directoryPath ,fileName)
            
    def move_folder(self,containerName : str,directoryPath : str,newContainerName : str,newDirectoryPath : str,isOverWrite :bool=True,isDeleteSourceFolder:bool=False)->bool:
        listFiles = self.ls_files(containerName,directoryPath,True)
        for i in listFiles:
            self.move_file(containerName,directoryPath,i,newContainerName,newDirectoryPath,i,True,isDeleteSourceFolder)
        return True
        
    def renema_file(self,containerName : str,directoryPath : str,fileName:str,newFileName:str):
        self.move_file(containerName,directoryPath,fileName,containerName,directoryPath,newFileName,True,True)
    
    def renema_folder(self,containerName : str,directoryPath : str,newDirectoryPath:str):
        self.move_folder(containerName,directoryPath,containerName,newDirectoryPath,True,True)
        
    def create_empty_file(self,containerName : str,directoryPath : str,fileName:str):
        container_client = self.__service_client.get_container_client(container=containerName)
        path = directoryPath +"/"+fileName
        blob_client = container_client.get_blob_client(path)
        blob_client.upload_blob('')
        
    def create_container(self,containerName : str):
        container_client = self.__service_client.create_container(name=containerName)
        
        
    def delete_container(self,containerName : str):
        self.__service_client.delete_container(name=containerName)