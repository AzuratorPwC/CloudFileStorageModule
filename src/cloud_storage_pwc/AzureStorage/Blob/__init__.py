#from typing import override
from multiprocessing import AuthenticationError
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import HttpResponseError,ResourceNotFoundError
import csv
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
from ..StorageAccountVirtualClass import StorageAccountVirtualClass
import os
from io import BytesIO
import numpy as np 
import pandas as pd
import polars as pl
from itertools import product
import time

from ..Utils import CONTAINER_ACCESS_TYPES,ENCODING_TYPES,ENGINE_TYPES,ORIENT_TYPES,DELIMITER_TYPES,QUOTING_TYPES,NAN_VALUES,add_tech_columns,DataFromExcel
import logging




class Blob(StorageAccountVirtualClass):
    """
    Classs
    """
    def __init__(self, url:str,access_key:str = None,tenant_id:str = None,application_id:str = None,application_secret:str = None):
        super().__init__()
        try:
            if access_key is not None and tenant_id is None and application_id is None and application_secret is None:
                logging.info("Blob-create by accesskey %s",url)
                self.__service_client = BlobServiceClient(account_url=url, credential=access_key)
            elif access_key is  None and tenant_id is None and application_id is None and application_secret is None:
                logging.info("Blob-create by defaultazurecredential %s",url)
                credential = DefaultAzureCredential()
                self.__service_client = BlobServiceClient(account_url=url, credential=credential)
            elif access_key is  None and tenant_id is not None and application_id is not None and application_secret is not None:
                logging.info("Blob-create by clientsecretcredential %s",url)
                token_credential = ClientSecretCredential(tenant_id,application_id,application_secret )
                self.__service_client = BlobServiceClient(account_url=url, credential=token_credential)
            self.__service_client.get_service_properties()
        except ResourceNotFoundError:
            logging.error("Storage account %s not found",url)
            raise ResourceNotFoundError("Storage account %s not found",url,exc_info=False)
        except HttpResponseError:
            logging.error("Storage account %s authorization error",url,exc_info=False)
            raise AuthenticationError("Storage account %s authorization error",url,exc_info=False)
        except Exception as e:
            logging.error(str(e))
            
            
    def check_is_dfs(self)->bool:
        """
        Classs
        """
        account_info= self.__service_client.get_account_information()
        return account_info['account_kind'] == 'StorageV2' and account_info['is_hns_enabled']

    def ls_files(self,container_name : str, directory_path : str, recursive:bool=False)->list:
        """
        List files under a path, optionally recursively
        """
        if not directory_path == '' and not directory_path.endswith('/'):
            directory_path += '/'

        container_client = self.__service_client.get_container_client(container=container_name)
        blob_iter = container_client.list_blobs(name_starts_with=directory_path)
        files = []
        for blob in blob_iter:
            relative_path = os.path.relpath(blob.name, directory_path)
            if recursive or not '\\' in relative_path:
                files.append(relative_path)
        return files

    def read_binary_file(self, container_name: str, directory_path: str, file_name: str) -> bytes:
        """
        Reads a binary file from the specified container, directory, and file name.

        Args:
            container_name (str): The name of the container.
            directory_path (str): The path to the directory where the file is located.
            file_name (str): The name of the file.

        Returns:
            bytes: The content of the binary file.
        """
        if not directory_path == '' and not directory_path.endswith('/'):
            directory_path += '/'
        container_client = self.__service_client.get_container_client(container=container_name)
        path = directory_path + file_name
        blob_client = container_client.get_blob_client(path)
        download = blob_client.download_blob()
        download_bytes = download.readall()
        return  download_bytes


    
    def save_binary_file(self, inputbytes:bytes,container_name : str,directory_path : str,file_name:str,source_encoding:ENCODING_TYPES = "UTF-8",is_overwrite :bool=True):
        """
        Save a binary file to the specified container in the cloud storage.

        Args:
            inputbytes (bytes): The binary data to be saved.
            container_name (str): The name of the container in the cloud storage.
            directory_path (str): The directory path within the container to save the file.
            file_name (str): The name of the file to be saved.
            source_encoding (ENCODING_TYPES, optional): The encoding type of the input data. Defaults to "UTF-8".
            is_overwrite (bool, optional): Flag indicating whether to overwrite the file if it already exists. Defaults to True.
        """
        container_client = self.__service_client.get_container_client(container=container_name)        
        if not directory_path == '' and not directory_path.endswith('/'):
            directory_path += '/'
        new_blob_client = container_client.get_blob_client(directory_path +file_name)
        #content_settings = ContentSettings(content_encoding=source_encoding,content_type = "text/csv")
        new_blob_client.upload_blob(bytes(inputbytes),overwrite=is_overwrite)

    def read_csv_file(self,container_name:str,directory_path:str,sourcefile_name:str,engine:ENGINE_TYPES = 'polars',source_encoding:ENCODING_TYPES = "UTF-8", column_delimiter:DELIMITER_TYPES = ',',is_first_row_as_header:bool = False,skip_rows:int=0,skip_blank_lines = True,tech_columns:bool=False):
        
        if not directory_path == '' and not directory_path.endswith('/'):
            path = directory_path +"/"+sourcefile_name
        else:
            path = directory_path + sourcefile_name
        
        download_bytes = self.read_binary_file(container_name,directory_path,sourcefile_name)
        if engine=='pandas':
            df = self.read_csv_bytes(download_bytes,engine,source_encoding,column_delimiter,is_first_row_as_header,skip_rows,skip_blank_lines)
        elif engine=='polars':
            df = self.read_csv_bytes(download_bytes,engine,source_encoding,column_delimiter,is_first_row_as_header,skip_rows,skip_blank_lines)
        if tech_columns:
            path = path.replace("\\","/")
            df = add_tech_columns(df,container_name,path[0:path.rfind("/")],path[path.rfind("/")+1:len(path)])
        return df
    
    def read_csv_folder(self,container_name:str,directory_path:str,engine: ENGINE_TYPES = 'polars',source_encoding:ENCODING_TYPES = "UTF-8", column_delimiter:DELIMITER_TYPES = ",",is_first_row_as_header:bool = False,skip_rows:int=0,skip_blank_lines=True,tech_columns:bool=False,recursive:bool=False) ->pd.DataFrame:
        list_files = self.ls_files(container_name,directory_path, recursive=recursive)
        #if  folders:
        #    new_list=[]
        #    for i in folders:
        #        for j in list_files:
        #            if  j.startswith(i.replace("/","\\")):
        #                new_list.append(j)
        #        list_files = list(set(list_files) - set(new_list))
        #    list_files = new_list
        df = pd.DataFrame()
        if list_files:
            for f in list_files:
                if engine=='pandas':
                    df_new = self.read_csv_file(container_name,directory_path,f,engine,source_encoding,column_delimiter,is_first_row_as_header,skip_rows,skip_blank_lines,tech_columns)
                    df = pd.concat([df, df_new], axis=0, join="outer", ignore_index=True)
                elif engine =='polars':
                    df_new = self.read_csv_file(container_name,directory_path,f,engine,source_encoding,column_delimiter,is_first_row_as_header,skip_rows,skip_blank_lines,tech_columns)
                    df = pl.concat([df, df_new])             
        return df
    def read_excel_file(self,container_name:str,directory_path:str,sourcefile_name:str,engine: ENGINE_TYPES ='polars',skip_rows:int = 0,is_first_row_as_header:bool = False,sheets:list()=None,tech_columns:bool=False):
        if not directory_path == '' and not directory_path.endswith('/'):
            path = directory_path +"/"+sourcefile_name
        else:
            path = directory_path + sourcefile_name
        download_bytes = self.read_binary_file(container_name,directory_path,sourcefile_name)
        if engine == 'pandas':
            if sourcefile_name.endswith(".xls"):
                workbook = pd.ExcelFile(BytesIO(download_bytes), engine='xlrd') 
            else:
                workbook = pd.ExcelFile(BytesIO(download_bytes))
        elif engine == 'polars':
            workbook = pl.read_excel(BytesIO(download_bytes), xlsx2csv_options={"skip_empty_lines": False},
                            read_csv_options={"has_header": is_first_row_as_header,"skip_rows":skip_rows})
            

        workbook_sheetnames = workbook.sheet_names # get all sheet names
        if bool(sheets):
            workbook_sheetnames=list(set(workbook_sheetnames) & set(sheets))
        list_of_dff = []
        if is_first_row_as_header:
            is_first_row_as_header=0
        else:
            is_first_row_as_header=None
        for sheet in workbook_sheetnames:
            dff = pd.read_excel(workbook, sheet_name = sheet,skip_rows=skip_rows, index_col = None, header = is_first_row_as_header)
            if tech_columns:
                path = path.replace("\\","/")
                df =  add_tech_columns(df,container_name,path[0:path.rfind("/")],path[path.rfind("/")+1:len(path)])
            
            list_of_dff.append(DataFromExcel(dff,sheet))
            
        return list_of_dff
    
    
    def save_dataframe_as_csv(self,df:[pd.DataFrame, pl.DataFrame],container_name : str,directory_path:str,file:str=None,partition_columns:list=None,source_encoding:ENCODING_TYPES= "UTF-8", column_delimiter:str = ";",is_first_row_as_header:bool = True,quoteChar:str=' ',quoting:['never', 'always', 'necessary']='never',escapeChar:str="\\", engine: ENGINE_TYPES ='polars'):
        
        quoting_dict = {'never':csv.QUOTE_NONE, 'always':csv.QUOTE_ALL, 'necessary':csv.QUOTE_MINIMAL}
        
        if isinstance(df, pd.DataFrame):
            if df.empty:
                return
            df = df.replace(r'\\n', '', regex=True) 
            if engine != 'pandas':
                df = pl.from_pandas(df)

        elif isinstance(df, pl.DataFrame):
            if df.is_empty():
                return
            df = df.with_columns(pl.col(pl.Utf8).str.replace_all(r"\\n", ""))
            if engine != 'polars':
                df = df.to_pandas(df)

                        
        if partition_columns:
            partitionDict = {}
            for x in partition_columns:
                partitionDict[x] = df[x].unique()
    
            partitionGroups = [dict(zip(partitionDict.keys(),items)) for items in product(*partitionDict.values())]
            partitionGroups = [d for d in partitionGroups if np.nan not in d.values()]

            for d in partitionGroups:
                df_part = df
                partitionPath=[]
                

                if isinstance(df_part, pd.DataFrame):
                    for d1 in d:
                        df_part = df_part[df_part[d1] == d[d1]]
                        partitionPath.append(f"{d1}={str(d[d1])}")

                if isinstance(df_part, pl.DataFrame):
                    for d1 in d:
                        df_part = df_part.filter(df_part[d1] == d[d1])
                        partitionPath.append(f"{d1}={str(d[d1])}")

                if isinstance(df_part, pd.DataFrame):
                    if not(df_part.empty):
                        buf = BytesIO()
                        df_reset = df_part.reset_index(drop=True)
                        df_reset.to_csv(buf,index=False, sep=column_delimiter,encoding=source_encoding,header=is_first_row_as_header,quotechar=quoteChar, quoting=quoting_dict[quoting],escapechar=escapeChar)
                        buf.seek(0)
                        self.save_binary_file(buf.getvalue(),container_name ,directory_path +"/" +"/".join(partitionPath),f"{uuid.uuid4().hex}.csv",True)


                if isinstance(df_part, pl.DataFrame):
                    if not(df_part.is_empty()):
                        buf = BytesIO()
                        df_reset = df_part
                        df_reset.write_csv(buf, separator=column_delimiter, has_header=is_first_row_as_header, quote_char='"', quote_style=quoting)
                        buf.seek(0)
                        self.save_binary_file(buf.getvalue(),container_name ,directory_path +"/" +"/".join(partitionPath),f"{uuid.uuid4().hex}.csv",True)
                                                
        
        else:
            buf = BytesIO()
            
            if isinstance(df, pd.DataFrame):
                df_reset = df.reset_index(drop=True)
                df_reset.to_csv(buf,index=False, sep=column_delimiter,encoding=source_encoding,header=is_first_row_as_header,quotechar=quoteChar, quoting=quoting_dict[quoting],escapechar=escapeChar)
            else:
                df_reset = df
                df_reset.write_csv(buf, separator=column_delimiter, has_header=is_first_row_as_header, quote_style=quoting)
    
            buf.seek(0)

            if file:
                file_name = file
            else:
                file_name = f"{uuid.uuid4().hex}.csv"
            self.save_binary_file(buf.getvalue(),container_name ,directory_path,file_name,True)


    
    def save_dataframe_as_parquet(self,df:pd.DataFrame,container_name : str,directory_path:str,partition_columns:list=None,compression:str=None):
        
        VALID_compression = {'snappy', 'gzip', 'brotli', None}
        if compression is not None:
            compression=compression.lower()
        if compression not in VALID_compression:
            raise ValueError("results: status must be one of %r." % VALID_compression)
        if not(df.empty):
            df = df.replace('\n', ' ', regex=True)  
        if partition_columns:
            partitionDict = {}
            for x in partition_columns:
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
                    self.save_binary_file(buf.getvalue(),container_name ,directory_path +"/" +"/".join(partitionPath),f"{uuid.uuid4().hex}.parquet",True)
                    
                    #new_client_parq = container_client.get_blob_client(directory_path+"/" +"/".join(partitionPath)+"/" +f"{uuid.uuid4().hex}.parquet")
                    #new_client_parq.upload_blob(buf.getvalue(),overwrite=True)
        else:
            if not(df.empty):
                buf = BytesIO()
                df.to_parquet(buf,allow_truncated_timestamps=True, use_deprecated_int96_timestamps=True,compression=compression)
                buf.seek(0)
                self.save_binary_file(buf.getvalue(),container_name ,directory_path,f"{uuid.uuid4().hex}.parquet",True)

                #new_client_parq = container_client.get_blob_client(directory_path +"/"+f"{uuid.uuid4().hex}.parquet")
                #new_client_parq.upload_blob(buf.getvalue(),overwrite=True)

        
    def save_dataframe_as_parqarrow(self,df:pd.DataFrame,container_name : str,directory_path:str,partition_columns:list=None,compression:str=None):
            
        VALID_compression = {'NONE','SNAPPY', 'GZIP', 'BROTLI', 'LZ4', 'ZSTD'}
        if compression is not None:
            compression=compression.upper()
        elif compression is None:
            compression = 'NONE'
        if not(df.empty):
            df = df.replace('\n', ' ', regex=True)
        if compression not in VALID_compression:
            raise ValueError("results: status must be one of %r." % VALID_compression)
        
        if partition_columns:
            partitionDict = {}
            for x in partition_columns:
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
                    self.save_binary_file(buf.getvalue().to_pybytes(),container_name ,directory_path +"/" +"/".join(partitionPath),f"{uuid.uuid4().hex}.parquet",True)

                    #new_client_parq = container_client.get_blob_client(directory_path +"/" +"/".join(partitionPath)+"/"+f"{uuid.uuid4().hex}.parquet")
                    #new_client_parq.upload_blob(buf.getvalue().to_pybytes(),overwrite=True)
        else:
            if not(df.empty):
                table = pa.Table.from_pandas(df)
                buf = pa.BufferOutputStream()
                pq.write_table(table, buf,use_deprecated_int96_timestamps=True, allow_truncated_timestamps=True,compression=compression)
                self.save_binary_file(buf.getvalue().to_pybytes(),container_name ,directory_path,f"{uuid.uuid4().hex}.parquet",True)

                #new_client_parq = container_client.get_blob_client(directory_path +"/"+f"{uuid.uuid4().hex}.parquet")
                #new_client_parq.upload_blob(buf.getvalue().to_pybytes(),overwrite=True)

    def save_json_file(self, df: [pd.DataFrame, pl.DataFrame], container_name: str, directory: str, file:str = None, engine: ENGINE_TYPES ='polars', orient:ORIENT_TYPES= 'records'):

        if isinstance(df, pd.DataFrame):
            if not(df.empty):
                return
            
        if isinstance(df, pl.DataFrame):
            if not(df.is_empty):
                return    
                 
        if isinstance(df, pd.DataFrame):
            buf = BytesIO()           
            buf.seek(0)
            if engine == 'pandas':           
                df.to_json(buf, orient=orient)
            else:
                polars_df = pl.from_pandas(df)
                polars_df.write_json(buf, row_oriented=(orient=='records'), pretty=True)

        elif isinstance(df, pl.DataFrame):
            buf = BytesIO()           
            buf.seek(0)
            if engine == 'pandas':
                pandas_df = df.to_pandas()
                pandas_df.to_json(buf, orient=orient)
            else:
                df.write_json(buf, row_oriented=(orient=='records'), pretty=True)
        else:
            raise Exception("DF argument is not Pandas or Polars DataFrame")

        if file:
            file_name = file
        else:
            file_name = f"{uuid.uuid4().hex}.json"
        self.save_binary_file(buf.getvalue(), container_name ,directory,file_name,True)

    
    def save_listdataframe_as_xlsx(self,list_df:list, sheets:list, container_name : str,directory_path:str ,file_name:str,index=False,header=False):
        # for multiple dfs
        buf = BytesIO() 
        with pd.ExcelWriter(buf, engine = 'openpyxl') as writer:
            for df, sheet_name in zip(list_df,sheets):
                df.to_excel(writer, index = index, header = header, sheet_name = sheet_name)
        buf.seek(0)
        self.save_binary_file(buf.getvalue(),container_name ,directory_path,f"{file_name}.xlsx",True)
    
    
    def read_parquet_file(self, container_name: str, directory_path: str,sourcefile_name:str, columns: list = None,tech_columns:bool=False)->pd.DataFrame:
        
        if not directory_path == '' and not directory_path.endswith('/'):
            path = directory_path +"/"+sourcefile_name
        else:
            path = directory_path + sourcefile_name
        download_bytes = self.read_binary_file(container_name,directory_path,sourcefile_name)
        df = self.read_parquet_bytes(bytes=download_bytes,columns=columns)
        
        if tech_columns:
            path = path.replace("\\","/")
            df =  add_tech_columns(df,container_name,path[0:path.rfind("/")],path[path.rfind("/")+1:len(path)])

        return df
    
    def read_parquet_folder(self,container_name:str,directory_path:str,folders:list=None,columns:list = None,tech_columns:bool=False,recursive:bool=False) ->pd.DataFrame:
        
        list_files = self.ls_files(container_name,directory_path, recursive=recursive)
        if  folders:
            new_list=[]
            for i in folders:
                for j in list_files:
                    if  j.startswith(i.replace("/","\\")):
                        new_list.append(j)
                list_files = list(set(list_files) - set(new_list))
            list_files = new_list
            
        df = pd.DataFrame()
        if list_files:
            for f in list_files:
                df_new = self.read_parquet_file(container_name,directory_path,f,columns,tech_columns)
                df = pd.concat([df, df_new], axis=0, join="outer", ignore_index=True)
        return df
    
    def delete_file(self,container_name : str,directory_path : str,file_name:str,wait:bool=True):
        container_client = self.__service_client.get_container_client(container=container_name)
        if directory_path =="" or directory_path is None:
            path=file_name
        else:
            path = "/".join( (directory_path,file_name)) 
        blob_client = container_client.get_blob_client(path)
        blob_client.delete_blob(delete_snapshots="include")
        
        if wait:
            blob_client = container_client.get_blob_client(path)
            check_if_exist = blob_client.exists()
            while check_if_exist:
                time.sleep(5)
                blob_client = container_client.get_blob_client(path)
                check_if_exist = blob_client.exists()
        
    def delete_folder(self,container_name : str,directory_path : str,wait:bool=True):
        list_files = self.ls_files(container_name,directory_path,True)
        for f in list_files:
            path = directory_path + "/" + f.replace("\\","/")
            self.delete_file(container_name,path[0:path.rfind("/")],path[path.rfind("/")+1:len(path)])
        
        if wait:
            list_files = self.ls_files(container_name,directory_path,True)
            while len(list_files)>0:
                time.sleep(5)
                list_files = self.ls_files(container_name,directory_path,True)
        
    def move_file(self,container_name : str,directory_path : str,file_name:str,new_container_name : str,new_directory_path : str,newfile_name:str,is_overwrite :bool=True,isDeleteSourceFile:bool=False):
        self.save_binary_file(self.read_binary_file(container_name,directory_path,file_name),new_container_name,new_directory_path,newfile_name,is_overwrite)
        if isDeleteSourceFile:
            self.delete_file(container_name ,directory_path ,file_name)
            
    def move_folder(self,container_name : str,directory_path : str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_folder:bool=False)->bool:
        list_files = self.ls_files(container_name,directory_path,True)
        for i in list_files:
            self.move_file(container_name,directory_path,i,new_container_name,new_directory_path,i,True,is_delete_source_folder)
        return True
        
    def renema_file(self,container_name : str,directory_path : str,file_name:str,newfile_name:str):
        self.move_file(container_name,directory_path,file_name,container_name,directory_path,newfile_name,True,True)
    
    def renema_folder(self,container_name : str,directory_path : str,new_directory_path:str):
        self.move_folder(container_name,directory_path,container_name,new_directory_path,True,True)
        
    def create_empty_file(self,container_name : str,directory_path : str,file_name:str):
        container_client = self.__service_client.get_container_client(container=container_name)
        if directory_path!='':
            directory_path=directory_path+'/'
        path = directory_path +file_name
        blob_client = container_client.get_blob_client(path)
        blob_client.upload_blob('')
        
    def create_container(self,container_name : str,public_access:CONTAINER_ACCESS_TYPES='Private'):
        """
        Creates a container in the Azure Blob Storage.

        Args:
            container_name (str): The name of the container to create.
            public_access (CONTAINER_ACCESS_TYPES, optional): The level of public access for the container. Defaults to None.

        Returns:
            None
        """
        container_client = self.__service_client.get_container_client(container=container_name)   
        if not container_client.exists():
            self.__service_client.create_container(name=container_name,public_access=public_access)

    def delete_container(self,container_name : str):
        container_client = self.__service_client.get_container_client(container=container_name)
        if not container_client.exists():
            container_client.delete_container()