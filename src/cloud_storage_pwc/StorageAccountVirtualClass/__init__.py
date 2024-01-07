
import abc
import pandas as pd
import polars as pl
import csv
from io import BytesIO
from typing import Literal

    #, 'N/A'
nanVal = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN',
                           '<NA>', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']

class DataFromExcel:
    def __init__(self,df,sheetName):
        self.data = df
        self.sheet = sheetName

def __addTechColumns__(df:[pd.DataFrame, pl.DataFrame],containerName: str=None,directoryPath:str=None,file:str = None):
    df['techContainer'] = containerName
    df['techFolderPath'] = directoryPath
    df['techSourceFile'] = file
    return df


class StorageAccountVirtualClass(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'save_binary_file') and 
                callable(subclass.save_binary_file) and 
                hasattr(subclass, 'read_binary_file') and 
                callable(subclass.read_binary_file) and 
                hasattr(subclass, 'read_csv_file') and 
                callable(subclass.read_csv_file) and
                hasattr(subclass, 'read_csv_folder') and 
                callable(subclass.read_csv_folder) and 
                hasattr(subclass, 'save_dataframe_as_parquet') and 
                callable(subclass.save_dataframe_as_parquet) and 
                hasattr(subclass, 'save_dataframe_as_csv') and 
                callable(subclass.save_dataframe_as_csv) and 
                hasattr(subclass, 'save_dataframe_as_parqarrow') and 
                callable(subclass.save_dataframe_as_parqarrow) and 
                hasattr(subclass, 'read_parquet_file') and 
                callable(subclass.read_parquet_file) and
                hasattr(subclass, 'read_parquet_folder') and 
                callable(subclass.read_parquet_folder) and
                hasattr(subclass, 'delete_file') and 
                callable(subclass.delete_file) and
                hasattr(subclass, 'delete_folder') and 
                callable(subclass.delete_folder) and
                hasattr(subclass, 'move_file') and 
                callable(subclass.move_file) and
                hasattr(subclass, 'move_folder') and 
                callable(subclass.move_folder) and
                hasattr(subclass, 'renema_file') and 
                callable(subclass.renema_file) and
                hasattr(subclass, 'renema_folder') and 
                callable(subclass.renema_folder) and
                hasattr(subclass, 'read_excel_file') and 
                callable(subclass.read_excel_file) and
                hasattr(subclass, 'save_listdataframe_as_xlsx') and 
                callable(subclass.save_listdataframe_as_xlsx) and
                hasattr(subclass, 'create_empty_file') and 
                callable(subclass.create_empty_file) and
                hasattr(subclass, '_check_is_blob') and 
                callable(subclass._check_is_blob) and
                hasattr(subclass, 'save_json_file') and
                callable(subclass.save_json_file)
                
                or  NotImplemented)
        
    _ENGINE_TYPES = Literal['pandas', 'polars']
    _ENCODING_TYPES = Literal['UTF-8', 'UTF-16']
    
    
    _ORIENT_TYPES = Literal['records', 'columns'] 
        
    @classmethod
    def read_csv_bytes(self,bytes:bytes,engine:_ENGINE_TYPES ='pandas',sourceEncoding :_ENCODING_TYPES= "UTF-8", columnDelimiter :str= ";",isFirstRowAsHeader :bool= False,skipRows:int=0, skipBlankLines = True) ->pd.DataFrame:
        if engine=='pandas':
            df = pd.read_csv(BytesIO(bytes),sep=columnDelimiter,quoting=3  ,engine="python",dtype='str',
                header= 0 if isFirstRowAsHeader==True else None ,
                encoding=sourceEncoding,skiprows=skipRows,keep_default_na=False,
                na_values=nanVal,skip_blank_lines=skipBlankLines)
        elif engine =='polars':
            df = pl.read_csv(BytesIO(bytes),separator=columnDelimiter,has_header=isFirstRowAsHeader,encoding=sourceEncoding,
                        skip_rows=skipRows,null_values=nanVal,infer_schema_length=0)
        return df

    @classmethod
    def read_parquet_bytes(self,bytes:bytes,engine:_ENGINE_TYPES ='pandas',columns:list=None) ->pd.DataFrame:
        if engine =='pandas':
            df = pd.read_parquet(BytesIO(bytes),'auto',columns)
        elif engine =='polars':
            df = pl.read_parquet(BytesIO(bytes),columns=columns)
        return df
    @classmethod
    def create_container(self,containerName : str):
        container_client = self.__service_client.get_container_client(container=containerName)   
        if not container_client.exists():
            self.__service_client.create_container(name=containerName)
        
    @classmethod
    def delete_container(self,containerName : str):
        self.__service_client.delete_container(name=containerName)    
    
    @abc.abstractmethod
    def _check_is_blob(self):
        """info"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_binary_file(self):
        """info"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_binary_file(self):
        """info"""
        raise NotImplementedError

    @abc.abstractmethod
    def read_csv_file(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_csv_folder(self) ->pd.DataFrame:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_excel_file(self):
        """infor"""
        raise NotImplementedError
    
    
    @abc.abstractmethod
    def save_dataframe_as_csv(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_parquet(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_parqarrow(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_listdataframe_as_xlsx(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_file(self)->pd.DataFrame:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_folder(self) ->pd.DataFrame:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_file(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_folder(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def move_file(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def move_folder(self)->bool:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def renema_file(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def renema_folder(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def create_empty_file(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def create_container(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_container(self):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_json_file(self):
        raise NotImplementedError
    
