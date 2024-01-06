
import abc
import pandas as pd
import polars as pl
import csv
from io import BytesIO

    #, 'N/A'
nanVal = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN',
                           '<NA>', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']

class DataFromExcel:
    def __init__(self,df,sheetName):
        self.data = df
        self.sheet = sheetName



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
                hasattr(subclass, 'create_container') and 
                callable(subclass.create_container) and
                hasattr(subclass, 'delete_container') and 
                callable(subclass.delete_container) and
                hasattr(subclass, '_check_is_blob') and 
                callable(subclass._check_is_blob) and
                hasattr(subclass, 'save_json_file') and
                callable(subclass.save_json_file)
                
                or  NotImplemented)
        
        
    @classmethod
    def read_csv_bytes(self,bytes:bytes,engine:('pandas','polars') ='pandas',sourceEncoding :str= "UTF-8", columnDelimiter :str= ";",isFirstRowAsHeader :bool= False,skipRows:int=0, skipBlankLines = True) ->pd.DataFrame:
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
    def read_parquet_bytes(self,bytes:bytes,engine:('pandas','polars') ='pandas',columns:list=None) ->pd.DataFrame:
        if engine =='pandas':
            df = pd.read_parquet(BytesIO(bytes),'auto',columns)
        elif engine =='polars':
            df = pl.read_parquet(BytesIO(bytes),columns=columns)
        return df
    
    @abc.abstractmethod
    def _check_is_blob(self):
        """info"""
        raise NotImplementedError

    
    
    
    @abc.abstractmethod
    def save_binary_file(self, inputbytes:bytes,containerName : str,directoryPath : str,fileName:str,sourceEncoding:str = "UTF-8",isOverWrite :bool=True):
        """info"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_binary_file(self,containerName : str,directoryPath : str,fileName:str):
        """info"""
        raise NotImplementedError

    @abc.abstractmethod
    def read_csv_file(self,containerName:str,directoryPath:str,sourceFileName:str,engine:('pandas','polars') ='pandas',sourceEncoding:str = "UTF-8", columnDelimiter:str = ";",isFirstRowAsHeader:bool = False,skipRows:int=0,skipBlankLines = True,addStrTechCol:bool=False):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_csv_folder(self,containerName:str,directoryPath:str,engine:('pandas','polars') ='pandas',includeSubfolders:list=None,sourceEncoding :str= "UTF-8", columnDelimiter:str = ";",isFirstRowAsHeader:bool = False,skipRows:int=0,skipBlankLines=True,addStrTechCol:bool=False,recursive:bool=False) ->pd.DataFrame:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_excel_file(self,containerName:str,directoryPath:str,sourceFileName:str,engine:('pandas','polars') ='pandas',skipRows:int = 0,isFirstRowAsHeader:bool = False,sheets:list()=None,addStrTechCol:bool=False):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_csv(self,df:pd.DataFrame,containerName : str,directoryPath:str,file:str=None,partitionCols:list=None,sourceEncoding:str= "UTF-8", columnDelimiter:str = ";",isFirstRowAsHeader:bool = True,quoteChar:str='',quoting:(csv.QUOTE_NONE,csv.QUOTE_ALL,csv.QUOTE_MINIMAL)=csv.QUOTE_NONE,escapeChar:str="\\"):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_parquet(self,df:pd.DataFrame,containerName : str,directoryPath:str,partitionCols:list=None,compression:str=None):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_parqarrow(self,df:pd.DataFrame,containerName : str,directoryPath:str,partitionCols:list=None,compression:str='NONE'):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_listdataframe_as_xlsx(self,list_df:list, list_sheetnames:list, containerName : str,directoryPath:str ,fileName:str):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_file(self, containerName: str, directoryPath: str,sourceFileName:str, columns: list = None,addStrTechCol:bool=False)->pd.DataFrame:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_folder(self,containerName:str,directoryPath:str,includeSubfolders:list=None,columns:list = None,addStrTechCol:bool=False,recursive:bool=False) ->pd.DataFrame:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_file(self,containerName : str,directoryPath : str,fileName:str,wait:bool=True):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_folder(self,containerName : str,directoryPath : str,wait:bool=True):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def move_file(self,containerName : str,directoryPath : str,fileName:str,newContainerName : str,newDirectoryPath : str,newFileName:str,isOverWrite :bool=True,isDeleteSourceFile:bool=False):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def move_folder(self,containerName : str,directoryPath : str,newContainerName : str,newDirectoryPath : str,isOverWrite :bool=True,isDeleteSourceFolder:bool=False)->bool:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def renema_file(self,containerName : str,directoryPath : str,fileName:str,newFileName:str):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def renema_folder(self,containerName : str,directoryPath : str,newDirectoryPath:str):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def create_empty_file(self,containerName : str,directoryPath : str,fileName:str):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def create_container(self,containerName : str):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_container(self,containerName : str):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_json_file(self, df:[pd.DataFrame, pl.DataFrame], containerName: str, directory: str, file:str = None, engine:['polars', 'pandas'] = 'polars', orient: ['records', 'columns'] = 'records'):
        raise NotImplementedError
    
