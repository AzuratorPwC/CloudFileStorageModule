
import abc
import pandas as pd
from io import BytesIO

    #, 'N/A'
nanVal = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN',
                           '<NA>', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']




class StorageAccountVirtualClass(metaclass=abc.ABCMeta):
    

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
                hasattr(subclass, 'save_dataframe_as_parquet_pandas') and 
                callable(subclass.save_dataframe_as_parquet_pandas) and 
                hasattr(subclass, 'save_dataframe_as_parquet_arrow') and 
                callable(subclass.save_dataframe_as_parquet_arrow) and 
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
                hasattr(subclass, 'create_empty_file') and 
                callable(subclass.create_empty_file) and
                hasattr(subclass, 'create_container') and 
                callable(subclass.create_container) and
                hasattr(subclass, 'delete_container') and 
                callable(subclass.delete_container)
                or  NotImplemented)
        
        
        
    
    def __read_csv_bytes(self,bytes:bytes,sourceEncoding :str= "UTF-8", columnDelimiter :str= ";",isFirstRowAsHeader :bool= False,skipRows:int=0) ->pd.DataFrame:
        df = pd.read_csv(BytesIO(bytes),sep=columnDelimiter,quoting=3  ,engine="python",dtype='str',
                header= 0 if isFirstRowAsHeader==True else None ,
                encoding=sourceEncoding,skiprows=skipRows,keep_default_na=False,
                na_values=nanVal)
        return df

    def __read_parquet_bytes(self,bytes:bytes,columns:list=None) ->pd.DataFrame:
        df = pd.read_parquet(BytesIO(bytes),'auto',columns)
        return df
    
    @abc.abstractmethod
    def save_binary_file(self, bytes:bytes,containerName : str,directoryPath : str,fileName:str,isOverWrite :bool=True):
        """info"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_binary_file(self,containerName : str,directoryPath : str,fileName:str):
        """info"""
        raise NotImplementedError

    @abc.abstractmethod
    def read_csv_file(self,containerName:str,directoryPath:str,sourceFileName:str,sourceEncoding:str = "UTF-8", columnDelimiter:str = ";",isFirstRowAsHeader:bool = False,skipRows:int=0):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_csv_folder(self,containerName:str,directoryPath:str,sourceEncoding :str= "UTF-8", columnDelimiter:str = ";",isFirstRowAsHeader:bool = False,skipRows:int=0,recursive:bool=False):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_parquet_pandas(self,df:pd.DataFrame,containerName : str,directoryPath:str,partitionCols:list=None,compression:str=None):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_parquet_arrow(self,df:pd.DataFrame,containerName : str,directoryPath:str,partitionCols:list=None,compression:str='NONE'):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_file(self, containerName: str, directoryPath: str,sourceFileName:str, columns: list = None):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_folder(self,containerName:str,mainDirectoryPath:str,includeSubfolders:list=None,columns:list = None,recursive:bool=False):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_file(self,containerName : str,directoryPath : str,fileName:str):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_folder(self,containerName : str,directoryPath : str):
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
