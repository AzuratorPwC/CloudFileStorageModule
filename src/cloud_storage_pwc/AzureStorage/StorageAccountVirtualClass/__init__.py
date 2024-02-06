import abc
from io import BytesIO
import pandas as pd
import polars as pl
from ..Utils import CONTAINER_ACCESS_TYPES,ENCODING_TYPES,ENGINE_TYPES,ORIENT_TYPES,DELIMITER_TYPES,QUOTING_TYPES,NAN_VALUES


class StorageAccountVirtualClass(metaclass=abc.ABCMeta):
    """Class representing a StorageAccountVirtualClass"""

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
                hasattr(subclass, 'check_is_dfs') and
                callable(subclass.check_is_dfs) and
                hasattr(subclass, 'save_json_file') and
                callable(subclass.save_json_file) and
                hasattr(subclass, 'ls_files') and
                callable(subclass.ls_files)
                or  NotImplemented)

    @classmethod
    def read_csv_bytes(cls,bytes:bytes,engine:ENGINE_TYPES ='pandas',source_encoding:ENCODING_TYPES= "UTF-8", column_delimiter :DELIMITER_TYPES= ',',is_first_row_as_header :bool= False,skip_rows:int=0, skip_blank_lines = True,quoting:QUOTING_TYPES='QUOTE_NONE') ->pd.DataFrame:
        """Class representing a StorageAccountVirtualClass"""
        if engine == 'pandas':
            df = pd.read_csv(BytesIO(bytes),sep=column_delimiter,quoting=3,quotechar='"',engine="python",dtype='str',
                header= 0 if is_first_row_as_header is True else None ,
                encoding=source_encoding,skiprows=skip_rows,keep_default_na=False,
                na_values=NAN_VALUES,skip_blank_lines=skip_blank_lines)
            
        elif engine =='polars':
            df = pl.read_csv(BytesIO(bytes),separator=column_delimiter,has_header=is_first_row_as_header,encoding=source_encoding,
                        skip_rows=skip_rows,null_values=NAN_VALUES,infer_schema_length=0)
        return df

    def read_parquet_bytes(self,bytes:bytes,engine:ENGINE_TYPES ='pandas',columns:list=None) ->pd.DataFrame:
        """Class representing a StorageAccountVirtualClass"""
        if engine =='pandas':
            df = pd.read_parquet(BytesIO(bytes),'auto',columns)
        elif engine =='polars':
            df = pl.read_parquet(BytesIO(bytes),columns=columns)
        return df
    @abc.abstractmethod
    def create_container(self,container_name : str,public_access:CONTAINER_ACCESS_TYPES='Private'):
        """info"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_container(self,container_name:str):
        """info"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def check_is_dfs(self)->bool:
        """info"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_binary_file(self, inputbytes:bytes,container_name : str,directory_path : str,file_name:str,source_encoding:ENCODING_TYPES = "UTF-8",is_overwrite :bool=True):
        """info"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_binary_file(self, container_name: str, directory_path: str, file_name: str):
        """info"""
        raise NotImplementedError

    @abc.abstractmethod
    def read_csv_file(self,container_name:str,directory_path:str,sourcefile_name:str,engine:ENGINE_TYPES = 'polars',source_encoding:ENCODING_TYPES = "UTF-8", column_delimiter:DELIMITER_TYPES = ',',is_first_row_as_header:bool = False,skip_rows:int=0,skip_blank_lines = True,tech_columns:bool=False):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_csv_folder(self,container_name:str,directory_path:str,engine: ENGINE_TYPES = 'polars',source_encoding:ENCODING_TYPES = "UTF-8", column_delimiter:DELIMITER_TYPES = ",",is_first_row_as_header:bool = False,skip_rows:int=0,skip_blank_lines=True,tech_columns:bool=False,recursive:bool=False) ->pd.DataFrame:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_excel_file(self,container_name:str,directory_path:str,sourcefile_name:str,engine: ENGINE_TYPES ='polars',skip_rows:int = 0,is_first_row_as_header:bool = False,sheets:list()=None,tech_columns:bool=False):
        """infor"""
        raise NotImplementedError
    
    
    @abc.abstractmethod
    def save_dataframe_as_csv(self,df:[pd.DataFrame, pl.DataFrame],container_name : str,directory_path:str,file:str=None,partition_columns:list=None,source_encoding:ENCODING_TYPES= "UTF-8", column_delimiter:str = ";",is_first_row_as_header:bool = True,quoteChar:str=' ',quoting:['never', 'always', 'necessary']='never',escapeChar:str="\\", engine: ENGINE_TYPES ='polars'):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_parquet(self,df:pd.DataFrame,container_name : str,directory_path:str,partition_columns:list=None,compression:str=None):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_parqarrow(self,df:pd.DataFrame,container_name : str,directory_path:str,partition_columns:list=None,compression:str=None):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_listdataframe_as_xlsx(self,list_df:list, sheets:list, container_name : str,directory_path:str ,file_name:str,index=False,header=False):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_file(self, container_name: str, directory_path: str,sourcefile_name:str, columns: list = None,tech_columns:bool=False)->pd.DataFrame:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_folder(self) ->pd.DataFrame:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_file(self,container_name : str,directory_path : str,file_name:str,wait:bool=True):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_folder(self,container_name : str,directory_path : str,wait:bool=True):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def move_file(self,container_name : str,directory_path : str,file_name:str,new_container_name : str,new_directory_path : str,newfile_name:str,is_overwrite :bool=True,is_delete_source_folder:bool=False):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def move_folder(self,container_name : str,directory_path : str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_folder:bool=False)->bool:
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def renema_file(self,container_name : str,directory_path : str,file_name:str,newfile_name:str):
        
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def renema_folder(self,container_name : str,directory_path : str,newdirectory_path:str):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def create_empty_file(self,container_name : str,directory_path : str,file_name:str):
        """infor"""
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_json_file(self, df: [pd.DataFrame, pl.DataFrame], container_name: str, directory: str, file:str = None, engine: ENGINE_TYPES ='polars', orient:ORIENT_TYPES= 'records'):
        raise NotImplementedError
    
    @abc.abstractmethod
    def ls_files(self,container_name : str, directory_path : str, recursive:bool=False):
        raise NotImplementedError
