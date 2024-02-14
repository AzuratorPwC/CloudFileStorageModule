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
        """
        Creates a container in the Azure Blob Storage.

        Args:
            container_name (str): The name of the container to create.
            public_access (CONTAINER_ACCESS_TYPES, optional): The level of public access for the container. Defaults to None.

        Returns:
            None
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_container(self,container_name:str):
        """
        Deletes a container from Azure Blob Storage, if it exists.

        Args:
            container_name (str): The name of the container to be deleted.

        Returns:
            None

        Raises:
            ContainerNotFound: If the specified container does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def check_is_dfs(self)->bool:
        """
        Check if the Azure Blob Storage account is a Data Lake Storage Gen2 account.

        Returns:
            bool: True if the account is a Data Lake Storage Gen2 account, False otherwise.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_binary_file(self, inputbytes:bytes,container_name : str,directory_path : str,file_name:str,source_encoding:ENCODING_TYPES = "UTF-8",is_overwrite :bool=True):
        """
        Save a binary file to the specified container in the cloud storage.

        Args:
            inputbytes (bytes): The binary data to be saved.
            container_name (str): The name of the container in the cloud storage.
            directory_path (str): The directory path within the container to save the file.
            file_name (str): The name of the file to be saved.
            source_encoding (ENCODING_TYPES, optional): The encoding type of the input data.
                Defaults to "UTF-8".
            is_overwrite (bool, optional): Flag indicating whether to overwrite the file if it
                already exists. Defaults to True.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_binary_file(self, container_name: str, directory_path: str, file_name: str):
        """
        Reads a binary file from the specified container, directory, and file name.

        Args:
            container_name (str): The name of the container.
            directory_path (str): The path to the directory where the file is located.
            file_name (str): The name of the file.

        Returns:
            bytes: The content of the binary file.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def read_csv_file(self,container_name:str,directory_path:str,sourcefile_name:str,engine:ENGINE_TYPES = 'polars',source_encoding:ENCODING_TYPES = "UTF-8", column_delimiter:DELIMITER_TYPES = ',',is_first_row_as_header:bool = False,skip_rows:int=0,skip_blank_lines = True,tech_columns:bool=False):
        """
        Read a CSV file from an Azure Blob Storage container and return the data as a DataFrame.

        Args:
            container_name (str): The name of the Azure Blob Storage container.
            directory_path (str): The path within the container where the CSV file is located.
            sourcefile_name (str): The name of the CSV file to be read.
            engine (ENGINE_TYPES, optional): The processing engine to use ('pandas' or 'polars').
                Defaults to 'polars'.
            source_encoding (ENCODING_TYPES, optional): The encoding type of the CSV file. Defaults
                to "UTF-8".
            column_delimiter (DELIMITER_TYPES, optional): The delimiter used in the CSV file.
                Defaults to ','.
            is_first_row_as_header (bool, optional): Flag indicating whether the first row is
                a header. Defaults to False.
            skip_rows (int, optional): Number of rows to skip from the beginning of the file.
                Defaults to 0.
            skip_blank_lines (bool, optional): Flag indicating whether to skip blank lines. Defaults
                to True.
            tech_columns (bool, optional): Flag indicating whether to add technical columns (e.g.,
                file path and name). Defaults to False.

        Returns:
            DataFrame: A DataFrame containing the data from the CSV file.
        """
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
        """
        Saves a Pandas DataFrame as Parquet format in Azure Blob Storage.

        Args:
            df (pd.DataFrame): The DataFrame to be saved.
            container_name (str): The name of the container in Azure Blob Storage.
            directory_path (str): The path of the directory within the container.
            partition_columns (list, optional): A list of columns to be used for partitioning the data. Defaults to None.
            compression (str, optional): The compression method for the Parquet file ('snappy', 'gzip', 'brotli', None).
                Defaults to None.

        Returns:
            None

        Raises:
            ValueError: If the compression method is not valid.
            ResourceNotFoundError: If the specified container or directory does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_dataframe_as_parqarrow(self,df:pd.DataFrame,container_name : str,directory_path:str,partition_columns:list=None,compression:str=None):
        """
        Saves a Pandas DataFrame as Parquet Arrow format in Azure Blob Storage.

        Args:
            df (pd.DataFrame): The DataFrame to be saved.
            container_name (str): The name of the container in Azure Blob Storage.
            directory_path (str): The path of the directory within the container.
            partition_columns (list, optional): A list of columns to be used for partitioning the data. Defaults to None.
            compression (str, optional): The compression method for the Parquet file ('NONE', 'SNAPPY', 'GZIP', 'BROTLI', 'LZ4', 'ZSTD').
                Defaults to None.

        Returns:
            None

        Raises:
            ValueError: If the compression method is not valid.
            ResourceNotFoundError: If the specified container or directory does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """   
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_listdataframe_as_xlsx(self,list_df:list, sheets:list, container_name : str,directory_path:str ,file_name:str,index=False,header=False):
        """
        Saves a list of Pandas DataFrames as separate sheets in an Excel file in Azure Blob Storage.

        Args:
            list_df (list): A list of Pandas DataFrames to be saved as sheets.
            sheets (list): A list of sheet names corresponding to the DataFrames.
            container_name (str): The name of the container in Azure Blob Storage.
            directory_path (str): The path of the directory within the container.
            file_name (str): The name of the Excel file.
            index (bool, optional): If True, includes the DataFrame index in the Excel file. Defaults to False.
            header (bool, optional): If True, includes the DataFrame column names in the Excel file. Defaults to False.

        Returns:
            None

        Raises:
            ResourceNotFoundError: If the specified container or directory does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_file(self, container_name: str, directory_path: str,sourcefile_name:str, columns: list = None,tech_columns:bool=False)->pd.DataFrame:
        """
        Reads a Parquet file from Azure Blob Storage and returns its content as a Pandas DataFrame.

        Args:
            container_name (str): The name of the container containing the Parquet file.
            directory_path (str): The path of the directory containing the Parquet file.
            sourcefile_name (str): The name of the Parquet file to be read.
            columns (list, optional): A list of column names to be read from the Parquet file. Defaults to None.
            tech_columns (bool, optional): If True, adds technical columns to the DataFrame. Defaults to False.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the content of the Parquet file.

        Raises:
            ResourceNotFoundError: If the specified container, directory, or file does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_parquet_folder(self) ->pd.DataFrame:
        """
        Reads multiple Parquet files from a folder in Azure Blob Storage and returns their content as a Pandas DataFrame.

        Args:
            container_name (str): The name of the container containing the Parquet files.
            directory_path (str): The path of the directory containing the Parquet files.
            folders (list, optional): A list of folder names to filter the files. Defaults to None.
            columns (list, optional): A list of column names to be read from the Parquet files. Defaults to None.
            tech_columns (bool, optional): If True, adds technical columns to the DataFrame. Defaults to False.
            recursive (bool, optional): If True, includes files from subdirectories. Defaults to False.

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the content of the Parquet files.

        Raises:
            ResourceNotFoundError: If the specified container or directory does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_file(self,container_name : str,directory_path : str,file_name:str,wait:bool=True):
        """
        Deletes a file from Azure Blob Storage.

        Args:
            container_name (str): The name of the container containing the file.
            directory_path (str): The path of the directory containing the file.
            file_name (str): The name of the file to be deleted.
            wait (bool, optional): If True, waits for the deletion to complete before returning. Defaults to True.

        Returns:
            None

        Raises:
            ResourceNotFoundError: If the specified container, directory, or file does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def delete_folder(self,container_name : str,directory_path : str,wait:bool=True):
        """
        Deletes a folder and its contents from Azure Blob Storage.

        Args:
            container_name (str): The name of the container containing the folder.
            directory_path (str): The path of the folder to be deleted.
            wait (bool, optional): If True, waits for the deletion to complete before returning. Defaults to True.

        Returns:
            None

        Raises:
            ResourceNotFoundError: If the specified container or directory does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def move_file(self,container_name : str,directory_path : str,file_name:str,new_container_name : str,new_directory_path : str,newfile_name:str,is_overwrite :bool=True,is_delete_source_folder:bool=False):
        """
        Moves a file from one location to another within Azure Blob Storage.

        Args:
            container_name (str): The name of the container containing the source file.
            directory_path (str): The path of the directory containing the source file.
            file_name (str): The name of the source file to be moved.
            new_container_name (str): The name of the container where the file will be moved.
            new_directory_path (str): The path of the directory where the file will be moved.
            new_file_name (str): The new name for the moved file.
            is_overwrite (bool, optional): If True, overwrites the destination file if it already exists. Defaults to True.
            is_delete_source_file (bool, optional): If True, deletes the source file after moving. Defaults to False.

        Returns:
            None

        Raises:
            ResourceNotFoundError: If the specified source container, directory, or file does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def move_folder(self,container_name : str,directory_path : str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_folder:bool=False)->bool:
        """
        Moves all files from one folder to another within Azure Blob Storage.

        Args:
            container_name (str): The name of the container containing the source folder.
            directory_path (str): The path of the source folder.
            new_container_name (str): The name of the container where the folder will be moved.
            new_directory_path (str): The path of the target directory where the folder will be moved.
            is_overwrite (bool, optional): If True, overwrites destination files if they already exist. Defaults to True.
            is_delete_source_folder (bool, optional): If True, deletes the source folder after moving. Defaults to False.

        Returns:
            bool: True if the folder move operation is successful.

        Raises:
            ResourceNotFoundError: If the specified source container or directory does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def renema_file(self,container_name : str,directory_path : str,file_name:str,newfile_name:str):
        """
        Renames a file within a specified container in Azure Blob Storage.

        Args:
            container_name (str): The name of the container containing the file.
            directory_path (str): The path of the directory containing the file.
            file_name (str): The name of the file to be renamed.
            new_file_name (str): The new name for the renamed file.

        Returns:
            None

        Raises:
            ResourceNotFoundError: If the specified container, directory, or source file does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def renema_folder(self,container_name : str,directory_path : str,newdirectory_path:str):
        """
        Renames a folder within a specified container in Azure Blob Storage.

        Args:
            container_name (str): The name of the container containing the folder.
            directory_path (str): The path of the folder to be renamed.
            new_directory_path (str): The new path for the renamed folder.

        Returns:
            None

        Raises:
            ResourceNotFoundError: If the specified container or source directory does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def create_empty_file(self,container_name : str,directory_path : str,file_name:str):
        """
        Creates an empty file within a specified container in Azure Blob Storage.

        Args:
            container_name (str): The name of the container where the file will be created.
            directory_path (str): The directory path within the container. Use an empty string for the root directory.
            file_name (str): The name of the file to be created.

        Returns:
            None

        Raises:
            ResourceNotFoundError: If the specified container does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_json_file(self, df: [pd.DataFrame, pl.DataFrame], container_name: str, directory: str, file:str = None, engine: ENGINE_TYPES ='polars', orient:ORIENT_TYPES= 'records'):
        """
        Saves a Pandas or Polars DataFrame to a JSON file in Azure Blob Storage.

        Args:
            df ([pd.DataFrame, pl.DataFrame]): The DataFrame to be saved.
            container_name (str): The name of the container in Azure Blob Storage.
            directory (str): The path of the directory within the container.
            file (str, optional): The name of the JSON file. If not provided, a random name will be generated. Defaults to None.
            engine (ENGINE_TYPES, optional): The DataFrame engine type ('pandas' or 'polars'). Defaults to 'polars'.
            orient (ORIENT_TYPES, optional): The JSON orientation type ('records', 'split', 'index', 'columns', or 'values'). Defaults to 'records'.

        Returns:
            None

        Raises:
            Exception: If the DataFrame argument is not a Pandas or Polars DataFrame.
            ResourceNotFoundError: If the specified container or directory does not exist.
            StorageErrorException: If there is an issue with the storage service.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def ls_files(self,container_name : str, directory_path : str, recursive:bool=False):
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
        raise NotImplementedError
