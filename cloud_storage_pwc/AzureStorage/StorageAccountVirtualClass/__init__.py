import abc
from atexit import register
from io import BytesIO
import pandas as pd
import polars as pl
from ..Utils import *
from ..Exceptions import *
from io import BytesIO
import uuid
import numpy as np 
import pandas as pd
from itertools import product
import polars as pl
from ..Utils import *
from ..Exceptions import *
import logging



class StorageAccountVirtualClass(metaclass=abc.ABCMeta):
    """Class representing a StorageAccountVirtualClass"""

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'save_binary_file') and
                callable(subclass.save_binary_file) and
                hasattr(subclass, 'read_binary_file') and
                callable(subclass.read_binary_file) and
                hasattr(subclass, 'delete_file') and
                callable(subclass.delete_file) and
                hasattr(subclass, 'delete_folder') and
                callable(subclass.delete_folder) and
                hasattr(subclass, 'move_file') and
                callable(subclass.move_file) and
                hasattr(subclass, 'move_folder') and
                callable(subclass.move_folder) and
                hasattr(subclass, 'rename_file') and
                callable(subclass.rename_file) and
                hasattr(subclass, 'rename_folder') and
                callable(subclass.rename_folder) and
                hasattr(subclass, 'create_empty_file') and
                callable(subclass.create_empty_file) and
                hasattr(subclass, 'ls_files') and
                callable(subclass.ls_files) and
                hasattr(subclass, 'delete_files_by_prefix') and
                callable(subclass.delete_files_by_prefix) and
                hasattr(subclass, 'file_exists') and
                callable(subclass.file_exists) and
                hasattr(subclass, 'folder_exists') and
                callable(subclass.folder_exists)
                or  NotImplemented)


    @classmethod
    def read_csv_bytes(cls,input_bytes:bytes,engine:ENGINE_TYPES ='pandas',encoding:ENCODING_TYPES= "UTF-8", delimiter :str= ',',is_first_row_as_header :bool= False,skip_rows:int=0, skip_blank_lines = True,quoting:QUOTING_TYPES=None):
        """Class representing a StorageAccountVirtualClass"""
        if engine == 'pandas':
            if quoting is not None:
                df = pd.read_csv(BytesIO(input_bytes),sep=delimiter,quoting=1,quotechar=quoting,engine="python",dtype='str',
                    header= 0 if is_first_row_as_header is True else None ,
                    encoding=encoding,skiprows=skip_rows,keep_default_na=False,
                    na_values=NAN_VALUES_REGEX_PANDAS,skip_blank_lines=skip_blank_lines)
            else:
                df = pd.read_csv(BytesIO(input_bytes),sep=delimiter,engine="python",dtype='str',
                    header= 0 if is_first_row_as_header is True else None ,
                    encoding=encoding,skiprows=skip_rows,keep_default_na=False,
                    na_values=NAN_VALUES_REGEX_PANDAS,skip_blank_lines=skip_blank_lines)
                    
        elif engine =='polars':
            if quoting is not None:
                df = pl.read_csv(BytesIO(input_bytes),separator=delimiter,has_header=is_first_row_as_header,encoding=encoding,
                        skip_rows=skip_rows,null_values=NAN_VALUES_REGEX_PANDAS,infer_schema_length=0,quote_char=quoting,)
            else:
                df = pl.read_csv(BytesIO(input_bytes),separator=delimiter,has_header=is_first_row_as_header,encoding=encoding,
                        skip_rows=skip_rows,null_values=NAN_VALUES_REGEX_PANDAS,infer_schema_length=0)
        return df

    def read_parquet_bytes(self,input_bytes:bytes,engine:ENGINE_TYPES ='pandas',columns:list=None):
        """Class representing a StorageAccountVirtualClass"""
        if engine =='pandas':
            df = pd.read_parquet(BytesIO(input_bytes),'auto',columns)
        elif engine =='polars':
            df = pl.read_parquet(BytesIO(input_bytes),columns=columns)
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
    def file_exists(self, container_name : str,directory_path : str,file_name:str)->bool:
        raise NotImplementedError
    
    @abc.abstractmethod
    def folder_exists(self, container_name : str, directory_path : str)->bool:
        raise NotImplementedError
    
    @abc.abstractmethod
    def save_binary_file(self, input_bytes:bytes, container_name:str, directory_path:str,file_name:str,is_overwrite:bool=True):
        """
        Save a binary file to the specified container in the cloud storage.

        Args:
            inputbytes (bytes): The binary data to be saved.
            container_name (str): The name of the container in the cloud storage.
            directory_path (str): The directory path within the container to save the file.
            file_name (str): The name of the file to be saved.
            encoding (ENCODING_TYPES, optional): The encoding type of the input data.
                Defaults to "UTF-8".
            is_overwrite (bool, optional): Flag indicating whether to overwrite the file if it
                already exists. Defaults to True.
        """
        raise NotImplementedError
    
    @abc.abstractmethod
    def read_binary_file(self, container_name: str, directory_path: str, file_name: str)-> bytes:
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

    def read_csv_file(self, container_name:str, directory_path:str, file_name:str,
                      engine:ENGINE_TYPES='polars', encoding:ENCODING_TYPES="UTF-8",
                      delimiter:str=',', is_first_row_as_header:bool=False,
                      skip_rows:int=0, skip_blank_lines=True,quoting:QUOTING_TYPES=None, tech_columns:bool=False):
        """
        Read a CSV file from an Azure Blob Storage container and return the data as a DataFrame.

        Args:
            container_name (str): The name of the Azure Blob Storage container.
            directory_path (str): The path within the container where the CSV file is located.
            sourcefile_name (str): The name of the CSV file to be read.
            engine (ENGINE_TYPES, optional): The processing engine to use ('pandas' or 'polars').
                Defaults to 'polars'.
            encoding (ENCODING_TYPES, optional): The encoding type of the CSV file. Defaults
                to "UTF-8".
            delimiter (str, optional): The delimiter used in the CSV file.
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
        download_bytes = self.read_binary_file(container_name,directory_path,file_name)
        df = self.read_csv_bytes(download_bytes, engine, encoding, delimiter,is_first_row_as_header, skip_rows, skip_blank_lines,quoting=quoting)
        
        if tech_columns:
            df =  add_tech_columns(df,container_name,directory_path.replace("\\","/"),file_name)
        return df
    
    def read_csv_folder(self,container_name:str,directory_path:str,engine: ENGINE_TYPES = 'polars',encoding:ENCODING_TYPES = "UTF-8", delimiter:str = ",",is_first_row_as_header:bool = False,skip_rows:int=0,skip_blank_lines=True,quoting:QUOTING_TYPES=None,tech_columns:bool=False,recursive:bool=False):
        list_files = self.ls_files(container_name,directory_path, recursive=recursive)
        df = None
        if list_files:
            for f in list_files:
                df_new = self.read_csv_file(container_name,directory_path,str(f).removeprefix(directory_path),engine,encoding,delimiter,is_first_row_as_header,skip_rows,skip_blank_lines,quoting, tech_columns)
                if engine=='pandas':
                    if df is None:
                        df = df_new
                    else:
                        df = pd.concat([df, df_new], axis=0, join="outer", ignore_index=True)
                elif engine =='polars':
                    if df is None:
                        df = df_new
                    else:
                        df = pl.concat([df, df_new])
            return df
        else:
            raise FolderDataNotFound(f"Folder data {directory_path} not found in container {container_name}")
    

    
    def read_excel_file(self,container_name:str,directory_path:str,file_name:str,engine: ENGINE_TYPES ='polars',skip_rows:int = 0,is_first_row_as_header:bool = False,sheets=None,is_check_sheet_exist:bool=False,tech_columns:bool=False):
        download_bytes = self.read_binary_file(container_name,directory_path,file_name)
        workbook = None
        if isinstance(sheets, str):
            sheets = (sheets,)
            
            
        if is_check_sheet_exist or sheets is None:
            if file_name.endswith(".xls"):
                workbook = pd.ExcelFile(BytesIO(download_bytes), engine='xlrd')
            else:
                workbook = pd.ExcelFile(BytesIO(download_bytes))
            workbook_sheetnames = workbook.sheet_names
            if bool(sheets):
                if  set(sheets).issubset(workbook_sheetnames) is False:
                    raise ExcelSheetNotFound(f"Sheet {sheets} not found in file {file_name}")
                else:
                    load_sheets = sheets
            else:
                load_sheets = workbook_sheetnames
        else:
            load_sheets = sheets
            
            
        list_of_dff = {}
        if is_first_row_as_header:
            is_first_row_as_header=0
        else:
            is_first_row_as_header=None
        
        if engine == 'pandas':
        
            if workbook is None:
                if file_name.endswith(".xls"):
                    workbook = pd.ExcelFile(BytesIO(download_bytes), engine='xlrd')
                else:
                    workbook = pd.ExcelFile(BytesIO(download_bytes))

            for sheet in load_sheets:
                dff = pd.read_excel(workbook, sheet_name = sheet,skiprows=skip_rows, index_col = None, header = is_first_row_as_header)
        
                if tech_columns:
                    dff =  add_tech_columns(dff,container_name,directory_path.replace("\\","/"),file_name)
                
                list_of_dff[sheet]=dff
        elif engine == 'polars':
            list_of_dff = pl.read_excel(BytesIO(download_bytes),engine="calamine",sheet_name=load_sheets
                        #,read_options={"has_header": False}
                )
            if tech_columns:
                for key in list_of_dff.keys():
                    list_of_dff[key] = add_tech_columns(list_of_dff[key],container_name,directory_path.replace("\\","/"),file_name)
        
        return list_of_dff
    
    
    def save_dataframe_as_csv(self,df,container_name : str,directory_path:str,file_name:str=None,partition_columns:list=None,encoding:ENCODING_TYPES= "UTF-8", delimiter:str = ";",is_first_row_as_header:bool = True,quoting:QUOTING_TYPES=None,escape:ESCAPE_TYPES=None, engine: ENGINE_TYPES ='polars'):
        
        if isinstance(df, pd.DataFrame):
            if df.empty:
                return
            df = df.replace('\n', ' ', regex=True)
            #if not (df.filter(regex=r'^\s*$').empty and df.filter(items=NAN_VALUES_REGEX_PANDAS).empty):
            df = df.astype(str)
            df = df.replace(regex=NAN_VALUES_REGEX_PANDAS, value="")
            
            #df = df.replace(NAN_VALUES_REGEX_PANDAS, '', regex=True)
            if engine != 'pandas':
                df = pl.from_pandas(df)

        elif isinstance(df, pl.DataFrame):
            if df.is_empty():
                return
            df = df.with_columns(pl.col(pl.Utf8).str.replace_all('\n', ' '))
            df=df.with_columns(pl.exclude(pl.Utf8).cast(str))
            df = df.with_columns(
                            pl.col(pl.Utf8).str.replace_all("#N/A","",literal=True),
                            pl.col(pl.Utf8).str.replace_all("N/A",literal=True),
                            pl.col(pl.Utf8).str.replace_all("#NA",literal=True),
                            pl.col(pl.Utf8).str.replace_all("-NaN",literal=True),
                            pl.col(pl.Utf8).str.replace_all("-nan",literal=True),
                            pl.col(pl.Utf8).str.replace_all("1.#IND",literal=True),
                            pl.col(pl.Utf8).str.replace_all("<NA>",literal=True),
                            pl.col(pl.Utf8).str.replace_all("#NA",literal=True),
                            pl.col(pl.Utf8).str.replace_all("-NaN",literal=True),
                            pl.col(pl.Utf8).str.replace_all("-nan",literal=True),
                            pl.col(pl.Utf8).str.replace_all("NA",literal=True),
                            pl.col(pl.Utf8).str.replace_all("NULL",literal=True),
                            pl.col(pl.Utf8).str.replace_all("NaN",literal=True),
                            pl.col(pl.Utf8).str.replace_all("n/a",literal=True),
                            pl.col(pl.Utf8).str.replace_all("nan",literal=True),
                            pl.col(pl.Utf8).str.replace_all("null",literal=True),
                            pl.col(pl.Utf8).str.replace_all("none",literal=True),
                            pl.col(pl.Utf8).str.replace_all("NONE",literal=True),
                            pl.col(pl.Utf8).str.replace_all("None",literal=True)
                            )
            if engine != 'polars':
                df = df.to_pandas(use_pyarrow_extension_array=True)
        
        if partition_columns:
            partition_dict = {}
            for x in partition_columns:
                partition_dict[x] = df[x].unique()
    
            partition_groups = [dict(zip(partition_dict.keys(),items)) for items in product(*partition_dict.values())]
            partition_groups = [d for d in partition_groups if np.nan not in d.values()]

            if file_name is not None:
                file_name = file_name + "/"
            else:
                file_name = ""
                
            for d in partition_groups:
                df_part = df
                partition_path=[]

                if isinstance(df_part, pd.DataFrame):
                    for d1 in d:
                        df_part = df_part[df_part[d1] == d[d1]]
                        partition_path.append(f"{d1}={str(d[d1])}")
                    
                    if not(df_part.empty):
                        buf = BytesIO()
                        df_reset = df_part.reset_index(drop=True)
                        if quoting is not None:
                            df_reset.to_csv(buf,index=False, sep=delimiter,encoding=encoding,header=is_first_row_as_header,quotechar=quoting, quoting=1,escapechar=escape)
                        else:
                            df_reset.to_csv(buf,index=False, sep=delimiter,encoding=encoding,header=is_first_row_as_header,escapechar=escape)
                        buf.seek(0)
                        self.save_binary_file(buf.getvalue(),container_name ,directory_path +"/" + file_name  +"/".join(partition_path),f"{uuid.uuid4().hex}.csv",True)


                if isinstance(df_part, pl.DataFrame):
                    for d1 in d:
                        df_part = df_part.filter(df_part[d1] == d[d1])
                        partition_path.append(f"{d1}={str(d[d1])}")
                                
                    if not(df_part.is_empty()):
                        buf = BytesIO()
                        df_reset = df_part
                        if quoting is not None:
                            df_reset.write_csv(buf, separator=delimiter, has_header=is_first_row_as_header, quote_char=quoting, quote_style='always')
                        else:
                            df_reset.write_csv(buf, separator=delimiter, has_header=is_first_row_as_header,quote_style='never')
                        buf.seek(0)
                        self.save_binary_file(buf.getvalue(),container_name ,directory_path +"/" + file_name + "/".join(partition_path),f"{uuid.uuid4().hex}.csv",True)                                      
        else:
            buf = BytesIO()
            
            if isinstance(df, pd.DataFrame):
                df.reset_index(drop=True,inplace=True)
                if quoting is not None:
                    df.to_csv(buf,index=False, sep=delimiter,encoding=encoding,header=is_first_row_as_header,quotechar=quoting, quoting=1,escapechar=escape)
                else:
                    df.to_csv(buf,index=False, sep=delimiter,encoding=encoding,header=is_first_row_as_header,escapechar=escape)
            else:
                if quoting is not None:
                    df.write_csv(buf, separator=delimiter, has_header=is_first_row_as_header,quote_char=quoting,  quote_style="always")
                else:
                    df.write_csv(buf, separator=delimiter, has_header=is_first_row_as_header, quote_style="never")
            buf.seek(0)

            if file_name:
                file_name_check = file_name
            else:
                file_name_check  = f"{uuid.uuid4().hex}.csv"
            self.save_binary_file(buf.getvalue(),container_name ,directory_path,file_name_check,True)
    
    def save_dataframe_as_parquet(self,df,container_name : str,directory_path:str,engine: ENGINE_TYPES ='polars',partition_columns:list=None,compression:COMPRESSION_TYPES=None):
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
    
        if isinstance(df, pd.DataFrame):
            if df.empty:
                return
            #df = df.replace(r'\\n', '', regex=True)
            if engine != 'pandas':
                df = pl.from_pandas(df)

        elif isinstance(df, pl.DataFrame):
            if df.is_empty():
                return
            #df = df.with_columns(pl.col(pl.Utf8).str.replace_all(r"\\n", ""))
            if engine != 'polars':
                df = df.to_pandas(use_pyarrow_extension_array=True)
                
        if partition_columns:
            partition_dict = {}
            for x in partition_columns:
                partition_dict[x] = df[x].unique()

            partition_groups = [dict(zip(partition_dict.keys(),items)) for items in product(*partition_dict.values())]
            partition_groups = [d for d in partition_groups if np.nan not in d.values()   ]
            
            for d in partition_groups:
                df_part = df
                partition_path=[]
            
            
                if isinstance(df_part, pd.DataFrame):
                    for d1 in d:
                        df_part = df_part[df_part[d1] == d[d1]]
                        partition_path.append(f"{d1}={str(d[d1])}")
                    
                    if not(df_part.empty):
                        buf = BytesIO()
                        df_part.to_parquet(buf,allow_truncated_timestamps=True, use_deprecated_int96_timestamps=True,compression=compression)
                        buf.seek(0)
                        self.save_binary_file(buf.getvalue(),container_name ,directory_path +"/" +"/".join(partition_path),f"{uuid.uuid4().hex}.parquet",True)

                if isinstance(df_part, pl.DataFrame):
                    for d1 in d:
                        df_part = df_part.filter(df_part[d1] == d[d1])
                        partition_path.append(f"{d1}={str(d[d1])}")
                                
                    if not(df_part.is_empty()):
                        buf = BytesIO()
                        df_part.write_parquet(buf,compression=compression)
                        buf.seek(0)
                        self.save_binary_file(buf.getvalue(),container_name ,directory_path +"/" + "/".join(partition_path),f"{uuid.uuid4().hex}.csv",True)
        else:
            buf = BytesIO()
            if isinstance(df, pd.DataFrame):
                df_reset = df.reset_index(drop=True)
                df_reset.to_parquet(buf,allow_truncated_timestamps=True, use_deprecated_int96_timestamps=True,compression=compression)
            else:
                df_reset = df
                df.write_parquet(buf,compression=compression)
            buf.seek(0)
            self.save_binary_file(buf.getvalue(),container_name ,directory_path,f"{uuid.uuid4().hex}.parquet",True)
    
    
    def save_dataframe_as_xlsx(self, df,container_name : str,directory_path:str ,file_name:str,sheet_name:str,engine:ENGINE_TYPES ='polars',index=False,header=False):
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
        if isinstance(df,pd.DataFrame):
            if df.empty:
                return
            df = df.replace('\n', ' ', regex=True)
            if engine != 'pandas':
                df = pl.from_pandas(df)

        elif isinstance(df,pl.DataFrame):
            if df.is_empty():
                return
            df = df.with_columns(pl.col(pl.Utf8).str.replace_all("\n", " "))
            if engine != 'polars':
                df = df.to_pandas(use_pyarrow_extension_array=True)
        check_if_file_exist = False

        if isinstance(df,pd.DataFrame):
            if check_if_file_exist:
                excel_buf =self.read_binary_file(container_name,directory_path,file_name)
                excel_buf=BytesIO(excel_buf)
                with pd.ExcelWriter(excel_buf, engine='openpyxl',mode="a") as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index= index, header=header)
            else:
                excel_buf = BytesIO()
                with pd.ExcelWriter(excel_buf, engine = 'openpyxl') as writer:
                    df.to_excel(writer, index = index, header = header, sheet_name = sheet_name)
        elif isinstance(df,pl.DataFrame):
            if check_if_file_exist:
                excel_buf=self.read_binary_file(container_name,directory_path,file_name)
                excel_buf=BytesIO(excel_buf)    
                df.write_excel(excel_buf, worksheet=sheet_name)
            else:
                excel_buf = BytesIO()
                df.write_excel(excel_buf, worksheet=sheet_name)
                
        excel_buf.seek(0)
        #excel_buf.close()
        self.save_binary_file(excel_buf.getvalue(),container_name ,directory_path,file_name,True)
    
    def read_parquet_file(self, container_name: str, directory_path: str,file_name:str,engine:ENGINE_TYPES ='polars', columns: list = None,tech_columns:bool=False):

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
        download_bytes = self.read_binary_file(container_name,directory_path,file_name)
        df = self.read_parquet_bytes(input_bytes=download_bytes,columns=columns,engine=engine)
        
        if tech_columns:
            df =  add_tech_columns(df,container_name,directory_path.replace("\\","/"),file_name)

        return df
    
    def read_parquet_folder(self,container_name:str,directory_path:str,engine:ENGINE_TYPES ='polars',columns:list = None,tech_columns:bool=False,recursive:bool=False):
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
        list_files = self.ls_files(container_name,directory_path, recursive=recursive)

        df = None
        if list_files:
            for f in list_files:
                df_new = self.read_parquet_file(container_name,directory_path,f.removeprefix(directory_path),engine,columns, tech_columns)
                if engine=='pandas':
                    if df is None:
                        df = df_new
                    else:
                        df = pd.concat([df, df_new], axis=0, join="outer", ignore_index=True)
                elif engine =='polars':
                    if df is None:
                        df = df_new
                    else:
                        df = pl.concat([df, df_new])
        return df
    
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
    def delete_files_by_prefix(self,container_name : str,directory_path : str,file_prefix:str, recursive:bool=False,wait:bool=True):
        """
        Deletes files from Azure Blob Storage based on a prefix.
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
    def move_file(self,container_name : str,directory_path : str,file_name:str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_file:bool=False):
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
    def move_folder(self,container_name : str,directory_path : str,new_container_name : str,new_directory_path : str,is_overwrite :bool=True,is_delete_source_folder:bool=False):
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
    def rename_file(self,container_name : str,directory_path : str,file_name:str,newfile_name:str):
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
    def rename_folder(self,container_name : str,directory_path : str,new_directory_path:str):
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
    
    def save_json_file(self, df, container_name: str, directory_path: str, file_name:str = None, engine: ENGINE_TYPES ='polars', orient:ORIENT_TYPES= 'records'):
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

        if isinstance(df,pd.DataFrame):
            if df.empty:
                return
            df = df.replace('\n', ' ', regex=True)
            if engine != 'pandas':
                df = pl.from_pandas(df)

        elif isinstance(df,pl.DataFrame):
            if df.is_empty():
                return
            df = df.with_columns(pl.col(pl.Utf8).str.replace_all("\n", " "))
            if engine != 'polars':
                df = df.to_pandas(use_pyarrow_extension_array=True)

        buf = BytesIO()   
        
        if isinstance(df, pd.DataFrame):
            df.to_json(buf, orient=orient,lines=True)
        elif isinstance(df, pl.DataFrame):
            df.write_json(buf, row_oriented=(orient=='records'), pretty=True)

        if file_name:
            file_name_check = file_name
        else:
            file_name_check = f"{uuid.uuid4().hex}.json"
        buf.seek(0)
        self.save_binary_file(buf.getvalue(), container_name ,directory_path,file_name_check,True)
    
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
