from typing import Literal
import pandas as pd
import polars as pl
import numpy as np

ENGINE_TYPES = Literal['pandas', 'polars']
#DELIMITER_TYPES = Literal[';', '^','~',',','\t','|','◇','◆',' ']
#QUOTING_TYPES = Literal['"','|','~','^']
#ESCAPE_TYPES = Literal['\\','|','~','^']
#QUOTING_TYPES = Literal['QUOTE_MINIMAL', 'QUOTE_ALL','QUOTE_NONNUMERIC','QUOTE_NONE']
ENCODING_TYPES = Literal['UTF-8', 'UTF-16']
COMPRESSION_TYPES = Literal['snappy', 'gzip', 'brotli']
ORIENT_TYPES = Literal['records', 'columns']
CONTAINER_ACCESS_TYPES = Literal['Container', 'Blob','Private']
NAN_VALUES_REGEX = [np.nan, '#N/A','N/A', '#NA', '-NaN', '-nan',
        '<NA>', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null','none',"NONE",'None' ]#,r'^\s*$']
AZURE_CREDENTIAL_TYPES =  Literal['DefaultAzureCredential','InteractiveBrowserCredential','DeviceCodeCredential']


def add_tech_columns(df,container_name: str=None,directory_path:str=None,file:str = None):
    """
    Adds technical columns to a DataFrame for tracking container information.

    Args:
       | df (pd.DataFrame, pl.DataFrame): The DataFrame to which technical columns will be added.
       | container_name (str, optional): The name of the container. Defaults to None.
       | directory_path (str, optional): The directory path of the container. Defaults to None.
       | file (str, optional): The file path within the container. Defaults to None.

    Returns:
        pd.DataFrame or pl.DataFrame: The DataFrame with added technical columns.

    Raises:
        None
    """
    if directory_path is None or directory_path == "":
        directory_path =  "/".join(file.split("/")[:-1])
        file = file.removeprefix(directory_path+"/")
    else:
        directory_path_from_file =  "/".join(file.split("/")[:-1])
        if directory_path_from_file != "":
            directory_path = directory_path+"/"+directory_path_from_file
            file = file.removeprefix(directory_path_from_file+"/")
    
    if isinstance(df, pd.DataFrame):
        df['techContainer'] = container_name
        df['techFolderPath'] = directory_path
        df['techSourceFile'] = file
    elif isinstance(df, pl.DataFrame):
        df = df.with_columns(techContainer = pl.lit(container_name),
                         techFolderPath = pl.lit(directory_path),
                         techSourceFile = pl.lit(file))
        
    return df