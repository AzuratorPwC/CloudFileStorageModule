from typing import Literal
import pandas as pd
import polars as pl

ENGINE_TYPES = Literal['pandas', 'polars']
#DELIMITER_TYPES = Literal[';', '^','~',',','\t','|','◇','◆',' ']
QUOTING_TYPES = Literal['"','|','~','^']
ESCAPE_TYPES = Literal['\\','|','~','^']
#QUOTING_TYPES = Literal['QUOTE_MINIMAL', 'QUOTE_ALL','QUOTE_NONNUMERIC','QUOTE_NONE']
ENCODING_TYPES = Literal['UTF-8', 'UTF-16']
COMPRESSION_TYPES = Literal['snappy', 'gzip', 'brotli']
ORIENT_TYPES = Literal['records', 'columns']
CONTAINER_ACCESS_TYPES = Literal['Container', 'Blob','Private']
NAN_VALUES = [ '#N/A', '#N/A','N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan',
        '1.#IND', '1.#QNAN',
        '<NA>', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null','none',"NONE",'None']

NAN_VALUES_REGEX = "?(i)#N/A|#N/A|N/A|#NA|-1.#IND|-1.#QNAN|-NaN|-nan|1.#IND|1.#QNAN|<NA>|NA|NULL|NaN|n/a|nan|null|none|NONE|None"



class Utils:
    pass

class DataFromExcel:
    """
    Creates a container in the Azure Blob Storage.
    """
    def __init__(self,df,sheet_name):
        self.data = df
        self.sheet_name = sheet_name
    
def add_tech_columns(df:[pd.DataFrame,pl.DataFrame],container_name: str=None,directory_path:str=None,file:str = None):
    """
    Creates a container in the Azure Blob Storage.
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