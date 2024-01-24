
import pandas as pd
import polars as pl


class Utils():
    def __init__(self):
        pass
    
    
    nanVal = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN',
                           '<NA>', 'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']

    class DataFromExcel:
        def __init__(self,df,sheetName):
            self.data = df
            self.sheet = sheetName

    def addTechColumns(df:[pd.DataFrame, pl.DataFrame],containerName: str=None,directoryPath:str=None,file:str = None):
        df['techContainer'] = containerName
        df['techFolderPath'] = directoryPath
        df['techSourceFile'] = file
        return df