
import re
from src.cloud_storage_pwc import azure_storage



#logging.debug('This will get logged')

aa=azure_storage("mystorageairflowdev","IdepCBLsltZ+1uLFOxk9jmqhnxMUziioFFODu6OCtQ/BjLBGnDVDftgEFACPKZ8kJIAfdTSLsEhe+AStk4KOMg==")

aa.create_container("aaggg")

#aa.create_empty_file("aaggg","folder1","file1.txt")
#aa.create_empty_file("aaggg","folder1","file2.txt")
#aa.create_empty_file("aaggg","folder2","file3.txt")
#files = aa.ls_files("aaggg","folder1",True)
#files = [f  for f in files if f.split("/")[-1].startswith("")]
#print(files)
#aa.delete_files_by_prefix("aaggg","source","p")
#df=aa.read_csv_file("aaggg","source","plikcsv.txt", delimiter="◆",engine="pandas")
#df = aa.read_csv_folder("aaggg","source",delimiter=";",engine="pandas",is_first_row_as_header=True)
#print(df)
#aa.save_dataframe_as_csv(df,"aaggg","source","plikcsv2.txt",engine="polars",is_first_row_as_header=True)

#aa.delete_container("aaaaaa")


#df=aa.read_excel_file("aaggg","source/source","hehe.xlsx",engine="polars",tech_columns=True)
#aa.save_dataframe_as_csv(df.data,"aaggg","source","plikcsv3.txt",engine="polars",partition_columns=["col1"])
#print(df.data)


df1=aa.read_csv_folder("aaggg","source/plikcsv3.txt",delimiter=";",engine="polars",recursive=True,is_first_row_as_header=True)
print(df1)
aa.save_json_file(df1,"aaggg","source/json","plik.json",engine="polars",orient="records")
#print(df1)
#aa.save_dataframe_as_parquet(df1,"aaggg","source/table2",engine="polars",partition_columns=["col1","col2"])


#fg=aa.read_parquet_folder("aaggg","source/table2/col1=555",recursive=True)
#print(fg)