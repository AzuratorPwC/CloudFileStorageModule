

from src.cloud_storage_pwc import azure_storage
#from azure.storage.filedatalake import DataLakeServiceClient
#aaa= DataLakeServiceClient(account_url="https://datakeeee2.dfs.core.windows.net/", credential="xzwfPjunJa4vz+j7h/2NLDHgQ+BKKWF1zHmLb308zQAbznpQXiCU6MVmlJ0J9HMsL0P+04fYQ7NU+AStmGGBrg==")

#fs=aaa.get_file_system_client("aaggg")

#bbb=fs.get_directory_client("/")
#t=bbb.get_sub_directory_client("folder1/fol4/eeee").create_directory()
#print(t)
#logging.debug('This will get logged')

aa=azure_storage("datakeeee2","xzwfPjunJa4vz+j7h/2NLDHgQ+BKKWF1zHmLb308zQAbznpQXiCU6MVmlJ0J9HMsL0P+04fYQ7NU+AStmGGBrg==")

aa.delete_container("aaggg")
#aa.create_container("aaggg")
#vvv=aa.ls_files("aaggg","folder1",False)

#df=aa.read_csv_folder("aaggg","folder1/folder2/fol3",delimiter=";",engine="polars",is_first_row_as_header=True,recursive=True)
#print(df)


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


#df1=aa.read_csv_folder("aaggg","source/plikcsv3.txt",delimiter=";",engine="polars",recursive=True,is_first_row_as_header=True)
#print(df1)
#aa.save_json_file(df1,"aaggg","source/json","plik.json",engine="polars",orient="records")
#print(df1)
#aa.save_dataframe_as_parquet(df1,"aaggg","source/table2",engine="polars",partition_columns=["col1","col2"])
#aa.save_dataframe_as_xlsx(df1,"aaggg","source/table3",file_name="plik.xlsx",sheet_name="Ark5551",engine="polars",header=True)
#fff = aa.read_parquet_file("aaggg","source",file_name="28684fc0a9b54d39b22c996163c4afad.parquet",tech_columns=False,engine="pandas",columns=["col1"])

#aa.delete_files_by_prefix("aaggg","source","")
#aa.renema_file("aaggg","","plikcsv4.txt","plikcsvgdgffd.txt")
#print(fff)
#fg=aa.read_parquet_folder("aaggg","source/table2/col1=555",recursive=True)
#print(fg)