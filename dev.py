
import logging
from traceback import print_tb
from cloud_storage_pwc import azure_storage
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient
import pandas as pd
import json




#sourcesheets_json = json.loads('["Zakres kont zmienne i stałe"]')

#print(sourcesheets_json[0])

#aaa= DataLakeServiceClient(account_url="https://datakeeee2.blob.core.windows.net/", credential="xzwfPjunJa4vz+j7h/2NLDHgQ+BKKWF1zHmLb308zQAbznpQXiCU6MVmlJ0J9HMsL0P+04fYQ7NU+AStmGGBrg==")
#aaa=azure_storage("datakeeee2","xzwfPjunJa4vz+j7h/2NLDHgQ+BKKWF1zHmLb308zQAbznpQXiCU6MVmlJ0J9HMsL0P+04fYQ7NU+AStmGGBrg==")

aaa=azure_storage("strwebfazzabidev","T9xPYlyWbvERtuSOHCKe8FO8N+TgaxrrtFoOqo7xx1zN8LL4o1AdK2g/KwUBCatIVGqgiZELE7Oh+AStov5kzw==")

aaa.delete_files_by_prefix("webstage-dev","shp/dane153","aaa_")
#bb=aaa.ls_files("webstage-dev","shp/dane153",True)

#cc=aaa.get_file_system_client("aaggg").get_directory_client("/").exists()
#b=aaa.get_file_system_client("aaggg").get_directory_client("/").get_sub_directory_client("folder1").get_file_client("file2.txt").exists() #.get_file_client("file1.txt").rename_file("aaaa.b")
#v=aaa.read_csv_folder("foldercheck1","folder1", delimiter=";",engine="pandas")
#print(aaa.__class__.__name__)
#vv=aaa.delete_folder("webstage-dev","shp/dane153",True)

#print(vv.get_directory_properties())

#print(v)
#aaa.save_dataframe_as_csv(v,"foldercheck1","folder2","plikcsvffff8gg.csv",engine="pandas",delimiter='◇')
#fs=aaa.get_file_system_client("aaggg").get_directory_client("/b").rename_directory

#bbb=fs.get_directory_client("/")
#t=bbb.get_sub_directory_client("folder1/fol4/eeee").create_directory()
#print(t)
#logging.debug('This will get logged')


#aa=azure_storage("strwebfazzabidev","T9xPYlyWbvERtuSOHCKe8FO8N+TgaxrrtFoOqo7xx1zN8LL4o1AdK2g/KwUBCatIVGqgiZELE7Oh+AStov5kzw==")

#dfs=aa.read_excel_file("webstage-dev","shp","dane.xlsx",engine="polars",sheets=("Dev"),tech_columns=True,is_first_row_as_header=True)
#aa.save_json_file(dfs,"webstage-dev","shp","dane11.json",engine="pandas",orient="records")


#dfff=pd.read_json('plikjson.json', orient='records',lines=True)
#print(dfff)
#print(dfs)
#aa.save_dataframe_as_csv(dfs['Dev'],"webstage-dev","shp","dev12.csv",engine="pandas",is_first_row_as_header=False)






#vvv=aa.ls_files("webstage","",True)
#print(vvv)
#df=aa.read_csv_folder("webstage-dev","shp/DEBBUG/1", delimiter="◇",engine="pandas",is_first_row_as_header=True,tech_columns=False)
#aa.save_dataframe_as_parquet(df,"webstage-dev","shp",compression="snappy")
#aa.save_dataframe_as_xlsx(df,"webstage-dev","shp",'pliczek111.xlsx',sheet_name="Sheet1",engine="pandas",header=True)
#aa.create_container("con1")
#bbb=aa.read_csv_folder("aaggg","source",delimiter=';',is_first_row_as_header=True,engine="pandas",tech_columns=True)
#bb=aa.read_parquet_folder("aaggg","folder10",engine="pandas")
#aa.create_container("dev22245")
#aa.delete_folder("dev22245","/aaa",True)
#aa.save_dataframe_as_csv(bb,"aaggg","newfol22","table1",delimiter=';',is_first_row_as_header=True,quoting="\"",engine="polars",partition_columns=["col1"])

#aa.save_dataframe_as_parquet(bb,"aaggg","newfvf9999",engine="pandas",partition_columns=["col1"],compression="snappy")

#print(bb)
#aa.create_container("aaggg11","Private")
#aa.save_dataframe_as_parquet(bbb,"aaggg","folder11",engine="polars",compression="gzip",partition_columns=["col1"])
#aa.save_dataframe_as_xlsx(bbb,"aaggg","folderxxx",'pliczek111.xlsx',sheet_name="Sheet1",engine="polars")
#df1=aa.read_parquet_folder("aaggg","folder11",recursive=True,engine="polars")
#print(df1)
#aa.rename_folder("aaggg","folder1","ggggfol222")
#aa.delete_container("aaggg")
#aa.create_container("aaggg","Private")
#vvv=aa.ls_files("aaggg","folder10",False)
#print(vvv)
#aa.delete_folder("aaggg","ggggfol222")
#df=aa.read_csv_folder("aaggg","folder1/folder2/fol3",delimiter=";",engine="polars",is_first_row_as_header=True,recursive=True)
#print(df)


#aa.create_empty_file("aaggg","","file1.txt")
#aa.create_empty_file("aaggg","folder1","file2.txt")
#aa.create_empty_file("aaggg","folder1/folder2","file3.txt")
#files = aa.ls_files("aaggg","folder1",True)
#aa.delete_container("aaggg")

#aa.rename_file("aaggg","folder1","file2.txt","aaaaa.txt")


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