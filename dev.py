
from src.cloud_storage_pwc import create_cloudstorage_reference

aa = create_cloudstorage_reference("httt:1247/devstore1","fgggdfgr464")

#aa.service_client.create_container("myblob")
aa.create_container("myblob")
#        aa.create_empty_file("myblob","dev1","plik.txt")
#       z = aa.ls_files('myblob',"dev1")