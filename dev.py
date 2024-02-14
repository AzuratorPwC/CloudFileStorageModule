from src.cloud_storage_pwc import azure_storage


import logging
logging.basicConfig(level=logging.CRITICAL)
#logging.debug('This will get logged')

aa=azure_storage("mystorageairflowdev","IdepCBLsltZ+1uLFOxk9jmqhnxMUziioFFODu6OCtQ/BjLBGnDVDftgEFACPKZ8kJIAfdTSLsEhe+AStk4KOMg==")

aa.create_container("aaggg")

#aa.create_empty_file("aaggg","folder1","file1.txt")
#aa.create_empty_file("aaggg","folder1","file2.txt")
#aa.create_empty_file("aaggg","folder2","file3.txt")
print(aa.ls_files("aaggg","folder1",True))



#aa.delete_container("aaaaaa")
