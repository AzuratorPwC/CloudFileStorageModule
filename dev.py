from src.cloud_storage_pwc import azure_storage


import logging
logging.basicConfig(level=logging.ERROR)
logging.debug('This will get logged')
aa=azure_storage("http://127.0.0.1:10000/devstoreaccount1","Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
print(aa.check_is_dfs())
aa.delete_container("test567")
#aa.create_empty_file("test","test","test")