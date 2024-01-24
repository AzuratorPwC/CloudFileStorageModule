#import unittest
#from  cloud_storage_pwc import create_cloudstorage_reference
import pytest
from cloud_storage_pwc import create_cloudstorage_reference

def test_record_matches_header2(storageaccount_name,account_key):
    try:
        #https://cloudstorageev.blob.core.windows.net/
        print(storageaccount_name)
        aa = create_cloudstorage_reference(storageaccount_name,account_key)
        
        #aa.service_client.create_container("myblob")
        aa.create_container("myblob")
        aa.create_empty_file("myblob","dev1","plik.txt")
        z = aa.ls_files('myblob',"dev1")
        if(len(z)==1):
            assert 1==1
        else:
            assert 1==0
        #aa.move_folder("con1","fol1","con1","fol2",True,isDeleteSourceFolder=True)
        assert 1==1
    except:
        assert 1==0
#if __name__ == '__main__':
#    unittest.main()
#https://cloudstorageev.blob.core.windows.net/
