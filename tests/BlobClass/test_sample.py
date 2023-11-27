#import unittest
#from  cloud_storage_pwc import create_cloudstorage_reference
import pytest
from cloud_storage_pwc import create_cloudstorage_reference
#class TestStringMethods(unittest.TestCase):
#def test_record_matches_header():
    #aa = create_cloudstorage_reference("http://127.0.0.1:10000/devstoreaccount1","Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
    #r = aa.move_folder("myblob","test111","myblob","test222",isDeleteSourceFolder=True)
    #def test_upper(self):
    #    aa = create_cloudstorage_reference("http://129.0.0.1:10000/devstoreaccount1","Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
    #    self.assertTrue(aa.move_folder("myblob","abc","myblob","abc2456c",isDeleteSourceFolder=True))
    #    #aa.move_folder("myblob","test111","myblob","test222",isDeleteSourceFolder=True)
    #    #self.a('foo'.upper(), 'FOO')
#    assert True == False
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
