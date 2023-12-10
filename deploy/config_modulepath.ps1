

$initFile = Get-ChildItem -Path src\cloud_storage_pwc\__init__.py
(Get-Content $initFile -Encoding UTF8) -replace "from src.cloud_storage_pwc.Blob import Blob", "from cloud_storage_pwc.Blob import Blob" | Set-Content $initFile
(Get-Content $initFile -Encoding UTF8) -replace "from src.cloud_storage_pwc.DataLake import DataLake", "from cloud_storage_pwc.DataLake import DataLake" | Set-Content $initFile
Write-Host "JS File changed -", $initFile

$initFile = Get-ChildItem -Path src\cloud_storage_pwc\Blob\__init__.py
(Get-Content $initFile -Encoding UTF8) -replace "from src.cloud_storage_pwc.StorageAccountVirtualClass import \*", "from cloud_storage_pwc.StorageAccountVirtualClass import *" | Set-Content $initFile
Write-Host "JS File changed -", $initFile

$initFile = Get-ChildItem -Path src\cloud_storage_pwc\DataLake\__init__.py
(Get-Content $initFile -Encoding UTF8) -replace "from src.cloud_storage_pwc.StorageAccountVirtualClass import \*", "from cloud_storage_pwc.StorageAccountVirtualClass import *" | Set-Content $initFile
Write-Host "JS File changed -", $initFile