param(
  [string]$pversion,
  [string]$pname
)

$tomlFile = Get-ChildItem -Path *.toml

(Get-Content $tomlFile -Encoding UTF8) -replace "version = ""0""", "version = ""$pversion"" " | Set-Content $tomlFile
(Get-Content $tomlFile -Encoding UTF8) -replace "name = ""null""", "name = ""$pname"" " | Set-Content $tomlFile

$requirementsFile = Get-ChildItem -Path requirements.txt
[string[]]$arrayFromFile = (Get-Content $requirementsFile -Encoding UTF8) 
$JoinedString = '"' + ($arrayFromFile -join '","') + '"' 
(Get-Content $tomlFile -Encoding UTF8) -replace "dependencies = \[\]", "dependencies = [ $JoinedString ]" | Set-Content $tomlFile

$testFiles = Get-ChildItem -Path tests\test_*.py -Recurse -Force
foreach($f in $testFiles)
{
  (Get-Content $f -Encoding UTF8) -replace "from cloud_storage_pwc import create_cloudstorage_reference", "from src.cloud_storage_pwc import create_cloudstorage_reference" | Set-Content $f
}

Write-Host "File", $tomlFile," package: ""$pname"" ,version: ""$pversion"" "