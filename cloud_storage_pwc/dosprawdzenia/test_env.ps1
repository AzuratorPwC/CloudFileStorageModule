
param(
    [Parameter(Mandatory)]
    [ValidateSet("create","drop")]
    $pprocess,
    [string]$ppkgname,
    [string]$pversion,
    [string]$prg,

    [string]$pstorageblob="",
    [string]$pstorageadl=""
  )

if ( $pprocess -eq "create" )
{
az deployment group create `
    --name testenviroment `
    --resource-group $prg `
    --template-file tests/infra.bicep `
    --mode Incremental `
    --parameters pkgName="$ppkgname" pkgVersion="$pversion"

$storageBlobName_out=az deployment group show `
    --name testenviroment `
    --resource-group $prg `
    --query properties.outputs.storageBlobName.value -o tsv 

$storageBlobNameKey_out=az deployment group show `
    --name testenviroment `
    --resource-group $prg `
    --query properties.outputs.storageBlobKey.value -o tsv 

$storageDatalakeName_out=az deployment group show `
    --name testenviroment `
    --resource-group $prg `
    --query properties.outputs.storageDatalakeName.value -o tsv 

$storageDatalakeKey_out=az deployment group show `
    --name testenviroment `
    --resource-group $prg `
    --query properties.outputs.storageDatalakeKey.value -o tsv

#echo $storageBlobName_out

Write-Output "##vso[task.setvariable variable=storageBlobNameTest]$storageBlobName_out"
Write-Output "##vso[task.setvariable variable=storageBlobNameKeyTest]$storageBlobNameKey_out"
Write-Output "##vso[task.setvariable variable=storageDatalakeNameTest]$storageDatalakeName_out"
Write-Output "##vso[task.setvariable variable=storageDatalakeKeyTest]$storageDatalakeKey_out"
}

if ( $pprocess -eq "drop" )
{
    az resource delete -g $prg -n $pstorageblob --resource-type "Microsoft.Storage/storageAccounts"
    az resource delete -g $prg -n $pstorageadl --resource-type "Microsoft.Storage/storageAccounts"
}