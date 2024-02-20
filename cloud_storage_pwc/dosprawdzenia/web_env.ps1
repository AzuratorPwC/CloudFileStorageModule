param(
    [string]$prg

  )

az deployment group create `
    --name docinfrastructure `
    --resource-group $prg `
    --template-file src/wep_app_doc/infra.bicep `
    --mode Incremental

$storageName_out=az deployment group show `
    --name docinfrastructure `
    --resource-group $prg `
    --query properties.outputs.storageName.value -o tsv 

$storageKey_out=az deployment group show `
    --name docinfrastructure `
    --resource-group $prg `
    --query properties.outputs.storageKey.value -o tsv 

Write-Output "##vso[task.setvariable variable=storageName;isOutput=true]$storageName_out"
Write-Output "##vso[task.setvariable variable=storageKey;isOutput=true]$storageKey_out"