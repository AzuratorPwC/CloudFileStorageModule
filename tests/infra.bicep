
param location string = resourceGroup().location
param pkgName string
param pkgVersion string


var BlobName = format('bl{0}{1}',substring(replace(replace(replace(replace(pkgName,' ',''),'-',''),'.',''),'_',''),0,15),pkgVersion)
var DlName = format('dl{0}{1}',substring(replace(replace(replace(replace(pkgName,' ',''),'-',''),'.',''),'_',''),0,15),pkgVersion)


resource storageBlob 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: BlobName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot' 
    allowBlobPublicAccess: false
    isHnsEnabled: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
  resource blob 'blobServices@2022-09-01' existing = {
    name: 'default'
    resource con 'containers@2022-09-01' = {
      name: 'blob'
    }
  }
}


resource storageDatalake 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: DlName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot' 
    allowBlobPublicAccess: false
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
  }
  resource blob 'blobServices@2022-09-01' existing = {
    name: 'default'
    resource con 'containers@2022-09-01' = {
      name: 'adl'
    }
  }
}

output storageBlobName string = storageBlob.name
output storageDatalakeName string = storageDatalake.name

output storageBlobKey string = storageBlob.listKeys().keys[0].value
output storageDatalakeKey string = storageDatalake.listKeys().keys[0].value
