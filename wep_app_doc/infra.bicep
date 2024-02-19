
param location string = resourceGroup().location

var docker_compose = '''
version: "3.3"
services:
  sphinx-demo:
    restart: always
    ports:
      - 8080:80
    environment:
      WEBSITES_ENABLE_APP_SERVICE_STORAGE: "True"
    image: nginx:1.20.1
    volumes:
      - sphinxdocumentation:/usr/share/nginx/html
      - nginxconfiguration:/etc/nginx
'''

var storageName ='containermetadocdev'
var serverfarmName='webappserverdocdev'
var webappName = 'webappcloudstoragedocdev'
//var resourceGroupName = 'storageaccountpythonlib'


var compose = format('{0}|{1}','COMPOSE',base64(docker_compose))

resource storage 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: storageName
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
  resource fs  'fileServices@2022-09-01' = {
    name: 'default'
    resource nginxconfiguration 'shares@2022-09-01' = {
      name: 'nginxconfiguration'
    }
    resource sphinxdocumentation 'shares@2022-09-01' = {
      name: 'sphinxdocumentation'
    }
  }
}

resource serverfarmdoc 'Microsoft.Web/serverfarms@2022-03-01' = {
  name: serverfarmName
  location: location
  sku: {
    name: 'B1'
    tier: 'Basic'
    size: 'B1'
    family: 'B'
    capacity: 1
  }
  kind: 'linux'
  properties: {
    perSiteScaling: false
    elasticScaleEnabled: false
    maximumElasticWorkerCount: 1
    isSpot: false
    reserved: true
    isXenon: false
    hyperV:false
    targetWorkerCount: 0
    targetWorkerSizeId: 0
    zoneRedundant: false
  }
}

resource web  'Microsoft.Web/sites@2022-03-01' = {
  name: webappName
  location:  location
  kind: 'app,linux,container'
  properties: {
    serverFarmId: serverfarmdoc.id
    httpsOnly: true
    virtualNetworkSubnetId: null
    clientAffinityEnabled: false
    siteConfig: {
      appSettings: [
        {
          name: 'DOCKER_REGISTRY_SERVER_URL'
          value: 'https://index.docker.io'
        }
        {
          name: 'DOCKER_REGISTRY_SERVER_USERNAME'
          value: ''
        }
        {
          name: 'DOCKER_REGISTRY_SERVER_PASSWORD'
          value: null
        }
        {
          name: 'WEBSITES_ENABLE_APP_SERVICE_STORAGE'
          value: 'false'
        }
      ]
      linuxFxVersion: compose
      appCommandLine: null
    }
    
  }
  resource s 'config@2022-03-01' = {
    name: 'web'
    properties: {
      minTlsVersion: '1.2'
      scmMinTlsVersion: '1.2'
      ftpsState: 'FtpsOnly' 
      functionsRuntimeScaleMonitoringEnabled: false
      azureStorageAccounts: {
        nginxconfiguration:{
          type: 'AzureFiles'
          accountName: storage.name
          shareName: storage::fs::nginxconfiguration.name
          mountPath: '/etc/nginx'
          accessKey: storage.listKeys().keys[0].value
        }
        sphinxdocumentation:{
          type: 'AzureFiles'
          accountName: storage.name
          shareName: storage::fs::sphinxdocumentation.name
          mountPath: '/usr/share/nginx/html'
          accessKey: storage.listKeys().keys[0].value
        }
      }
    }
  }
}


output storageName string = storageName
output storageKey string = storage.listKeys().keys[0].value
