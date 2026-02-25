# download_spark.ps1
# Execute na raiz do projeto com:
#   .\download_spark.ps1

$ErrorActionPreference = "Stop"

# Cria a pasta docker/ se nao existir
New-Item -ItemType Directory -Force -Path "docker" | Out-Null

Write-Host "Baixando Spark 3.5.1..." -ForegroundColor Cyan
Invoke-WebRequest `
  -Uri "https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz" `
  -OutFile "docker\spark-3.5.1-bin-hadoop3.tgz"

Write-Host "Baixando hadoop-aws JAR..." -ForegroundColor Cyan
Invoke-WebRequest `
  -Uri "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" `
  -OutFile "docker\hadoop-aws-3.3.4.jar"

Write-Host "Baixando aws-java-sdk-bundle JAR..." -ForegroundColor Cyan
Invoke-WebRequest `
  -Uri "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" `
  -OutFile "docker\aws-java-sdk-bundle-1.12.262.jar"

Write-Host ""
Write-Host "Concluido! Arquivos em docker\:" -ForegroundColor Green
Get-ChildItem docker\ | Select-Object Name, @{N="Tamanho";E={"{0:N1} MB" -f ($_.Length / 1MB)}}