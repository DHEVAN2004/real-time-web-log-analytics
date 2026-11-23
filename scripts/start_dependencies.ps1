$ErrorActionPreference = "Stop"

$serviceRoot = Join-Path $env:LOCALAPPDATA "WebLogAnalytics"
$kafkaVersion = "4.1.1"
$kafkaLibraries = Join-Path $serviceRoot "kafka-$kafkaVersion\lib"
$kafkaConfig = Join-Path $serviceRoot "server.properties"
$kafkaData = Join-Path $serviceRoot "kafka-data"
$kafkaLog = Join-Path $serviceRoot "kafka.log"
$java = (Get-Command java.exe -ErrorAction Stop).Source

New-Item -ItemType Directory -Force -Path $serviceRoot | Out-Null

$mysqlService = Get-Service -Name "MySQL80" -ErrorAction SilentlyContinue
if (-not $mysqlService) {
    $mysqlService = Get-Service -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match "^MySQL\d+$" } |
        Sort-Object Name |
        Select-Object -First 1
}
if (-not $mysqlService) {
    throw "A MySQL 8 Windows service was not found. Install MySQL 8 and configure the backend connection before starting the application."
}
if ($mysqlService.Status -ne "Running") {
    Start-Service -Name $mysqlService.Name
}

$mysqlReady = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    $connection = New-Object System.Net.Sockets.TcpClient
    try {
        $connection.Connect("127.0.0.1", 3306)
        $mysqlReady = $true
        break
    }
    catch {
        Start-Sleep -Seconds 2
    }
    finally {
        $connection.Dispose()
    }
}
if (-not $mysqlReady) {
    throw "MySQL did not accept connections on 127.0.0.1:3306."
}

$kafkaJar = Join-Path $kafkaLibraries "kafka_2.13-$kafkaVersion.jar"
if (-not (Test-Path $kafkaJar)) {
    $maven = (Get-Command mvn.cmd -ErrorAction Stop).Source
    New-Item -ItemType Directory -Force -Path $kafkaLibraries | Out-Null
    Write-Host "Downloading Apache Kafka $kafkaVersion runtime from Maven Central..."
    & $maven -q -f (Join-Path $PSScriptRoot "..\services\kafka\pom.xml") `
        dependency:copy-dependencies "-DincludeScope=runtime" "-DoutputDirectory=$kafkaLibraries"
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $kafkaJar)) {
        throw "Could not prepare the Apache Kafka runtime."
    }
}

$classpath = Join-Path $kafkaLibraries "*"
$escapedKafkaData = $kafkaData.Replace("\", "/")
if (-not (Test-Path $kafkaConfig)) {
    @"
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$escapedKafkaData
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
"@ | Set-Content -Path $kafkaConfig -Encoding ascii
}

New-Item -ItemType Directory -Force -Path $kafkaData | Out-Null
$metadataFile = Join-Path $kafkaData "meta.properties"
if (-not (Test-Path $metadataFile)) {
    $existingKafkaData = @(Get-ChildItem -Force $kafkaData)
    if ($existingKafkaData.Count -gt 0) {
        throw "Kafka data exists but has no KRaft metadata. Preserve and inspect it before formatting: $kafkaData"
    }
    $uuidOutput = & $java -cp $classpath kafka.tools.StorageTool random-uuid
    if ($LASTEXITCODE -ne 0) {
        throw "Kafka could not generate a KRaft cluster ID."
    }
    $clusterId = $uuidOutput | Where-Object { $_ -match "^[A-Za-z0-9_-]{20,}$" } | Select-Object -Last 1
    if (-not $clusterId) {
        throw "Kafka returned an invalid KRaft cluster ID."
    }
    & $java -cp $classpath kafka.tools.StorageTool format --standalone -t $clusterId -c $kafkaConfig
    if ($LASTEXITCODE -ne 0) {
        throw "Kafka storage formatting failed."
    }
}

$kafkaReady = $false
for ($attempt = 0; $attempt -lt 120; $attempt++) {
    $connection = New-Object System.Net.Sockets.TcpClient
    try {
        $connection.Connect("127.0.0.1", 9092)
        $kafkaReady = $true
        break
    }
    catch {
        if ($attempt -eq 0) {
            Write-Host "Starting Apache Kafka..."
            Start-Process -FilePath $java `
                -ArgumentList @("-Xms512m", "-Xmx1g", "-cp", "`"$classpath`"", "kafka.Kafka", "`"$kafkaConfig`"") `
                -WorkingDirectory $serviceRoot `
                -WindowStyle Hidden `
                -RedirectStandardOutput $kafkaLog `
                -RedirectStandardError "$kafkaLog.err" | Out-Null
        }
        Start-Sleep -Seconds 2
    }
    finally {
        $connection.Dispose()
    }
}
if (-not $kafkaReady) {
    $logTail = if (Test-Path $kafkaLog) { Get-Content $kafkaLog -Tail 30 } else { "Kafka log was not created." }
    throw "Kafka did not start on 127.0.0.1:9092.`n$($logTail -join [Environment]::NewLine)"
}

$env:KAFKA_HOME = $serviceRoot
if (-not $env:SPARK_HOME -and (Test-Path "C:\spark\bin\spark-submit.cmd")) {
    $env:SPARK_HOME = "C:\spark"
}
if (-not $env:SPARK_VERSION) {
    $env:SPARK_VERSION = "4.2.0"
}
if (-not $env:SPARK_HOME) {
    $pythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($pythonCommand) {
        $detectedSparkHome = & $pythonCommand.Source -c "import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent)" 2>$null
        if ($LASTEXITCODE -eq 0 -and $detectedSparkHome) {
            $env:SPARK_HOME = ($detectedSparkHome | Select-Object -Last 1).Trim()
        }
    }
}
if (-not $env:HADOOP_HOME) {
    $localHadoopHome = Join-Path $serviceRoot "hadoop-3.3.6"
    if (Test-Path (Join-Path $localHadoopHome "bin\winutils.exe")) {
        $env:HADOOP_HOME = $localHadoopHome
    }
}
if ($env:HADOOP_HOME) {
    $hadoopBin = Join-Path $env:HADOOP_HOME "bin"
    if (Test-Path $hadoopBin) {
        $env:PATH = "$hadoopBin;$env:PATH"
    }
}
if (-not $env:SPARK_HOME -or -not (Test-Path (Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"))) {
    $sparkReady = $false
    Write-Warning "Spark is not installed/configured; the core dashboard, MySQL, and Kafka remain available."
} elseif ($env:OS -eq "Windows_NT" -and
    (-not $env:HADOOP_HOME -or -not (Test-Path (Join-Path $env:HADOOP_HOME "bin\winutils.exe")))) {
    $sparkReady = $false
    Write-Warning "Spark on Windows requires winutils.exe under HADOOP_HOME\bin."
} else {
    $sparkReady = $true
}

if ($sparkReady) {
    Write-Host "MySQL, Kafka, and Spark prerequisites are ready."
} else {
    Write-Host "MySQL and Kafka are ready. Spark is unavailable and will be reported as NOT_CONFIGURED."
}
