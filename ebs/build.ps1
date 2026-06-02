$ErrorActionPreference = "Stop"

function Require-Command {
    param([string]$Name)
    if (-not (Get-Command -Name $Name -ErrorAction SilentlyContinue)) {
        throw "Missing required command: $Name"
    }
}

Require-Command docker
Require-Command mvn
Require-Command java

$composeUpDone = $false

try {
    docker compose up -d
    if (-not $?) { throw "docker compose up failed" }
    $composeUpDone = $true

    Write-Host "Waiting for Kafka on localhost:9092..."
    $kafkaReady = $false
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient
            $tcp.Connect("localhost", 9092)
            $tcp.Close()
            $kafkaReady = $true
            Write-Host "Kafka is ready."
            break
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $kafkaReady) { throw "Kafka did not become ready within 120 seconds" }

    mvn clean package -DskipTests
    if (-not $?) { throw "mvn package failed" }

    $jar = Get-ChildItem -Path "target" -Filter "*-jar-with-dependencies.jar" | Select-Object -First 1
    if (-not $jar) {
        throw "Could not find jar-with-dependencies in target"
    }

    java -jar $jar.FullName
    if (-not $?) { throw "java -jar failed" }
}
finally {
    if ($composeUpDone) {
        docker compose down
    }
}
