# Cum se ruleaza EBS cu Kafka

## Prerequisites

- **Java 17+** (verifica: `java -version`)
- **Maven** (verifica: `mvn -v`)
- **Docker Desktop** instalat si pornit (verifica: `docker ps`)
- **PowerShell** (Windows) – pentru `build.ps1`

## Metoda 1 – Automat (recomandat)

Rulezi totul dintr-un singur script:

```powershell
cd ebs
.\build.ps1
```

Scriptul face automat:
1. `docker compose up -d` – porneste Kafka + ZooKeeper
2. Asteapta pana cand portul 9092 e deschis (verificare TCP)
3. `mvn clean package -DskipTests` – compileaza si genereaza fat-jar
4. `java -jar target/ebs-generator-1.0-SNAPSHOT-jar-with-dependencies.jar` – ruleaza aplicatia
5. `docker compose down` – opreste Kafka

Daca scriptul e blocat de politici de executie:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\build.ps1
```

## Metoda 2 – Manual (pas cu pas)

```powershell
cd ebs

# Pas 1: Porneste Kafka
docker compose up -d

# Pas 2: Asteapta sa fie gata (30-60 sec)
Start-Sleep -Seconds 30
# Sau verifica manual: docker logs ebs-kafka --tail 20

# Pas 3: Compileaza si ruleaza
mvn clean package -DskipTests
java -jar .\target\ebs-generator-1.0-SNAPSHOT-jar-with-dependencies.jar

# Pas 4: Opreste Kafka
docker compose down
```

## Metoda 3 – Doar compilare (fara Kafka)

Daca vrei doar sa compilezi proiectul:

```powershell
cd ebs
mvn clean package -DskipTests
```

## Configuratie

Toate setarile se modifica in:

**`src/main/java/org/ebs/pubsub/config/SystemConfig.java`**

```java
BROKER_COUNT = 3;           // Numar brokeri
PUBLISHER_COUNT = 2;        // Numar publisheri
SUBSCRIBER_COUNT = 3;       // Numar subscriberi
SUBSCRIPTION_COUNT = 10000; // Numar subscriptii totale
FEED_DURATION_MS = 180_000; // Durata feed (ms) – 3 minute
PUBS_PER_SECOND = 200;      // Rata de publicare
```

**Kafka remote** (daca nu folosesti Docker local):

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "your-kafka-host:9092"
.\build.ps1
```

## Dependency (pom.xml)

Proiectul foloseste:

- `kafka-clients:3.6.0` – client Kafka
- `protobuf-java:3.21.12` – serializare binara Protocol Buffers
- `slf4j-simple:2.0.9` – logging (optional, elimina warning-ul SLF4J)

## Structura topic-urilor Kafka

La fiecare rulare se creeaza topic-uri cu sufix unic (random 8 caractere) pentru a izola scenariile:

| Topic | Partitii | Scop |
|---|---|---|
| `ebs-pubs-{runId}` | 1 | Publicatii de la publisheri la brokeri |
| `ebs-broker-fwd-{runId}` | 3 | Forward intre brokeri (1 partitie per broker) |
| `ebs-sub-reg-{runId}` | 1 | Inregistrare subscriptii |
| `ebs-delivery-{runId}-sub-{i}` | 1 | Livrare catre subscriberul i (0, 1, 2) |

## Verificare functionare

Dupa pornirea Docker-ului, verifica:

```powershell
docker ps                       # Containerele ruleaza?
docker logs ebs-kafka --tail 20 # Kafka e ready?
```

In logurile Kafka, cauta mesajul: `[KafkaServer id=1] started`

## Rezultate asteptate

O rulare completa (4 scenarii A+B+C+D) dureaza ~8 minute.

Output-ul contine pentru fiecare scenariu:
- Publicatii emise
- Livrari totale
- Livrari / publicatie
- Rata de potrivire (matching)
- Debit livrari (msg/sec)
- Latenta medie (ms)
- Defalcare per subscriber

La final se afiseaza un **Raport de Evaluare** complet cu tabel, analiza si concluzii.

## Troubleshooting

| Problema | Solutie |
|---|---|
| `mvn: command not found` | Instaleaza Maven si adauga-l in PATH |
| `docker: command not found` | Instaleaza Docker Desktop |
| Kafka nu devine ready | Verifica Docker Desktop, ruleaza `docker compose logs` |
| `TopicExistsException` | Se ignora – topic-urile se creeaza o singura data |
| SLF4J warning | Se ignora – nu afecteaza functionarea |
| Port 9092 deja folosit | Opreste alte procese pe portul 9092 |
