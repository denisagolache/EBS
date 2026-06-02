# EBS – Event-Based System cu rutare avansata pe brokeri

Sistem publish-subscribe cu **filtrare bazata pe continut**, **distributie balansata a subscriptiilor** pe 3 brokeri si **rutare in lant** a publicatiilor. Transportul mesajelor se face prin **Apache Kafka** (Docker), iar filtrarea, stocarea si rutarea raman in Java pur.

## Arhitectura

```
┌──────────┐   ebs-pubs (Kafka)    ┌──────────────────┐
│ Publisher│ ──── key=brokerId ───>│  BrokerNode 0    │──┐
│  (x2)    │                       │ (entry broker)    │  │
└──────────┘                       └──────────────────┘  │
                                                         │ ebs-broker-fwd (Kafka)
                                                         │ key=targetBrokerId
                                                         v
                                                  ┌──────────────────┐
                                                  │  BrokerNode 1    │──┐
                                                  │ (forward chain)  │  │
                                                  └──────────────────┘  │
                                                                       │
                                                                       v
                                                                ┌──────────────────┐
                                                                │  BrokerNode 2    │
                                                                │ (ultimul broker) │
                                                                └──────────────────┘
                                                                       │
                                              ebs-delivery-sub-{id}   │
                                                                       v
                                                                ┌──────────────────┐
                                                                │  SubscriberNode  │
                                                                │     (x3)         │
                                                                └──────────────────┘
```

Subscriptiile se trimit pe topicul `ebs-sub-reg`.

### Componente

| Componenta | Numar | Rol |
|---|---|---|
| **PublisherNode** | 2 | Genereaza publicatii cu valori aleatoare si le trimite in retea |
| **BrokerNode** | 3 | Stocheaza subscriptii, filtreaza continut, ruteaza in lant |
| **SubscriberNode** | 3 | Se conecteaza la brokeri, inregistreaza subscriptii, primeste livrari |
| **KafkaBus** (MessageBus) | 1 | Transport Kafka – doar livrare, fara logica de business |

### Fluxul unei publicatii

1. **Publisher** genereaza o publicatie si o trimite la brokerul de intrare (topic `ebs-pubs`, key = brokerId)
2. Brokerul de intrare creeaza un **BrokerMessage** si il forwardeaza la **Broker 0** (topic `ebs-broker-fwd`, key = 0)
3. **Broker 0** verifica subscriptiile proprii, adauga subscriberii match-uiti, forwardeaza la **Broker 1**
4. **Broker 1** verifica subscriptiile proprii, adauga, forwardeaza la **Broker 2**
5. **Broker 2** (ultimul) verifica subscriptiile proprii si **livreaza** subscriberilor match-uiti (topic `ebs-delivery-sub-{id}`)

Fiecare broker gestioneaza **doar o parte** din subscriptii (distribuite prin consistent hashing).

### Distributia subscriptiilor

Subscriptiile aceluiasi subscriber sunt distribuite balansat pe toti brokerii:

```
brokerDestinatie = hash(subscriberId + "_" + indexSubscriptie) % 3
```

### Filtrare (Content-Based Matching)

Fiecare subscriptie contine campuri cu operatori: `=`, `!=`, `<`, `<=`, `>`, `>=`.
O publicatie match-e o subscriptie daca **toate** campurile subscriptiei sunt satisfacute de valorile publicatiei.
Suporta tipuri `String`, `Double` si `LocalDate`.

### Separarea responsabilitatilor

| Strat | Tehnologie | Responsabilitate |
|---|---|---|
| Livrare mesaje | **Apache Kafka** (Docker) | Transport intre noduri |
| Stocare subscriptii | **Java** (`SubscriptionStore`) | Memorie locala per broker |
| Filtrare continut | **Java** (`evaluate()`) | Operator matching logic |
| Rutare | **Java** (`BrokerNode`) | Lant secvential 0 -> 1 -> 2 -> livrare |
| Serializare | **Jackson JSON** (`ObjectMapper`) | Codare/decodare mesaje |

## Structura proiectului

```
ebs/
├── pom.xml                           # Maven + dependente (Kafka, Jackson)
├── docker-compose.yml                # Infrastructura Kafka (Confluent)
├── build.ps1                         # Script automat Docker + build + run
├── howtorun.md                       # Instructiuni detaliate de rulare
└── src/main/java/org/ebs/
    ├── publication/                  # Modele publicatii + generare
    │   ├── FieldConfig.java
    │   ├── Publication.java
    │   ├── PublicationField.java
    │   └── PublicationGenerator.java
    ├── subscription/                 # Modele subscriptii + generare
    │   ├── FieldPlan.java
    │   ├── Subscription.java
    │   ├── SubscriptionField.java
    │   └── SubscriptionGenerator.java
    └── pubsub/
        ├── EvaluationMain.java       # Punct de pornire + raport
        ├── config/
        │   └── SystemConfig.java     # Configuratie (brokeri, rate, etc.)
        ├── msg/
        │   ├── MessageBus.java       # Interfata abstracta de transport
        │   └── KafkaBus.java         # Implementare Kafka cu PubCodec JSON
        ├── model/
        │   ├── PubMessage.java
        │   ├── SubRegistration.java
        │   ├── BrokerMessage.java
        │   └── DeliveryReport.java
        ├── broker/
        │   ├── BrokerNode.java
        │   └── SubscriptionStore.java
        ├── publisher/
        │   └── PublisherNode.java
        ├── subscriber/
        │   └── SubscriberNode.java
        └── util/
            └── Hashing.java
```

## Rezultate evaluare

Configuratie: 3 brokeri, 2 publisheri, 3 subscriberi, **10.000 subscriptii**, 200 pub/sec, feed 180 sec.

Arhitectura: Apache Kafka (Docker) + Jackson JSON + Java (filtrare, stocare, rutare).

### Partea 1 – Toate campurile (configuratie normala)

| Metric | A (eq=100%) | B (eq=25%) |
|---|---|---|
| Publicatii emise | 23.124 | 23.154 |
| Livrari totale | 69.372 | 69.462 |
| Rata matching | **100,00%** | **100,00%** |
| Debit livrari | 385,37 msg/sec | 385,90 msg/sec |
| Latenta medie | 82,965 ms | 71,129 ms |

### Partea 2 – Doar campul `value` in subscriptii (impact pur al operatorilor)

| Metric | C (eq=100%) | D (eq=25%) |
|---|---|---|
| Publicatii emise | 3.861 | 3.860 |
| Livrari totale | **376** | **11.580** |
| Rata matching | **3,25%** | **100,00%** |
| Debit livrari | 12,53 msg/sec | 385,99 msg/sec |
| Latenta medie | 123,965 ms | 128,587 ms |

### Interpretare

**(a) Publicatii livrate cu succes** – Fiecare publicatie ajunge la toti cei 3 subscriberi.
In configuratia normala: ~23.000 publicatii emise in 3 minute -> ~69.000 livrari (3 x 23.000).

**(b) Latenta medie** – Valori rezonabile pentru o infrastructura Kafka locala in Docker:
- Scenariile A/B: ~70-80 ms (predominant timpul de serializare/transport Kafka)
- Scenariile C/D: ~120-130 ms (impactul suplimentar al testelor izolate cu 30 sec)

Factorii principali: serializarea JSON cu Jackson, overhead Kafka, procesare matching.

**(c) Rata de matching** – Diferenta dramatica intre cele doua cazuri izolate:
- **eqFreq = 100%** (scenariul C): matching rate ~3,25%. Operatorul `=` cere egalitate exacta intre ~99.000 de valori posibile pentru campul numeric `value`. Sansele de potrivire sunt foarte mici.
- **eqFreq = 25%** (scenariul D): matching rate **100%**. Ceilalti operatori (`!=`, `>`, `>=`, `<`, `<=`) sunt mult mai permisivi – aproape orice valoare a publicatiei satisface cel putin o subscriptie.

In configuratia cu toate campurile (A si B), matching-ul este 100% in ambele cazuri deoarece campurile suplimentare (`company`, `drop`, `variation`) cu operatori mixti domina matching-ul.

## Tehnologii

- **Java 17** – pattern matching `switch`, `instanceof` cu tipare, text blocks
- **Apache Kafka 3.6.0** (kafka-clients) – transport mesaje
- **Jackson 2.16.0** (jackson-databind, jackson-datatype-jsr310) – serializare JSON
- **Confluent Kafka 7.5.0** (Docker) – infrastructura Kafka
- **Maven** – build
- **Docker Desktop** – rulare Kafka

## Specificatii tehnice

- Thread pooling separat: fiecare broker, publisher si subscriber ruleaza in propriul pool
- Consistent hashing: distributie determinista a subscriptiilor pe brokeri
- Serializare JSON cu Jackson (tree model) pentru mesaje eterogene
- Retry cu backoff exponential la conectarea la Kafka
- Topic-uri unice per rulare (fara coliziuni intre scenarii)
