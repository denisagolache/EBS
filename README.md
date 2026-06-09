# EBS – Sistem Pub-Sub cu rutare avansata pe brokeri

## Cerinta

Implementati un sistem publish-subscribe cu **filtrare bazata pe continut**, **distributie balansata a subscriptiilor** pe 3 brokeri si **rutare in lant** a publicatiilor, cu suport de persistenta in PostgreSQL 17 si recuperare dupa caderea unui broker.

## Arhitectura

```
┌──────────────┐     topic: ebs-pubs (Kafka, key=brokerId)
│  Publisher 1 │─────┐
│  Publisher 2 │─────┤
└──────────────┘     │
                     │  Intrarea se face la brokerul ales prin hash(pubId) % 3
                     v
             ┌───────────────────┐
             │  Broker (entry)   │  ← oricare 0/1/2, doar forward la Broker 0
             │  handlePublication│
             └────────┬──────────┘
                      │ topic: ebs-broker-fwd (Kafka, key=0)
                      v
┌─────────────────────────────────────────────────────────────┐
│                   Lant secvential de brokeri                 │
│                                                              │
│  ┌──────────┐   fwd   ┌──────────┐   fwd   ┌──────────┐     │
│  │ Broker 0 │ ──────> │ Broker 1 │ ──────> │ Broker 2 │     │
│  │ match    │         │ match    │         │ match +  │     │
│  │ subs 0   │         │ subs 1   │         │ deliver  │     │
│  └──────────┘         └──────────┘         └──────────┘     │
│        ▲                   ▲                   ▲             │
│        │                   │                   │             │
│   ┌────┴────┐         ┌────┴────┐         ┌────┴────┐       │
│   │PostgreSQL│         │PostgreSQL│         │PostgreSQL│     │
│   │(broker_0)│         │(broker_1)│         │(broker_2)│     │
│   └─────────┘         └─────────┘         └─────────┘       │
└─────────────────────────────────────────────────────────────┘
                                                              │
                    topic: ebs-delivery-sub-{id} (Kafka)      │
                                                              v
┌─────────────────────────────────────────────────────────────┐
│                     Subscriberi (x3)                         │
│  Fiecare subscriber primeste pe topicul sau personal         │
│  doar publicatiile care au match-uit cel putin o subscriptie │
│  de-a lui                                               │
└─────────────────────────────────────────────────────────────┘
```

### Fluxul unei publicatii

1. **Publisher** genereaza o publicatie → hash(pubId) % 3 → trimite la entry broker pe topicul `ebs-pubs`
2. **Entry broker** primeste, creeaza un `BrokerMessage(index=0, target=0)` si il forwardeaza pe `ebs-broker-fwd`
3. **Broker 0** consuma → face match pe subsetul sau de subscriptii → adauga rezultatul in setul comun → forwardeaza la Broker 1
4. **Broker 1** → match pe subsetul sau → forward la Broker 2
5. **Broker 2** (ultimul) → match pe subsetul sau → **deliver()** → trimite `DeliveryReport` subscriberilor match-uiti

### Distributia subscriptiilor (balansata)

```
brokerDestinatie = (subscriberId + "_" + indexSubscriptie).hashCode() % 3
```

Fiecare subscriptie ajunge la exact un broker. Subscriptiile aceluiasi subscriber sunt **distribuite uniform** pe toti brokerii.

### Filtrare bazata pe continut

Filtrarea se face in **SubscriptionStore.java** (pe fiecare broker, pe subsetul local):

1. `match(Publication)` → itereaza toate subscriptiile brokerului
2. Pentru fiecare pereche (sub, pub):
   - `findField()` cauta campul cu acelasi nume in publicatie
   - `evaluate(operator, valoarePublicatie, valoareSubscriptie)` aplica operatorul
3. Returneaza subscriberId-urile cu cel putin o subscriptie potrivita

Operatorii suportati: `=`, `!=`, `<`, `<=`, `>`, `>=`
Tipuri suportate: `String`, `Double`, `LocalDate`

### Rolul Apache Kafka

**Kafka** este folosit **doar pentru transportul mesajelor** intre noduri. Nu contine logica de business.

| Server | Versiune | Rol |
|--------|----------|-----|
| **Confluent Kafka 7.5.0** (Docker) | kafka-clients 3.6.0 | Transport mesaje intre publisheri, brokeri si subscriberi |
| `localhost:9092` | bootstrap.servers | Punct de conexiune |

Topicuri utilizate:
- `ebs-pubs-{runId}` (1 partitie) — publicatii de la publisheri
- `ebs-broker-fwd-{runId}` (3 partitii) — forward intre brokeri
- `ebs-sub-reg-{runId}` (1 partitie) — inregistrare subscriptii
- `ebs-delivery-{runId}-sub-{id}` (cate unul per subscriber) — livrari

### Rolul PostgreSQL 17

**PostgreSQL 17** (Docker, port 5433) asigura **persistenta subscriptiilor** pentru recuperare dupa caderea unui broker.

```
CREATE TABLE subscriptions (
    broker_id INT NOT NULL,
    subscriber_id VARCHAR(255) NOT NULL,
    sub_index INT NOT NULL,
    subscription_data TEXT NOT NULL,
    PRIMARY KEY (broker_id, subscriber_id, sub_index)
);
```

Fiecare subscriptie e salvata cu `broker_id`-ul brokerului asignat.

### Caderea si recuperarea unui broker (Test C)

```
Faza 1 (45s):   Functionare normala — toti 3 brokerii activi
Faza 2 (45s):   BROKER 1 CADUT
                - consumer Kafka oprit
                - subscriptii sterse din memorie (0 in memorie)
                - subscriptii raman in PostgreSQL (broker_id=1)
                - publicatiile ce trec prin broker 1 se acumuleaza in Kafka
Faza 3 (90s):   BROKER 1 RECUPERAT
                - consumer Kafka repornit
                - incarca subscriptiile din DB: SELECT * WHERE broker_id = 1
                - proceseaza mesajele acumulate in Kafka
```

## Componente

| Componenta | Numar | Rol |
|---|---|---|
| **PublisherNode** | 2 | Genereaza publicatii (valori aleatoare, 100 pub/sec fiecare) |
| **BrokerNode** | 3 | Stocheaza subscriptii, filtreaza, ruteaza in lant |
| **SubscriberNode** | 3 | Se conecteaza la brokeri, inregistreaza subscriptii, primeste livrari |
| **KafkaBus** | 1 | Transport Kafka, Protobuf + Base64 |
| **SubscriptionDatabase** | 1 | Persistenta PostgreSQL 17 |

## Evaluare

Configuratie: 3 brokeri, 2 publisheri, 3 subscriberi, **10.000 subscriptii**, target 200 pub/sec,
feed continuu de **3 minute**, doar campul `value` (pentru masurarea pura a matching-ului).

### Rezultate

#### Test A: eqFreq = 100% (operatori `=` pe `value`)

```
  REZULTATE A (100% eq, 3 min)
  -------------------------------------------------------
  (a) Publicatii livrate cu succes:       2,351
  (b) Latenta medie de livrare:         942.298 ms
  (c) Rata de potrivire:                  0.0010%
```

#### Test B: eqFreq = 25% (25% `=`, 75% operatori de comparatie)

```
  REZULTATE B (25% eq, 3 min)
  -------------------------------------------------------
  (a) Publicatii livrate cu succes:      69,507
  (b) Latenta medie de livrare:         761.443 ms
  (c) Rata de potrivire:                 37.3309%
```

#### Tabel comparativ

| Metrica | A (100% eq) | B (25% eq) |
|---|---|---|
| (a) Publicatii livrate cu succes | 2.351 | 69.507 |
| (b) Latenta medie de livrare | 942.298 ms | 761.443 ms |
| (c) Rata de potrivire | 0,0010% | 37,3309% |

#### Test C: Cadere broker 1 cu recuperare din PostgreSQL 17

```
  REZULTATE C (crash broker 1, 3 min)
  -------------------------------------------------------
  (a) Publicatii livrate cu succes:       2,418
  (b) Latenta medie de livrare:        7566.606 ms
  (c) Rata de potrivire:                 0.0011%
  -------------------------------------------------------
  STATISTICI PERSISTENTA PostgreSQL 17:
  Subscriptii INAINTE de cadere:   3,334
  Subscriptii DUPA recuperare:     3,334
  Concluzie: Recuperare perfecta — toate subscriptiile restaurate corect.
```

Latența mare in Test C (7566 ms) se datoreaza mesajelor acumulate in Kafka in timpul celor ~45s
cat broker 1 a fost cazut — latenta e calculata de la timestamp-ul original al publicarii,
nu de la momentul procesarii efective.

### Interpretare

**(a) Publicatii livrate cu succes:**
- Test A (100% `=`): doar 2.351 livrari. Publicatiile sunt livrate doar daca fac match cu cel putin o subscriptie — sansele de egalitate exacta pe `Double` in intervalul [10, 1000] cu 2 zecimale sunt ~1/99000.
- Test B (25% `=`, 75% comparatie): 69.507 livrari. Operatorii `<`, `>`, `<=`, `>=` acopera intervale mult mai largi.

**(b) Latenta medie:**
- 942 ms (A) / 761 ms (B). Cauzele principale sunt:
  - 4-5 hop-uri Kafka secventiale cu `acks=all`
  - Serializare/deserializare Protobuf + Base64 la fiecare hop
  - Procesare sincrona a mesajelor in consumator
  - Matching-ul propriu-zis e neglijabil (< 2ms per broker)

**(c) Rata de potrivire:**
- **0,001%** la 100% egalitate vs **37,33%** la 25% egalitate.
- Diferenta demonstreaza impactul tipului de operator asupra selectivitatii filtrarii:
  operatorii de comparatie sunt mult mai permisivi decat egalitatea exacta pe un camp continuu.

## Algoritmi utilizati

| Aspect | Algoritm |
|---|---|
| **Filtrare** | Potrivire naiva (`O(N)` per publicatie, fara indexuri) |
| **Rutare** | Lant secvential cu acumulare (nu flooding) |
| **Distributie subscriptii** | Modulo hash pe `subscriberId_index` |
| **Rutare publicatii** | Hash pe `pubId` pentru entry broker, apoi lant fix 0→1→2 |
| **Persistenta** | PostgreSQL 17, incarcare la start si dupa crash |

## Tehnologii

- **Java 17** — pattern matching `switch`, `instanceof` cu tipare
- **Apache Kafka 3.6.0** (kafka-clients) — transport mesaje
- **Protocol Buffers (protobuf)** + **Base64** — serializare binara
- **Confluent Kafka 7.5.0** (Docker) — infrastructura Kafka
- **PostgreSQL 17** (Docker, port 5433) — persistenta subscriptii
- **Maven** — build
- **Docker Desktop** — rulare Kafka + PostgreSQL
