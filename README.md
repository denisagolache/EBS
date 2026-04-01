# Event-Based-Systems-HW1 — Parallelization & Benchmark (EBS Generator)

## Parallelization type

We opted for **threads** for the parallel approach of generating **subscriptions** and **publications**.

Why threads:
- Threads within the same process share the same memory space, which makes inter-thread communication and data sharing much more efficient compared to inter-process communication (IPC).
- The generator workload (creating many independent lines: publications/subscriptions) is easy to split into parallel tasks and benefits from multi-core CPUs.

### Thread-safety for adding fields

Threads add fields to a specific publication/subscription using a synchronized method:

```java
public void addField(PublicationField field) {
    synchronized (fields) {
        fields.add(field);
    }
}

public void addField(SubscriptionField field) {
    synchronized (fields) {
        fields.add(field);
    }
}
```

Where `fields` is a **synchronized list** in both cases, instantiated in the constructor:

```java
this.fields = Collections.synchronizedList(new ArrayList<>());
```

This ensures correctness when multiple threads may try to add fields concurrently.

---

## Benchmark — EBS Generator

```
========================================================================
  EBS Generator – benchmark
  Mesaje: 100,000 publicatii + 100,000 subscriptii
  CPU: 16 nuclee logice
========================================================================
  Scris: publications_1t.txt (100000 linii)
  Scris: subscriptions_1t.txt (100000 linii)
```

### 1 thread (sequential)

```
[1 thread – secvential]
  pub=112ms  sub=199ms
```

**Exemple publicații:**
- `{(company, "Tesla"); (value, 923.27); (drop, 14.63); (variation, -0.38); (date, 15.08.2021)}`
- `{(company, "Meta"); (value, 344.95); (drop, 17.36); (variation, 4.06); (date, 25.11.2022)}`
- `{(company, "Google"); (value, 695.9); (drop, 43.56); (variation, 2.2); (date, 19.11.2024)}`

**Exemple subscripții:**
- `{(company, =, "Meta"); (value, =, 992.59); (drop, =, 49.86); (date, =, 10.06.2021)}`
- `{(drop, !=, 11.19); (variation, <, 2.65); (date, !=, 24.07.2021)}`
- `{(company, =, "Microsoft"); (drop, <=, 19.75); (variation, !=, 0.93)}`

**Verificare frecvențe (config vs real):**
- `company     freq: cfg=0.90 real=0.9000  |  eq: cfg=0.70 real=0.7000`
- `value       freq: cfg=0.80 real=0.8000  |  eq: cfg= N/A real=0.1667`
- `drop        freq: cfg=0.60 real=0.6000  |  eq: cfg= N/A real=0.1667`
- `variation   freq: cfg=0.70 real=0.7000  |  eq: cfg= N/A real=0.1667`
- `date        freq: cfg=0.50 real=0.5000  |  eq: cfg=0.40 real=0.4000`

Output:
- `publications_1t.txt` (100000 linii)
- `subscriptions_1t.txt` (100000 linii)

---

### 2 threads

```
  Scris: publications_2t.txt (100000 linii)
  Scris: subscriptions_2t.txt (100000 linii)

[2 threaduri]
  pub=109ms  sub=80ms
```

Output:
- `publications_2t.txt` (100000 linii)
- `subscriptions_2t.txt` (100000 linii)

---

### 4 threads

```
  Scris: publications_4t.txt (100000 linii)
  Scris: subscriptions_4t.txt (100000 linii)

[4 threaduri]
  pub=11ms  sub=30ms
```

Output:
- `publications_4t.txt` (100000 linii)
- `subscriptions_4t.txt` (100000 linii)

---

### 8 threads

```
  Scris: publications_8t.txt (100000 linii)
  Scris: subscriptions_8t.txt (100000 linii)

[8 threaduri]
  pub=3ms  sub=21ms
```

Output:
- `publications_8t.txt` (100000 linii)
- `subscriptions_8t.txt` (100000 linii)

---

## Benchmark summary

```
========================================================================
  SUMAR BENCHMARK
========================================================================
  Secvential (1 thread)  | threads=1 | pub= 112ms | sub= 199ms | total= 311ms speedup=1.00x
  Paralel (2 threaduri)  | threads=2 | pub= 109ms | sub=  80ms | total= 189ms speedup=1.65x
  Paralel (4 threaduri)  | threads=4 | pub=  11ms | sub=  30ms | total=  41ms speedup=7.59x
  Paralel (8 threaduri)  | threads=8 | pub=   3ms | sub=  21ms | total=  24ms speedup=12.96x
========================================================================
```

## Observations

- Subscription generation benefits significantly from parallelization (199ms → 21ms).
- Publication generation scales even more strongly (112ms → 3ms).
- With **8 threads**, the total runtime improves from **311ms** to **24ms**, achieving a **12.96x speedup** in this benchmark setup.
