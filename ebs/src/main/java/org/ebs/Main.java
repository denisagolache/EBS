package org.ebs;

import org.ebs.publication.FieldConfig;
import org.ebs.publication.Publication;
import org.ebs.publication.PublicationGenerator;
import org.ebs.subscription.Subscription;
import org.ebs.subscription.SubscriptionGenerator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
    static final int COUNT = 100_000;

    static List<FieldConfig> buildFieldConfigs() {
        List<FieldConfig> configs = new ArrayList<>();
        configs.add(new FieldConfig("company",
                List.of("Google", "Microsoft", "Apple", "Amazon", "Meta", "Tesla")));
        configs.add(new FieldConfig("value",      10.0, 1000.0));
        configs.add(new FieldConfig("drop",        0.0,   50.0));
        configs.add(new FieldConfig("variation",  -5.0,    5.0));
        configs.add(new FieldConfig("date",
                LocalDate.of(2020, 1, 1),
                LocalDate.of(2024, 12, 31)));
        return configs;
    }

    static final Map<String, Double> FIELD_FREQ = Map.of(
            "company",   0.90,
            "value",     0.80,
            "drop",      0.60,
            "variation", 0.70,
            "date",      0.50
    );

    static final Map<String, Double> EQ_FREQ = Map.of(
            "company", 0.70,
            "date",    0.40
    );

    public static void main(String[] args) throws Exception {

        List<FieldConfig>     fieldConfigs = buildFieldConfigs();
        PublicationGenerator  pubGen       = new PublicationGenerator(fieldConfigs);
        SubscriptionGenerator subGen       = new SubscriptionGenerator(fieldConfigs);

        int cpus = Runtime.getRuntime().availableProcessors();

        System.out.println("=".repeat(72));
        System.out.println("  EBS Generator – benchmark");
        System.out.printf ("  Mesaje: %,d publicatii + %,d subscriptii%n", COUNT, COUNT);
        System.out.println("  CPU: " + cpus + " nuclee logice");
        System.out.println("=".repeat(72));

        List<BenchmarkResult> results = new ArrayList<>();

        {
            long t0 = System.currentTimeMillis();
            List<Publication> pubs = pubGen.generate(COUNT);
            long pubMs = System.currentTimeMillis() - t0;

            t0 = System.currentTimeMillis();
            List<Subscription> subs = subGen.generate(COUNT, FIELD_FREQ, EQ_FREQ);
            long subMs = System.currentTimeMillis() - t0;

            results.add(new BenchmarkResult("Secvential (1 thread)", COUNT, 1, pubMs, subMs));

            writeLines("publications_1t.txt",   toStrings(pubs));
            writeLines("subscriptions_1t.txt",  toStrings(subs));

            System.out.println("\n[1 thread – secvential]");
            System.out.println("  pub=" + pubMs + "ms  sub=" + subMs + "ms");
            printExamples(pubs, subs);
            System.out.println("  Verificare frecvente:");
            verifyFrequencies(subs, COUNT);
        }

        {
            long t0 = System.currentTimeMillis();
            List<Publication> pubs = pubGen.generateParallel(COUNT, 2);
            long pubMs = System.currentTimeMillis() - t0;

            t0 = System.currentTimeMillis();
            List<Subscription> subs = subGen.generateParallel(COUNT, FIELD_FREQ, EQ_FREQ, 2);
            long subMs = System.currentTimeMillis() - t0;

            results.add(new BenchmarkResult("Paralel (2 threaduri)", COUNT, 2, pubMs, subMs));
            writeLines("publications_2t.txt",  toStrings(pubs));
            writeLines("subscriptions_2t.txt", toStrings(subs));

            System.out.println("\n[2 threaduri]");
            System.out.println("  pub=" + pubMs + "ms  sub=" + subMs + "ms");
        }

        {
            long t0 = System.currentTimeMillis();
            List<Publication> pubs = pubGen.generateParallel(COUNT, 4);
            long pubMs = System.currentTimeMillis() - t0;

            t0 = System.currentTimeMillis();
            List<Subscription> subs = subGen.generateParallel(COUNT, FIELD_FREQ, EQ_FREQ, 4);
            long subMs = System.currentTimeMillis() - t0;

            results.add(new BenchmarkResult("Paralel (4 threaduri)", COUNT, 4, pubMs, subMs));
            writeLines("publications_4t.txt",  toStrings(pubs));
            writeLines("subscriptions_4t.txt", toStrings(subs));

            System.out.println("\n[4 threaduri]");
            System.out.println("  pub=" + pubMs + "ms  sub=" + subMs + "ms");
        }

        {
            long t0 = System.currentTimeMillis();
            List<Publication> pubs = pubGen.generateParallel(COUNT, 8);
            long pubMs = System.currentTimeMillis() - t0;

            t0 = System.currentTimeMillis();
            List<Subscription> subs = subGen.generateParallel(COUNT, FIELD_FREQ, EQ_FREQ, 8);
            long subMs = System.currentTimeMillis() - t0;

            results.add(new BenchmarkResult("Paralel (8 threaduri)", COUNT, 8, pubMs, subMs));
            writeLines("publications_8t.txt",  toStrings(pubs));
            writeLines("subscriptions_8t.txt", toStrings(subs));

            System.out.println("\n[8 threaduri]");
            System.out.println("  pub=" + pubMs + "ms  sub=" + subMs + "ms");
        }

        System.out.println("\n" + "=".repeat(72));
        System.out.println("  SUMAR BENCHMARK");
        System.out.println("=".repeat(72));
        long baseline = results.get(0).totalMs();
        for (BenchmarkResult r : results) {
            double speedup = baseline > 0 ? (double) baseline / r.totalMs() : 1.0;
            System.out.printf("  %s  speedup=%.2fx%n", r, speedup);
        }
        System.out.println("=".repeat(72));
    }

    static void verifyFrequencies(List<Subscription> subs, int count) {
        for (String field : List.of("company", "value", "drop", "variation", "date")) {

            long fieldCount = subs.stream()
                    .flatMap(s -> s.getFields().stream())
                    .filter(f -> f.getFieldName().equals(field))
                    .count();

            long eqCount = subs.stream()
                    .flatMap(s -> s.getFields().stream())
                    .filter(f -> f.getFieldName().equals(field)
                            && f.getOperator().equals("="))
                    .count();

            double freqReal = (double) fieldCount / count;
            double eqReal   = fieldCount > 0 ? (double) eqCount / fieldCount : 0.0;

            double freqCfg = FIELD_FREQ.getOrDefault(field, 0.0);
            Double eqCfg   = EQ_FREQ.getOrDefault(field, null);

            System.out.printf(
                    "    %-10s  freq: cfg=%.2f real=%.4f  |  eq: cfg=%s real=%.4f%n",
                    field, freqCfg, freqReal,
                    eqCfg != null ? String.format("%.2f", eqCfg) : " N/A",
                    eqReal);
        }
    }

    static void printExamples(List<Publication> pubs, List<Subscription> subs) {
        System.out.println("  Exemple publicatii:");
        pubs.subList(0, Math.min(3, pubs.size()))
                .forEach(p -> System.out.println("    " + p));
        System.out.println("  Exemple subscriptii:");
        subs.subList(0, Math.min(3, subs.size()))
                .forEach(s -> System.out.println("    " + s));
    }

    static <T> List<String> toStrings(List<T> items) {
        List<String> lines = new ArrayList<>(items.size());
        for (T item : items) lines.add(item.toString());
        return lines;
    }

    static void writeLines(String path, List<String> lines) {
        try (BufferedWriter w = new BufferedWriter(new FileWriter(path))) {
            for (String line : lines) {
                w.write(line);
                w.newLine();
            }
            System.out.println("  Scris: " + path + " (" + lines.size() + " linii)");
        } catch (IOException e) {
            System.err.println("Eroare la scriere " + path + ": " + e.getMessage());
        }
    }
}