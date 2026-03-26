package org.ebs;

public class BenchmarkResult {

    public final String label;
    public final int    count;
    public final int    threads;
    public final long   pubMs;
    public final long   subMs;

    public BenchmarkResult(String label, int count, int threads, long pubMs, long subMs) {
        this.label   = label;
        this.count   = count;
        this.threads = threads;
        this.pubMs   = pubMs;
        this.subMs   = subMs;
    }

    public long totalMs() { return pubMs + subMs; }

    @Override
    public String toString() {
        return String.format("%-22s | threads=%-2d | pub=%5dms | sub=%5dms | total=%5dms",
                label, threads, pubMs, subMs, totalMs());
    }
}