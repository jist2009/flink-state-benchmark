package com.benchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichMapFunction;

// 1. On contraint T à étendre Timestamped et Serializable
public class BenchmarkMetricsListener<T extends Timestamped & Serializable> extends RichMapFunction<T, T> implements Serializable {

    private final String mode;
    private transient long lastReportTime;
    private transient long eventCounter;
    private transient List<Long> latencies;
    private transient MemoryMXBean memoryBean;
    private transient PrintWriter writer;
    private transient int secondCounter;

    public BenchmarkMetricsListener(String mode) {
        this.mode = mode;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.lastReportTime = System.currentTimeMillis();
        this.eventCounter = 0;
        this.latencies = new ArrayList<>(200000);
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.secondCounter = 0;

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        String filename = "metrics_" + mode + "_subtask_" + subtaskIndex + ".csv";
        this.writer = new PrintWriter(new FileWriter(filename, false), true);
        this.writer.println("seconde,mode,subtask,debit_rec_s,p50_ms,p99_ms,heap_mo");
    }

    @Override
    public T map(T value) throws Exception {
        long eventTime = value.getTimestamp(); 
        long now = System.currentTimeMillis();
        // 2. On utilise eventTime ici au lieu de value.timestamp
        long latency = Math.max(0, now - eventTime);

        eventCounter++;
        latencies.add(latency);

        if (now - lastReportTime >= 1000) {
            secondCounter++;
            double durationSec = (now - lastReportTime) / 1000.0;
            double throughput = eventCounter / durationSec;

            Collections.sort(latencies);
            int size = latencies.size();
            long p50 = size > 0 ? latencies.get((int) (size * 0.50)) : 0;
            long p99 = size > 0 ? latencies.get((int) (size * 0.99)) : 0;
            long usedHeapMb = memoryBean.getHeapMemoryUsage().getUsed() / (1024 * 1024);
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

            if (writer != null) {
                writer.printf("%d,%s,%d,%.0f,%d,%d,%d%n",
                    secondCounter, mode, subtaskIndex, throughput, p50, p99, usedHeapMb
                );
            }

            eventCounter = 0;
            latencies.clear();
            lastReportTime = now;
        }

        return value;
    }

    @Override
    public void close() {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }
}