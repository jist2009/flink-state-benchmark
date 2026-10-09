package com.benchmark;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

public class PersonSource implements SourceFunction<Person> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Person> ctx) throws Exception {
        Random rand = new Random();
        long id = 1000;
        while (running) {
            long ts = System.currentTimeMillis();
            ctx.collect(new Person(id++, "User_" + id, ts));
            Thread.sleep(10); // Ajuste la cadence selon tes besoins
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}