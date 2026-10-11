package com.benchmark;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

public class BidSource implements SourceFunction<Bid> {
    private volatile boolean isRunning = true;
    private static final long RUN_DURATION_MS = 300_010L; 

    @Override
    public void run(SourceContext<Bid> ctx) throws Exception {
        Random rand = new Random(42);
        long auctionCount = 1000; // cardinalité des clés (1000 objets différents)
        long startTime = System.currentTimeMillis();
        
        while (isRunning && (System.currentTimeMillis() - startTime < RUN_DURATION_MS)) {
            long auctionId = rand.nextInt((int) auctionCount);
            double price = Math.round((rand.nextDouble() * 200.0) * 100.0) / 100.0; // prix entre 0 et 200
            long timestamp = System.currentTimeMillis();

            ctx.collect(new Bid(auctionId, price, timestamp));
            Thread.sleep(10); // génère ~200 événements/sec pour le test local
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
