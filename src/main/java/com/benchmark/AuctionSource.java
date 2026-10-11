package com.benchmark;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.util.Random;

public class AuctionSource extends RichParallelSourceFunction<Auction> {
    private volatile boolean running = true;
    private static final long RUN_DURATION_MS = 300_010L; // 5 minutes

    @Override
    public void run(SourceContext<Auction> ctx) throws Exception {
        Random rand = new Random();
        int subtask = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        long auctionId = subtask;
        long startTime = System.currentTimeMillis();

        while (running && (System.currentTimeMillis() - startTime < RUN_DURATION_MS)) {
            long ts = System.currentTimeMillis();
            long sellerId = 1000L + rand.nextInt(500);
            ctx.collect(new Auction(auctionId, sellerId, 10.0 + rand.nextDouble() * 100, ts));
            auctionId += totalSubtasks;
            Thread.sleep(10); // génère ~100 événements/sec pour le test local
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}