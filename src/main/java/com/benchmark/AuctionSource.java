package com.benchmark;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

public class AuctionSource implements SourceFunction<Auction> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Auction> ctx) throws Exception {
        Random rand = new Random();
        long auctionId = 1;
        while (running) {
            long ts = System.currentTimeMillis();
            // Génère des enchères associées aux vendeurs récents
            long sellerId = 1000 + rand.nextInt(500);
            ctx.collect(new Auction(auctionId++, sellerId, 10.0 + rand.nextDouble() * 100, ts));
            Thread.sleep(5);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}