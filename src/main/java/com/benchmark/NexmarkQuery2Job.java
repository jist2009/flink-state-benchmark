package com.benchmark;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.io.Serializable;

public class NexmarkQuery2Job {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpointing activé à 1 s pour évaluer le coût minimal de sauvegarde sans état
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        DataStream<Bid> bids = env.addSource(new BidSource());

        // Mesure du débit d'ingestion brut en amont
        bids.map(new BenchmarkMetricsListener<>("QUERY_2_SELECTION"))
            // Filtrage stateless : sélection des 5 auctions cibles
            .filter(bid -> bid.auction == 1007 
                        || bid.auction == 1020 
                        || bid.auction == 2001 
                        || bid.auction == 2019 
                        || bid.auction == 2087)
            // Projection : sélection uniquement de (auction, price)
            .map(bid -> new BidPriceProjection(bid.auction, bid.price))
            .sinkTo(new DiscardingSink<>());

        env.execute("Nexmark Query 2 - Selection");
    }

    // DTO de projection pour Rstream(auction, price)
    public static class BidPriceProjection implements Serializable {
        public long auction;
        public double price;

        public BidPriceProjection() {}

        public BidPriceProjection(long auction, double price) {
            this.auction = auction;
            this.price = price;
        }

        @Override
        public String toString() {
            return "BidPriceProjection{auction=" + auction + ", price=" + price + "}";
        }
    }
}