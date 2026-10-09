package com.benchmark;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

public class StatelessBidFilterJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint toutes les 1 secondes pour mesurer le coût de sauvegarde
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        
        DataStream<Bid> bids = env.addSource(new BidSource());

        bids.map(new BenchmarkMetricsListener<>("STATELESS"))
            .keyBy(bid -> bid.auction)
            .filter(bid -> bid.price > 100.0)
            .sinkTo(new DiscardingSink<>());
        env.execute("Stateless Bid Filter Job");
    }
}
