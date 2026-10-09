package com.benchmark;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

public class StatefulMaxBidFilterJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Même intervalle de checkpointing que le stateless (5s)
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        DataStream<Bid> bids = env.addSource(new BidSource());


        bids.map(new BenchmarkMetricsListener<>("STATEFUL")) // Mesure l'ingestion brute ici
            .keyBy(bid -> bid.auction)                      // KeyedStream créé ici
            .process(new MaxBidKeyedProcessFunction())       // KeyedProcessFunction redevient valide
            .sinkTo(new DiscardingSink<>());
        env.execute("Stateful Max Bid Filter Job");
    }

    public static class MaxBidKeyedProcessFunction extends KeyedProcessFunction<Long, Bid, Bid> {

        // État mémorisé par clé (auction)
        private transient ValueState<Double> maxPriceState;

        @Override
        public void open(OpenContext openContext) {
            ValueStateDescriptor<Double> descriptor = 
                new ValueStateDescriptor<>("maxBidPrice", Double.class);
            maxPriceState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Bid bid, Context ctx, Collector<Bid> out) throws Exception {
            Double currentMax = maxPriceState.value();

            // Si c'est la 1ère enchère pour cet objet ou si elle bat le record actuel
            if (currentMax == null || bid.price > currentMax) {
                maxPriceState.update(bid.price);
                out.collect(bid);
            }
        }
    }
}
