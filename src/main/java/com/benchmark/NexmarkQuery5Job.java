package com.benchmark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class NexmarkQuery5Job {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint toutes les secondes pour tester la surcharge de l'état étendu
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // Définition de la stratégie de Watermark basée sur le timestamp des événements
        WatermarkStrategy<Bid> watermarkStrategy = WatermarkStrategy
                .<Bid>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((bid, recordTimestamp) -> bid.timestamp);

        DataStream<Bid> bids = env.addSource(new BidSource())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // ÉTAPE 1 : Agrégation par fenêtre glissante (60 min de taille, glissement 1 min)
        // (Pour un test synthétique court, vous pouvez réduire à Time.seconds(10) et Time.seconds(1))
        DataStream<AuctionCount> windowCounts = bids
                .keyBy(bid -> bid.auction)
                .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(1)))
                .aggregate(new CountAggregator(), new WindowResultFunction());

        // ÉTAPE 2 : Trouver le(s) objet(s) ayant le maximum d'enchères par fenêtre
        DataStream<AuctionCount> hotItems = windowCounts
                .keyBy(ac -> ac.windowEnd)
                .process(new TopAuctionProcessFunction());

        // Instrumentation des métriques et sortie
        hotItems.map(new BenchmarkMetricsListener<>("QUERY_5_HOT_ITEMS"))
                .sinkTo(new DiscardingSink<>());

        env.execute("Nexmark Query 5 - Hot Items");
    }

    // --- Classes DTO ---

    public static class AuctionCount implements Serializable, Timestamped {
        public long auction;
        public long count;
        public long windowEnd;

        public AuctionCount() {}

        @Override public long getTimestamp() { return this.windowEnd; }
        
        public AuctionCount(long auction, long count, long windowEnd) {
            this.auction = auction;
            this.count = count;
            this.windowEnd = windowEnd;
        }
    }

    // --- Opérateurs de Fenêtrage (Étape 1) ---

    public static class CountAggregator implements AggregateFunction<Bid, Long, Long> {
        @Override
        public Long createAccumulator() { return 0L; }

        @Override
        public Long add(Bid value, Long accumulator) { return accumulator + 1L; }

        @Override
        public Long getResult(Long accumulator) { return accumulator; }

        @Override
        public Long merge(Long a, Long b) { return a + b; }
    }

    public static class WindowResultFunction 
            extends ProcessWindowFunction<Long, AuctionCount, Long, TimeWindow> {
        @Override
        public void process(Long auctionId, Context context, Iterable<Long> elements, Collector<AuctionCount> out) {
            long count = elements.iterator().next();
            out.collect(new AuctionCount(auctionId, count, context.window().getEnd()));
        }
    }

    // --- Recherche du Maximum par Fenêtre (Étape 2) ---

    public static class TopAuctionProcessFunction 
            extends KeyedProcessFunction<Long, AuctionCount, AuctionCount> {

        private transient ListState<AuctionCount> windowCountsState;

        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<AuctionCount> descriptor = 
                new ListStateDescriptor<>("windowCounts", Types.POJO(AuctionCount.class));
            windowCountsState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(AuctionCount value, Context ctx, Collector<AuctionCount> out) throws Exception {
            windowCountsState.add(value);
            // On enregistre un timer qui se déclenchera dès la clôture de la fenêtre
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AuctionCount> out) throws Exception {
            long maxCount = 0L;
            List<AuctionCount> allCounts = new ArrayList<>();

            for (AuctionCount ac : windowCountsState.get()) {
                allCounts.add(ac);
                if (ac.count > maxCount) {
                    maxCount = ac.count;
                }
            }

            // Émettre les objets qui ont num >= ALL(...)
            for (AuctionCount ac : allCounts) {
                if (ac.count == maxCount) {
                    out.collect(ac);
                }
            }

            // Nettoyage de l'état pour libérer le Heap JVM
            windowCountsState.clear();
        }
    }
}