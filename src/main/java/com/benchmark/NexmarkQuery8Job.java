package com.benchmark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;

public class NexmarkQuery8Job {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpointing régulier pour mesurer le coût de sauvegarde d'un double état
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);

        // Configuration des watermarks pour les deux flux
        WatermarkStrategy<Person> personWatermark = WatermarkStrategy
                .<Person>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((person, ts) -> person.timestamp);

        WatermarkStrategy<Auction> auctionWatermark = WatermarkStrategy
                .<Auction>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((auction, ts) -> auction.timestamp);

        // Sources des flux (remplacer par vos générateurs réels ou sources Kafka)
        DataStream<Person> persons = env.addSource(new PersonSource())
                .assignTimestampsAndWatermarks(personWatermark);

        DataStream<Auction> auctions = env.addSource(new AuctionSource())
                .assignTimestampsAndWatermarks(auctionWatermark);

        // Interval Join : A.seller = P.id et A.timestamp dans [P.timestamp, P.timestamp + 12 HOURS]
        // (Pour un test synthétique rapide sur machine locale, réduisez à Time.minutes(1) ou Time.seconds(30))
        DataStream<NewUserAuctionResult> joinedStream = persons.keyBy(p -> p.id)
                .intervalJoin(auctions.keyBy(a -> a.seller))
                .between(Time.milliseconds(0), Time.hours(12))
                .process(new ProcessJoinFunction<Person, Auction, NewUserAuctionResult>() {
                    @Override
                    public void processElement(
                            Person person,
                            Auction auction,
                            Context ctx,
                            Collector<NewUserAuctionResult> out) {
                        out.collect(new NewUserAuctionResult(
                                person.id,
                                person.name,
                                auction.reserve,
                                auction.timestamp
                        ));
                    }
                });

        // Mesure des performances et consommation de la sortie
        joinedStream.map(new BenchmarkMetricsListener<>("QUERY_8_NEW_USERS"))
                .sinkTo(new DiscardingSink<>());

        env.execute("Nexmark Query 8 - Interval Join");
    }

    // Structure de résultat
    public static class NewUserAuctionResult implements Serializable, Timestamped{
        public long personId;
        public String personName;
        public double reservePrice;
        public long timestamp;

        public NewUserAuctionResult() {}

        @Override public long getTimestamp() { return this.timestamp; }

        public NewUserAuctionResult(long personId, String personName, double reservePrice, long timestamp) {
            this.personId = personId;
            this.personName = personName;
            this.reservePrice = reservePrice;
            this.timestamp = timestamp;
        }
    }
}