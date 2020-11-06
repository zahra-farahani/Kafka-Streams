package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaStreams streams = new KafkaStreams(createTopology(), config);
        streams.start();

        //8. Printing Topology
        System.out.println(streams.toString());

        //9. Add shutdown hook to correctly close the streams app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Topology createTopology(){

        StreamsBuilder builder = new StreamsBuilder();

        //1. Stream from Kafka
        KStream<String, String> wordCountStream = builder.stream("word-count-input");

        //2. Map values to lowercase
        //wordCountStream.mapValues(String::toLowerCase)
        KTable<String, Long> wordCounts = wordCountStream.mapValues(value -> value.toLowerCase())
                //3. Flatmap values split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                //4. SelectKey to apply a key (we discard the old key)
                .selectKey((ignoredKey, word) -> word)
                //5. GroupBy key before aggregation
                .groupByKey()
                //6. count occurrences
                .count(Materialized.as("Counts"));

        //7. in order to write the results back to kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(),Serdes.Long()));

        return builder.build();

    }
}
