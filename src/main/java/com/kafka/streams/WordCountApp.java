package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

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

        KStreamBuilder builder = new KStreamBuilder();

        //1. Stream from Kafka
        KStream<String, String> wordCountStream = builder.stream("word-count-input");

        //2. Map values to lowercase
        //wordCountStream.mapValues(value -> value.toLowerCase())
        KTable<String, Long> wordCounts = wordCountStream.mapValues(String::toLowerCase)
                //3. Flatmap values split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                //4. SelectKey to apply a key (we discard the old key)
                .selectKey((ignoredKey, word) -> word)
                //5. GroupBy key before aggregation
                .groupByKey()
                //6. count occurrences
                .count("Counts");

        //7. in order to write the results back to kafka
        wordCounts.to(Serdes.String(),Serdes.Long(),"word-count-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        //8. Printing Topology
        System.out.println(streams.toString());

        //9. Add shutdown hook to correctly close the streams app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
