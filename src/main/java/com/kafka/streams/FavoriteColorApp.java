package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in transformation - not in Production
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> textLines = builder.stream("favorite-color-input");//all the keys are null here
        KStream<String, String> usersAndColors = textLines.filter((k, v) -> v.contains(","))
                                                    .selectKey((k, v) -> v.split(",")[0].toLowerCase())
                                                    .mapValues(value -> value.split(",")[1].toLowerCase())
                                                    .filter((user, color) -> Arrays.asList("red","blue","green").contains(color));

        usersAndColors.to("user-keys-and-colors");

        KTable<String, String> usersAndColorsTable = builder.table("user-keys-and-colors");

        KTable<String, Long> favoriteColors = usersAndColorsTable.groupBy((user,color)->new KeyValue<>(color,color)).count("CountsByColors");

        favoriteColors.to(Serdes.String(),Serdes.Long(),"favorite-colors-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();//only do this in dev - not prod
        streams.start();

        //8. Printing Topology
        System.out.println(streams.toString());

        //9. Add shutdown hook to correctly close the streams app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
