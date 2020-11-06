package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class UserEvenEnricherApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        //we get a global table out of kafka.this table will be replicated on each kafka streams app
        //the key of our globalKTable is the User ID
        GlobalKTable<String, String> userGlobalKTable = builder.globalTable("user-table");

        //we get a stream of user purchases
        KStream<String, String> userPurchases = builder.stream("user-purchase");

        //we want to enrich that stream
        KStream<String, String> userPurchasesEnrichedJoin = userPurchases.join(userGlobalKTable,
                (key, value) -> key,//map from the (key,value) of this stream to the key of GlobalKTable
                (userPurchase, userInfo) -> "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]");

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        //we want to enrich that stream using a Left join
        KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchases.leftJoin(userGlobalKTable,
                (key, value) -> key,//map from the (key,value) of this stream to the key of GlobalKTable
                (userPurchase, userInfo) -> {
                                if(userInfo != null){
                                    return "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]";
                                }else {
                                    return "Purchase=" + userPurchase + ",UserInfo=null";
                                }});

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();//only do this in dev - not prod
        streams.start();

        //8. Printing Topology
        System.out.println(streams.toString());

        //9. Add shutdown hook to correctly close the streams app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
