package com.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionProducer {

    public static void main(String[] args) {

        //Note : we'll use exactly-once for this practice

        // Step1 : Create Producer Properties
        // --> you can get a list of config options in :
        // https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //because kafka client converts the messages to bytes and send to kafka we have to set these two props to
        // know how to serialize(convert to byte) them and in this case both are String so we use StringSerializer
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");//strong producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,"3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"1");//just in dev, not in prod

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");//ensure we don't push duplicate

        // Step2 : Create the Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // Step3 : Send Data - asynchronous
        int i = 0;
        while (true){
            i++;
            System.out.println("Producing batch " + i);
            try{
                kafkaProducer.send(newRandomTransaction("john"));
                Thread.sleep(100);
                kafkaProducer.send(newRandomTransaction("stephan"));
                Thread.sleep(100);
                kafkaProducer.send(newRandomTransaction("alice"));
                Thread.sleep(100);
            }catch (InterruptedException e){
                break;
            }
        }

        //flush and close producer
        kafkaProducer.close();

    }

    public static ProducerRecord<String, String> newRandomTransaction(String name) {
        //create an empty json {}
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        //{"amount": randomNumber}
        Integer amount = ThreadLocalRandom.current().nextInt(0,100);
        //get current time
        Instant now = Instant.now();
        //write date to JSON document
        transaction.put("name",name);
        transaction.put("amount",amount);
        transaction.put("time",now.toString());
        return new ProducerRecord<>("bank-transactions",name,transaction.toString());
    }
}
