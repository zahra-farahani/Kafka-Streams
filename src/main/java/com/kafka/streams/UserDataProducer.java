package com.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UserDataProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

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
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //FYI: we do .get() to ensure the writes to the topics are sequential,for the sake of the teaching exercise
        //do'nt do this in production or in any producer. blocking a future is bad!

        //1- we create a new user, then we send some data to kafka
        System.out.println("\nExample 1 - new User\n");
        producer.send(userRecord("john","First=John,Last=Doe,Email=john.doe@gmail.com")).get();
        producer.send(purchaseRecord("john","Apples and Bananas (1)")).get();

        Thread.sleep(10000);

        //2- we receive user purchase, but it doesn't exist in kafka
        System.out.println("\nExample 2 - non existing user\n");
        producer.send(purchaseRecord("bob","Kafka udemy course (2)")).get();

        Thread.sleep(10000);

        //3- we update user 'john' and send a new transaction
        System.out.println("\nExample 3 - update User\n");
        producer.send(userRecord("john","First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
        producer.send(purchaseRecord("john","Oranges (3)")).get();

        Thread.sleep(10000);

        //4- we send a user purchase for stephan but it exists in kafka later
        System.out.println("\nExample 4 - not existing user then user\n");
        producer.send(purchaseRecord("stephan","Computer (4)")).get();
        producer.send(userRecord("stephan","First=Stephan,Last=Maarek,Github=simplesteph")).get();
        producer.send(purchaseRecord("stephan","Books (4)")).get();
        producer.send(userRecord("stephan",null)).get(); // delete for cleanup

        Thread.sleep(10000);

        //5 - we create a user, but it gets deleted before any purchase comes through
        System.out.println("\nExample 5 - user then delete then data\n");
        producer.send(userRecord("alice","First=Alice")).get();
        producer.send(userRecord("alice",null)).get(); // delete
        producer.send(purchaseRecord("alice","Apache Kafka Series (5)")).get();

        Thread.sleep(10000);

        System.out.println("Ebd of Demo");
        producer.close();
    }

    private static ProducerRecord<String, String> purchaseRecord(String key, String value) {
        return new ProducerRecord<>("user-purchase",key,value);
    }

    private static ProducerRecord<String, String> userRecord(String key, String value) {
        return new ProducerRecord<>("user-table",key,value);
    }
}
