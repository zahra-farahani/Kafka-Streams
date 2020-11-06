package com.kafka.streams;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class WordCountAppTests {

    //for more info :
    //https://kafka.apache.org/11/documentation/streams/developer-guide/testing.html

    TopologyTestDriver testDriver;

    StringSerializer stringSerializer = new StringSerializer();

    ConsumerRecordFactory<String,String> recordFactory = new ConsumerRecordFactory<>(stringSerializer,stringSerializer);

    @Before
    public void setUpTopologyTestDriver(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"dummy:1234");//whatever you want because it doesn't actually connect to kafka
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        testDriver = new TopologyTestDriver(WordCountApp.createTopology(),config);
    }

    @After
    public void closeTestDriver(){
        testDriver.close();//it's required to cleanup to tests pass
    }

    @Test
    public void makeSureCountsAreCorrect(){
        String firstExample = "testing Kafka streams";
        pushNewInputRecord(firstExample);
        OutputVerifier.compareKeyValue(readOutput(),"testing",1L);
        OutputVerifier.compareKeyValue(readOutput(),"kafka",1L);
        OutputVerifier.compareKeyValue(readOutput(),"streams",1L);
        assertEquals(readOutput(),null);//because there are no more output to read

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);
        OutputVerifier.compareKeyValue(readOutput(),"testing",2L);
        OutputVerifier.compareKeyValue(readOutput(),"kafka",2L);
        OutputVerifier.compareKeyValue(readOutput(),"again",1L);
    }

    @Test
    public void makeSureWordsBecomeLowerCase(){
        String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);
        OutputVerifier.compareKeyValue(readOutput(),"kafka",1L);
        OutputVerifier.compareKeyValue(readOutput(),"kafka",2L);
        OutputVerifier.compareKeyValue(readOutput(),"kafka",3L);
    }

    private void pushNewInputRecord(String value){
        testDriver.pipeInput(recordFactory.create("word-count-input",null,value));
    }

    private ProducerRecord<String,Long> readOutput(){
        return testDriver.readOutput("word-count-output",new StringDeserializer(),new LongDeserializer());
    }
}
