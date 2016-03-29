package com.anavationllc.twitter;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

public class TwitterConsumer
{
    private static final Logger log = Logger.getLogger(TwitterConsumer.class);

    private boolean done = false;
    private KafkaConsumer<String, JsonNode> kafkaConsumer;

    public TwitterConsumer()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.99.100:9092");
        props.put("group.id", "aaron-tester");
        props.put("client.id", "intellij");
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");

        this.kafkaConsumer = new KafkaConsumer(props);
    }

    public KafkaConsumer getKafkaConsuner() {
        return this.kafkaConsumer;
    }

    public void process() {
        try{
            while (!done) {
                ConsumerRecords<String, JsonNode> records = this.kafkaConsumer.poll(1000);
                log.info("# of records:" + records.count());
                for (ConsumerRecord<String, JsonNode> record : records) {
                    log.info("\t" + record.toString());
                    log.info("\t" + record.value());
                }
            }
        } catch (WakeupException e) {
            log.warn(e);
        } finally {
            kafkaConsumer.close();
            log.info("Closed Kafka Consumer");
        }
    }

    private void init()
    {
        log.info("Subscribing...");
        this.kafkaConsumer.subscribe(Collections.singletonList("twitter"));
        log.info("done.");
    }

    public static void main(String[] args) {
        final TwitterConsumer twitterConsumer = new TwitterConsumer();

        // Add Shutdown hook
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Exiting..");
                twitterConsumer.getKafkaConsuner().wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        twitterConsumer.init();
        twitterConsumer.process();
    }
}
