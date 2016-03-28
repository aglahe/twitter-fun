package com.anavationllc.twitter;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Created by aarong on 3/22/16.
 */
public class TwitterConsumer
{
    private KafkaConsumer consumer;

    public TwitterConsumer()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.99.100");
        props.put("group.id", "aaron-tester");
        props.put("key.deseralizer", "StringDeserializer.class.getName()");
        props.put("value.deseralizer", "StringDeserializer.class.getName()");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

    }
}
