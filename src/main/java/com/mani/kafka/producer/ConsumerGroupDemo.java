package com.mani.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroupDemo {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Producer.class);
        Properties properties = new Properties();
        String bootstrap = "localhost:9092";
        String groupId= "third-application";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe the consumer to topic(s)
        consumer.subscribe(Collections.singleton("second-topic"));
        //poll for new data

        while(true) {

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord consumerRecord:records){
                logger.info("Key:"+consumerRecord.key()+" Value:"+consumerRecord.value());
                logger.info("Partition:"+consumerRecord.partition()+" Offset:"+consumerRecord.offset());
            }

        }

    }
}
