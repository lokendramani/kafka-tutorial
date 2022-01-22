package com.mani.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        //Logger
        final Logger logger = LoggerFactory.getLogger(Producer.class);


        //creating property
        String localhost="localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,localhost);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        final KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        String topicName="first_topic";
        String msg = "This message produced from application2";
        ProducerRecord record = new ProducerRecord(topicName,msg);

        kafkaProducer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    logger.info("Topic:"+recordMetadata.topic());
                    logger.info("Partition:"+recordMetadata.partition());
                    logger.info("Timestamp:"+recordMetadata.timestamp());
                    logger.info("Offset:"+recordMetadata.offset());
                   /* System.out.println("Topic:"+recordMetadata.topic());
                    System.out.println("Partition:"+recordMetadata.partition());
                    System.out.println("Timestamp:"+recordMetadata.timestamp());
                    System.out.println("Offset:"+recordMetadata.offset());*/
                }else{
                    logger.info("Error while producing:"+e);
                }


            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();



        //creating producer



    }

}
