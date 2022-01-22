package com.mani.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {
    public static void main(String[] args) throws InterruptedException {
        //Logger
        final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);


        //creating property
        String localhost="localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,localhost);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        final KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
            String topicName = "second-topic";
            String msg = "Message "+i+" produced from application";
            String key = "ID_"+Integer.toString(i);
            logger.info("Key:"+key);
            //ID_0->Partition 0
            //ID-1->Partition 1
            //Id-2->Partition 2
            //ID-3->Partition 0
            //ID-4->Partition 0
            //ID-5->Partition 2
            //ID-6->Partition 1
            //Id-7->Partition 2
            //ID-8->Partition 1
            //ID-9->Partition 2

            ProducerRecord record = new ProducerRecord(topicName, key,msg);

            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic:" + recordMetadata.topic());
                        logger.info("Partition:" + recordMetadata.partition());
                        logger.info("Timestamp:" + recordMetadata.timestamp());
                        logger.info("Offset:" + recordMetadata.offset());
                   /* System.out.println("Topic:"+recordMetadata.topic());
                    System.out.println("Partition:"+recordMetadata.partition());
                    System.out.println("Timestamp:"+recordMetadata.timestamp());
                    System.out.println("Offset:"+recordMetadata.offset());*/
                    } else {
                        logger.info("Error while producing:" + e);
                    }


                }
            });
            Thread.sleep(1000);
        }
        kafkaProducer.flush();
        kafkaProducer.close();


        //creating producer



    }

}
