package com.mani.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.core.event.Event;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    public  TwitterProducer(){}
    public static void main(String[] args) {
        //create a twitter client
        new TwitterProducer().run();
    }

    String consumerKey="CCd3ubUgMhHX5X6jCdrBx5L89";
    String consumerSecret ="M9h8eEQxjVTRytduM6zor3DlYKGxn0OfPJRh3D5NmmoSKT6Pa2";
    String token="723851715645214721-M08Q6A1Et3eshy8zl2NktFJ1O1O5fek";
    String secret="IlYk0cOITyc1zdl3tp3pMLHtt5VbQ7jrNfxNq60ZbpNQ1";
    String bearerToken = "AAAAAAAAAAAAAAAAAAAAAKaZYQEAAAAAIKqOBASptvma1slvgfTaYs3Pd7U%3DLG2KknOKz6bTIWVkw5u9biOwnlfCAze0ppcBE8RSCuugkoBC23";

    public void run(){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client hosebirdClient = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        hosebirdClient.connect();
        //Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("stopping application");
            logger.info("shutting down client from twitter");
            hosebirdClient.stop();
            logger.info("closing producer");
            kafkaProducer().close();
            logger.info("done");
        }));
        KafkaProducer<String,String> kafkaProducer = kafkaProducer();
        while(!hosebirdClient.isDone()){
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();

            }
            if(msg!=null){
                kafkaProducer.send(new ProducerRecord<>("twitter-topic", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            logger.error("Something bad happened!!!",e);
                        }
                    }
                });
                logger.info(msg);
            }
        }
        logger.info("End of Reading!!!");



    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("kafka");
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String,String> kafkaProducer(){
        String localhost="localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,localhost);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        return  kafkaProducer;
    }
}
