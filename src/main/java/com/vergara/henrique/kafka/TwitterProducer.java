package com.vergara.henrique.kafka;


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
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.primefaces.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    String consumerSecret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    String token = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
    String secret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

    public TwitterProducer(){}

    public static void main(String[] args){
        new TwitterProducer().run();
    }


    public void run(){

        logger.info("Novo tweet!");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        Client client = createTwitterClient(msgQueue);

        client.connect();


        KafkaProducer<String,String> producer = KafkaProducerFactory.getProducer();

        while (!client.isDone()) {

            String msg = null;

            try {
                msg = msgQueue.poll(5L, TimeUnit.SECONDS);
            }catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }

            if (msg != null){

                String tweet = msg;
                JSONObject tweetJson = new JSONObject(tweet);

                logger.info("Usuário: " + tweetJson.getJSONObject("user").getString("name") + "tweetou " + tweetJson.getString("text"));

                producer.send(new ProducerRecord<String, String>("twitter_tweets", null,"Usuário: " + tweetJson.getJSONObject("user").getString("name") + "tweetou " + tweetJson.getString("text")), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null){
                            logger.error("Algo de ruim aconteceu =(");
                        }
                    }
                });
            }
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("Pokémon");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                        // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

}
