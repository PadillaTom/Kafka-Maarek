package com.padillatomas;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    final String consumerKey = "KXJicYOiuJuiBG5VcNtEmvNeh";
    final String consumerSecret = "k3ce2HoMvffu8eMnInmMQ19nCN51qcFdvXsY3c67tDMAjwkjjR";
    final String accessToken = "1560548237983064064-VbGR12DfW56zCualOaXaCsFubeBtct";
    final String tokenSecret = "orQGH0p0o8DCUtmzR8iUHXvVLHBjMWUgWWIXWYOoTBqe1";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("START of my App");

//        Create Message Queue:
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

//        Create Twitter Client:
        Client myClient = createTwitterClient(msgQueue);
        myClient.connect();

//        Kafka Producer:

//        Loop to send Tweets to Kafka:
        while (!myClient.isDone()) {
            String myMessage = null;
            try {
                myMessage = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                myClient.stop();
            }
            if (myMessage != null) {
                logger.info(myMessage);
//                something(msg);
//                profit();
            }

        }
        logger.info("END of my App");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
// === Connection Information ===
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(
                consumerKey,
                consumerSecret,
                accessToken,
                tokenSecret);

// === Creating a Client ===
        return new ClientBuilder()
                .name("Hosebird-Client-01")  // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .build();
    }

}
