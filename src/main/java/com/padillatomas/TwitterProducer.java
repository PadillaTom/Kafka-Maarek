package com.padillatomas;

import com.google.common.collect.Lists;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsOAuth2;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Get2TweetsIdResponse;
import com.twitter.clientlib.model.ResourceUnauthorizedProblem;
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    final String clientId = "dU1SU1V0QklIcG5aRUFFOS1lT3M6MTpjaQ";
    final String clientSecret = "S3cQwaoiA6x9Kkdg8TB5nTpjY4vmnwrJVDKEpl5ChwHSdi_skQ";
    final String accessToken = "AAAAAAAAAAAAAAAAAAAAAKeKgAEAAAAAyAmAhPLK9ky60atiUp0kizq66%2BI%3DifxcUvpffiBJ2WGWnqvi5MniQRAUoFFWiVS4PN4PLL9Z8VH2FX";
    final String tokenSecret = "orQGH0p0o8DCUtmzR8iUHXvVLHBjMWUgWWIXWYOoTBqe1";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("START of my App");

        /*
         * ===================
         * TwitterHBC
         * ===================
         * */
////        Create Message Queue:
//        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
//
////        Create Twitter Client:
//        Client myClient = createTwitterClient(msgQueue);
//        myClient.connect();
//
////        Kafka Producer:
//
////        Loop to send Tweets to Kafka:
//        while (!myClient.isDone()) {
//            String myMessage = null;
//            try {
//                myMessage = msgQueue.poll(5, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//                myClient.stop();
//            }
//            if (myMessage != null) {
//                logger.info(myMessage);
////                something(msg);
////                profit();
//            }
//        }

        /*
         * ===================
         * Twitter API
         * ===================
         * */
        createTwitterAPIClient();

        logger.info("END of my App");
    }

    /*
     * ===================
     * TwitterHBC
     * ===================
     * */
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
// === Connection Information ===
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(
                clientId,
                clientSecret,
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

    /*
     * ===================
     * Twitter API
     * ===================
     * */
    private void createTwitterAPIClient() {

//        Credentials:
        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsOAuth2(
                clientId,
                clientSecret,
                accessToken,
                tokenSecret
        ));

//        Twitter Fields:
        Set<String> tweetFields = new HashSet<>();
        tweetFields.add("author_id");
        tweetFields.add("id");
        tweetFields.add("created_at");

        try {
            // findTweetById
            Get2TweetsIdResponse result = apiInstance.tweets().findTweetById("20")
                    .tweetFields(tweetFields)
                    .execute();
            if (result.getErrors() != null && result.getErrors().size() > 0) {
                System.out.println("Error:");
                result.getErrors().forEach(e -> {
                    System.out.println(e.toString());
                    if (e instanceof ResourceUnauthorizedProblem) {
                        System.out.println(((ResourceUnauthorizedProblem) e).getTitle() + " " + ((ResourceUnauthorizedProblem) e).getDetail());
                    }
                });
            } else {
                System.out.println("findTweetById - Tweet Text: " + result.toString());
            }
        } catch (ApiException e) {
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }

    }

}
