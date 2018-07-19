package com.cg.app;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;


public class TwitterSparkAnalyticsApplication {

	public static JavaSparkContext sc;
	
	public static void main(String[] args) throws IOException, InterruptedException{
		
	    final String consumerKey = "gPMRdqRwLmKzxsHkELhzuZwZR";
        final String consumerSecret = "lprFTP5Bn1iiLqcRl4YLWHyHh9vnByZMAf5NMzX0UNR7gGh4KA";
        final String accessToken = "535167288-kihKYAIj6NQoJsm3EoGGRdwj6rGvzCw7rCeR2qS0";
        final String accessTokenSecret = "ARb3fjnJWq2ggEAwyPMiXnmOk0DKhOjVRlqhNlzVvqjP2";
	 
	        SparkConf sparkConf = new SparkConf().setAppName("TwitterSparkStreamingHashtagAnalytics").setMaster("local[2]");
	 
	        JavaStreamingContext jssc = new JavaStreamingContext( sparkConf, Durations.seconds(10)); // Every 10 sec the data will be processed

	        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
	        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
	        System.setProperty("twitter4j.oauth.accessToken", accessToken);
	        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
	        

	        String[] filters = {"happy"};

	        JavaReceiverInputDStream<Status> twitterStream  = TwitterUtils.createStream(jssc, filters);
 
	        twitterStream.filter(t->t.getGeoLocation()!=null)
            .map(h->h.getText().toLowerCase())
            .filter(h->!h.equals("happy"))
            .countByValue()
            .print();
	 
	        jssc.start();
	        jssc.awaitTermination();
	    }
	}
