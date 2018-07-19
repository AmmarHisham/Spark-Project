package com.cg.app;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;


import com.cg.app.model.User;
import com.cg.app.util.TransactionDecoder;

import kafka.serializer.StringDecoder;


public class KafkaSparkAnalyticsApplication {

	public static JavaSparkContext sc;
	
	public static void main(String[] args) throws IOException, InterruptedException{
		
		 	String brokers = "localhost:9092";
	        String topics = "tweets-topic";
	 
	        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingAnalytics").setMaster("local[2]");
	 
	        JavaStreamingContext jssc = new JavaStreamingContext( sparkConf, Durations.seconds(10)); // Every 10 sec the data will be processed
	 	        
	        Set<String> topicsSet = Collections.singleton(topics);
	        
	        HashMap<String, String> kafkaParams = new HashMap<>(); 
	        kafkaParams.put("metadata.broker.list", brokers);
	        kafkaParams.put("auto.offset.reset", "smallest");
	 
	        // Dstream (Discretized Stream)  &  Streams of RDD = DStream
	        
	     /*   Input DStream 
	        1.Basic Source -> File system,Socket Connection
	        2. Advance Source -> Kafka,Flume,Kinesis
	        		
	     */
	        JavaPairInputDStream<String, User> directKafkaStreams = KafkaUtils.createDirectStream( jssc, String.class, User.class, StringDecoder.class,   
	        		TransactionDecoder.class, kafkaParams, topicsSet );

	             
	        directKafkaStreams.foreachRDD( rdd ->{
	        	
	        	System.out.println("---New RDD with "+rdd.partitions().size()+" partions and "+rdd.count() +" records" );
	        	
	        	rdd.foreach( records -> {
	        		System.out.println(" Got the Records "+records._2);
	        		
	        	}
	        	);
	        });
	 
	        jssc.start();
	        jssc.awaitTermination();
	    }
	}
