package com.kafkatest;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class PartitionerProducer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.1.104:9092,192.168.1.102:9092,192.168.1.103:9092");
		props.put("partitioner.class", "com.kafkatest.SimplePartitioner");
		props.put("request.required.acks", "1");
		Producer<String, String> producer = 
				new Producer<String, String>(new ProducerConfig(props));
	    String topic = "partition_test";
	    for(int i=0; i<12; i++) {
	    	String k = i+"";
	    	String v = "key:"+k+"--value:" + new Date();
	    	producer.send(new KeyedMessage<String, String>(topic,k,v));
	    }
	    producer.close();
	}
}

