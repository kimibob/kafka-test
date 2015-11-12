package com.kafkatest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaConsumerGroup {
	private final ConsumerConnector consumer;
	private ExecutorService executor;

	public KafkaConsumerGroup() {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
				createConsumerConfig());
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public void run(int a_numThreads) { 
		// 创建并发的consumers
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("partition_test", new Integer(a_numThreads)); // 描述读取哪个topic，需要几个线程读
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap); // 创建Streams
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("partition_test"); // 每个线程对应于一个KafkaStream

		// now launch all the threads
		executor = Executors.newFixedThreadPool(a_numThreads);

		// now create an object to consume the messages
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new Runnable() {  
                public void run() {  
                    for (MessageAndMetadata<byte[], byte[]>  mam : stream) {  
            			System.out.println(Thread.currentThread()+"||topic:"+mam.topic()+" => Partition [" + mam.partition()
            					+ "] Message: [" + new String(mam.message()) + "] ..");
                    }  
                }  
            });
		}
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		//props.put("auto.offset.reset", "smallest"); 
		props.put("zookeeper.connect","192.168.1.101:2182,192.168.1.102:2182,192.168.1.103:2182");
		props.put("group.id", "zqgroup2");
		props.put("zookeeper.session.timeout.ms", "60000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}

	public static void main(String[] args) {

		KafkaConsumerGroup example = new KafkaConsumerGroup();
		example.run(4);

		try {
			Thread.sleep(10000);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}
		System.out.println("shutdown...");
		//example.shutdown();
	}
}