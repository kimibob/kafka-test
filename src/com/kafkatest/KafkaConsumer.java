package com.kafkatest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaConsumer {

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		//props.put("auto.offset.reset", "smallest"); 
		props.put("zookeeper.connect","192.168.1.101:2182,192.168.1.102:2182,192.168.1.103:2182");
		props.put("group.id", "zqgroup2");
		props.put("zookeeper.session.timeout.ms", "6000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000");
		//确保rebalance.max.retries * rebalance.backoff.ms > zookeeper.session.timeout.ms
		props.put("rebalance.backoff.ms", "5000");
		props.put("rebalance.max.retries", "10");
		return new ConsumerConfig(props);
	}

	public static void main(String[] args) {
		String topic = "partition_test";
		ConsumerConnector consumer = Consumer
				.createJavaConsumerConnector(createConsumerConfig());
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> mam = it.next();
			System.out.println("consume topic:"+mam.topic()+" => Partition [" + mam.partition()
					+ "] Message: [" + new String(mam.message()) + "] ..");
		}
		System.out.println("stop/");
		if (consumer != null) consumer.shutdown();   //其实执行不到，因为上面的hasNext会block

	}
}
