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
		props.put("group.id", "zqgroup1");
		props.put("zookeeper.session.timeout.ms", "60000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000");

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
		if (consumer != null) consumer.shutdown();   //其实执行不到，因为上面的hasNext会block

	}
}
