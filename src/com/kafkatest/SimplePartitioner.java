package com.kafkatest;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {

	public SimplePartitioner(VerifiableProperties props) {

	}

	@Override
	public int partition(Object key, int numPartitions) {
		int hashCode;
		if (key == null) {
			hashCode = new Random().nextInt(255);
		} else {
			//HashCode可以为负数，这样操作后可以保证它为一个正整数.然后以分区的长度取模，得到该对象的分区索引
			hashCode = key.hashCode() & Integer.MAX_VALUE;
		}
		if (numPartitions <= 0) {
			return 0;
		}
		return hashCode % numPartitions;
	}
}
