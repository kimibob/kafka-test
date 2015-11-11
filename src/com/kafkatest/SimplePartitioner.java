package com.kafkatest;

public class SimplePartitioner {

	public int partition(Object key, int numPartitions) {

		return (Integer)key % numPartitions;

	}
}
