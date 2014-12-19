package com.xb.flume.kafka.partition;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;


public class HashSimplePartitioner implements Partitioner
{
	public HashSimplePartitioner(VerifiableProperties props)
	{
	}

	@Override
	public int partition(Object obj, int a_numPartitions)
	{
		int partition = obj.hashCode() % a_numPartitions;
		return partition;
	}
}
