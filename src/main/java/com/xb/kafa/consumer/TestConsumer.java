package com.xb.kafa.consumer;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class TestConsumer extends Thread
{
	private final ConsumerConnector consumer;
	private final String topic;

	public static void main(String[] args)
	{
		TestConsumer consumerThread = new TestConsumer("flume-kafka-storm-001");
		consumerThread.start();
	}

	public TestConsumer(String topic)
	{
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig()
	{
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.95.129:2181,192.168.95.130:2181,192.168.95.131:2181");
		props.put("group.id", "0");
		props.put("zookeeper.session.timeout.ms", "10000");
		return new ConsumerConfig(props);
	}

	public void run()
	{
		Map<String, Integer> topickMap = new HashMap<String, Integer>();
		topickMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topickMap);
		KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		System.out.println("*********Results********");
		while (true)
		{
			if (it.hasNext())
			{
				try
				{
					System.err.println("get data:" + new String(it.next().message(), "utf-8"));
				}
				catch (UnsupportedEncodingException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
//			try
//			{
//				Thread.sleep(1000);
//			}
//			catch (InterruptedException e)
//			{
//				e.printStackTrace();
//			}
		}
	}
}