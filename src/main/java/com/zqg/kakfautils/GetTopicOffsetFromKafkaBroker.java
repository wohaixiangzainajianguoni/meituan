package com.zqg.kakfautils;

import com.google.common.collect.ImmutableMap;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 测试之前需要启动kafka
 * @author root
 *
 */
public class GetTopicOffsetFromKafkaBroker {
	public static void main(String[] args) {
		
		Map<TopicAndPartition, Long> topicOffsets = getTopicOffsets("master:9092", "qqq");
		Set<Entry<TopicAndPartition, Long>> entrySet = topicOffsets.entrySet();
		for(Entry<TopicAndPartition, Long> entry : entrySet) {
			TopicAndPartition topicAndPartition = entry.getKey();
			Long offset = entry.getValue();
			String topic = topicAndPartition.topic();
			int partition = topicAndPartition.partition();
			System.out.println("topic = "+topic+",partition = "+partition+",offset = "+offset);
		}
	
	}
	
	/**
	 * 从kafka集群中得到当前topic，生产者在每个分区中生产消息的偏移量位置
	 * @param KafkaBrokerServer
	 * @param topic
	 * @return
	 */
	public static Map<TopicAndPartition,Long> getTopicOffsets(String KafkaBrokerServer, String topic){
		Map<TopicAndPartition,Long> retVals = new HashMap<TopicAndPartition,Long>();
		
		for(String broker:KafkaBrokerServer.split(",")){
			
			SimpleConsumer simpleConsumer = new SimpleConsumer(broker.split(":")[0],Integer.valueOf(broker.split(":")[1]), 64*10000,1024,"consumer");
			TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
			TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);
			
			for (TopicMetadata metadata : topicMetadataResponse.topicsMetadata()) {
				for (PartitionMetadata part : metadata.partitionsMetadata()) {
					Broker leader = part.leader();
					if (leader != null) { 
						TopicAndPartition topicAndPartition = new TopicAndPartition(topic, part.partitionId());
						
						PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 10000);
						OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());
						OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);
						
						if (!offsetResponse.hasError()) { 
							long[] offsets = offsetResponse.offsets(topic, part.partitionId()); 
							retVals.put(topicAndPartition, offsets[0]);
						}
					}
				}
			}
			simpleConsumer.close();
		}
		return retVals;
	}
}
