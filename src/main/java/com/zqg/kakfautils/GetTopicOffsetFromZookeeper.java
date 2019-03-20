package com.zqg.kakfautils;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.common.TopicAndPartition;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class GetTopicOffsetFromZookeeper {
	
	public static Map<TopicAndPartition,Long> getConsumerOffsets(String zkServers, String groupID, String topic) {
		Map<TopicAndPartition,Long> retVals = new HashMap<TopicAndPartition,Long>();
		
		ObjectMapper objectMapper = new ObjectMapper();
		CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
				.connectString(zkServers).connectionTimeoutMs(1000)
				.sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
		
		curatorFramework.start();
		
		try{
			String nodePath = "/consumers/"+groupID+"/offsets/" + topic;
			if(curatorFramework.checkExists().forPath(nodePath)!=null){
				List<String> partitions=curatorFramework.getChildren().forPath(nodePath);
				for(String partiton:partitions){
					int partitionL=Integer.valueOf(partiton);
					Long offset=objectMapper.readValue(curatorFramework.getData().forPath(nodePath+"/"+partiton),Long.class);
					TopicAndPartition topicAndPartition=new TopicAndPartition(topic,partitionL);
					retVals.put(topicAndPartition, offset);
				}
			}
			else {

			}

		}catch(Exception e){
			e.printStackTrace();
		}
		curatorFramework.close();
		
		return retVals;
	} 
	
	
	public static void main(String[] args) {
		Map<TopicAndPartition, Long> consumerOffsets = getConsumerOffsets("master:2181","group2","qqq");
		Set<Entry<TopicAndPartition, Long>> entrySet = consumerOffsets.entrySet();
		for(Entry<TopicAndPartition, Long> entry : entrySet) {
			TopicAndPartition topicAndPartition = entry.getKey();
			String topic = topicAndPartition.topic();
			int partition = topicAndPartition.partition();
			Long offset = entry.getValue();
			System.out.println("topic = "+topic+",partition = "+partition+",offset = "+offset);
		}
	}
}
