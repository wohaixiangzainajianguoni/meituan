package com.zqg.kakfautils;

import kafka.common.TopicAndPartition;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Map;

public class UseZookeeperManageOffset {
	/**
	 * 使用log4j打印日志，“UseZookeeper.class” 设置日志的产生类
	 */
	static final Logger logger = Logger.getLogger(UseZookeeperManageOffset.class);
	
	
	public static void main(String[] args) {
		/**
		 * 加载log4j的配置文件，方便打印日志
		 */
		ProjectUtil.LoadLogConfig();
		logger.info("project is starting...");
		
		/**
		 * 从kafka集群中得到topic每个分区中生产消息的最大偏移量位置
		 */
		Map<TopicAndPartition, Long> topicOffsets = GetTopicOffsetFromKafkaBroker.getTopicOffsets("master:9092", "qqq");
		
		/**
		 * 从zookeeper中获取当前topic每个分区 consumer 消费的offset位置
		 */
		Map<TopicAndPartition, Long> consumerOffsets =
				GetTopicOffsetFromZookeeper.getConsumerOffsets("master:2181","group2","qqq");
		
		/**
		 * 合并以上得到的两个offset ，
		 * 	思路是：
		 * 		如果zookeeper中读取到consumer的消费者偏移量，那么就zookeeper中当前的offset为准。
		 * 		否则，如果在zookeeper中读取不到当前消费者组消费当前topic的offset，就是当前消费者组第一次消费当前的topic，
		 * 			offset设置为topic中消息的最大位置。
		 */
		if(null!=consumerOffsets && consumerOffsets.size()>0){
            topicOffsets.putAll(consumerOffsets);
        }
		/**
		 * 如果将下面的代码解开，是将topicOffset 中当前topic对应的每个partition中消费的消息设置为0，就是从头开始。
		 */
//		for(Map.Entry<TopicAndPartition, Long> item:topicOffsets.entrySet()){
//          item.setValue(0l);
//		}
		
		/**
		 * 构建SparkStreaming程序，从当前的offset消费消息
		 */
		JavaStreamingContext jsc = SparkStreamingDirect.getStreamingContext(topicOffsets,"group2");


		jsc.sparkContext().setLogLevel("warn");
		jsc.start();
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jsc.close();
		
	}
}
