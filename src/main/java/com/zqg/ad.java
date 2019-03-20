package com.zqg;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.zqg.kakfautils.GetTopicOffsetFromKafkaBroker;
import com.zqg.kakfautils.GetTopicOffsetFromZookeeper;
import com.zqg.kakfautils.Log;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import net.sf.json.JSONObject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 *  实施流量统计
 */
public class ad {

    static  final  String zkList="192.168.178.133:2181";
    static  final  String  brokerList="192.168.178.133:9092";
    static  final  String topic="zzz";
    static  final  String groupId="group2";
    public static void main(String[] args) throws InterruptedException {
        /**
         * 业务要求：黑名单的定义：用户在【一天】内对某个广告点击的次数 超过100次。
         *                        那么这个用户就是黑名单用户。
         * 思路梳理：
         * 1）第一步： 首先从kafka去获取数据
         * 2）第二步：根据黑名单过滤数据
         * 3）第三步：生成黑名单         无所谓
         * 4）第四步：实时统计每天各省各城市的广告点击量
         * 5）第五步：实时各区域的热门广告
         *             =》 实时统计各省份的热门广告（点击次数多的）  分组求TopN
         *            DStream.transform-> rdd->DataSet/DataFrame->SparkSQL
         * 6)实时统计每天每个广告在最近一个小时的滑动窗口的点击趋势
         */
        Map<TopicAndPartition, Long> topicOffsets = GetTopicOffsetFromKafkaBroker.getTopicOffsets(brokerList, topic);
        Map<TopicAndPartition, Long> consumerOffsets = GetTopicOffsetFromZookeeper.getConsumerOffsets(zkList, groupId, topic);
        if(null!=consumerOffsets && consumerOffsets.size()>0){
            topicOffsets.putAll(consumerOffsets);
        }
//		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingOnKafkaDirect");
//		conf.set("spark.streaming.kafka.maxRatePerPartition", "10");
//                 JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));
//                 context.checkpoint("hdfs://192.168.178.133:9000/adv");
//        JavaStreamingContext context =
//                SparkStreamingDirect.getMessage(
//                topicOffsets, groupId
//        );
//
//
//        context.start();
//        context.awaitTermination();
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingOnKafkaDirect");
        conf.set("spark.streaming.kafka.maxRatePerPartition", "10");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaSparkContext context = jsc.sparkContext();

        JavaRDD<String> parallelize =  getBlickList();

//        JavaRDD<String> parallelize = context.parallelize(Arrays.asList("7198e4bd-6f87-4144-968b-ddacfc4e72cd", "193879cf-b7c4-49db-9e47-c76aa2ec74db"));


        Broadcast<List<String>> broadcast = context.broadcast(parallelize.collect());


//        jsc.checkpoint("/checkpoint");
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","master:9092");
//        kafkaParams.put("group.id","MyFirstConsumerGroup");

        for(Map.Entry<TopicAndPartition,Long> entry:topicOffsets.entrySet()){
            System.out.println(entry.getKey().topic()+"\t"+entry.getKey().partition()+"\t"+entry.getValue());
        }

        JavaInputDStream<String> message = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParams,
                topicOffsets,
                new Function<MessageAndMetadata<String,String>,String>() {
                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    public String call(MessageAndMetadata<String, String> v1)throws Exception {

                        return v1.message();
                    }
                }
        );
        JavaInputDStream<String> messageNew=message;

        JavaDStream<Log>  filtered= BlackListFilter(messageNew,broadcast);

        filtered.print();








        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        JavaDStream<String> lines = message.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
                                                          /**
                                                           *
                                                           */
                                                          private static final long serialVersionUID = 1L;
                                                          @Override
                                                          public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                                                              OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                                                              offsetRanges.set(offsets);
                                                              return rdd;
                                                          }
                                                      }
        );

        message.foreachRDD(new VoidFunction<JavaRDD<String>>(){

            private static final long serialVersionUID = 1L;
            @Override
            public void call(JavaRDD<String> t) throws Exception {
                /**
                 * 更新偏移量
                 */
                ObjectMapper objectMapper = new ObjectMapper();
                CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                        .connectString("master:2181").connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
                curatorFramework.start();
                for (OffsetRange offsetRange : offsetRanges.get()) {
                    long fromOffset = offsetRange.fromOffset();
                    long untilOffset = offsetRange.untilOffset();
                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(offsetRange.untilOffset());
                    String nodePath = "/consumers/"+groupId+"/offsets/" + offsetRange.topic()+ "/" + offsetRange.partition();
//                    System.out.println("nodePath = "+nodePath);
//                    System.out.println("fromOffset = "+fromOffset+",untilOffset="+untilOffset);
                    if(curatorFramework.checkExists().forPath(nodePath)!=null){
                        curatorFramework.setData().forPath(nodePath,offsetBytes);
                    }else{
                        curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
                    }
                }
                curatorFramework.close();
                /**
                 * 更新偏移量
                 */
//                t.foreach(new VoidFunction<String>() {
//                    @Override
//                    public void call(String s) throws Exception {
//                        String url = "jdbc:mysql://192.168.178.128:3306/bigdata?useUnicode=true&characterEncoding=utf8";
//                        Connection connection = DriverManager.getConnection(url, "root", "111111");
//                        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO `wordcount` (`word`) VALUES (?)");
//                            preparedStatement.setString(1,s);
//                            preparedStatement.addBatch();
//                            preparedStatement.execute();
//
//                    }
//                });
//                t.saveAsTextFile("d://data.txt");
            }
        });

		lines.print();



        jsc.start();
        jsc.awaitTermination();




    }

    private static JavaRDD<String> getBlickList() {

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local");
        sparkConf.setAppName("mysql");

        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();
        Properties readConnProperties1 = new Properties();
        readConnProperties1.put("driver", "com.mysql.jdbc.Driver");
        readConnProperties1.put("user", "root");
        readConnProperties1.put("password", "111111");

        readConnProperties1.put("fetchsize", "3");

        Dataset<Row> jdbc = session.read().jdbc(
                "jdbc:mysql://192.168.178.133:3306/bigdata",
                "ClickCount",
                readConnProperties1);

        jdbc.show();

        return    null;



    }

    private static JavaDStream<Log> BlackListFilter(JavaInputDStream<String> message, Broadcast<List<String>> broadcast) {

        JavaDStream<Log> transform = message.transform(new Function<JavaRDD<String>, JavaRDD<Log>>() {
            @Override
            public JavaRDD<Log> call(JavaRDD<String> stringJavaRDD) throws Exception {
                JavaRDD<Log> map = stringJavaRDD.map(new Function<String, Log>() {
                    @Override
                    public Log call(String s) throws Exception {
                        return jsontoLog(s);
                    }
                });
                return map;
            }
        });


        List<String> value = broadcast.value();


        JavaDStream<Log> filter = transform.filter(new Function<Log, Boolean>() {
            @Override
            public Boolean call(Log log) throws Exception {

                if (value.contains(log.getUserId())) {
                    return false;
                } else {
                    return true;
                }
            }
        });


        return  filter;


    }
    public static Log jsontoLog(String json)
    {

        JSONObject fromObject = JSONObject.fromObject(json);
        Object bean = JSONObject.toBean(fromObject,Log.class);
        return  (Log)bean;
    }


}
