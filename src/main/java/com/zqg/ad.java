package com.zqg;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.zqg.kakfautils.GetTopicOffsetFromKafkaBroker;
import com.zqg.kakfautils.GetTopicOffsetFromZookeeper;
import com.zqg.models.Log;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import net.sf.json.JSONObject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 *  实施流量统计
 */
public class ad    {

    static    String zkList="192.168.178.134:2181";
    static    String  brokerList="192.168.178.134:9092";
    static    String topic="zzz";
    static    String groupId="group2";
    static Broadcast<List<String>> broadcast=null;
     static JavaStreamingContext jsc=null;
     static  JavaSparkContext  sparkContext=null;
     static  SparkSession  session=null;
     static  SparkConf sparkConf=null;
    static  List<String> list=null;
     static {
         sparkConf=new SparkConf();
          sparkConf.setMaster("local[*]").setAppName("ad");
          session=SparkSession.builder().config(sparkConf).getOrCreate();
//         SparkContext sparkContext1 =
         sparkContext=JavaSparkContext.fromSparkContext(session.sparkContext());
//          sparkContext.setLogLevel("warn");
         jsc=new JavaStreamingContext(sparkContext,Durations.seconds(5));


     }
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
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","master:9092");
//        kafkaParams.put("group.id","MyFirstConsumerGroup");

        for(Map.Entry<TopicAndPartition,Long> entry:topicOffsets.entrySet()){
            System.out.println(entry.getKey().topic()+""+entry.getKey().partition()+""+entry.getValue());
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
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        JavaDStream<String> lines = message.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
                                                          private static final long serialVersionUID = 1L;
                                                          @Override
                                                          public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                                                              OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                                                              offsetRanges.set(offsets);
                                                              return rdd;
                                                          }
                                                      }
        );
     lines.print();

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
                update(t.context(),true);
                /**
                 * 更新偏移量
                 */
//                t.foreach(new VoidFunction<String>() {
//                    @Override
//                    public void call(String s) throws Exception {
//                        String url = "jdbc:mysql://192.168.178.128:3306/bigdata?useUnicode=true&characterEncoding=utf8";
//                        Connection connection = DriverManager.getConnection(url, "root", "111111");
//                        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO wordcount (word) VALUES (?)");
//                            preparedStatement.setString(1,s);
//                            preparedStatement.addBatch();
//                            preparedStatement.execute();
//                    }
//                });
//                t.saveAsTextFile("d://data.txt");
            }
        });

//		lines.print();
		getInstance(sparkContext);
        JavaDStream<Log> transform = lines.transform(new Function<JavaRDD<String>, JavaRDD<Log>>() {
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

        JavaDStream<Log> filter = transform.filter(new Function<Log, Boolean>() {
            @Override
            public Boolean call(Log log) throws Exception {
                if(list.contains(log.getUserId()))
                {
                    return  false;
                }
                else
                {
                    return   true;
                }
            }
        });
//        filter.print();
//        generaterBlackList(filtered);
        filter.print();
        jsc.start();
        jsc.awaitTermination();

    }


    public  static void  update( SparkContext sparkContext,  Boolean blocking  ) {
         if(blocking==null)
         {
             blocking=false;
         }

        if (broadcast != null){
            broadcast.unpersist(blocking);
            JavaSparkContext context = JavaSparkContext.fromSparkContext(sparkContext);
            broadcast     = context.broadcast(getBlickList());
        }
    }
    private static void generaterBlackList(JavaDStream<Log> message) {

     message.mapToPair(new PairFunction<Log, String, Integer>() {
         @Override
         public Tuple2<String, Integer> call(Log log) {
             String accessTime = log.getAccessTime();
             String userId = log.getUserId();
             String advId = log.getAdvId();

             return  new Tuple2<>(accessTime+"__"+userId+"__"+advId,1);

         }
     }).reduceByKey(new Function2<Integer, Integer, Integer>() {
         @Override
         public Integer call(Integer integer, Integer integer2) throws Exception {
             return  integer+integer2;
         }
     }).foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
         @Override
         public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
               stringIntegerJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                   @Override
                   public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {

                       String url = "jdbc:mysql://192.168.178.134:3306/bigdata?useUnicode=true&characterEncoding=utf8";
                       Connection connection = DriverManager.getConnection(url, "root", "111111");
                  while (tuple2Iterator.hasNext())
                  {
                      Tuple2<String, Integer> next = tuple2Iterator.next();
                      String s = next._1;
                      String[] s1 = s.split("__");
                      Integer integer = next._2;
                      PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO clickcount (user_id, adv_id, time, click_time) VALUES (?,?, ?, ?)");
                      preparedStatement.setString(1,s1[1]);
                      preparedStatement.setString(2,s1[2]);
                      preparedStatement.setString(3,s1[0]);
                      preparedStatement.setInt(4,integer);
                      preparedStatement.addBatch();
                      preparedStatement.execute();
                  }
                  connection.close();
                   }
               });
         }
     });
//     生成的数据防卫记录中查询出黑名单
        Properties  properties=new Properties();
        properties.put("user","root");
        properties.put("password","111111");
        properties.put("driver","com.mysql.jdbc.Driver");
        properties.put("fetchsize","3");
        Dataset<Row> blacklist = session.read().jdbc(
                "jdbc:mysql://192.168.178.134:3306/bigdata",
                "clickcount",
                properties
        );
         blacklist.createOrReplaceTempView("clickcount");
        Dataset<Row> sql = session.sql("SELECT" +
                "  user_id  from (" +
                "  SELECT" +
                "  sum(click_time) c_count," +
                "  time," +
                "  user_id," +
                "  adv_id" +
                "  FROM" +
                "  clickcount  " +
                "  GROUP BY" +
                "  time," +
                "  user_id," +
                "  adv_id" +
                "  ) tmp" +
                "  WHERE" +
                "  tmp.c_count > 100");
//        sql.show();
//        sql.write().mode(SaveMode.Overwrite).jdbc(
//                "jdbc:mysql://192.168.178.133:3306/bigdata",
//                "blacklist",
//                properties
//        );
    }


    private static List<String> getBlickList() {
        Properties readConnProperties1 = new Properties();
        readConnProperties1.put("driver", "com.mysql.jdbc.Driver");
        readConnProperties1.put("user", "root");
        readConnProperties1.put("password", "111111");
        readConnProperties1.put("fetchsize", "3");
        Dataset<Row> jdbc = session.read().jdbc(
                "jdbc:mysql://192.168.178.134:3306/bigdata",
                "blacklist",
                readConnProperties1);
        JavaRDD<Row> javaRDD = jdbc.javaRDD();
        JavaRDD<String> map = javaRDD.map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return row.get(0).toString();
            }
        });
     list= map.collect();
        return   list;
    }

    private static JavaDStream<Log> BlackListFilter(  JavaDStream<String> message) {

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
//        List<String> value = broadcast.value();
        JavaDStream<Log> filter = transform.filter(new Function<Log, Boolean>() {
            @Override
            public Boolean call(Log log) throws Exception {
                if(list.contains(log.getUserId()))
                {
                    return  false;
                }
                else {
                    return  true;
                }
            }
        });

        return  filter;
    }

    public  static       Broadcast<List<String>> getInstance(JavaSparkContext sparkContext)  {
                if (broadcast == null) {
                     broadcast = sparkContext.broadcast(getBlickList());
                }
             return broadcast;
    }


    public static Log jsontoLog(String json)
    {
        JSONObject fromObject = JSONObject.fromObject(json);
        Object bean = JSONObject.toBean(fromObject,Log.class);
        return  (Log)bean;
    }





}
