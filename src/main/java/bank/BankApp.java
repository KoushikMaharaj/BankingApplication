package bank;

import bank.util.PushData;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BankApp {
    private static final Logger log = Logger.getLogger(BankApp.class);

    public static void main(String[] args) throws InterruptedException {

        System.out.println("Spark Streaming started...............");
        log.info("Spark Streaming started...............");
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        SparkConf conf = new SparkConf().setAppName("banking").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("OFF");
        // JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(20));
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(10));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.12:9092");
        Set<String> topics = Collections.singleton("demo");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jssc, String.class,
                String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println(rdd.count());
            if (rdd.count() > 0) {
                rdd.collect().forEach(record -> {
                    try {
                        PushData.pushDataToMongodb(record._2);
                    } catch (Exception e) {
                        System.out.println(e + " " + e.getMessage());
                        log.error(e.getMessage());
                    }
                });
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
