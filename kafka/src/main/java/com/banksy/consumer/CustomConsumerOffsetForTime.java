package com.banksy.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 指定时间进行消费
 */
public class CustomConsumerOffsetForTime {
    public static void main(String[] args) {
        //  0、配置
        Properties properties = new Properties();
        //  连接集群 bootstrap.server
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop111:9092");
        //  反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //  配置消费者ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        //  1、创建Kafka消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //  2、定义主题topic
        ArrayList<String> topic = new ArrayList<>();
        topic.add("first");
        kafkaConsumer.subscribe(topic);

        //  指定位置进行消费
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        //  将时间转换对应的offset
        HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();
        //  封装对应的集合（得到一天前的offset）
        for (TopicPartition topicPartition : assignment) {
            topicPartitionLongHashMap.put(topicPartition,System.currentTimeMillis() -1 * 24 * 36 * 1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(topicPartitionLongHashMap);

        //  保证分区分配方案已经制定完毕
        while (assignment.size() == 0){
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment = kafkaConsumer.assignment();
        }
        //  指定消费的offset，从offset=100开始往后消费
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
            kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
        }

        //  3、消费数据
        while (true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }

    }

}
