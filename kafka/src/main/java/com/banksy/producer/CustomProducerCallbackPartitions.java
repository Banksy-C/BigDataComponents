package com.banksy.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 自定义分区 & 非自定义分区
 */
public class CustomProducerCallbackPartitions {
    public static void main(String[] args) {
        //  0、配置
        Properties properties = new Properties();
        //  连接集群 bootstrap.server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop111:9092");
        //  指定对应key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 添加自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.banksy.kafka.producer.MyPartitioner");

        //  1、创建Kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //  2、发送数据
        for (int i = 0; i < 5; i++){
            // 指定数据发送到 1 号分区， key 为空；（IDEA 中 ctrl + p 查看参数）
            //kafkaProducer.send(new ProducerRecord<>("first", 1,"","banksy" + i), new Callback() {
            // 依次指定 key 值为 a,b,f ，数据 key 的 hash 值与 3 个分区求余，分别发往 1、 2、 0
            //kafkaProducer.send(new ProducerRecord<>("first","a","banksy " + i), new Callback() {

            //自定义（上面有自定义分区器）
            kafkaProducer.send(new ProducerRecord<>("first", "banksy" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        System.out.println(" 主 题 ： " + metadata.topic() + "->" + "分区： " + metadata.partition());
                    }
                }
            });
        }
        //  3、关闭资源
        kafkaProducer.close();

    }
}
