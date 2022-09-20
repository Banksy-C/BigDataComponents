package com.banksy.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 带回调的异步发送
 */
public class CustomProducerCallback {
    public static void main(String[] args) {
        //  0、配置
        Properties properties = new Properties();
        //  连接集群 bootstrap.server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop111:9092");
        //  指定对应key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //  1、创建Kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //  2、发送数据
        for (int i = 0; i < 5; i++){
            kafkaProducer.send(new ProducerRecord<>("first", "banksy333" + i), new Callback() {
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
