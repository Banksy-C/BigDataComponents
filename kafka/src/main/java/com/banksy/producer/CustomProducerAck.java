package com.banksy.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 *  生产经验——数据可靠性（ACK应答级别）
 */
public class CustomProducerAck {
    public static void main(String[] args) {
        //  0、配置
        Properties properties = new Properties();
        //  连接集群 bootstrap.server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop111:9092");
        //  指定对应key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置 ack
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        // 重试次数 retries，默认是 int 最大值， 2147483647
        properties.put(ProducerConfig.RECEIVE_BUFFER_CONFIG,3);

        //  1、创建Kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //  2、发送数据
        for (int i = 0; i < 5; i++){
            kafkaProducer.send(new ProducerRecord<>("first","banksy" +i));
        }
        //  3、关闭资源
        kafkaProducer.close();

    }
}
