package com.banksy.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 *  事务
 */
public class CustomProducerTransactions {
    public static void main(String[] args) {
        //  0、配置
        Properties properties = new Properties();
        //  连接集群 bootstrap.server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop111:9092");
        //  指定对应key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置事务 id（必须），事务 id 任意起名
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transaction_id_0");

        //  1、创建Kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 初始化事务
        kafkaProducer.initTransactions();
        // 开启事务
        kafkaProducer.beginTransaction();

        //  2、发送数据
        try {
            for (int i = 0; i < 5; i++){
                kafkaProducer.send(new ProducerRecord<>("first","banksy" +i));
            }
            // 提交事务
            kafkaProducer.commitTransaction();
        }catch (Exception exception){
            // 终止事务
            kafkaProducer.abortTransaction();
        }finally {
            //  3、关闭资源
            kafkaProducer.close();
        }
    }
}
