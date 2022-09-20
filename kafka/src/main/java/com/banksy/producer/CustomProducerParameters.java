package com.banksy.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产经验——生产者如何提高吞吐量
 */
public class CustomProducerParameters {
    public static void main(String[] args) {
        //  0、配置
        Properties properties = new Properties();
        //  连接集群 bootstrap.server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop111:9092");
        //  指定对应key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //RecordAccumulator：缓冲区大小，默认32M；修改为64m;   buffer.memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,67108864);
        //batch.size：批次大小，默认16k, 修改为32K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,32768);
        //linger.ms：等待时间，默认 0, 修改为5-100ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG,6);
        //compression.type：压缩，默认 none，可配置值 gzip、 snappy、lz4 和 zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

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
