package com.mysqlkafka.producer.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author duzj
 * @create 2019-07-07 8:39
 */
public class MyKafkaProducer {
    //修改服务端kafka 的两个listen 的ip  并且配置本地hosts(服务端ip hostname)
    public static void main(String[] args){
        int events = 100;
        Properties props = new Properties();
        //集群地址，多个服务器用"，"分隔
        props.put("bootstrap.servers", "192.168.13.9:9092");
        //key、value的序列化，此处以字符串为例，使用kafka已有的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("partitioner.class", "com.kafka.demo.Partitioner");//分区操作，此处未写
        props.put("request.required.acks", "1");
        //创建生产者
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        for (int i = 0; i < events; i++){
            long runtime = System.currentTimeMillis();
            String ip = "192.168.1." + i;
            String msg = runtime + "时间的模拟ip：" + ip;
            //写入名为"test-partition-1"的topic
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("mykafka", "key-"+i, msg);
            producer.send(producerRecord);
            System.out.println("写入test-partition-1：" + msg);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    public static void send(String topic,String msg){
        Properties props = new Properties();
        //集群地址，多个服务器用"，"分隔
        props.put("bootstrap.servers", "192.168.13.9:9092");
        //key、value的序列化，此处以字符串为例，使用kafka已有的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("partitioner.class", "com.kafka.demo.Partitioner");//分区操作，此处未写
        props.put("request.required.acks", "1");
        //创建生产者
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, msg);
        //ProducerCallbackDemo producerCallbackDemo = new ProducerCallbackDemo();
        producer.send(producerRecord,new ProducerCallback(producerRecord,0));
    }

}
