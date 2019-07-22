package com.mysqlkafka.consumer.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author duzj
 * @create 2019-07-22 10:30
 */
public class MyKafkaConsumer {
    public static void consume(){
        //记得在本机配置 host 就是kafka所在的
        Properties props = new Properties();
        //集群地址，多个地址用"，"分隔
        props.put("bootstrap.servers","192.168.13.9:9092");
        //设置消费者的group id
        props.put("group.id", "group1");
        //如果为真，consumer所消费消息的offset将会自动的同步到zookeeper。如果消费者死掉时，由新的consumer使用继续接替
        props.put("enable.auto.commit", "true");
        //consumer向zookeeper提交offset的频率
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("mykafka"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("-----------------");
                System.out.printf("value = %s", record.value());
                System.out.println();

                String value = record.value();
                JSONObject jsonObject = JSON.parseObject(value);
                String action = jsonObject.get("action").toString();
                String database = jsonObject.get("database").toString();
                String table = jsonObject.get("table").toString();

                StringBuffer sql = new StringBuffer("");
                if("update".equals(action)){
                    JSONArray chage = jsonObject.getJSONArray("chage");
                    JSONArray where = jsonObject.getJSONArray("where");
                    sql.append("update ").append(database).append(".").append(table).append(" set id=").append(chage.get(0)).append(" , name=").append(chage.get(1))
                            .append(" where id=").append(where.get(0)).append(" and name=").append(where.get(1));
                }else if("insert".equals(action)){
                    String chage = jsonObject.get("chage").toString();
                }else {
                    String where = jsonObject.get("where").toString();
                }
                System.out.println(sql);
            }
        }

    }
}
