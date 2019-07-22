package com.mysqlkafka.producer.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.LogManager;

/**
 * @author duzj
 * @create 2019-07-09 15:05
 */
public class ProducerCallback implements Callback {
    public final org.apache.log4j.Logger logger = LogManager.getLogger(this.getClass());

    MyKafkaProducer kafkaProducerDemo;

    ProducerRecord<String, String> record;
    int sendSeq = 0;


    public ProducerCallback(ProducerRecord record, int sendSeq) {
        this.record = record;
        this.sendSeq = sendSeq;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        //send success
        if (null == e) {
            String meta = "topic:" + recordMetadata.topic() + ", partition:" + recordMetadata.topic() + ", offset:" + recordMetadata.offset();
            logger.info("send message success, record:" + record.toString() + ", meta:" + meta);
            return;
        }

        //send failed
        logger.error("send message failed, seq:" + sendSeq + ", record:" + record.toString() + ", errmsg:" + e.getMessage());
        if (sendSeq < 1) {
            MyKafkaProducer.send(record.topic(),record.key());
        }

    }
}
