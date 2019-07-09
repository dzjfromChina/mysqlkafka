package com.mysqlkafka.mysqlkafka.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.LogManager;

import java.util.logging.Logger;

/**
 * @author duzj
 * @create 2019-07-09 15:05
 */
public class ProducerCallbackDemo implements Callback {
    public final org.apache.log4j.Logger logger = LogManager.getLogger(this.getClass());

    KafkaProducerDemo kafkaProducerDemo;

    ProducerRecord<String, String> record;
    int sendSeq = 0;


    public ProducerCallbackDemo(ProducerRecord record, int sendSeq) {
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
            KafkaProducerDemo.send(record.topic(),record.key());
        }

    }
}
