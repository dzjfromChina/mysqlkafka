package com.mysqlkafka.consumer.main;

import com.mysqlkafka.consumer.kafka.MyKafkaConsumer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * @author duzj
 * @create 2019-07-22 10:35
 */
@Component
public class KafkaConsumerClient implements CommandLineRunner {
    @Async
    @Override
    public void run(String... args) throws Exception {
        MyKafkaConsumer.consume();
    }
}
