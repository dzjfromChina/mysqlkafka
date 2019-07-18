package com.mysqlkafka.mysqlkafka.main;

import com.alibaba.fastjson.JSON;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.mysqlkafka.mysqlkafka.kafka.KafkaProducerDemo;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;

/**
 * @author dzj
 * @create 2019-07-06 17:40
 */
@Component
public class BinlogClientRunner implements CommandLineRunner {
    public final Logger logger = LogManager.getLogger(this.getClass());


    @Value("${binlog.host}")
    private String host;

    @Value("${binlog.port}")
    private int port;

    @Value("${binlog.user}")
    private String user;

    @Value("${binlog.password}")
    private String password;

    // binlog server_id
    @Value("${server.id}")
    private long serverId;

    // kafka话题
    @Value("${kafka.topic}")
    private String topic;

    // kafka分区
    @Value("${kafka.partNum}")
    private int partNum;

    // Kafka备份数
    @Value("${kafka.repeatNum}")
    private short repeatNum;

    // kafka地址
    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaHost;

    // 指定监听的数据表
    @Value("${binlog.database.table}")
    private String database_table;
/*
    @Autowired
    KafkaSender kafkaSender;*/

    @Async
    @Override
    public void run(String... args) throws Exception {
        System.out.println("开始---------------------------");
        // 创建topic
        //kafkaSender.createTopic(kafkaHost, topic, partNum, repeatNum);
        // 获取监听数据表数组
        List<String> databaseList = Arrays.asList(database_table.split(","));
        System.out.println(databaseList );
        HashMap<Long, String> tableMap = new HashMap<Long, String>();
        // 创建binlog监听客户端
        BinaryLogClient client = new BinaryLogClient(host, port, user, password);
        client.setServerId(serverId);
        client.registerEventListener((event -> {
            // binlog事件
            EventData data = event.getData();
            if (data != null) {
                //System.out.println(data);
                if (data instanceof TableMapEventData) {
                    TableMapEventData tableMapEventData = (TableMapEventData) data;
                    String database = tableMapEventData.getDatabase();
                    if(database.equals("test")){
                        System.out.println("database:"+database);
                    }
                    tableMap.put(tableMapEventData.getTableId(), tableMapEventData.getDatabase() + "." + tableMapEventData.getTable());
                }
                // update数据
                if (data instanceof UpdateRowsEventData) {
                    UpdateRowsEventData updateRowsEventData = (UpdateRowsEventData) data;
                    List<Map.Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
                    String tableName = tableMap.get(updateRowsEventData.getTableId());
                    if (tableName != null && databaseList.contains(tableName)) {
                        for (Map.Entry<Serializable[], Serializable[]> row : updateRowsEventData.getRows()) {
                            Map<String,Object> map = new HashMap<>(3);
                            String[] split = tableName.split("\\.");
                            map.put("database",split[0]);
                            map.put("table",split[1]);
                            map.put("action","update");
                            map.put("where",shuzuToList(row.getKey()));
                            map.put("chage",shuzuToList(row.getValue()));
                            StringBuilder sb = new StringBuilder("update数据：");
                            sb.append(map);
                            logger.info(sb);
                            KafkaProducerDemo.send(topic, JSON.toJSONString(map));
                        }
                    }
                }
                // insert数据
                else if (data instanceof WriteRowsEventData) {
                    WriteRowsEventData writeRowsEventData = (WriteRowsEventData) data;
                    String tableName = tableMap.get(writeRowsEventData.getTableId());
                    if (tableName != null && databaseList.contains(tableName)) {
                        for (Serializable[] row : writeRowsEventData.getRows()) {
                            Map<String,Object> map = new HashMap<>(3);
                            String[] split = tableName.split("\\.");
                            map.put("database",split[0]);
                            map.put("table",split[1]);
                            map.put("action","insert");
                            map.put("where","");
                            map.put("chage",shuzuToList(row));
                            StringBuilder sb = new StringBuilder("insert数据：");
                            sb.append(map);
                            logger.info(sb);
                            KafkaProducerDemo.send(topic, JSON.toJSONString(map));

                        }
                    }
                }
                // delete数据
                else if (data instanceof DeleteRowsEventData) {
                    DeleteRowsEventData deleteRowsEventData = (DeleteRowsEventData) data;
                    String tableName = tableMap.get(deleteRowsEventData.getTableId());
                    if (tableName != null && databaseList.contains(tableName)) {
                        String eventKey = tableName + ".delete";
                        for (Serializable[] row : deleteRowsEventData.getRows()) {
                            Map<String,Object> map = new HashMap<>(3);
                            String[] split = tableName.split("\\.");
                            map.put("database",split[0]);
                            map.put("table",split[1]);
                            map.put("action","delete");
                            map.put("where",shuzuToList(row));
                            map.put("chage","");
                            StringBuilder sb = new StringBuilder("delete数据：");
                            sb.append(map);
                            logger.info(sb);
                            KafkaProducerDemo.send(topic, JSON.toJSONString(map));

                        }
                    }
                }
            }
        }));
        client.connect();
    }

    private List<Object> shuzuToList(Object[] objects){
        List<Object> objectList = new ArrayList<>(objects.length);
        for (int i = 0; i <objects.length ; i++) {
            objectList.add(objects[i]);
        }
        return objectList;
    }
}
