package com.zh.telecom.kafka;

import com.zh.telecom.hbase.HBaseDao;
import com.zh.telecom.utils.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;

/**
 * @Author zhanghe
 * @Desc: Hbase消费者
 * @Date 2019/4/9 20:34
 */
public class HBaseConsumer {

    public static void main(String[] args) {
        // 连接kafka，使用KafkaConsumer消费
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(PropertiesUtil.properties);
        // 指定要消费的主题
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));

        HBaseDao dao = new HBaseDao();
        while (true){
            ConsumerRecords<String ,String> records = kafkaConsumer.poll(100);
            for(ConsumerRecord<String, String> cr : records){
                String ori = cr.value();
                System.out.println(ori);
                dao.put(ori);
            }
        }

    }

}
