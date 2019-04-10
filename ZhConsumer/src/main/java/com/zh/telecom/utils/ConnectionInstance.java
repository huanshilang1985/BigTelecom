package com.zh.telecom.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


/**
 * @Author zhanghe
 * @Desc: 连接实例
 * @Date 2019/4/9 20:31
 */
public class ConnectionInstance {

    private static Connection conn;

    /**
     * 返回连接实例
     * @param conf
     * @return
     */
    public static synchronized Connection getConnection(Configuration conf) {
        try {
            if(conn == null || conn.isClosed()) {
                conn = ConnectionFactory.createConnection(conf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

}
