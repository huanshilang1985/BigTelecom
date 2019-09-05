package com.zh.telecom.hbase;

import com.zh.telecom.utils.ConnectionInstance;
import com.zh.telecom.utils.HBaseUtil;
import com.zh.telecom.utils.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/4/10 14:26
 */
public class HBaseDao {

    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    private int regions;      //分区数
    private String namespace;  //命名空间
    private String tableName;  //表名

    private List<Put> cacheList = new ArrayList<Put>();
    private HTable table;
    private Connection connection;

    //初始化配置
    private static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
    }

    public HBaseDao() {
        try {
            //拿到配置文件参数
            regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
            namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
            tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
            //没有表的话，创建表
            if (!HBaseUtil.isExistTable(conf, tableName)) {
                HBaseUtil.initNameSpace(conf, namespace);
                HBaseUtil.createTable(conf, tableName, regions, "f1", "f2");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * ori数据样式：18576581848,17269452013,2017-08-14 13:38:31,1761
     *
     * @param ori
     */
    public void put(String ori) {
        try {
            //如果List为空，连接实例，创建表
            if(cacheList.size() == 0){
                connection = ConnectionInstance.getConnection(conf);
                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                table.setAutoFlushTo(false);  //刷新，关掉Hbase默认设置
                table.setWriteBufferSize(2 * 1024 * 1024); //设置缓存空间
            }

            //切分数据源
            String[] splitOri = ori.split(",");
            String caller = splitOri[0];
            String callee = splitOri[1];
            String buildTime = splitOri[2];
            String duration = splitOri[3];
            String regionCode = HBaseUtil.genRegionCode(caller, buildTime, regions);
            //转换时间格式
            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));
            String buildTimeTs = String.valueOf(sdf1.parse(buildTime).getTime()); //时间戳
            //生成RowKey
            String rowKey = HBaseUtil.getRowKey(regionCode, caller, buildTimeReplace, callee, "1", duration);
            //向表中插入数据
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("caller"), Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("callee"), Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTimeTs"), Bytes.toBytes(buildTimeTs));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
            cacheList.add(put);

            if(cacheList.size() != 0){
                table.put(cacheList);
                table.flushCommits();
                table.close();
                cacheList.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}