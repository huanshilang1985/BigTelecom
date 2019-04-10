package com.zh.telecom.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @Author zhanghe
 * @Desc:  Hbase工具类
 * 1. NameSpace 命名空间
 * 2. createTable  创建表
 * 3. isTable   判断表是否存在
 * 4. Region、RowKey、
 *
 * @Date 2019/4/9 21:09
 */
public class HBaseUtil {

    /**
     * 初始化命名空间
     * @param conf    配置对象
     * @param namespace  命名空间名
     * @throws IOException
     */
    public static void initNameSpace(Configuration conf, String namespace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //命名空间描述器
        NamespaceDescriptor nd = NamespaceDescriptor.create(namespace)
                .addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis()))
                .addConfiguration("AUTHOR", "zhanghe")
                .build();
        //通过Admin创建命名空间
        admin.createNamespace(nd);
    }

    /**
     * 创建表
     * @param conf     配置对象
     * @param tableName 表名
     * @param regions
     * @param columnFamily  列簇，原则上不超过3个，会影响分裂
     * @throws IOException
     */
    public static void createTable(Configuration conf, String tableName, int regions, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //判断表
        if(isExistTable(conf, tableName)){
            return;
        }
        //表描述器 HTableDescriptor
        HTableDescriptor hd = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf : columnFamily){
            //字段描述器 HColumnDescriptor
            hd.addFamily(new HColumnDescriptor(cf));
        }
//        hd.addCoprocessor("hbase.CalleeWriteObserver");  //协处理器
        admin.createTable(hd, genSplitKeys(regions));
        close(admin,connection);
    }

    /**
     * 判断表是否存在
     * @param conf       配置对象
     * @param tableName  表名
     * @return
     * @throws IOException
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        close(admin, connection);
        return result;
    }

    /**
     * 生成rowkey
     * rowkey是唯一的，可以快速定位数据，需要按照业务需求设计
     * regionCode_caller_buildTime_callee_flag_duration
     * @param regionCode 分区编码
     * @param caller     主叫手机号
     * @param buildTime  呼叫时间
     * @param callee     被叫手机号
     * @param flag       标签，确认是主叫还是被叫
     * @param duration   通话时间
     * @return
     */
    public static String getRowKey(String regionCode, String caller, String buildTime, String callee, String flag, String duration) {
        StringBuilder sb = new StringBuilder()
                .append(regionCode + "_")
                .append(caller + "_")
                .append(buildTime + "_")
                .append(callee + "_")
                .append(flag + "_")
                .append(duration);
        return sb.toString();
    }

    /**
     * 关闭Admin对象和connection对象
     * @param admin      Admin对象
     * @param connection connection对象
     * @throws IOException
     */
    private static void close(Admin admin, Connection connection) throws IOException {
        if(admin != null){
            admin.close();
        }
        if(connection != null){
            connection.close();
        }
    }

    /**
     * 分区键，用于创建region，防止数据倾斜
     * @param regions
     * @return
     */
    private static byte[][] genSplitKeys(int regions) {
        // 定义一个存放分区键的数组
        String[] keys = new String[regions];
        // 目前推算，region个数不会超过2位数，所以region分区键格式化为两位数据所代表的字符串
        // 格式化 00| 01| 02| 03| 04| 05|
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regions; i++) {
            keys[i] = df.format(i) + "|";
        }
        // 生成byte[][]类型的分区键的时候，一定要保证分区键是有序的
        byte[][] splitKeys = new byte[regions][];
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regions; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }
        //迭代输出
        int index = 0;
        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        while (splitKeysIterator.hasNext()) {
            byte[] b = splitKeysIterator.next();
            splitKeys[index++] = b;
        }
        return splitKeys;
    }

}
