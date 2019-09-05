package com.zh.telecom.hbase;

import com.zh.telecom.utils.HBaseUtil;
import com.zh.telecom.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * @Author zhanghe
 * @Desc:  自定义协处理器
 *     协处理器要指定控制的表，否则会作用到所有表上
 * @Date 2019/4/10 21:07
 */
public class CalleeWriteObserver extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

        //        super.postPut(e, put, edit, durability);
        //获取要操作的目标表
        String targetTableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
        int regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));

        //获取当前put数据表：环境-区域-表-表名
        String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();
        //不是要操作的表，直接返回
        if(!targetTableName.equals(currentTableName)){
            return;
        }
        //获得rowkey 01_158373123456_20170110154530_13737312345_1_0360
        String oriRowKey = Bytes.toString(put.getRow());
        String[] splitOriRowKey = oriRowKey.split("_");

        String oldFlag = splitOriRowKey[4];
        //如果当前插入的是被叫数据，则直接返回(因为默认提供的数据全部为主叫数据)
        if(oldFlag.equals("0")){
            return;
        }

        String caller = splitOriRowKey[1];
        String callee = splitOriRowKey[3];
        String buildTime = splitOriRowKey[2];
        String flag = "0";
        String duration = splitOriRowKey[5];
        String regionCode = HBaseUtil.genRegionCode(callee, buildTime, regions);
        String calleeRowKey = HBaseUtil.getRowKey(regionCode, callee, buildTime, caller, flag, duration);

        String buildTimeTs = "";
        try {
            buildTimeTs = String.valueOf(sdf.parse(buildTime).getTime());
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        // 组织插入的数据
        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs));
        Bytes.toBytes(100L);

        Table table = e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        table.put(calleePut);
        table.close();
    }

}
