package com.zh.analysis.mapreduce;

import com.zh.analysis.bean.ComDimension;
import com.zh.analysis.value.CountDurationValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/4/12 17:43
 */
public class CountDurationRunner implements Tool {

    private Configuration conf = null;

    @Override
    public void setConf(Configuration conf) {
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] strings) throws Exception {
        //得到conf
        Configuration conf = this.getConf();
        //实例化Job
        Job job = Job.getInstance();
        job.setJarByClass(CountDurationMapper.class);

        //组装Mapper InputForamt
        initHBaseInputConfig(job);
        //组装Reducer Outputformat
        initReducerOutputConfig(job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void initHBaseInputConfig(Job job) {
        Connection connection = null;
        Admin admin = null;
        try {
            String tableName = "ns_ct:calllog";
            connection = ConnectionFactory.createConnection(job.getConfiguration());
            admin = connection.getAdmin();
            if(!admin.tableExists(TableName.valueOf(tableName))) throw new RuntimeException("无法找到目标表.");
            //可以优化
            TableMapReduceUtil.initTableMapperJob(
                    tableName,
                    new Scan(),
                    CountDurationMapper.class,
                    ComDimension.class,
                    Text.class,
                    job,
                    true);


        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(admin != null){
                    admin.close();
                }
                if(connection != null && !connection.isClosed()){
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void initReducerOutputConfig(Job job) {
        job.setReducerClass(CountDurationReducer.class);
        job.setOutputKeyClass(ComDimension.class);
        job.setOutputValueClass(CountDurationValue.class);
        job.setOutputFormatClass(MysqlOutputFormat.class); //自定义输出类
    }

    public static void main(String[] args) {
        try {
            int status = ToolRunner.run(new CountDurationRunner(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
