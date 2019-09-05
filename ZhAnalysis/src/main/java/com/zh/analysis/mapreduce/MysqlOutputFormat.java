package com.zh.analysis.mapreduce;

import com.zh.analysis.bean.ComDimension;
import com.zh.analysis.converter.DimensionConverterImpl;
import com.zh.analysis.util.JDBCInstance;
import com.zh.analysis.value.CountDurationValue;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/4/12 16:55
 */
public class MysqlOutputFormat extends OutputFormat<ComDimension, CountDurationValue> {

    @Override
    public RecordWriter<ComDimension, CountDurationValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection conn = null;
        try {
            conn = JDBCInstance.getInstance();
            conn.setAutoCommit(false);  //关闭自动提交
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
        return new MysqlRecordWriter(conn); //返回
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }

    /**
     * RecordWriter:其实主要就是负责将task的key/value结果写入内存或者磁盘
     */
    private class MysqlRecordWriter extends RecordWriter<ComDimension, CountDurationValue> {

        private DimensionConverterImpl dci = new DimensionConverterImpl();
        private Connection conn = null;
        private PreparedStatement preparedStatement = null;
        private String insertSQL = null;
        private int count = 0;
        private final int BATCH_SIZE = 500;

        public MysqlRecordWriter(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void write(ComDimension key, CountDurationValue value) throws IOException, InterruptedException {
            try {
                //tb_call
                //id_date_contact, id_date_dimension, id_contact, call_sum, call_duration_sum

                //year month day
                int idDateDimension = dci.getDimensionID(key.getDateDimension());
                //telephone name
                int idContactDimension = dci.getDimensionID(key.getContactDimension());

                String idDateContact = idDateDimension + "_" + idContactDimension;

                int callSum = Integer.valueOf(value.getCallSum());
                int callDurationSum = Integer.valueOf(value.getCallDurationSum());

                if(insertSQL == null){
                    insertSQL = "INSERT INTO `tb_call` (`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id_date_contact` = ?;";
                }

                if(preparedStatement == null){
                    preparedStatement = conn.prepareStatement(insertSQL);
                }
                //本次SQL
                int i = 0;
                preparedStatement.setString(++i, idDateContact);
                preparedStatement.setInt(++i, idDateDimension);
                preparedStatement.setInt(++i, idContactDimension);
                preparedStatement.setInt(++i, callSum);
                preparedStatement.setInt(++i, callDurationSum);
                //无则插入，有则更新的判断依据
                preparedStatement.setString(++i, idDateContact);
                preparedStatement.addBatch();
                count++;
                if(count >= BATCH_SIZE){
                    preparedStatement.executeBatch(); //提交
                    conn.commit();
                    count = 0;
                    preparedStatement.clearBatch(); //清除本批次
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }
    }
}
