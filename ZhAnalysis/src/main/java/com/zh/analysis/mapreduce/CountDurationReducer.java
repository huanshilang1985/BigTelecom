package com.zh.analysis.mapreduce;

import com.zh.analysis.bean.ComDimension;
import com.zh.analysis.value.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/4/12 16:33
 */
public class CountDurationReducer extends Reducer<ComDimension, Text, ComDimension, CountDurationValue> {

    CountDurationValue countDurationValue = new CountDurationValue();

    @Override
    protected void reduce(ComDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int callSum = 0;
        int callDurationSum = 0;
        for(Text t : values) {
           callSum++;
           callDurationSum += Integer.valueOf(t.toString());
        }
        countDurationValue.setCallSum(String.valueOf(callSum));
        countDurationValue.setCallDurationSum(String.valueOf(callDurationSum));
        context.write(key, countDurationValue);
    }
}
