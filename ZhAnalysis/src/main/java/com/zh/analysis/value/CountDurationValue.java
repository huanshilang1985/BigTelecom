package com.zh.analysis.value;

import com.zh.analysis.base.BaseValue;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc: 总次数值
 * @Date 2019/4/11 20:10
 */
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class CountDurationValue extends BaseValue {

    private String callSum;
    private String callDurationSum;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(callSum);
        out.writeUTF(callDurationSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.callSum = in.readUTF();
        this.callDurationSum = in.readUTF();
    }

}
