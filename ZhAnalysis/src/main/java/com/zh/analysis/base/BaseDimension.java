package com.zh.analysis.base;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/4/11 19:49
 */
public abstract class BaseDimension implements WritableComparable<BaseDimension> {

    @Override
    public abstract int compareTo(BaseDimension o);

    @Override
    public abstract void write(DataOutput dataOutput) throws IOException;

    @Override
    public abstract void readFields(DataInput dataInput) throws IOException;

}
