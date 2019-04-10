package com.zh.telecom.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author zhanghe
 * @Desc: 读取配置文件
 * @Date 2019/4/9 20:26
 */
public class PropertiesUtil {

    public static Properties properties = null;

    //读取properties文件，加载给变量
    static {
        InputStream is = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取配置文件的值
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

}
