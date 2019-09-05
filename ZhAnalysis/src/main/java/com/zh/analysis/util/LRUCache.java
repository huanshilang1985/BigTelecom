package com.zh.analysis.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author zhanghe
 * @Desc: 缓存工具类
 * @Date 2019/4/12 17:11
 */
public class LRUCache extends LinkedHashMap<String, Integer> {

    private static final long serialVersionUID = 1L;

    protected int maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
        return (size() > this.maxElements);
    }


}
