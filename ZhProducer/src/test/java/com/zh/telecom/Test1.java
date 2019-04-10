package com.zh.telecom;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/4/9 18:11
 */
public class Test1 {

    public static void main(String[] args) {
        while (true){
            try {
                Thread.sleep(200L);
                System.out.println(Math.random());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
