package com.maven.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @Classname com.maven.test.MavenMain
 * @Description TODO
 * @Date 2021/11/25 下午10:02
 * @Created by mac
 */
public class MavenMain {
    public static void main(String[] args  ){
        // BufferedReader是可以按行读取文件
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Thread
                .currentThread().getContextClassLoader().getResourceAsStream("Maven.class")));

        StringBuilder config = new StringBuilder();
        String str = null;
        try {
            while ((str = bufferedReader.readLine()) != null) {
                config.append(str);
            }
            System.out.println(config.toString());
        } catch (Exception e) {

        } finally {
            try {
                bufferedReader.close();
            } catch (Exception e1) {

            }
        }

    }
}
