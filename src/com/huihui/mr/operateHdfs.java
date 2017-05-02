package com.huihui.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class operateHdfs {
	public static void main(String args[]) throws IOException {
        //测试 创建新文件
        byte[] contents = "hello world\n--created by eclipse\n".getBytes();
        createFile("/input/first.txt", contents);    //或 createFile("hdfs://192.168.137.56:9000/eclipse/first.txt", contents); 
    }

    //1、创建新文件（直接生成指定路径下的first.txt，即：/eclipse/first.txt）
    public static void createFile(String dst, byte[] contents) throws IOException {
        Configuration conf = new Configuration();
        System.out.println("-----------:"+conf);
        conf.set("fs.defaultFS", "hdfs://localhost:9000");    //master
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(dst); // 目标路径
        // 打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
    }
}
