package com.huihui.mr;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	  
	  /*Hadoop没有使用Java.util.Properties管理配置文件，也没有使用Apache Jakarta Commons Configuration管理配置文件，
	   * 而是使用了一套独有的配置文件管理系统，并提供自己的API，即使用org.apache.hadoop.conf.Configuration处理配置信息。
	   */
    Configuration conf = new Configuration();
    /*
     * 这里分析以下Hadoop的GenericOptionsParser类
它能够解析命令行参数的基本类。它能够辨别一些标准的命令行参数。
比如这里的-D mapreduce.job.queuename  就被它识别了，并且配置到了参数文件中去，而函数getRemainingArgs()就是获取了
剩余的两个参数"xrli/STJoin_in","xrli/STJoin_out"，并且将它们组合为数组otherArgs。

它能够识别的参数包括： fs jt libjars files archives D tokenCacheFile
     */
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    conf.set("fs.defaultFS", "hdfs://localhost:9000");
  //创建一个新作业
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    
  //指定各种特定于作业的参数 
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //Path    对路径进行解析，将参数转换为标准的URI格式，对Path的参数作判断，标准化，字符化等操作。为了便于理解Path，
    String input = "hdfs://localhost:9000/input/";
    String output = "hdfs://localhost:9000/user/hdfs/log_kpi/browser1";
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
  //提交作业，然后轮询进度，直到作业完成
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
