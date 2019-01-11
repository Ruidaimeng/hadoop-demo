package com.imooc.hadoop.demo;/*
 * Description
 *@author Ruimeng
 *@Date 2019/1/11 14:04
 * 使用MapReduce计算WordCount
 *
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountApp {


    /*
    * 自定义Map类，用于处理输入文件
    * */
    //前两个是输入的key ，value （读取的起始位置，读取到的文件内容）
    //后面两个是输出的key,value(统计分类后的单词《字符串》，和出现的频次)
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
        LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           // super.map(key, value, context);
            //接受到的每一行数据
            String line = value.toString();
            //按规则就行数据拆分
            String [] words = line.split(" ") ;
            for (String word:words) {
               //通过上下文将map处理的结果输出
                context.write(new Text(word),one);

            }

        }
    }
/*
* 自定义reducer,进行归并操作
* */
//前2个是输入，单词和出现的次数，也就是上面Mapper的输出
 //后2个是输出，统计后的单词和出现的次数，
    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        long sum =0 ;
        //求单词key出现的次数
        for (LongWritable value:values) {
                    sum=sum+value.get();
        }
        context.write(key,new LongWritable(sum));
    }
}
/*
* 定义Driver
* */
    public static void main(String[] args) throws Exception {
        Configuration configuration =new Configuration();
        //创建作业
       Job job = Job.getInstance(configuration,"WordCount");
       //设置job的处理类
        job.setJarByClass(WordCountApp.class);
        //设置作业处理的输入路径 ,调用main方法时、输入
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //设置map相关参数
         job.setMapperClass(MyMapper.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(LongWritable.class);

         //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

      //设置作业的输出处理路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(   job.waitForCompletion(true)?0:1);

    }
}
