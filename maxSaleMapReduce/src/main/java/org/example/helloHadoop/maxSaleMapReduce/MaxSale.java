package org.example.helloHadoop.maxSaleMapReduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 启动程序
 */
public class MaxSale {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(MaxSale.class);
        job.setJobName("MaxSale");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxSaleMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MaxSaleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置Reduce任务数
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
