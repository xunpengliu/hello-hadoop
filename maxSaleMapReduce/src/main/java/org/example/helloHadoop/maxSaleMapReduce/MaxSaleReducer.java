package org.example.helloHadoop.maxSaleMapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 查找每一个用户的最大销售值
 */
public class MaxSaleReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = 0;
        for (IntWritable value : values) {
            if (value.get() > max) {
                max = value.get();
            }
        }
        context.write(key, new IntWritable(max));
    }
}
