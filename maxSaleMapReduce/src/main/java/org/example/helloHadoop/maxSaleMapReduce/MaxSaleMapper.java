package org.example.helloHadoop.maxSaleMapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.helloHadoop.common.entity.SaleDataEntity;
import org.example.helloHadoop.common.parse.SaleDataParse;
import org.example.helloHadoop.common.parse.TextParse;

import java.io.IOException;

/**
 * 解析输入的文本数据
 */
public class MaxSaleMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
    protected TextParse<SaleDataEntity> saleDataParse = new SaleDataParse();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        SaleDataEntity data = saleDataParse.parse(s);
        if (data != null) {
            //写入输出给Reducer
            context.write(new LongWritable(data.userId), new IntWritable(data.saleCount));
        }
    }
}
