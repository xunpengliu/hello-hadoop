package org.example.helloHadoop.maxSaleMapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.helloHadoop.common.entity.RuntimeInfoEntity;
import org.example.helloHadoop.common.entity.SaleDataEntity;
import org.example.helloHadoop.common.parse.SaleDataParse;
import org.example.helloHadoop.common.parse.TextParse;
import org.example.helloHadoop.common.tool.RuntimeInfoTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * 解析输入的文本数据
 */
public class MaxSaleMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    protected TextParse<SaleDataEntity> saleDataParse = new SaleDataParse();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        RuntimeInfoEntity runInfo = RuntimeInfoTool.getRunInfo();
        String startTime = null;
        if (runInfo.getStartTime() != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            startTime = sdf.format(runInfo.getStartTime());
        }
        logger.info("mapper task,ip->{},startTime->{},mainClass->{},pid->{}",
                runInfo.getIpv4(),
                startTime,
                runInfo.getMainClass(),
                runInfo.getPid());
    }

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
