package org.example.helloHadoop.maxSaleMapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.helloHadoop.common.entity.RuntimeInfoEntity;
import org.example.helloHadoop.common.tool.RuntimeInfoTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

/**
 * 查找每一个用户的最大销售值
 */
public class MaxSaleReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        RuntimeInfoEntity runInfo = RuntimeInfoTool.getRunInfo();
        String startTime = null;
        if (runInfo.getStartTime() != null) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            startTime = sdf.format(runInfo.getStartTime());
        }
        logger.info("reducer task setup,ip->{},startTime->{},mainClass->{},pid->{}",
                runInfo.getIpv4(),
                startTime,
                runInfo.getMainClass(),
                runInfo.getPid());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        RuntimeInfoEntity runInfo = RuntimeInfoTool.getRunInfo();
        logger.info("reducer task cleanup,ip->{},pid->{},mainClass->{},args->{}",
                runInfo.getIpv4(),
                runInfo.getPid(),
                runInfo.getMainClass(),
                Arrays.toString(runInfo.getArgs()));
    }

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
