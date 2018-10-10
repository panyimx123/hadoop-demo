package com.ypan2.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by test on 2018/9/10.
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    private static final Logger logger = LoggerFactory.getLogger(MaxTemperatureMapper.class);

    @Override
    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15, 19);
        logger.info("line {}, year {}, line.charAt(87) {}", line, year);
        int airTemperature;
        if (line.charAt(37) == '+') {
            airTemperature = Integer.parseInt(line.substring(38, 42));
        } else {
            airTemperature = Integer.parseInt(line.substring(37, 42));
        }
        String quality = line.substring(42, 43);
        System.out.println("airTemperature :" + airTemperature + "quality :" + quality);
        if (airTemperature != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable((airTemperature)));
        }
    }
}
