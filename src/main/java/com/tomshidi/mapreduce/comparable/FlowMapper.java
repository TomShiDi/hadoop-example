package com.tomshidi.mapreduce.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author TomShiDi
 * @since 2023/5/24 17:36
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private Text outV = new Text();

    private FlowBean outK = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = line.split("\t");

        outK.setUpFlow(Long.parseLong(strings[1]));
        outK.setDownFlow(Long.parseLong(strings[2]));
        outK.setSumFlow();
        outV.set(strings[0]);

        context.write(outK, outV);
    }
}
