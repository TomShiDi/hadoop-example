package com.tomshidi.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author TomShiDi
 * @since 2023/5/24 17:36
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();

    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = line.split(",");

        String phone = strings[1];
        String upFlow = strings[strings.length - 3];
        String downFlow = strings[strings.length - 2];

        outK.set(phone);
        outV.setUpFlow(Long.parseLong(upFlow));
        outV.setDownFlow(Long.parseLong(downFlow));
        outV.setSumFlow();

        context.write(outK, outV);
    }
}
