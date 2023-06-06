package com.tomshidi.mapreduce.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author TomShiDi
 * @since 2023/5/24 18:54
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
