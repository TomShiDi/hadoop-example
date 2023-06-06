package com.tomshidi.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author TomShiDi
 * @since 2023/5/24 18:54
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long totalUp = 0;
        long totalDown = 0;

        for (FlowBean flowBean : values) {
            totalUp = totalUp + flowBean.getUpFlow();
            totalDown = totalDown + flowBean.getDownFlow();
        }
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        context.write(key, outV);
    }
}
