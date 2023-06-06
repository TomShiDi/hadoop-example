package com.tomshidi.mapreduce.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @author TomShiDi
 * @since 2023-5-25 19:18:30
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        int partition = Integer.parseInt(prePhone) % 5;

        return partition;
    }
}
