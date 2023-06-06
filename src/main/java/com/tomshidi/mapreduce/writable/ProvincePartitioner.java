package com.tomshidi.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @author TomShiDi
 * @since 2023/5/25 14:25
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        int partition = Integer.parseInt(prePhone) % 5;

        return partition;
    }
}
