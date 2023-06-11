package com.tomshidi.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author TomShiDi
 * @since 2023/6/11 15:47
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(" ");
        if (validation(value.toString(), context)) {
            context.write(value, NullWritable.get());
        }
    }

    private boolean validation(String line, Context context) {
        String[] fields = line.split(" ");
        return fields.length > 11;
    }
}
