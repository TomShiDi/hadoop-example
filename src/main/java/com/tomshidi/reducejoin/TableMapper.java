package com.tomshidi.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author TomShiDi
 * @since 2023/6/5 21:42
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String typeName = null;

    private Text outK = new Text();

    private TableBean outV = new TableBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        typeName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (typeName.contains("order")) {
            String[] splits = line.split("\t");
            outK.set(splits[1]);
            outV.setId(splits[0]);
            outV.setPid(splits[1]);
            outV.setAmount(Integer.parseInt(splits[2]));
            outV.setPname("");
            outV.setFrom("order");
        } else if (typeName.contains("pd")) {
            String[] splits = line.split("\t");
            outK.set(splits[0]);
            outV.setId("");
            outV.setPid(splits[0]);
            outV.setAmount(0);
            outV.setPname(splits[1]);
            outV.setFrom("pd");
        }
        context.write(outK, outV);
    }
}
