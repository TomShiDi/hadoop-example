package com.tomshidi.mapreduce.comparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author TomShiDi
 * @since 2023/5/24 18:58
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowReducer.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\output"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\output-second"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
