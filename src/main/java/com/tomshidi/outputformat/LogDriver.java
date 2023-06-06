package com.tomshidi.outputformat;

import com.tomshidi.mapreduce.writable.FlowBean;
import com.tomshidi.mapreduce.writable.FlowMapper;
import com.tomshidi.mapreduce.writable.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author TomShiDi
 * @since 2023/5/26 11:50
 */
public class LogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(LogDriver.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定义分区器
//        job.setPartitionerClass(ProvincePartitioner.class);
        // 设置reducer个数
//        job.setNumReduceTasks(5);

        // 指定自定义输出类
        job.setOutputFormatClass(LogOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\inputlog\\web-simple.log"));
        // 由于上面制定了仅输出一个_SUCCESS标记文件
        FileOutputFormat.setOutputPath(job, new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\output"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
