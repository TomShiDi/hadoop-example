package com.tomshidi.etl;

import com.tomshidi.reducejoin.TableBean;
import com.tomshidi.reducejoin.TableDriver;
import com.tomshidi.reducejoin.TableMapper;
import com.tomshidi.reducejoin.TableReducer;
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
 * @since 2023/6/11 15:53
 */
public class WebLogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(WebLogDriver.class);
        job.setMapperClass(WebLogMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\inputlog\\web.log"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\output-log"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
