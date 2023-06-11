package com.tomshidi.mapperjoin;

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
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author TomShiDi
 * @since 2023/6/11 15:15
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // 1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 2.设置jar包路径
        job.setJarByClass(MapJoinDriver.class);
        // 3.关联mapper和reducer
        job.setMapperClass(MapJoinMapper.class);

        // 4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5.设置最终输出的kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("file:///D:/Personal-Projects/hdfsclient/src/main/resources/inputtable/pd.txt"));
        job.setNumReduceTasks(0);

        // 6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\inputtable\\order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\output-table-2"));
        // 7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
