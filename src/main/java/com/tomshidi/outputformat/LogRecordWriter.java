package com.tomshidi.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author TomShiDi
 * @since 2023/5/26 11:41
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream tommshidiOutputStream;

    private FSDataOutputStream othersOutputStream;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            tommshidiOutputStream = fs.create(new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\output\\tomshidi.log"));
            othersOutputStream = fs.create(new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\output\\others.log"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String s = key.toString();
        if (s.contains("tomshidi")) {
            tommshidiOutputStream.writeBytes(s + "\n");
        } else {
            othersOutputStream.writeBytes(s + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(tommshidiOutputStream, othersOutputStream);
    }
}
