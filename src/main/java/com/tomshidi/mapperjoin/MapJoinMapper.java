package com.tomshidi.mapperjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author TomShiDi
 * @since 2023/6/11 15:21
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pdMap = new HashMap<>();

    private Text outK = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] strings = line.split("\t");
            pdMap.put(strings[0], strings[1]);
        }
        IOUtils.closeStreams(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = line.split("\t");
        String pname = pdMap.get(strings[1]);
        outK.set(strings[0] + "\t" + pname + "\t" + strings[2]);
        context.write(outK, NullWritable.get());
    }
}
