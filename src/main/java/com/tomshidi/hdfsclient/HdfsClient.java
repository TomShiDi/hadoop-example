package com.tomshidi.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFS 客户端操作
 */
public class HdfsClient {

    private static FileSystem fileSystem;

    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {
        URI uri = new URI("hdfs://hadoop2:8020");
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        String user = "root";
        fileSystem = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void testMkDir() throws URISyntaxException, IOException, InterruptedException {

        fileSystem.mkdirs(new Path("/xiyou/huaguoshan"));
        fileSystem.close();
    }

    @Test
    public void testUpload() throws IOException {
        Path srcPath = new Path("D:\\Personal-Projects\\hdfsclient\\src\\main\\resources\\log4j.properties");
        Path dstPath = new Path("/temp/slf4j.properties");
        fileSystem.copyFromLocalFile(false, true, srcPath, dstPath);
    }

    @Test
    public void testDownload() throws IOException {
        Path srcPath = new Path("/temp/slf4j.properties");
        Path dstPath = new Path("D://slf4j.properties");
        fileSystem.copyToLocalFile(false, srcPath, dstPath, false);
    }

    @Test
    public void testDel() throws IOException {
        Path srcPath = new Path("/temp/slf4j.properties");
        fileSystem.delete(srcPath, false);
    }
}
