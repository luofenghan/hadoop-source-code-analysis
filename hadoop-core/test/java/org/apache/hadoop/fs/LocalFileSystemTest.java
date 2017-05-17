package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/**
 * Created by cwc on 2017/4/27 0027.
 */
@RunWith(JUnit4.class)
public class LocalFileSystemTest {
    private Configuration configuration;

    @Before
    public void setup() {
        configuration = new Configuration();
        configuration.setClass("fs.file.impl", LocalFileSystem.class, FileSystem.class);
    }

    @Test
    public void crc() throws IOException {
        LocalFileSystem localFileSystem = FileSystem.getLocal(configuration);

        Path path = new Path("C://Users/cwc/Desktop/cwc.txt");
        FSDataInputStream in = localFileSystem.open(path);

        readBuffer(in,554);

        IOUtils.cleanup(null, in);
    }

    private void read(FSDataInputStream in) throws IOException {
        for (int i = 0, len = in.available(); i < len; i++) {
            System.out.println(in.read());
        }

    }

    private void readBuffer(FSDataInputStream in,int len) throws IOException {
        byte[] buffer = new byte[len];
        int read = -1;
        while ((read = in.read(buffer, 0, buffer.length)) != -1) {
            System.out.println(new String(buffer, "utf-8"));
        }
    }
}