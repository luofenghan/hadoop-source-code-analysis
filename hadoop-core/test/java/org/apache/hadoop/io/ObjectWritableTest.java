package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Created by cwc on 2017/3/31 0031.
 */
@RunWith(JUnit4.class)
public class ObjectWritableTest {
    @Test
    public void readFields() throws Exception {

    }

    private void writeAndRead(Object value) throws Exception {
        ObjectWritable before = new ObjectWritable(value);
        before.setConf(new Configuration());

        DataOutputBuffer out = new DataOutputBuffer();
        before.write(out);

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());

        ObjectWritable after = new ObjectWritable();
        after.setConf(before.getConf());
        after.readFields(in);
        System.out.printf("before:%s, after:%s \n", before.get(), after.get());
        Assert.assertEquals(value, after.get());
    }


    @Test
    public void testWROfString() throws Exception {
        writeAndRead("123");
    }

    @Test
    public void testWROfArray() throws Exception {
        writeAndRead(new String[]{"123", "456", "789"});
    }

    @Test
    public void testWROfInt() throws Exception {
        writeAndRead(new Integer(24));
    }

    @Test
    public void testWROfIntWritable() throws Exception {
        writeAndRead(new IntWritable(24));

    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }


}