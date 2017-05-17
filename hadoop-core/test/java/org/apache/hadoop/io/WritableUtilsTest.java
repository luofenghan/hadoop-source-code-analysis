package org.apache.hadoop.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by cwc on 2017/3/31 0031.
 */
@RunWith(JUnit4.class)
public class WritableUtilsTest {
    private static final Log LOG = LogFactory.getLog(WritableUtilsTest.class);

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {

    }

    private void testValue(int val, int vIntLen) throws IOException {
        DataOutputBuffer out = new DataOutputBuffer();
        DataInputBuffer in = new DataInputBuffer();
        WritableUtils.writeVInt(out, val);

        if (LOG.isDebugEnabled()) {
            System.out.println("Value = " + val);
            BytesWritable printer = new BytesWritable();
            printer.set(out.getData(), 0, out.getLength());
            System.out.println("Buffer = " + printer);
        }
        in.reset(out.getData(), 0, out.getLength());
        assertEquals(val, WritableUtils.readVInt(in));
        assertEquals(vIntLen, out.getLength());
        assertEquals(vIntLen, WritableUtils.getVIntSize(val));
        assertEquals(vIntLen, WritableUtils.decodeVIntSize(out.getData()[0]));
    }

    /**
     * 65536 <= x :4
     *
     * @throws Exception
     */
    @Test
    public void test_gt65536() throws Exception {
        testValue(65535, 3);
        testValue(65536, 4);
        testValue(65545, 4);
    }

    /**
     * 256 <= x < 65536 :3
     *
     * @throws Exception
     */
    @Test
    public void test_gt256_lt65536() throws Exception {
        testValue(255, 2);
        testValue(256, 3);
        testValue(257, 3);
        testValue(65535, 3);
        testValue(65536, 4);
    }

    /**
     * 128 <= x < 256 : 2
     *
     * @throws Exception
     */
    @Test
    public void test_gt128_lt256() throws Exception {
        testValue(127, 1);
        testValue(128, 2);
        testValue(255, 2);
        testValue(256, 3);
    }

    /**
     * -112 <= x < 128 ：1
     */
    @Test
    public void test_gtNe112_lt128() throws IOException {
        testValue(-113, 2);
        testValue(-112, 1);
        testValue(127, 1);
        testValue(128, 2);
    }

    /**
     * -256 <= x < -112 : 2
     */
    @Test
    public void test_gtNe256_ltNe112() throws IOException {
        testValue(-257, 3);
        testValue(-256, 2);
        testValue(-113, 2);
        testValue(-112, 1);
    }

    /**
     * -65536 <= x < -256 :3
     */
    @Test
    public void test_gtNe65536_ltNe256() throws IOException {
        testValue(-65537, 4);
        testValue(-65536, 3);
        testValue(-257, 3);
        testValue(-256, 2);

    }

    /**
     * x < -65536 :4
     */
    @Test
    public void test_gtNe65536() throws IOException {
        testValue(-65538, 4);
        testValue(-65537, 4);
        testValue(-65536, 3);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testVInt() throws Exception {
        testValue(-65538, 4);
        testValue(-65537, 4);
        testValue(-65536, 3);
        testValue(-257, 3);
        testValue(-256, 2);
        testValue(-113, 2);
        testValue(-112, 1);
        testValue(127, 1);
        testValue(128, 2);
        testValue(255, 2);
        testValue(256, 3);
        testValue(257, 3);
        testValue(65535, 3);
        testValue(65536, 4);
    }
}