package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by cwc on 2017/3/31 0031.
 */
@RunWith(JUnit4.class)
public class WritableTest {
    public static class SimpleWritable implements Writable {

        int state;

        public SimpleWritable() {
        }

        public SimpleWritable(int state) {
            this.state = state;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.write(state);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            state = in.readInt();
        }

        public static SimpleWritable read(DataInput in) throws IOException {
            SimpleWritable result = new SimpleWritable();
            result.readFields(in);
            return result;
        }

        public boolean equals(Object o) {
            if (!(o instanceof SimpleWritable))
                return false;
            SimpleWritable other = (SimpleWritable) o;
            return this.state == other.state;
        }
    }

    private Writable testWritable(Writable before, Configuration conf) throws IOException {
        DataOutputBuffer out = new DataOutputBuffer();
        before.write(out);

        Writable after = ReflectionUtils.newInstance(before.getClass(), conf);

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());

        after.readFields(in);
        Assert.assertEquals(before, after);
        return after;

    }

    private Writable testWritable(Writable before) throws IOException {
        return testWritable(before, null);
    }

    @Test
    public void testSimpleWritable() throws IOException {
        testWritable(new SimpleWritable(256));
    }

    @Test
    public void testByteWritable() throws IOException {
        testWritable(new ByteWritable((byte) 128));
    }

    @Test
    public void testDoubleWritable() throws IOException {
        testWritable(new DoubleWritable(12.3D));
    }


}