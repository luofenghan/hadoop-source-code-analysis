/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Checksum;

/**
 * This is a generic output stream for generating checksums for
 * data before it is written to the underlying stream
 */

abstract public class FSOutputSummer extends OutputStream {
    private Checksum sum; //计算校验和的Checksum
    private byte buf[]; //输出数据缓冲区，大小一般为512（待计算校验和的数据）
    private byte checksum[]; // 校验和缓冲区
    private int count; //buf已使用空间计数

    protected FSOutputSummer(Checksum sum, int maxChunkSize, int checksumSize) {
        this.sum = sum;
        this.buf = new byte[maxChunkSize];
        this.checksum = new byte[checksumSize];
        this.count = 0;
    }

    /* write the data chunk in <code>b</code> staring at <code>offset</code> with
     * a length of <code>len</code>, and its checksum
     */
    protected abstract void writeChunk(byte[] b, int offset, int len, byte[] checksum)
            throws IOException;

    /**
     * Write one byte
     */
    public synchronized void write(int b) throws IOException {
        sum.update(b);
        buf[count++] = (byte) b;
        if (count == buf.length) {
            flushBuffer();
        }
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> and generate a checksum for
     * each data chunk.
     * <p>
     * <p> This method stores bytes from the given array into this
     * stream's buffer before it gets checksumed. The buffer gets checksumed
     * and flushed to the underlying output stream when all data
     * in a checksum chunk are in the buffer.  If the buffer is empty and
     * requested length is at least as large as the size of next checksum chunk
     * size, this method will checksum and write the chunk directly
     * to the underlying output stream.  Thus it avoids uneccessary data copy.
     *
     * @param b   当前要输出的数据缓冲区.
     * @param off 数据的起始偏移量.
     * @param len 要输出数据的长度.
     * @throws IOException if an I/O error occurs.
     */
    public synchronized void write(byte b[], int off, int len)
            throws IOException {
        //如果 起始偏移量<0 或 长度<0 或 起始偏移量在要输出的数据之后
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        /*n表示已写的字节数*/
        for (int n = 0; n < len; n += write1(b, off + n, len - n)) ;
    }

    /**
     * Write a portion of an array, flushing to the underlying
     * stream at most once if necessary.
     */
    private int write1(byte b[], int off, int len) throws IOException {
        if (count == 0 && len >= buf.length) {
            //本地缓冲区为空 并且 输出数据大于一个校验块，则可以计算出校验和
            final int length = buf.length; //已输出数据的长度
            sum.update(b, off, length);//为校验和算法提供数据，并更新校验和
            writeChecksumChunk(b, off, length, false);//输出数据，包括'数据文件'和'校验和数据'
            return length;
        }
        /*说明此时 本地缓冲区有部分数据 或者 输出数据长度小于一个校验块*/

        // copy user data to local buffer
        int bytesToCopy = buf.length - count;// 理论上 缓冲区可以存入的数据，剩余空间长度决定
        bytesToCopy = Math.min(len, bytesToCopy); // 实际上 缓冲区能够存入的数据，输出数据长度决定
        sum.update(b, off, bytesToCopy); //为校验和算法提供数据，并更新校验和
        System.arraycopy(b, off, buf, count, bytesToCopy);
        count += bytesToCopy; //更新缓冲区的数据长度
        if (count == buf.length) {
            flushBuffer(); //如果缓冲区已满，已经读满一个校验块，可以输出
        }
        return bytesToCopy;
    }

    /* Forces any buffered output bytes to be checksumed and written out to
     * the underlying output stream.
     */
    protected synchronized void flushBuffer() throws IOException {
        flushBuffer(false);
    }

    /* Forces any buffered output bytes to be checksumed and written out to
     * the underlying output stream.  If keep is true, then the state of
     * this object remains intact.
     */
    protected synchronized void flushBuffer(boolean keep) throws IOException {
        if (count != 0) {
            int chunkLen = count;
            count = 0;
            writeChecksumChunk(buf, 0, chunkLen, keep);
            if (keep) {
                count = chunkLen;
            }
        }
    }

    /**
     * Generate checksum for the data chunk and output data chunk & checksum
     * to the underlying output stream. If keep is true then keep the
     * current checksum intact, do not reset it.
     */
    private void writeChecksumChunk(byte b[], int off, int len, boolean keep)
            throws IOException {
        int tempChecksum = (int) sum.getValue();
        if (!keep) {
            sum.reset();
        }
        int2byte(tempChecksum, checksum);
        writeChunk(b, off, len, checksum);
    }

    /**
     * Converts a checksum integer value to a byte stream
     */
    static public byte[] convertToByteStream(Checksum sum, int checksumSize) {
        return int2byte((int) sum.getValue(), new byte[checksumSize]);
    }

    static byte[] int2byte(int integer, byte[] bytes) {
        bytes[0] = (byte) ((integer >>> 24) & 0xFF);
        bytes[1] = (byte) ((integer >>> 16) & 0xFF);
        bytes[2] = (byte) ((integer >>> 8) & 0xFF);
        bytes[3] = (byte) ((integer >>> 0) & 0xFF);
        return bytes;
    }

    /**
     * Resets existing buffer with a new one of the specified size.
     */
    protected synchronized void resetChecksumChunk(int size) {
        sum.reset();
        this.buf = new byte[size];
        this.count = 0;
    }
}
