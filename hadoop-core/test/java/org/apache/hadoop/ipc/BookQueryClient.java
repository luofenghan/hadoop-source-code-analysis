package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by cwc on 2017/4/18 0018.
 */
public class BookQueryClient {
    public static void main(String[] args) throws IOException {
        InetSocketAddress address = new InetSocketAddress("localhost", BookQueryServer.QUERY_PORT);
        Library query = (Library) RPC.getProxy(Library.class, BookQueryServer.IPC_VER, address, new Configuration());

        Book book = query.queryBook("Hadoop权威指南");

        System.out.println("查到的书为：" + book.toString());

        RPC.stopProxy(query);
    }
}
