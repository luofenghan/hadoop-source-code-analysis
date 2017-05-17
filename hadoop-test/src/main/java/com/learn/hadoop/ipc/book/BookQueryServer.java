package com.learn.hadoop.ipc.book;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

/**
 * Created by cwc on 2017/4/18 0018.
 */
public class BookQueryServer {
    public static final int QUERY_PORT = 33121;
    public static final long IPC_VER = 5473L;

    public static void main(String[] args) {
        try {
            Library library = new LibraryImpl();

            Server server = RPC.getServer(library, "0.0.0.0", QUERY_PORT, new Configuration());
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
