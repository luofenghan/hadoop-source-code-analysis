package org.apache.hadoop.ipc;

import java.io.IOException;

/**
 * Created by cwc on 2017/4/18 0018.
 */
public class LibraryImpl implements Library {

    @Override
    public Book queryBook(String bookName) {
        return new Book(bookName, "落枫寒", 23);
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        System.out.println(protocol);
        return BookQueryServer.IPC_VER;
    }
}
