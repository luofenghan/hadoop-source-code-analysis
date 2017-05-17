package com.learn.hadoop.ipc.book;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Created by cwc on 2017/4/18 0018.
 */
public interface Library extends VersionedProtocol {
    Book queryBook(String bookName);
}
