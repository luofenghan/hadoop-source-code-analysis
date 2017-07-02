package org.apache.hadoop.ipc;

/**
 * Created by cwc on 2017/4/18 0018.
 */
public interface Library extends VersionedProtocol {
    Book queryBook(String bookName);
}
