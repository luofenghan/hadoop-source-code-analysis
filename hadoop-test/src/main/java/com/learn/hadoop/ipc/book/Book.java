package com.learn.hadoop.ipc.book;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by cwc on 2017/4/18 0018.
 */
public class Book implements Writable {
    private Text author;
    private Text bookName;
    private IntWritable price;

    public Book() {
        author = new Text();
        bookName = new Text();
        price = new IntWritable();
    }

    public Book(String bookName, String author, int price) {
        this.author = new Text(author);
        this.bookName = new Text(bookName);
        this.price = new IntWritable(price);
    }

    public void setAuthor(Text author) {
        this.author = author;
    }

    public Text getBookName() {
        return bookName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Book book = (Book) o;

        if (author != null ? !author.equals(book.author) : book.author != null) return false;
        if (bookName != null ? !bookName.equals(book.bookName) : book.bookName != null) return false;
        return price != null ? price.equals(book.price) : book.price == null;
    }

    @Override
    public int hashCode() {
        int result = author != null ? author.hashCode() : 0;
        result = 31 * result + (bookName != null ? bookName.hashCode() : 0);
        result = 31 * result + (price != null ? price.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Book{" +
                "author=" + author +
                ", bookName=" + bookName +
                ", price=" + price +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        author.write(out);
        bookName.write(out);
        price.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        author.readFields(in);
        bookName.readFields(in);
        price.readFields(in);
    }
}
