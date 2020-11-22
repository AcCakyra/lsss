package com.accakyra.lsss.lsm.util.iterators;

import com.accakyra.lsss.CloseableIterator;

import java.util.Iterator;

public class CloseableIteratorImpl<T> implements CloseableIterator<T> {

    private final Iterator<T> iterator;
    private final AutoCloseable closeable;

    public CloseableIteratorImpl(Iterator<T> iterator, AutoCloseable closeable) {
        this.iterator = iterator;
        this.closeable = closeable;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public void close() throws Exception {
        closeable.close();
    }
}
