package com.accakyra.lsss.lsm.util.iterators;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

public class DistinctIterator<T extends Comparable<T>> implements Iterator<T> {

    private PeekingIterator<T> iterator;

    public DistinctIterator(Iterator<T> iterator) {
        this.iterator = Iterators.peekingIterator(iterator);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        T record = iterator.next();
        if (hasNext()) {
            T next = iterator.peek();
            while (record.compareTo(next) == 0) {
                iterator.next();
                if (hasNext()) next = iterator.peek();
                else break;
            }
        }
        return record;
    }
}
