package com.accakyra.lsss.lsm.util.iterators;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

public class DistinctIterator<T extends Comparable<T>> implements Iterator<T> {

    private final PeekingIterator<T> iterator;

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
        while (hasNext() && record.compareTo(iterator.peek()) == 0) {
            iterator.next();
        }
        return record;
    }
}
