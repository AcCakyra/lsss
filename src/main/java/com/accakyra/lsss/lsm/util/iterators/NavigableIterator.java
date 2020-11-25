package com.accakyra.lsss.lsm.util.iterators;

import java.util.Iterator;
import java.util.NavigableSet;

public class NavigableIterator<T extends Comparable<T>> implements Iterator<T> {

    private final NavigableSet<T> resource;
    private T current;
    private final T to;

    public NavigableIterator(NavigableSet<T> resource, T from, T to) {
        this.resource = resource;
        this.current = resource.ceiling(from);
        this.to = to;
    }

    @Override
    public boolean hasNext() {
        if (current == null) return false;
        if (to != null) return current.compareTo(to) < 0;
        return true;
    }

    @Override
    public T next() {
        T next = current;
        nextKey();
        return next;
    }

    private void nextKey() {
        current = resource.higher(current);
    }
}
