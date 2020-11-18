package com.accakyra.lsss.lsm.util.iterators;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.*;

public class MergedIterator<T extends Comparable<T>> implements Iterator<T> {

    private class IndexedIterator implements Comparable<IndexedIterator> {
        PeekingIterator<T> iterator;
        int index;

        public IndexedIterator(PeekingIterator<T> iterator, int index) {
            this.iterator = iterator;
            this.index = index;
        }

        @Override
        public int compareTo(IndexedIterator o) {
            int compare = iterator.peek().compareTo(o.iterator.peek());
            if (compare == 0) {
                return index - o.index;
            } else return compare;
        }
    }

    private final Queue<IndexedIterator> heap;
    private T current;

    public MergedIterator(List<Iterator<T>> iterators) {
        this.heap = new PriorityQueue<>();
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<? extends T> iterator = iterators.get(i);
            if (iterator.hasNext()) {
                heap.add(new IndexedIterator(Iterators.peekingIterator(iterator), i));
            }
        }
        nextRecord();
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public T next() {
        T record = current;
        nextRecord();
        return record;
    }

    private void nextRecord() {
        if (!heap.isEmpty()) {
            IndexedIterator nextIterator = heap.poll();
            current = nextIterator.iterator.next();
            if (nextIterator.iterator.hasNext()) {
                heap.add(nextIterator);
            }
        } else {
            current = null;
        }
    }
}
