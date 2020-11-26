package com.accakyra.lsss.lsm.util.iterators;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.*;

public class MergedIterator<T extends Comparable<T>> implements Iterator<T> {

    private class IndexedIterator implements Comparable<IndexedIterator> {

        private final PeekingIterator<T> iterator;
        private final int index;

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

    public MergedIterator(List<Iterator<T>> iterators) {
        this.heap = new PriorityQueue<>();
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<T> iterator = iterators.get(i);
            IndexedIterator indexedIterator = new IndexedIterator(Iterators.peekingIterator(iterator), i);
            addIterator(indexedIterator);
        }
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public T next() {
        IndexedIterator nextIterator = heap.poll();
        T record = nextIterator.iterator.next();
        addIterator(nextIterator);
        return record;
    }

    private void addIterator(IndexedIterator indexedIterator) {
        if (indexedIterator.iterator.hasNext()) {
            heap.add(indexedIterator);
        }
    }
}
