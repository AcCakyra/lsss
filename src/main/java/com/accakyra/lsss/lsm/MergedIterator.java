package com.accakyra.lsss.lsm;

import java.util.*;

public class MergedIterator<T extends Comparable<T>> implements Iterator<T> {

    private class IterRecord implements Comparable<IterRecord> {

        private final T record;
        private final int iter;

        public IterRecord(T record, int iter) {
            this.record = record;
            this.iter = iter;
        }

        public T getRecord() {
            return record;
        }

        public int getIter() {
            return iter;
        }

        @Override
        public int compareTo(IterRecord o) {
            int compare = record.compareTo(o.record);
            if (compare == 0) {
                return iter - o.getIter();
            } else {
                return compare;
            }
        }
    }

    private final List<Iterator<T>> iterators;
    private final Queue<IterRecord> heap;
    private IterRecord current;

    public MergedIterator(List<Iterator<T>> iterators) {
        this.iterators = iterators;
        this.heap = new PriorityQueue<>();

        for (int i = 0; i < iterators.size(); i++) {
            Iterator<T> iterator = iterators.get(i);
            if (iterator.hasNext()) heap.add(new IterRecord(iterator.next(), i));
        }

        if (!heap.isEmpty()) {
            current = heap.poll();
            if (iterators.get(current.iter).hasNext()) {
                heap.add(new IterRecord(iterators.get(current.iter).next(), current.iter));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public T next() {
        T record = current.getRecord();
        nextRecord();
        return record;
    }

    private void nextRecord() {
        if (!heap.isEmpty()) {
            IterRecord next = heap.poll();
            if (iterators.get(next.iter).hasNext()) {
                heap.add(new IterRecord(iterators.get(next.iter).next(), next.iter));
            }
            while (current.record.equals(next.record)) {
                next = heap.poll();
                if (iterators.get(next.iter).hasNext()) {
                    heap.add(new IterRecord(iterators.get(next.iter).next(), next.iter));
                }
            }
            current = next;
        } else {
            current = null;
        }
    }
}
