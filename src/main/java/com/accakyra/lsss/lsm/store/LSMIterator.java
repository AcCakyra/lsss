package com.accakyra.lsss.lsm.store;

import com.accakyra.lsss.Record;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

public class LSMIterator implements Iterator<Record> {

    private class IterRecord implements Comparable<IterRecord> {

        private Record record;
        private int iter;

        public IterRecord(Record record, int iter) {
            this.record = record;
            this.iter = iter;
        }

        public Record getRecord() {
            return record;
        }

        public int getIter() {
            return iter;
        }

        @Override
        public int compareTo(IterRecord o) {
            int compare = record.compareTo(o.record);
            if (compare != 0) {
                return iter - o.getIter();
            } else {
                return compare;
            }
        }
    }

    private ReadWriteLock mutex;
    private List<Iterator<Record>> iterators;
    private Queue<IterRecord> heap;
    private IterRecord current;

    public LSMIterator(List<Iterator<Record>> iterators, ReadWriteLock mutex) {
        this.mutex = mutex;
        this.iterators = iterators;
        this.heap = new PriorityQueue<>();

        for (int i = 0; i < iterators.size(); i++) {
            Iterator<Record> iterator = iterators.get(i);
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
    public Record next() {
        mutex.readLock().lock();
        Record record = new Record(current.getRecord().getKey(), current.getRecord().getValue());
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
        mutex.readLock().unlock();
        return record;
    }
}
