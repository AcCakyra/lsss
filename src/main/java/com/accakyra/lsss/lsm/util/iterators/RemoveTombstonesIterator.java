package com.accakyra.lsss.lsm.util.iterators;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.LSMTree;

import java.util.Iterator;

public class RemoveTombstonesIterator implements Iterator<Record> {

    private Iterator<Record> iterator;
    private Record current;

    public RemoveTombstonesIterator(Iterator<Record> iterator) {
        this.iterator = iterator;
        nextRecord();
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }

    @Override
    public Record next() {
        Record record = current;
        nextRecord();
        return record;
    }

    private void nextRecord() {
        Record next = null;
        while (iterator.hasNext()) {
            next = iterator.next();
            if (next.getValue().equals(LSMTree.TOMBSTONE)) {
                next = null;
            } else {
                break;
            }
        }
        current = next;
    }
}
