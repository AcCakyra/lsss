package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.MergedIterator;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.nio.ByteBuffer;
import java.util.*;

public class Level implements Resource {

    private final Map<Integer, SST> sstables;

    public Level() {
        this.sstables = new TreeMap<>(Comparator.reverseOrder());
    }

    public void add(SST sst) {
        sstables.put(sst.getId(), sst);
    }

    public void add(List<SST> sstList) {
        for (SST sst : sstList) {
            add(sst);
        }
    }

    @Override
    public Record get(ByteBuffer key) {
        for (SST sst : sstables.values()) {
            Record record = sst.get(key);
            if (record != null) return record;
        }
        return null;
    }

    public int getSize() {
        return sstables.size();
    }

    public Collection<SST> getSstables() {
        return sstables.values();
    }

    @Override
    public Iterator<Record> iterator() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables.values()) iterators.add(sst.iterator());
        return new MergedIterator<>(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables.values()) iterators.add(sst.iterator(from));
        return new MergedIterator<>(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables.values()) iterators.add(sst.iterator(from, to));
        return new MergedIterator<>(iterators);
    }
}
