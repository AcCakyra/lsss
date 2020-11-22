package com.accakyra.lsss.lsm.data.persistent.level;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.util.iterators.IteratorsUtil;

import java.nio.ByteBuffer;
import java.util.*;

public class Level0 implements Level {

    private final Set<SST> sstables;

    public Level0() {
        this.sstables = new TreeSet<>(Comparator.comparingInt(SST::getId).reversed());
    }

    @Override
    public void add(SST sst) {
        sstables.add(sst);
    }

    @Override
    public Record get(ByteBuffer key) {
        for (SST sst : sstables) {
            Record record = sst.get(key);
            if (record != null) {
                return record;
            }
        }
        return null;
    }

    @Override
    public Set<SST> getSstables() {
        return sstables;
    }

    @Override
    public int size() {
        return sstables.size();
    }

    @Override
    public Iterator<Record> iterator() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables) iterators.add(sst.iterator());
        return IteratorsUtil.mergeIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables) iterators.add(sst.iterator(from));
        return IteratorsUtil.mergeIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables) iterators.add(sst.iterator(from, to));
        return IteratorsUtil.mergeIterator(iterators);
    }
}
