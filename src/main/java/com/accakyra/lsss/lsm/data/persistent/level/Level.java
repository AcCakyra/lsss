package com.accakyra.lsss.lsm.data.persistent.level;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.data.Run;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.google.common.collect.Iterators;

import java.nio.ByteBuffer;
import java.util.*;

public class Level implements Run {

    private final Set<SST> sstables;

    public Level() {
        this.sstables = new TreeSet<>(Comparator.comparing(o -> o.getIndex().firstKey()));
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
    public int size() {
        return sstables.size();
    }

    @Override
    public Set<SST> getSstables() {
        return sstables;
    }

    @Override
    public Iterator<Record> iterator() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables) iterators.add(sst.iterator());
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables) iterators.add(sst.iterator(from));
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables) iterators.add(sst.iterator(from, to));
        return Iterators.concat(iterators.iterator());
    }
}
