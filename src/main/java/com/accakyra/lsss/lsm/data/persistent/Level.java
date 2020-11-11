package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.MergeIterator;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Level implements Resource {

    private List<SST> ssts;

    public Level() {
        this.ssts = new ArrayList<>();
    }

    public Level(List<SST> ssts) {
        this.ssts = ssts;
    }

    public boolean contains(ByteBuffer key) {
        return ssts.stream().anyMatch(sst -> sst.contains(key));
    }

    public void add(SST sst) {
        ssts.add(sst);
    }

    @Override
    public Record get(ByteBuffer key) {
        for (SST sst : ssts) {
            if (sst.contains(key)) {
                return sst.get(key);
            }
        }
        return null;
    }

    @Override
    public Iterator<Record> iterator() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (Resource sst : ssts) iterators.add(sst.iterator());
        return new MergeIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (Resource sst : ssts) iterators.add(sst.iterator(from));
        return new MergeIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (Resource sst : ssts) iterators.add(sst.iterator(from, to));
        return new MergeIterator(iterators);
    }
}
