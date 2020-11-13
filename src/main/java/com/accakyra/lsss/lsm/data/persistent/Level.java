package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.MergeIterator;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Level implements Resource {

    private List<Resource> resources;

    public Level() {
        this.resources = new ArrayList<>();
    }

    public void add(Resource resource) {
        resources.add(resource);
    }

    public void add(List<? extends Resource> resource) {
        resources.addAll(resource);
    }

    @Override
    public Record get(ByteBuffer key) {
        for (Resource sst : resources) {
            Record record = sst.get(key);
            if (record != null) return record;
        }
        return null;
    }

    @Override
    public Iterator<Record> iterator() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (Resource sst : resources) iterators.add(sst.iterator());
        return new MergeIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (Resource sst : resources) iterators.add(sst.iterator(from));
        return new MergeIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (Resource sst : resources) iterators.add(sst.iterator(from, to));
        return new MergeIterator(iterators);
    }
}
