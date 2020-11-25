package com.accakyra.lsss.lsm.data.persistent.level;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.google.common.collect.Iterators;

import java.nio.ByteBuffer;
import java.util.*;

public class LevelN implements Level {

    private final NavigableMap<ByteBuffer, SST> sstables;

    public LevelN() {
        this.sstables = new TreeMap<>();
    }

    @Override
    public void add(SST sst) {
        sstables.put(sst.firstKey(), sst);
    }

    @Override
    public Record get(ByteBuffer key) {
        NavigableMap<ByteBuffer, SST> subMap = subMap(key);
        return subMap.firstEntry().getValue().get(key);
    }

    @Override
    public int size() {
        return sstables.size();
    }

    @Override
    public Collection<SST> getSstables() {
        return sstables.values();
    }

    @Override
    public Iterator<Record> iterator() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        for (SST sst : sstables.values()) iterators.add(sst.iterator());
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        NavigableMap<ByteBuffer, SST> subMap = subMap(from);
        for (SST sst : subMap.values()) iterators.add(sst.iterator(from));
        return Iterators.concat(iterators.iterator());
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        NavigableMap<ByteBuffer, SST> subMap = subMap(from, to);
        for (SST sst : subMap.values()) iterators.add(sst.iterator(from, to));
        return Iterators.concat(iterators.iterator());
    }

    private NavigableMap<ByteBuffer, SST> subMap(ByteBuffer from) {
        ByteBuffer floorKey = sstables.floorKey(from);
        if (floorKey == null) {
            return sstables;
        } else {
            return sstables.tailMap(floorKey, true);
        }
    }

    private NavigableMap<ByteBuffer, SST> subMap(ByteBuffer key, ByteBuffer to) {
        NavigableMap<ByteBuffer, SST> tailMap = subMap(key);
        ByteBuffer higherKey = sstables.higherKey(to);
        if (higherKey == null) {
            return tailMap;
        } else {
            return sstables.headMap(higherKey, false);
        }
    }
}
