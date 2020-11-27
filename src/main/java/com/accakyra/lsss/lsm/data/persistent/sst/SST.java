package com.accakyra.lsss.lsm.data.persistent.sst;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.data.TableConverter;
import com.accakyra.lsss.lsm.io.FileReader;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.util.iterators.IteratorsUtil;
import com.google.common.collect.Comparators;
import com.google.common.collect.Iterators;
import org.checkerframework.checker.units.qual.K;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collector;

public class SST implements Resource {

    private final NavigableMap<ByteBuffer, KeyInfo> index;
    private final Path sstFileName;
    private final Path indexFileName;
    private final int id;
    private final int level;

    public SST(NavigableMap<ByteBuffer, KeyInfo> index, int id,
               Path sstFileName, Path indexFileName, int level) {
        this.index = index;
        this.sstFileName = sstFileName;
        this.indexFileName = indexFileName;
        this.id = id;
        this.level = level;
    }

    public int getId() {
        return id;
    }

    public int getLevel() {
        return level;
    }

    @Override
    public Record get(ByteBuffer key) {
        ByteBuffer fromKey = index.floorKey(key);
        ByteBuffer toKey = index.ceilingKey(key);

        if (fromKey == null || toKey == null) {
            return null;
        }

        NavigableMap<ByteBuffer, KeyInfo> candidateMap = loadIndex(fromKey, toKey);
        for (Map.Entry<ByteBuffer, KeyInfo> candidate : candidateMap.entrySet()) {
            if (candidate.getKey().equals(key)) {
                return get(candidate.getKey(), candidate.getValue());
            }
        }
        return null;
    }

    public ByteBuffer firstKey() {
        return index.firstKey();
    }

    public ByteBuffer lastKey() {
        return index.lastKey();
    }

    @Override
    public Iterator<Record> iterator() {
        return loadIndex(index.firstKey(), index.lastKey())
                .entrySet()
                .stream()
                .map(entry -> get(entry.getKey(), entry.getValue()))
                .iterator();
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        ByteBuffer fromKey = index.floorKey(from);
        if (fromKey == null) fromKey = index.firstKey();

        return loadIndex(fromKey, index.lastKey())
                .entrySet()
                .stream()
                .map(entry -> get(entry.getKey(), entry.getValue()))
                .filter(record -> record.getKey().compareTo(from) >= 0)
                .iterator();
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        ByteBuffer fromKey = index.floorKey(from);
        if (fromKey == null) fromKey = index.firstKey();

        ByteBuffer toKey = index.ceilingKey(to);
        if (toKey == null) toKey = index.lastKey();

        return loadIndex(fromKey, toKey)
                .entrySet()
                .stream()
                .map(entry -> get(entry.getKey(), entry.getValue()))
                .filter(record -> record.getKey().compareTo(from) >= 0)
                .filter(record -> record.getKey().compareTo(to) < 0)
                .iterator();
    }

    private NavigableMap<ByteBuffer, KeyInfo> loadIndex(ByteBuffer fromKey, ByteBuffer toKey) {
        int from = index.get(fromKey).getIndexOffset();
        KeyInfo toKeyInfo = index.get(toKey);
        int to = toKeyInfo.getIndexOffset() + toKeyInfo.getKeySize() + 12;
        int length = to - from;

        ByteBuffer buffer = FileReader.read(indexFileName, from + 4, length);
        return TableConverter.parseIndexBuffer(buffer, false);
    }

    private Record get(ByteBuffer key, KeyInfo info) {
        ByteBuffer value = FileReader.read(sstFileName, info.getSstOffset(), info.getValueSize());
        return new Record(key, value);
    }
}
