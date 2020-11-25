package com.accakyra.lsss.lsm.data.persistent.sst;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.io.FileReader;
import com.accakyra.lsss.lsm.data.Resource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedSet;

public class SST implements Resource {

    private final NavigableMap<ByteBuffer, KeyInfo> index;
    private final Path fileName;
    private final int id;
    private final int level;

    public SST(NavigableMap<ByteBuffer, KeyInfo> index, int id, Path fileName, int level) {
        this.index = index;
        this.fileName = fileName;
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
        KeyInfo keyInfo = index.get(key);
        if (keyInfo == null) return null;
        ByteBuffer value = FileReader.read(fileName, keyInfo.getOffset(), keyInfo.getValueSize());
        return new Record(key, value);
    }

    public ByteBuffer firstKey() {
        return index.firstKey();
    }

    public ByteBuffer lastKey() {
        return index.lastKey();
    }

    @Override
    public Iterator<Record> iterator() {
        return iterator(index.navigableKeySet());
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        return iterator(index.navigableKeySet().tailSet(from));
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        return iterator(index.navigableKeySet().subSet(from, to));
    }

    private Iterator<Record> iterator(SortedSet<ByteBuffer> keys) {
        return keys
                .stream()
                .map(key -> {
                    KeyInfo keyInfo = index.get(key);
                    ByteBuffer value = FileReader.read(fileName, keyInfo.getOffset(), keyInfo.getValueSize());
                    return new Record(key, value);
                })
                .iterator();
    }
}
