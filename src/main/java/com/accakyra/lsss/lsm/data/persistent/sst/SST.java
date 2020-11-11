package com.accakyra.lsss.lsm.data.persistent.sst;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.io.FileReader;
import com.accakyra.lsss.lsm.data.Resource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.SortedSet;

public class SST implements Resource {

    private final Index index;
    private final Path fileName;
    private final int id;

    public SST(Index index, int id, Path fileName) {
        this.index = index;
        this.fileName = fileName;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public int getLevel() {
        return index.getLevel();
    }

    @Override
    public Record get(ByteBuffer key) {
        KeyInfo keyInfo = index.getKeyInfo(key);
        if (keyInfo == null) return null;
        ByteBuffer value = FileReader.read(fileName, keyInfo.getOffset(), keyInfo.getValueSize());
        return new Record(key, value);
    }

    public boolean contains(ByteBuffer key) {
        return index.getKeyInfo(key) != null;
    }

    @Override
    public Iterator<Record> iterator() {
        return iterator(index.keys());
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        return iterator(index.keys().tailSet(from));
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        return iterator(index.keys().subSet(from, to));
    }

    private Iterator<Record> iterator(SortedSet<ByteBuffer> keys) {
        return keys
                .stream()
                .map(key -> {
                    KeyInfo keyInfo = index.getKeyInfo(key);
                    ByteBuffer value = FileReader.read(fileName, keyInfo.getOffset(), keyInfo.getValueSize());
                    return new Record(key, value);
                })
                .iterator();
    }
}
