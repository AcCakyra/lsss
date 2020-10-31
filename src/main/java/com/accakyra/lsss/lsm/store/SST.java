package com.accakyra.lsss.lsm.store;

import com.accakyra.lsss.lsm.Record;
import com.accakyra.lsss.lsm.io.SSTReader;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class SST implements Resource {

    private final Index index;
    private final File data;

    public SST(Index index, File data) {
        this.index = index;
        this.data = data;
    }

    @Override
    public Record get(ByteBuffer key) {
        KeyInfo keyInfo = index.getKeyInfo(key);
        if (keyInfo == null) return null;
        ByteBuffer value = SSTReader.getValueFromStorage(data, keyInfo, index.getGeneration());
        return new Record(key, value);
    }

    @Override
    public Iterator<Record> iterator() {
        return index.keys()
                .stream()
                .map(key -> {
                    KeyInfo keyInfo = index.getKeyInfo(key);
                    ByteBuffer value = SSTReader.getValueFromStorage(data, keyInfo, index.getGeneration());
                    return new Record(key, value);
                })
                .iterator();
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        return index.keys()
                .tailSet(from)
                .stream()
                .map(key -> {
                    KeyInfo keyInfo = index.getKeyInfo(key);
                    ByteBuffer value = SSTReader.getValueFromStorage(data, keyInfo, index.getGeneration());
                    return new Record(key, value);
                })
                .iterator();
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        return index.keys()
                .subSet(from, to)
                .stream()
                .map(key -> {
                    KeyInfo keyInfo = index.getKeyInfo(key);
                    ByteBuffer value = SSTReader.getValueFromStorage(data, keyInfo, index.getGeneration());
                    return new Record(key, value);
                })
                .iterator();
    }
}
