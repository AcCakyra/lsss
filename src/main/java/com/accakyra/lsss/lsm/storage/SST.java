package com.accakyra.lsss.lsm.storage;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.io.read.FileReader;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.SortedSet;

public class SST implements Resource {

    private final Index index;
    private final Path filePath;

    public SST(Index index, int generation, Path folderName) {
        this.index = index;
        this.filePath = FileNameUtil.buildSstableFileName(folderName, generation);
    }

    @Override
    public Record get(ByteBuffer key) {
        KeyInfo keyInfo = index.getKeyInfo(key);
        if (keyInfo == null) return null;
        ByteBuffer value = FileReader.read(filePath, keyInfo.getOffset(), keyInfo.getValueSize());
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
                    ByteBuffer value = FileReader.read(filePath, keyInfo.getOffset(), keyInfo.getValueSize());
                    return new Record(key, value);
                })
                .iterator();
    }
}
