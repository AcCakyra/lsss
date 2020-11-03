package com.accakyra.lsss.lsm;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.io.read.FileReader;
import com.accakyra.lsss.lsm.io.write.TableWriter;
import com.accakyra.lsss.lsm.storage.*;
import com.accakyra.lsss.lsm.storage.Index;
import com.accakyra.lsss.lsm.storage.SST;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;

public class TableManager {

    private final Metadata metadata;
    private final File data;
    private final TableWriter writer;

    public TableManager(File data) {
        this.data = data;
        this.metadata = new Metadata(data);
        this.writer = new TableWriter();
    }

    public SST writeMemtable(Memtable memtable) {
        // Overall size of key in index file is: 12 + key size
        // 4 bytes for storing length of key
        // 4 bytes for storing offset in sst file for value of this key
        // 4 bytes for storing length of value
        int indexBufferSize = memtable.getKeysCapacity() + 12 * memtable.getUniqueKeysCount();
        int alignedIndexBufferSize = Integer.highestOneBit(indexBufferSize) * 2;

        int memtableSize = memtable.getTotalBytesCapacity();
        int alignedMemtableSize = Integer.highestOneBit(memtableSize) * 2;

        ByteBuffer indexBuffer = ByteBuffer.allocate(alignedIndexBufferSize);
        ByteBuffer sstBuffer = ByteBuffer.allocate(alignedMemtableSize);

        NavigableMap<ByteBuffer, KeyInfo> indexKeys = new TreeMap<>();

        int valueOffset = 0;
        for (Record record : memtable) {
            byte[] key = record.getKey().array();
            byte[] value = record.getValue().array();

            sstBuffer.put(key);
            sstBuffer.put(value);

            valueOffset += key.length;
            indexBuffer.putInt(key.length);
            indexBuffer.put(key);
            indexBuffer.putInt(valueOffset);
            indexBuffer.putInt(value.length);
            indexKeys.put(record.getKey(), new KeyInfo(valueOffset, value.length));
            valueOffset += value.length;
        }

        sstBuffer.flip();
        indexBuffer.flip();
        writer.writeTable(sstBuffer, indexBuffer, metadata, data.toPath());

        Index index = new Index(indexKeys);
        return new SST(index, metadata.getAndIncrementIndexGeneration(), data.toPath());
    }

    public List<SST> readSSTs() {
        int maxGeneration = metadata.getSstGeneration();
        List<SST> ssts = new ArrayList<>(maxGeneration);
        for (int i = maxGeneration - 1; i >= 0; i--) {
            Path indexFileName = FileNameUtil.buildIndexFileName(data.toPath(), i);
            Index index = parseIndex(FileReader.read(indexFileName));
            SST sst = new SST(index, i, data.toPath());
            ssts.add(sst);
        }
        return ssts;
    }

    private Index parseIndex(ByteBuffer buffer) {
        NavigableMap<ByteBuffer, KeyInfo> keys = new TreeMap<>();
        while (buffer.hasRemaining()) {
            int keySize = buffer.getInt();
            byte[] key = new byte[keySize];
            buffer.get(key);
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            int offset = buffer.getInt();
            int valueSize = buffer.getInt();
            keys.put(keyBuffer, new KeyInfo(offset, valueSize));
        }
        return new Index(keys);
    }

    public void close() {
        metadata.close();
        writer.close();
    }
}
