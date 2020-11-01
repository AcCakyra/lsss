package com.accakyra.lsss.lsm;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.io.read.TableReader;
import com.accakyra.lsss.lsm.io.write.TableWriter;
import com.accakyra.lsss.lsm.storage.*;
import com.accakyra.lsss.lsm.storage.Index;
import com.accakyra.lsss.lsm.storage.SST;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

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

        ByteBuffer indexBuffer = ByteBuffer.allocate(indexBufferSize);
        ByteBuffer sstBuffer = ByteBuffer.allocate(memtable.getTotalBytesCapacity());
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
        return new SST(index,  metadata.getAndIncrementIndexGeneration(), data.toPath().toString());
    }

    public List<Resource> readSSTs() {
        int maxGeneration = metadata.getSstGeneration();
        List<Resource> ssts = new ArrayList<>(maxGeneration);
        for (int i = maxGeneration - 1; i >= 0; i--) {
            String indexFileName = FileNameUtil.buildIndexFileName(data.getAbsolutePath(), i);
            Index index = TableReader.readIndex(indexFileName);
            SST sst = new SST(index, i, data.toPath().toString());
            ssts.add(sst);
        }
        return ssts;
    }

    public void close() {
        metadata.close();
        writer.close();
    }
}
