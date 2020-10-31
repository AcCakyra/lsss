package com.accakyra.lsss.lsm.io;

import com.accakyra.lsss.lsm.Record;
import com.accakyra.lsss.lsm.store.*;
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
import java.util.concurrent.*;

public class TableManager {

    private final Metadata metadata;
    private final ExecutorService writer;
    private final File data;
    private Future<?> flushTaskResult;

    public TableManager(File data) {
        this.data = data;
        this.metadata = new Metadata(data);
        this.writer = Executors.newSingleThreadExecutor();
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
        writeTable(sstBuffer, indexBuffer);

        Index index = new Index(indexKeys, metadata.getAndIncrementIndexGeneration());
        return new SST(index, data);
    }

    public List<Resource> readSSTs() {
        int maxGeneration = metadata.getSstGeneration();
        List<Resource> ssts = new ArrayList<>(maxGeneration);
        for (int i = maxGeneration - 1; i >= 0; i--) {
            ssts.add(readSST(i));
        }
        return ssts;
    }

    public void close() {
        try {
            metadata.close();
            writer.shutdown();
            writer.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private SST readSST(int generation) {
        String indexFileName = FileNameUtil.buildIndexFileName(data.getAbsolutePath(), generation);
        try (RandomAccessFile indexFile = new RandomAccessFile(indexFileName, "r");
             FileChannel indexChannel = indexFile.getChannel()) {
            MappedByteBuffer indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
            indexBuffer.load();

            NavigableMap<ByteBuffer, KeyInfo> keys = new TreeMap<>();
            while (indexBuffer.hasRemaining()) {
                int keySize = indexBuffer.getInt();
                byte[] key = new byte[keySize];
                indexBuffer.get(key);
                ByteBuffer keyBuffer = ByteBuffer.wrap(key);
                int offset = indexBuffer.getInt();
                int valueSize = indexBuffer.getInt();
                keys.put(keyBuffer, new KeyInfo(offset, valueSize));
            }
            Index index = new Index(keys, generation);
            return new SST(index, data);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void writeTable(ByteBuffer sstBuffer, ByteBuffer indexBuffer) {
        waitUntilMemtableIsFlushed();
        WriteSSTableTask writeSSTableTask = new WriteSSTableTask(sstBuffer, indexBuffer, data.toPath(), metadata);
        flushTaskResult = writer.submit(writeSSTableTask);
    }

    private void waitUntilMemtableIsFlushed() {
        try {
            if (flushTaskResult != null) {
                flushTaskResult.get();
                flushTaskResult = null;
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
