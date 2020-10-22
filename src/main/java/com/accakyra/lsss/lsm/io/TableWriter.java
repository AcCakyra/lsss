package com.accakyra.lsss.lsm.io;

import com.accakyra.lsss.lsm.LSMProperties;
import com.accakyra.lsss.lsm.store.MemTable;
import com.accakyra.lsss.lsm.store.MetaData;
import com.accakyra.lsss.lsm.store.SST;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

public class TableWriter {

    private final ExecutorService writer;
    private final File data;
    private final MetaData metaData;

    public TableWriter(MetaData metaData, File data) {
        this.data = data;
        this.metaData = metaData;
        this.writer = Executors.newSingleThreadExecutor();
    }

    public void scheduleFlushing(MemTable memtable) {
        SST sstable = convertMemTableToSSTable(memtable);
        WriteSSTableTask writeSSTableTask = new WriteSSTableTask(sstable, data.toPath(), metaData);
        writer.submit(writeSSTableTask);
    }

    public void close() {
        try {
            writer.shutdown();
            writer.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private SST convertMemTableToSSTable(MemTable memTable) {
        ByteBuffer indexBuffer = ByteBuffer.allocate(LSMProperties.MEMTABLE_THRESHOLD * 4);
        ByteBuffer dataBuffer = ByteBuffer.allocate(LSMProperties.MEMTABLE_THRESHOLD);
        Iterator<Map.Entry<ByteBuffer, ByteBuffer>> memtableIterator = memTable.getIterator();

        int dataOffset = 0;
        while (memtableIterator.hasNext()) {
            Map.Entry<ByteBuffer, ByteBuffer> entry = memtableIterator.next();
            byte[] key = entry.getKey().array();
            byte[] value = entry.getValue().array();
            dataBuffer.put(key);
            dataBuffer.put(value);

            indexBuffer.putInt(key.length);
            indexBuffer.put(key);

            dataOffset += key.length;
            indexBuffer.putInt(dataOffset);
            indexBuffer.putInt(value.length);
            dataOffset += value.length;
        }

        dataBuffer.flip();
        indexBuffer.flip();

        return new SST(dataBuffer, indexBuffer);
    }
}
