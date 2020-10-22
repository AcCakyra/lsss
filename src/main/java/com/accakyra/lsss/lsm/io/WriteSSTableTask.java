package com.accakyra.lsss.lsm.io;

import com.accakyra.lsss.lsm.store.MetaData;
import com.accakyra.lsss.lsm.store.SST;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class WriteSSTableTask implements Runnable {

    private final SST sstable;
    private final Path storage;
    private final MetaData metaData;

    public WriteSSTableTask(SST sstable, Path storage, MetaData metaData) {
        this.sstable = sstable;
        this.storage = storage;
        this.metaData = metaData;
    }

    @Override
    public void run() {
        int generation = metaData.getSstGeneration();
        String sstableFileName = FileNameUtil.buildSstableFileName(storage.toString(), generation);
        String indexFileName = FileNameUtil.buildIndexFileName(storage.toString(), generation);
        flushFile(sstableFileName, sstable.getSstable());
        flushFile(indexFileName, sstable.getIndex());
        metaData.increment();
    }

    private void flushFile(String fullFileName, ByteBuffer data) {
        try (RandomAccessFile writer = new RandomAccessFile(fullFileName, "rw");
             FileChannel fileChannel = writer.getChannel()) {
            while (data.hasRemaining()) {
                fileChannel.write(data);
            }
            fileChannel.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
