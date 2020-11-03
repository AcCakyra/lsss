package com.accakyra.lsss.lsm.io.write;

import com.accakyra.lsss.lsm.Metadata;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class WriteSSTableTask implements Runnable {

    private final ByteBuffer sstBuffer;
    private final ByteBuffer indexBuffer;
    private final Path dataPath;
    private final Metadata metaData;

    public WriteSSTableTask(ByteBuffer sstBuffer, ByteBuffer indexBuffer, Path dataPath, Metadata metaData) {
        this.sstBuffer = sstBuffer;
        this.indexBuffer = indexBuffer;
        this.dataPath = dataPath;
        this.metaData = metaData;
    }

    @Override
    public void run() {
        int generation = metaData.getSstGeneration();
        Path sstableFileName = FileNameUtil.buildSstableFileName(dataPath, generation);
        Path indexFileName = FileNameUtil.buildIndexFileName(dataPath, generation);
        flushFile(sstableFileName, sstBuffer);
        flushFile(indexFileName, indexBuffer);
        metaData.incrementSstGeneration();
    }

    private void flushFile(Path fullFileName, ByteBuffer data) {
        try (RandomAccessFile writer = new RandomAccessFile(fullFileName.toString(), "rw");
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
