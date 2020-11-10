package com.accakyra.lsss.lsm.io.write;

import com.accakyra.lsss.lsm.storage.Index;
import com.accakyra.lsss.lsm.storage.Level;
import com.accakyra.lsss.lsm.storage.SST;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WriteSSTableTask implements Runnable {

    private final ByteBuffer sstBuffer;
    private final ByteBuffer indexBuffer;
    private final int id;
    private final Path folderPath;
    private final Level level;
    private final Index index;
    private final ReentrantReadWriteLock lock;

    public WriteSSTableTask(ByteBuffer sstBuffer, ByteBuffer indexBuffer,
                            int id, Path folderPath, Level level,
                            Index index, ReentrantReadWriteLock lock) {
        this.sstBuffer = sstBuffer;
        this.indexBuffer = indexBuffer;
        this.id = id;
        this.folderPath = folderPath;
        this.level = level;
        this.index = index;
        this.lock = lock;
    }

    @Override
    public void run() {
        Path sstableFileName = FileNameUtil.buildSstableFileName(folderPath, id);
        Path indexFileName = FileNameUtil.buildIndexFileName(folderPath, id);
        flushFile(sstableFileName, sstBuffer);
        flushFile(indexFileName, indexBuffer);

        SST sst = new SST(index, id, sstableFileName);
        lock.writeLock().lock();
        level.add(sst);
        lock.writeLock().unlock();
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
