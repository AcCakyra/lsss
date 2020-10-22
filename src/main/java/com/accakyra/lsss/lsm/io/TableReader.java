package com.accakyra.lsss.lsm.io;

import com.accakyra.lsss.lsm.store.Key;
import com.accakyra.lsss.lsm.store.MetaData;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TableReader {

    private final MetaData metaData;
    private final File data;

    public TableReader(MetaData metaData, File data) {
        this.metaData = metaData;
        this.data = data;
    }

    public ByteBuffer get(ByteBuffer key) {
        int sstableCount = metaData.getSstGeneration();
        for (int generation = sstableCount - 1; generation >= 0; generation--) {
            MappedByteBuffer indexBuffer = readIndexFile(generation);
            while (indexBuffer != null && indexBuffer.hasRemaining()) {
                Key storedKey = readNextKey(indexBuffer);
                if (storedKey.getKey().equals(key)) {
                    return getValueFromSSTable(generation, storedKey);
                }
            }
        }
        return null;
    }

    private MappedByteBuffer readIndexFile(int generation) {
        String indexFileName = FileNameUtil.buildIndexFileName(data.getAbsolutePath(), generation);
        try (RandomAccessFile indexFile = new RandomAccessFile(indexFileName, "r")) {
            FileChannel indexChannel = indexFile.getChannel();
            MappedByteBuffer indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
            indexBuffer.load();
            return indexBuffer;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Key readNextKey(MappedByteBuffer buffer) {
        int keySize = buffer.getInt();
        byte[] key = new byte[keySize];
        buffer.get(key);
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        int offset = buffer.getInt();
        int valueSize = buffer.getInt();
        return new Key(keySize, keyBuffer, offset, valueSize);
    }

    private ByteBuffer getValueFromSSTable(int generation, Key key) {
        String sstFileName = FileNameUtil.buildSstableFileName(data.getAbsolutePath(), generation);
        try (RandomAccessFile indexFile = new RandomAccessFile(sstFileName, "r")) {
            FileChannel channel = indexFile.getChannel();
            MappedByteBuffer dataBuffer = channel.map(FileChannel.MapMode.READ_ONLY, key.getOffset(), key.getValueSize());
            byte[] value = new byte[key.getValueSize()];
            dataBuffer.get(value);
            return ByteBuffer.wrap(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
