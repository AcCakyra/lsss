package com.accakyra.lsss.lsm.io;

import com.accakyra.lsss.lsm.store.Index;
import com.accakyra.lsss.lsm.store.Key;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

public class TableReader {

    private final File data;

    public TableReader(File data) {
        this.data = data;
    }

    public ByteBuffer get(Key key, int generation) {
        return getValueFromSSTable(key, generation);
    }

    public List<Index> readIndexes(int lastGeneration) {
        List<Index> indexes = new ArrayList<>(lastGeneration);
        for (int generation = 0; generation < lastGeneration; generation++) {
            indexes.add(readIndexFile(generation));
        }
        return indexes;
    }

    private Index readIndexFile(int generation) {
        String indexFileName = FileNameUtil.buildIndexFileName(data.getAbsolutePath(), generation);
        try (RandomAccessFile indexFile = new RandomAccessFile(indexFileName, "r");
             FileChannel indexChannel = indexFile.getChannel()) {
            MappedByteBuffer indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
            indexBuffer.load();

            NavigableSet<Key> keys = new TreeSet<>();
            while (indexBuffer.hasRemaining()) {
                keys.add(readNextKey(indexBuffer));
            }
            return new Index(keys, generation);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Key readNextKey(MappedByteBuffer buffer) {
        int keySize = buffer.getInt();
        byte[] key = new byte[keySize];
        buffer.get(key);
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        int offset = buffer.getInt();
        int valueSize = buffer.getInt();
        return new Key(keyBuffer, offset, valueSize);
    }

    private ByteBuffer getValueFromSSTable(Key key, int generation) {
        String sstFileName = FileNameUtil.buildSstableFileName(data.getAbsolutePath(), generation);
        try (RandomAccessFile indexFile = new RandomAccessFile(sstFileName, "r");
             FileChannel channel = indexFile.getChannel()) {

            MappedByteBuffer dataBuffer = channel.map(FileChannel.MapMode.READ_ONLY, key.getOffset(), key.getValueSize());
            byte[] value = new byte[key.getValueSize()];
            dataBuffer.get(value);
            return ByteBuffer.wrap(value);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
