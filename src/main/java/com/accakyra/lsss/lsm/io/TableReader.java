package com.accakyra.lsss.lsm.io;

import com.accakyra.lsss.lsm.store.Index;
import com.accakyra.lsss.lsm.store.KeyInfo;
import com.accakyra.lsss.lsm.store.SST;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class TableReader {

    private final File data;

    public TableReader(File data) {
        this.data = data;
    }

    public List<SST> readSSTs(int lastGeneration) {
        List<SST> indexes = new ArrayList<>(lastGeneration);
        for (int generation = lastGeneration - 1; generation >= 0; generation--) {
            indexes.add(readSSTFile(generation));
        }
        return indexes;
    }

    public ByteBuffer getValueFromStorage(KeyInfo keyInfo, int generation) {
        String sstFileName = FileNameUtil.buildSstableFileName(data.getAbsolutePath(), generation);
        try (RandomAccessFile indexFile = new RandomAccessFile(sstFileName, "r");
             FileChannel channel = indexFile.getChannel()) {

            MappedByteBuffer dataBuffer = channel.map(FileChannel.MapMode.READ_ONLY, keyInfo.getOffset(), keyInfo.getValueSize());
            byte[] value = new byte[keyInfo.getValueSize()];
            dataBuffer.get(value);
            return ByteBuffer.wrap(value);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private SST readSSTFile(int generation) {
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
            return new SST(index, this);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
