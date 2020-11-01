package com.accakyra.lsss.lsm.io.read;

import com.accakyra.lsss.lsm.storage.Index;
import com.accakyra.lsss.lsm.storage.KeyInfo;
import com.accakyra.lsss.lsm.storage.SST;
import com.accakyra.lsss.lsm.util.FileNameUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TableReader {

    public static Index readIndex(String fileName) {
        try (RandomAccessFile indexFile = new RandomAccessFile(fileName, "r");
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
            return new Index(keys);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
