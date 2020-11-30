package com.accakyra.lsss;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PerformanceTest extends TestBase {

    void writePerformance() throws IOException {
        int recordCount = 1_000_000;
        Map<ByteBuffer, ByteBuffer> records = new HashMap<>();
        for (int i = 0; i < recordCount; i++) {
            records.put(randomKey(), randomBuffer(100));
        }

        long start;
        try (DAO dao = createDao()) {
            start = System.currentTimeMillis();
            for (Map.Entry<ByteBuffer, ByteBuffer> record : records.entrySet()) {
                dao.upsert(record.getKey(), record.getValue());
            }
        }
        double time = (System.currentTimeMillis() - start) / 1000.;

        System.out.println(time + " total time");
        System.out.println(((16 + 100) * recordCount / (1024 * 1024) / time) + " MB per second");
    }

    void readPerformance() throws IOException {
        int recordCount = 1_000_000;
        Map<ByteBuffer, ByteBuffer> records = new HashMap<>();
        for (int i = 0; i < recordCount; i++) {
            records.put(randomKey(), randomBuffer(100));
        }

        try (DAO dao = createDao()) {
            for (Map.Entry<ByteBuffer, ByteBuffer> record : records.entrySet()) {
                dao.upsert(record.getKey(), record.getValue());
            }

            long start = System.currentTimeMillis();
            for (Map.Entry<ByteBuffer, ByteBuffer> record : records.entrySet()) {
                dao.get(record.getKey());
            }
            double time = (System.currentTimeMillis() - start) / 1000.;

            System.out.println(time + " total time");
            System.out.println(((16 + 100) * recordCount / (1024 * 1024) / time) + " MB per second");
        }
    }
}
