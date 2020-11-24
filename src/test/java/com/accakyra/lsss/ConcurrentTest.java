package com.accakyra.lsss;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConcurrentTest extends TestBase {

    @Test
    void manyThreads() throws IOException, InterruptedException {
        int threadsCount = 10;
        ExecutorService clients = Executors.newFixedThreadPool(threadsCount);

        int records = 10000;
        try (DAO dao = createDao()) {
            for (int i = 0; i < threadsCount; i++) {
                clients.submit(() -> {
                    TreeMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
                    for (int j = 0; j < records; j++) {
                        ByteBuffer key = randomKey();
                        ByteBuffer value = randomValue();
                        map.put(key, value);
                        dao.upsert(key, value);
                    }

                    for (Map.Entry<ByteBuffer, ByteBuffer> entry : map.entrySet()) {
                        ByteBuffer key = entry.getKey();
                        ByteBuffer expectedValue = entry.getValue();
                        assertEquals(expectedValue, dao.get(key));
                    }
                });
            }
            clients.shutdown();
            clients.awaitTermination(1, TimeUnit.HOURS);
        }
    }
}
