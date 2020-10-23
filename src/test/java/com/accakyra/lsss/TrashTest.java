package com.accakyra.lsss;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TrashTest extends TestBase {

    @Test
    void ignoreTrashFiles(@TempDir File data) throws IOException {
        ByteBuffer key = randomKey();
        ByteBuffer value = randomValue();

        try (DAO dao = DAOFactory.create(data)) {
            dao.upsert(key, value);
        }

        createTrashFile(data, "123trash.dat");
        createTrashFile(data, "trash123.txt");
        createTrashFile(data, "trash.txt", randomValue());
        createTrashFile(data, "trash.db", randomValue());
        createTrashDirectory(data, "dir_trash");
        createTrashDirectory(data, "trash_dir");

        try (DAO dao = DAOFactory.create(data)) {
            assertEquals(value, dao.get(key));
        }
    }

    private static void createTrashFile(File dir, String name) throws IOException {
        assertTrue(new File(dir, name).createNewFile());
    }

    private static void createTrashDirectory(File dir, String name) {
        assertTrue(new File(dir, name).mkdir());
    }

    private static void createTrashFile(File dir, String name, ByteBuffer content) throws IOException {
        try (FileChannel ch =
                     FileChannel.open(
                             Paths.get(dir.getAbsolutePath(), name),
                             StandardOpenOption.CREATE,
                             StandardOpenOption.WRITE)) {
            ch.write(content);
        }
    }
}
