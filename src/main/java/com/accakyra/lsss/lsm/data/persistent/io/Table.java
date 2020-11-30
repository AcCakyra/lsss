package com.accakyra.lsss.lsm.data.persistent.io;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
@AllArgsConstructor
public class Table {

    private final ByteBuffer indexBuffer;
    private final ByteBuffer sstBuffer;

}
