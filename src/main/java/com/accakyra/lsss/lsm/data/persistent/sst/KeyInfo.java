package com.accakyra.lsss.lsm.data.persistent.sst;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class KeyInfo {

    private final int indexOffset;
    private final int sstOffset;
    private final int keySize;
    private final int valueSize;

}
