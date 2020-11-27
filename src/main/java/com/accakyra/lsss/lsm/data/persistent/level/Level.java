package com.accakyra.lsss.lsm.data.persistent.level;

import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.util.Collection;

public interface Level extends Resource {

    Level copy();

    void add(SST sst);

    Collection<SST> getSstables();

    int size();
}