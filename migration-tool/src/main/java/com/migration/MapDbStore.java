package com.migration;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;

public class MapDbStore {
    private final DB db;
    private final HTreeMap<String, String> ridMap;

    public MapDbStore(String dbPath) {
        this.db = DBMaker.fileDB(new File(dbPath))
                .fileMmapEnable()
                .fileMmapEnableIfSupported()
                .closeOnJvmShutdown()
                .make();
                
        this.ridMap = db.hashMap("ridMap", Serializer.STRING, Serializer.STRING).createOrOpen();
    }

    public void put(String oldRid, String newRid) {
        ridMap.put(oldRid, newRid);
    }

    public String get(String oldRid) {
        return ridMap.get(oldRid);
    }

    public void close() {
        if (!db.isClosed()) {
            db.close();
        }
    }
}
